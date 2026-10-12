<?php

declare(strict_types=1);

namespace Workflow\V2\Support;

use Closure;
use Illuminate\Database\Connection;
use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\DB;

/**
 * The database connection that holds the engine's tables: the configured
 * `workflows.storage.connection`, or the application's default connection
 * when none is configured. This is the connection the engine's models use.
 *
 * Transactions that read or lock engine rows must open here. A transaction on
 * the default connection does not cover queries the models send to a separate
 * storage connection, so `lockForUpdate()` inside it holds no lock.
 *
 * Only `workflows.storage.connection` selects this connection. A configured
 * model that sets its own `$connection` is not consulted: a transaction
 * covers one connection, and the engine locks rows across many models, so
 * one engine-wide setting is the only consistent answer. Apps that keep the
 * engine's tables off the default connection must set the config key.
 */
final class StorageConnection
{
    public static function name(): ?string
    {
        $configured = config('workflows.storage.connection');

        return is_string($configured) ? $configured : null;
    }

    public static function connection(): Connection
    {
        /** @var Connection */
        return DB::connection(self::name());
    }

    /**
     * @template TReturn
     *
     * @param Closure(Connection): TReturn $callback
     * @return TReturn
     */
    public static function transaction(Closure $callback, int $attempts = 1): mixed
    {
        return self::connection()->transaction($callback, $attempts);
    }

    /**
     * Runs a task claim in its own storage transaction, returning
     * `$onLocked()` when a peer holds the rows it needs to lock.
     *
     * Contention is only absorbed when this call opens the outermost storage
     * transaction. Inside an open one, a deadlock has already rolled back the
     * caller's whole transaction, so the error must reach the caller.
     *
     * @template TReturn
     * @template TLocked
     *
     * @param Closure(Connection): TReturn $callback
     * @param Closure(): TLocked $onLocked
     * @return TReturn|TLocked
     */
    public static function claimTransaction(Closure $callback, Closure $onLocked): mixed
    {
        if (self::transactionLevel() > 0) {
            return self::transaction($callback);
        }

        try {
            return self::transaction($callback);
        } catch (QueryException $exception) {
            if (! self::isLockContention($exception)) {
                throw $exception;
            }

            return $onLocked();
        }
    }

    /**
     * Whether the query failed because another transaction holds a row lock
     * it needed: a lock wait timeout, a deadlock, or a NOWAIT refusal.
     */
    public static function isLockContention(QueryException $exception): bool
    {
        $sqlState = (string) ($exception->errorInfo[0] ?? '');
        $driverCode = (int) ($exception->errorInfo[1] ?? 0);

        if ($sqlState === '40P01' || $sqlState === '55P03') {
            return true;
        }

        // MySQL: 1205 lock wait timeout, 1213 deadlock, 3572 NOWAIT lock refused.
        return in_array($driverCode, [1205, 1213, 3572], true);
    }

    public static function transactionLevel(): int
    {
        return self::connection()->transactionLevel();
    }

    /**
     * Runs the callback after the storage connection's open transaction
     * commits, or immediately when none is open.
     *
     * When the storage connection is separate from the default connection,
     * the callback also waits for an open default-connection transaction,
     * so work the application started inside its own transaction is not
     * published before that transaction commits, or at all if it rolls back.
     */
    public static function afterCommit(callable $callback): void
    {
        $storage = self::connection();

        if ($storage->transactionLevel() > 0) {
            $storage->afterCommit(static fn () => self::afterDefaultCommit($storage, $callback));

            return;
        }

        self::afterDefaultCommit($storage, $callback);
    }

    private static function afterDefaultCommit(Connection $storage, callable $callback): void
    {
        /** @var Connection $default */
        $default = DB::connection();

        if ($default !== $storage && $default->transactionLevel() > 0) {
            $default->afterCommit($callback);

            return;
        }

        $callback();
    }
}
