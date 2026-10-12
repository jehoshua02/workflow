<?php

declare(strict_types=1);

namespace Workflow\V2\Support;

use Closure;
use Illuminate\Database\Connection;
use Illuminate\Support\Facades\DB;

/**
 * The database connection that holds the engine's tables: the configured
 * `workflows.storage.connection`, or the application's default connection
 * when none is configured. This is the connection the engine's models use.
 *
 * Transactions that read or lock engine rows must open here. A transaction on
 * the default connection does not cover queries the models send to a separate
 * storage connection, so `lockForUpdate()` inside it holds no lock.
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
