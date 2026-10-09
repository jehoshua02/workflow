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
     */
    public static function afterCommit(callable $callback): void
    {
        if (self::transactionLevel() > 0) {
            self::connection()->afterCommit($callback);

            return;
        }

        $callback();
    }
}
