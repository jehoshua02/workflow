<?php

declare(strict_types=1);

namespace Workflow\V2;

use Workflow\V2\Exceptions\InvalidConcurrencyKeyException;

/**
 * A shared-resource lock an activity must hold while it runs.
 *
 * The engine takes `key` verbatim from the default cache store before it
 * claims the task. While another holder has the key, the task stays ready and
 * is retried every `releaseAfter` seconds without spending an attempt. After
 * `maxWait` seconds of waiting, the engine charges one attempt with a
 * {@see Exceptions\ConcurrencyKeyBusyException} and the retry policy decides.
 */
final class ConcurrencyKey
{
    /**
     * @param string $key Full cache lock key. The engine never prefixes or re-derives it.
     * @param int $expiresAfter Lock TTL in seconds. Must be > 0 and >= the activity's start-to-close timeout.
     * @param int $releaseAfter Seconds between lock attempts while the task waits. Must be >= 1.
     * @param int $maxWait Seconds from the first deferral before the engine gives up. Must be >= 0.
     */
    public function __construct(
        public readonly string $key,
        public readonly int $expiresAfter,
        public readonly int $releaseAfter,
        public readonly int $maxWait,
    ) {
        if (trim($key) === '') {
            throw new InvalidConcurrencyKeyException('Concurrency key must be a non-empty string.');
        }

        if ($expiresAfter <= 0) {
            throw new InvalidConcurrencyKeyException(sprintf(
                'Concurrency key [%s] must expire: expiresAfter must be greater than 0, got %d.',
                $key,
                $expiresAfter,
            ));
        }

        if ($releaseAfter < 1) {
            throw new InvalidConcurrencyKeyException(sprintf(
                'Concurrency key [%s] releaseAfter must be at least 1 second, got %d.',
                $key,
                $releaseAfter,
            ));
        }

        if ($maxWait < 0) {
            throw new InvalidConcurrencyKeyException(sprintf(
                'Concurrency key [%s] maxWait must not be negative, got %d.',
                $key,
                $maxWait,
            ));
        }
    }

    public function assertCoversStartToClose(?int $startToCloseTimeout): void
    {
        if ($startToCloseTimeout !== null && $this->expiresAfter < $startToCloseTimeout) {
            throw new InvalidConcurrencyKeyException(sprintf(
                'Concurrency key [%s] expiresAfter (%ds) must be at least the activity start-to-close timeout (%ds).',
                $this->key,
                $this->expiresAfter,
                $startToCloseTimeout,
            ));
        }
    }
}
