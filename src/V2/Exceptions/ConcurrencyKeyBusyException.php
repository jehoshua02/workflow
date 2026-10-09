<?php

declare(strict_types=1);

namespace Workflow\V2\Exceptions;

use RuntimeException;

/** Charged to an attempt when an activity waited past its concurrency key's maxWait. */
final class ConcurrencyKeyBusyException extends RuntimeException
{
    public static function afterWaiting(string $key, int $maxWait): self
    {
        return new self(sprintf('Resource busy: concurrency key [%s] was still held after %ds.', $key, $maxWait));
    }
}
