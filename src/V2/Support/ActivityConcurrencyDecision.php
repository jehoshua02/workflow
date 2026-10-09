<?php

declare(strict_types=1);

namespace Workflow\V2\Support;

use Throwable;
use Workflow\V2\Activity;
use Workflow\V2\ConcurrencyKey;

/**
 * @internal The result of {@see ActivityConcurrencyGate::evaluate()} for one claim.
 */
final class ActivityConcurrencyDecision
{
    private function __construct(
        public readonly string $kind,
        public readonly ?Activity $activity = null,
        public readonly ?ConcurrencyKey $key = null,
        public readonly ?string $lockOwner = null,
        public readonly ?Throwable $failure = null,
        public readonly ?string $reason = null,
    ) {
    }

    public static function unkeyed(?Activity $activity = null): self
    {
        return new self('unkeyed', $activity);
    }

    public static function acquired(Activity $activity, ConcurrencyKey $key, string $lockOwner): self
    {
        return new self('acquired', $activity, $key, $lockOwner);
    }

    public static function deferred(): self
    {
        return new self('deferred', reason: ActivityConcurrencyGate::REASON_DEFERRED);
    }

    public static function charge(string $reason, Throwable $failure, ?Activity $activity): self
    {
        return new self('charge', $activity, failure: $failure, reason: $reason);
    }

    public function claims(): bool
    {
        return $this->kind !== 'deferred';
    }

    public function charges(): bool
    {
        return $this->kind === 'charge';
    }
}
