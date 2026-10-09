<?php

declare(strict_types=1);

namespace Workflow\V2\Support;

use Carbon\CarbonInterface;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use ReflectionMethod;
use Throwable;
use Workflow\V2\Activity;
use Workflow\V2\ConcurrencyKey;
use Workflow\V2\Enums\HistoryEventType;
use Workflow\V2\Enums\TaskStatus;
use Workflow\V2\Exceptions\ConcurrencyKeyBusyException;
use Workflow\V2\Models\ActivityAttempt;
use Workflow\V2\Models\ActivityExecution;
use Workflow\V2\Models\WorkflowHistoryEvent;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowTask;
use Workflow\V2\WorkflowStub;

/**
 * Enforces {@see Activity::concurrencyKey()} at the claim point.
 *
 * Every claim path (queue job, poll mode, external worker bridge) runs
 * through {@see ActivityTaskClaimer}, which calls {@see self::evaluate()}
 * inside its transaction while the task, execution and run rows are locked.
 */
final class ActivityConcurrencyGate
{
    public const REASON_DEFERRED = 'concurrency_deferred';

    public const REASON_KEY_FAILED = 'concurrency_key_failed';

    public const REASON_RESOURCE_BUSY = 'concurrency_resource_busy';

    public const OUTCOME_ACQUIRED = 'acquired';

    public const OUTCOME_GAVE_UP = 'gave_up';

    /**
     * @var array<class-string, bool>
     */
    private static array $declaresKey = [];

    public static function evaluate(
        WorkflowTask $task,
        ActivityExecution $execution,
        WorkflowRun $run,
        CarbonInterface $now,
    ): ActivityConcurrencyDecision {
        $activityClass = self::declaringActivityClass($execution);

        if ($activityClass === null) {
            return ActivityConcurrencyDecision::unkeyed();
        }

        $activity = null;

        try {
            // A savepoint keeps user-code query errors from poisoning the claim transaction.
            $key = DB::transaction(static function () use (
                $activityClass,
                $execution,
                $run,
                $task,
                &$activity
            ): ?ConcurrencyKey {
                $activity = RuntimeObjectFactory::activity($activityClass, $execution, $run, $task->id);
                $key = $activity->concurrencyKey();
                $key?->assertCoversStartToClose(self::startToCloseTimeout($execution));

                return $key;
            });
        } catch (Throwable $throwable) {
            return ActivityConcurrencyDecision::charge(self::REASON_KEY_FAILED, $throwable, $activity);
        }

        /** @var Activity $activity */
        if (! $key instanceof ConcurrencyKey) {
            return ActivityConcurrencyDecision::unkeyed($activity);
        }

        $lock = Cache::lock($key->key, $key->expiresAfter);

        if ($lock->get()) {
            if ($task->deferred_since !== null) {
                self::recordDeferralEnded($run, $task, $execution, $key, self::OUTCOME_ACQUIRED, $now);
            }

            return ActivityConcurrencyDecision::acquired($activity, $key, (string) $lock->owner());
        }

        $firstDeferral = $task->deferred_since === null;
        $deferredSince = $task->deferred_since ?? $now->copy();
        $deadline = $deferredSince->copy()
            ->addSeconds($key->maxWait);

        $task->forceFill([
            'concurrency_key' => $key->key,
            'deferred_since' => $deferredSince,
            'deferral_deadline_at' => $deadline,
        ]);

        if ($firstDeferral) {
            self::recordDeferred($run, $task, $execution, $key, $now);
        }

        if ($now->greaterThanOrEqualTo($deadline)) {
            self::recordDeferralEnded($run, $task, $execution, $key, self::OUTCOME_GAVE_UP, $now);

            return ActivityConcurrencyDecision::charge(
                self::REASON_RESOURCE_BUSY,
                ConcurrencyKeyBusyException::afterWaiting($key->key, $key->maxWait),
                $activity,
            );
        }

        $task->forceFill([
            'available_at' => $now->copy()
                ->addSeconds($key->releaseAfter),
            'deferral_count' => ((int) $task->deferral_count) + 1,
        ])->save();

        return ActivityConcurrencyDecision::deferred();
    }

    public static function release(?string $key, ?string $owner): void
    {
        if ($key === null || $key === '' || $owner === null || $owner === '') {
            return;
        }

        try {
            Cache::restoreLock($key, $owner)->release();
        } catch (Throwable $throwable) {
            // The lock still expires after expiresAfter; a failed release must not fail the outcome.
            report($throwable);
        }
    }

    public static function releaseForAttempt(string $attemptId): void
    {
        /** @var ActivityAttempt|null $attempt */
        $attempt = ActivityAttempt::query()
            ->whereKey($attemptId)
            ->whereNotNull('concurrency_lock_owner')
            ->first(['id', 'concurrency_key', 'concurrency_lock_owner']);

        if ($attempt instanceof ActivityAttempt) {
            self::release($attempt->concurrency_key, $attempt->concurrency_lock_owner);
        }
    }

    /**
     * A deferred task is waiting on purpose until its deadline, so repair and
     * health surfaces must not treat it as overdue.
     */
    public static function isWaiting(WorkflowTask $task, ?CarbonInterface $now = null): bool
    {
        return $task->status === TaskStatus::Ready
            && $task->deferral_deadline_at !== null
            && $task->deferral_deadline_at->gt($now ?? now());
    }

    public static function isDeferred(WorkflowTask $task): bool
    {
        return $task->status === TaskStatus::Ready && $task->deferred_since !== null;
    }

    /**
     * @param \Illuminate\Database\Eloquent\Builder<WorkflowTask>|\Illuminate\Database\Query\Builder $query
     */
    public static function applyNotWaiting($query, CarbonInterface $now, string $table = ''): void
    {
        $column = ($table === '' ? '' : $table . '.') . 'deferral_deadline_at';

        $query->where(static function ($notWaiting) use ($column, $now): void {
            $notWaiting->whereNull($column)
                ->orWhere($column, '<=', $now);
        });
    }

    /**
     * @return class-string<Activity>|null
     */
    private static function declaringActivityClass(ActivityExecution $execution): ?string
    {
        if (! is_string($execution->activity_class) || $execution->activity_class === '') {
            return null;
        }

        if (WorkflowStub::hasMock($execution->activity_class)) {
            return null;
        }

        try {
            $activityClass = TypeRegistry::resolveActivityClass(
                $execution->activity_class,
                $execution->activity_type,
            );
        } catch (Throwable) {
            // External workers may serve activity types with no local class.
            return null;
        }

        return self::declaresKey($activityClass) ? $activityClass : null;
    }

    private static function declaresKey(string $activityClass): bool
    {
        if (! array_key_exists($activityClass, self::$declaresKey)) {
            self::$declaresKey[$activityClass] = is_subclass_of($activityClass, Activity::class)
                && (new ReflectionMethod($activityClass, 'concurrencyKey'))->getDeclaringClass()
                    ->getName() !== Activity::class;
        }

        return self::$declaresKey[$activityClass];
    }

    private static function startToCloseTimeout(ActivityExecution $execution): ?int
    {
        $retryPolicy = is_array($execution->retry_policy) ? $execution->retry_policy : [];

        return is_int($retryPolicy['start_to_close_timeout'] ?? null)
            ? $retryPolicy['start_to_close_timeout']
            : null;
    }

    private static function recordDeferred(
        WorkflowRun $run,
        WorkflowTask $task,
        ActivityExecution $execution,
        ConcurrencyKey $key,
        CarbonInterface $now,
    ): void {
        WorkflowHistoryEvent::record($run, HistoryEventType::ActivityDeferred, array_merge(
            self::basePayload($task, $execution, $key),
            [
                'deferred_since' => $now->toJSON(),
                'deferral_deadline_at' => $task->deferral_deadline_at?->toJSON(),
                'release_after_seconds' => $key->releaseAfter,
                'max_wait_seconds' => $key->maxWait,
                'expires_after_seconds' => $key->expiresAfter,
            ],
        ), $task);
    }

    private static function recordDeferralEnded(
        WorkflowRun $run,
        WorkflowTask $task,
        ActivityExecution $execution,
        ConcurrencyKey $key,
        string $outcome,
        CarbonInterface $now,
    ): void {
        WorkflowHistoryEvent::record($run, HistoryEventType::ActivityDeferralEnded, array_merge(
            self::basePayload($task, $execution, $key),
            [
                'outcome' => $outcome,
                'deferred_since' => $task->deferred_since?->toJSON(),
                'ended_at' => $now->toJSON(),
                'deferral_count' => (int) $task->deferral_count,
            ],
        ), $task);
    }

    /**
     * @return array<string, mixed>
     */
    private static function basePayload(WorkflowTask $task, ActivityExecution $execution, ConcurrencyKey $key): array
    {
        return [
            'activity_execution_id' => $execution->id,
            'activity_class' => $execution->activity_class,
            'activity_type' => $execution->activity_type,
            'sequence' => $execution->sequence,
            'workflow_task_id' => $task->id,
            'concurrency_key' => $key->key,
        ];
    }
}
