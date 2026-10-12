<?php

declare(strict_types=1);

namespace Workflow\V2\Support;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Throwable;
use Workflow\V2\Contracts\HistoryProjectionRole;
use Workflow\V2\Enums\ActivityAttemptStatus;
use Workflow\V2\Enums\ActivityStatus;
use Workflow\V2\Enums\TaskStatus;
use Workflow\V2\Enums\TaskType;
use Workflow\V2\Models\ActivityAttempt;
use Workflow\V2\Models\ActivityExecution;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowTask;

final class ActivityTaskClaimer
{
    /**
     * @return array{0: ActivityTaskClaim|null, 1: int|null}
     */
    public static function claim(
        string $taskId,
        ?string $leaseOwner = null,
        bool $releaseFutureTasks = false,
    ): array {
        $result = self::claimDetailed($taskId, $leaseOwner, $releaseFutureTasks);

        return [$result['claim'], $result['retry_after_seconds']];
    }

    /**
     * @return array{
     *     claim: ActivityTaskClaim|null,
     *     retry_after_seconds: int|null,
     *     reason: string|null,
     *     backend_error: string|null,
     *     compatibility_reason: string|null
     * }
     */
    public static function claimDetailed(
        string $taskId,
        ?string $leaseOwner = null,
        bool $releaseFutureTasks = false,
    ): array {
        $lock = null;

        try {
            $result = self::claimInTransaction($taskId, $leaseOwner, $releaseFutureTasks, $lock);
        } catch (Throwable $throwable) {
            ActivityConcurrencyGate::release($lock['key'] ?? null, $lock['owner'] ?? null);

            throw $throwable;
        }

        $charge = $result['charge'] ?? null;
        unset($result['charge']);

        if ($charge === null) {
            return $result;
        }

        self::chargeAttempt($result['claim'], $charge);

        return self::claimFailure($charge->reason ?? ActivityConcurrencyGate::REASON_KEY_FAILED);
    }

    /**
     * @param array{key: string, owner: string}|null $lock
     * @return array{
     *     claim: ActivityTaskClaim|null,
     *     retry_after_seconds: int|null,
     *     reason: string|null,
     *     backend_error: string|null,
     *     compatibility_reason: string|null,
     *     charge?: ActivityConcurrencyDecision|null
     * }
     */
    private static function claimInTransaction(
        string $taskId,
        ?string $leaseOwner,
        bool $releaseFutureTasks,
        ?array &$lock,
    ): array {
        return DB::transaction(static function () use (
            $taskId,
            $leaseOwner,
            $releaseFutureTasks,
            &$lock
        ): array {
            /** @var WorkflowTask|null $task */
            $task = WorkflowTask::query()
                ->lockForUpdate()
                ->find($taskId);

            if ($task === null) {
                return self::claimFailure('task_not_found');
            }

            if ($task->task_type !== TaskType::Activity) {
                return self::claimFailure('task_not_activity');
            }

            if ($task->status !== TaskStatus::Ready) {
                return self::claimFailure('task_not_ready');
            }

            if ($task->available_at !== null && $task->available_at->isFuture()) {
                // A deferral already queued its own delayed wakeup, so an early
                // delivery of a deferred task is a duplicate and must not fork a second chain.
                return self::claimFailure(
                    'task_not_due',
                    $releaseFutureTasks && $task->deferred_since === null
                        ? self::releaseDelaySeconds($task)
                        : null,
                );
            }

            $activityExecutionId = $task->payload['activity_execution_id'] ?? null;

            if (! is_string($activityExecutionId) || $activityExecutionId === '') {
                return self::claimFailure('activity_execution_missing');
            }

            /** @var ActivityExecution|null $execution */
            $execution = ActivityExecution::query()
                ->lockForUpdate()
                ->find($activityExecutionId);

            if (! $execution instanceof ActivityExecution) {
                return self::claimFailure('activity_execution_not_found');
            }

            /** @var WorkflowRun|null $run */
            $run = WorkflowRun::query()
                ->lockForUpdate()
                ->find($execution->workflow_run_id);

            if (! $run instanceof WorkflowRun) {
                return self::claimFailure('workflow_run_missing');
            }

            TaskCompatibility::sync($task, $run);

            $backendError = TaskBackendCapabilities::recordClaimFailureIfUnsupported($task);

            if ($backendError !== null) {
                self::historyProjectionRole()->projectRun(self::projectionRun($run));

                return self::claimFailure('backend_unsupported', null, $backendError);
            }

            if (! TaskCompatibility::supported($task, $run)) {
                self::historyProjectionRole()->projectRun(self::projectionRun($run));

                return self::claimFailure(
                    'compatibility_unsupported',
                    null,
                    null,
                    TaskCompatibility::mismatchReason($task, $run),
                );
            }

            $now = now();
            if (ActivityTimeoutEnforcer::hasExpiredDeadline($execution, $now)) {
                // The normal timeout sweep owns retry/failure recording. Do not
                // replace the expired deadline while waiting for that sweep.
                return self::claimFailure('task_not_claimable');
            }

            $decision = ActivityConcurrencyGate::evaluate($task, $execution, $run, $now);

            if (! $decision->claims()) {
                TaskDispatcher::dispatch($task);

                return self::claimFailure(ActivityConcurrencyGate::REASON_DEFERRED, null);
            }

            if ($decision->lockOwner !== null && $decision->key !== null) {
                $lock = [
                    'key' => $decision->key->key,
                    'owner' => $decision->lockOwner,
                ];
            }

            $attemptId = (string) Str::ulid();
            $attemptCount = ((int) $task->attempt_count) + 1;

            $task->forceFill([
                'status' => TaskStatus::Leased,
                'leased_at' => $now,
                'lease_owner' => self::nonEmptyString($leaseOwner) ?? $taskId,
                'lease_expires_at' => ActivityLease::expiresAt(),
                'attempt_count' => $attemptCount,
                'last_claim_failed_at' => null,
                'last_claim_error' => null,
                'deferred_since' => null,
                'deferral_deadline_at' => null,
            ])->save();

            $retryPolicy = is_array($execution->retry_policy) ? $execution->retry_policy : [];
            $startToCloseTimeout = is_int($retryPolicy['start_to_close_timeout'] ?? null)
                ? $retryPolicy['start_to_close_timeout']
                : null;
            $closeDeadlineAt = $startToCloseTimeout !== null
                ? $now->copy()
                    ->addSeconds($startToCloseTimeout)
                : null;

            $heartbeatTimeout = is_int($retryPolicy['heartbeat_timeout'] ?? null)
                ? $retryPolicy['heartbeat_timeout']
                : null;
            $heartbeatDeadlineAt = $heartbeatTimeout !== null
                ? $now->copy()
                    ->addSeconds($heartbeatTimeout)
                : null;

            $execution->forceFill([
                'status' => ActivityStatus::Running,
                'attempt_count' => $attemptCount,
                'current_attempt_id' => $attemptId,
                'started_at' => $now,
                'last_heartbeat_at' => null,
                'close_deadline_at' => $closeDeadlineAt,
                'heartbeat_deadline_at' => $heartbeatDeadlineAt,
            ])->save();

            /** @var ActivityAttempt $attempt */
            $attempt = ActivityAttempt::query()->create([
                'id' => $attemptId,
                'workflow_run_id' => $run->id,
                'activity_execution_id' => $execution->id,
                'workflow_task_id' => $task->id,
                'attempt_number' => $attemptCount,
                'status' => ActivityAttemptStatus::Running->value,
                'lease_owner' => $task->lease_owner,
                'started_at' => $execution->started_at ?? $now,
                'lease_expires_at' => $task->lease_expires_at,
                'concurrency_key' => $lock['key'] ?? null,
                'concurrency_lock_owner' => $lock['owner'] ?? null,
            ]);

            self::historyProjectionRole()->recordActivityStarted($run, $execution, $attempt, $task);

            $result = self::claimSuccess(new ActivityTaskClaim(
                $task,
                $run,
                $execution,
                $attempt,
                $decision->activity,
                $lock['key'] ?? null,
                $lock['owner'] ?? null,
            ));
            $result['charge'] = $decision->charges() ? $decision : null;

            return $result;
        });
    }

    /**
     * Records the failure for an attempt the gate charged before the activity ran,
     * so the retry policy decides what happens next.
     */
    private static function chargeAttempt(?ActivityTaskClaim $claim, ActivityConcurrencyDecision $charge): void
    {
        if (! $claim instanceof ActivityTaskClaim || $charge->failure === null) {
            return;
        }

        $execution = $claim->execution;
        $attemptCount = $claim->attemptNumber();
        $activity = $charge->activity;

        $outcome = ActivityOutcomeRecorder::record(
            $claim->task->id,
            $claim->attemptId(),
            $attemptCount,
            null,
            $charge->failure,
            $activity === null
                ? ActivityRetryPolicy::maxAttemptsFromSnapshot($execution)
                : ActivityRetryPolicy::maxAttempts($execution, $activity),
            $activity === null
                ? ActivityRetryPolicy::backoffSecondsFromSnapshot($execution, $attemptCount)
                : ActivityRetryPolicy::backoffSeconds($execution, $activity, $attemptCount),
        );

        if ($outcome['next_task'] instanceof WorkflowTask) {
            TaskDispatcher::dispatch($outcome['next_task']);
        }
    }

    /**
     * @return array{
     *     claim: ActivityTaskClaim|null,
     *     retry_after_seconds: int|null,
     *     reason: string|null,
     *     backend_error: string|null,
     *     compatibility_reason: string|null
     * }
     */
    private static function claimSuccess(ActivityTaskClaim $claim): array
    {
        return [
            'claim' => $claim,
            'retry_after_seconds' => null,
            'reason' => null,
            'backend_error' => null,
            'compatibility_reason' => null,
        ];
    }

    /**
     * @return array{
     *     claim: ActivityTaskClaim|null,
     *     retry_after_seconds: int|null,
     *     reason: string|null,
     *     backend_error: string|null,
     *     compatibility_reason: string|null
     * }
     */
    private static function claimFailure(
        string $reason,
        ?int $retryAfterSeconds = null,
        ?string $backendError = null,
        ?string $compatibilityReason = null,
    ): array {
        return [
            'claim' => null,
            'retry_after_seconds' => $retryAfterSeconds,
            'reason' => $reason,
            'backend_error' => $backendError,
            'compatibility_reason' => $compatibilityReason,
        ];
    }

    private static function releaseDelaySeconds(WorkflowTask $task): int
    {
        $remainingMilliseconds = max(1, $task->available_at->getTimestampMs() - now()->getTimestampMs());

        return (int) ceil($remainingMilliseconds / 1000);
    }

    private static function historyProjectionRole(): HistoryProjectionRole
    {
        /** @var HistoryProjectionRole $role */
        $role = app(HistoryProjectionRole::class);

        return $role;
    }

    private static function projectionRun(WorkflowRun $run): WorkflowRun
    {
        return $run->fresh(['instance', 'tasks', 'activityExecutions', 'failures']) ?? $run;
    }

    private static function nonEmptyString(mixed $value): ?string
    {
        return is_string($value) && $value !== ''
            ? $value
            : null;
    }
}
