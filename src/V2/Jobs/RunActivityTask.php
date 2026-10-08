<?php

declare(strict_types=1);

namespace Workflow\V2\Jobs;

use DateInterval;
use DateTimeInterface;
use Illuminate\Bus\Queueable;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Pipeline\Pipeline;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\InteractsWithTime;
use Throwable;
use Workflow\V2\Enums\TaskStatus;
use Workflow\V2\Models\ActivityExecution;
use Workflow\V2\Models\WorkflowTask;
use Workflow\V2\Support\ActivityOutcomeRecorder;
use Workflow\V2\Support\ActivityRetryPolicy;
use Workflow\V2\Support\ActivityTaskClaim;
use Workflow\V2\Support\ActivityTaskClaimer;
use Workflow\V2\Support\EntryMethod;
use Workflow\V2\Support\RuntimeObjectFactory;
use Workflow\V2\Support\TaskDispatcher;
use Workflow\V2\Support\TypeRegistry;
use Workflow\V2\Support\WorkerCompatibilityFleet;
use Workflow\V2\Testing\ActivityFakeContext;
use Workflow\V2\WorkflowStub;

final class RunActivityTask implements ShouldQueue
{
    use Dispatchable;
    use InteractsWithQueue;
    use InteractsWithTime;
    use Queueable;
    use SerializesModels;

    public const DEFERRED_BY_MIDDLEWARE = 'Activity middleware deferred the task.';

    public int $tries = 1;

    private ?Throwable $constructionError = null;

    public function __construct(
        public readonly string $taskId,
    ) {
        $this->afterCommit();
    }

    public function handle(): void
    {
        WorkerCompatibilityFleet::heartbeat(
            is_string($this->connection ?? null) ? $this->connection : null,
            is_string($this->queue ?? null) ? $this->queue : null,
        );

        $claimEntered = false;

        try {
            (new Pipeline(Container::getInstance()))
                ->send($this)
                ->through($this->activityMiddleware())
                ->then(function () use (&$claimEntered): void {
                    $claimEntered = true;
                    $this->claimAndRun();
                });
        } catch (Throwable $error) {
            if ($claimEntered) {
                throw $error;
            }

            // Failing the queue job here would leave the task ready for repair to
            // redeliver forever, so an error from the activity's own middleware is
            // charged to an attempt like an error from handle().
            $this->claimAndRun($error);
        }
    }

    /**
     * Activity middleware calls this to decline the task. Releasing the queue
     * message would spend this job's single transport try, so the durable task
     * is deferred instead, leaving its attempt count untouched.
     *
     * The deferral is recorded as a claim failure whose repair backoff ends
     * when the task is due, so the repair pass is the only redelivery path.
     * Redispatching from here as well would race repair's overdue check and
     * start a second delivery chain on every lap the resource stays held.
     *
     * @param DateTimeInterface|DateInterval|int $delay
     */
    public function release($delay = 0): void
    {
        $now = now();
        $availableAt = $now->copy()
            ->addSeconds($this->secondsUntil($delay));

        WorkflowTask::query()
            ->whereKey($this->taskId)
            ->where('status', TaskStatus::Ready->value)
            ->update([
                'available_at' => $availableAt,
                'last_claim_failed_at' => $now,
                'last_claim_error' => self::DEFERRED_BY_MIDDLEWARE,
                'repair_available_at' => $availableAt,
            ]);
    }

    private function claimAndRun(?Throwable $preClaimError = null): void
    {
        [$claim, $releaseIn] = $this->claimTask();

        if ($releaseIn !== null) {
            // An early wakeup is not a failed execution or a transport retry.
            $task = WorkflowTask::query()->find($this->taskId);

            if ($task instanceof WorkflowTask) {
                TaskDispatcher::dispatch($task);
            }

            return;
        }

        if ($claim === null) {
            return;
        }

        $attemptId = $claim->attemptId();
        $attemptCount = $claim->attemptNumber();

        /** @var ActivityExecution $execution */
        $execution = $claim->execution;
        $execution->setRelation('run', $claim->run);

        if ($this->constructionError !== null) {
            throw $this->constructionError;
        }

        if ($preClaimError !== null) {
            $this->recordOutcome(
                $execution,
                $attemptId,
                $attemptCount,
                null,
                $preClaimError,
                ActivityRetryPolicy::maxAttemptsFromSnapshot($execution),
                ActivityRetryPolicy::backoffSecondsFromSnapshot($execution, $attemptCount),
            );

            return;
        }

        $activityClass = TypeRegistry::resolveActivityClass($execution->activity_class, $execution->activity_type);
        $activity = RuntimeObjectFactory::activity($activityClass, $execution, $execution->run, $this->taskId);
        $entryMethod = EntryMethod::forActivity($activity);
        $arguments = $activity->resolveMethodDependencies($execution->activityArguments(), $entryMethod);
        $activityArguments = $execution->activityArguments();

        $result = null;
        $throwable = null;

        if (WorkflowStub::faked()) {
            WorkflowStub::recordDispatched($execution->activity_class, $activityArguments);
        }

        if (WorkflowStub::hasMock($execution->activity_class)) {
            $mocked = WorkflowStub::mockedResult(
                $execution->activity_class,
                new ActivityFakeContext(
                    run: $execution->run,
                    execution: $execution,
                    taskId: $this->taskId,
                    sequence: (int) $execution->sequence,
                    activity: $execution->activity_class,
                ),
                $activityArguments,
            );

            $result = $mocked['result'];
            $throwable = $mocked['throwable'];
        } else {
            try {
                $result = $activity->{$entryMethod->getName()}(...$arguments);
            } catch (Throwable $error) {
                $throwable = $error;
            }
        }

        $maxAttempts = ActivityRetryPolicy::maxAttempts($execution, $activity);
        $backoffSeconds = ActivityRetryPolicy::backoffSeconds($execution, $activity, $attemptCount);

        $this->recordOutcome($execution, $attemptId, $attemptCount, $result, $throwable, $maxAttempts, $backoffSeconds);
    }

    private function recordOutcome(
        ActivityExecution $execution,
        string $attemptId,
        int $attemptCount,
        mixed $result,
        ?Throwable $throwable,
        int $maxAttempts,
        int $backoffSeconds,
    ): void {
        $outcome = ActivityOutcomeRecorder::record(
            $this->taskId,
            $attemptId,
            $attemptCount,
            $result,
            $throwable,
            $maxAttempts,
            $backoffSeconds,
        );

        $nextTask = $outcome['next_task'];

        if ($nextTask instanceof WorkflowTask) {
            TaskDispatcher::dispatch($nextTask);
        }
    }

    /**
     * @return list<object>
     */
    private function activityMiddleware(): array
    {
        $task = WorkflowTask::query()->find($this->taskId);

        if (! $task instanceof WorkflowTask
            || $task->status !== TaskStatus::Ready
            || ($task->available_at !== null && $task->available_at->isFuture())) {
            return [];
        }

        $executionId = $task->payload['activity_execution_id'] ?? null;
        $execution = is_string($executionId)
            ? ActivityExecution::query()->with('run')->find($executionId)
            : null;

        if (! $execution instanceof ActivityExecution
            || $execution->run === null
            || WorkflowStub::hasMock($execution->activity_class)) {
            return [];
        }

        try {
            $activityClass = TypeRegistry::resolveActivityClass($execution->activity_class, $execution->activity_type);
            $activity = RuntimeObjectFactory::activity($activityClass, $execution, $execution->run, $this->taskId);
        } catch (Throwable $error) {
            // Building the activity is infrastructure, not activity code: after the
            // claim it fails the transport, not an attempt. Keep that by claiming first.
            $this->constructionError = $error;

            return [];
        }

        return $activity->middleware();
    }

    /**
     * @return array{0: ActivityTaskClaim|null, 1: int|null}
     */
    private function claimTask(): array
    {
        return ActivityTaskClaimer::claim($this->taskId, $this->taskId, releaseFutureTasks: true);
    }
}
