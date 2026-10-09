<?php

declare(strict_types=1);

namespace Tests\Unit\V2;

use Illuminate\Contracts\Cache\Lock;
use Illuminate\Database\QueryException;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Worker;
use Illuminate\Queue\WorkerOptions;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use PDOException;
use PHPUnit\Framework\Attributes\DataProvider;
use RuntimeException;
use Tests\Fixtures\V2\TestConcurrencyKeyActivity;
use Tests\Fixtures\V2\TestConcurrencyKeyOtherActivity;
use Tests\Fixtures\V2\TestConcurrencyKeyWorkflow;
use Tests\TestCase;
use Workflow\V2\ConcurrencyKey;
use Workflow\V2\Contracts\ActivityTaskBridge;
use Workflow\V2\Enums\TaskStatus;
use Workflow\V2\Enums\TaskType;
use Workflow\V2\Exceptions\InvalidConcurrencyKeyException;
use Workflow\V2\Jobs\RunActivityTask;
use Workflow\V2\Models\ActivityAttempt;
use Workflow\V2\Models\ActivityExecution;
use Workflow\V2\Models\WorkflowHistoryEvent;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowTask;
use Workflow\V2\StartOptions;
use Workflow\V2\Support\ActivityConcurrencyGate;
use Workflow\V2\Support\HistoryTimeline;
use Workflow\V2\Support\OperatorMetrics;
use Workflow\V2\Support\TaskRepairPolicy;
use Workflow\V2\TaskWatchdog;
use Workflow\V2\WorkflowStub;

/**
 * Adversarial scenarios S1-S11 and C1-C5 from the rejected activity-middleware
 * branch, rewritten for Activity::concurrencyKey(), plus the key's own rules.
 */
final class ActivityConcurrencyKeyTest extends TestCase
{
    private const KEY = 'probe-resource';

    /**
     * @var list<JobFailed>
     */
    private array $failures = [];

    protected function setUp(): void
    {
        parent::setUp();

        Schema::dropIfExists('jobs');
        Schema::create('jobs', static function (Blueprint $table): void {
            $table->bigIncrements('id');
            $table->string('queue')
                ->index();
            $table->longText('payload');
            $table->unsignedTinyInteger('attempts');
            $table->unsignedInteger('reserved_at')
                ->nullable();
            $table->unsignedInteger('available_at');
            $table->unsignedInteger('created_at');
        });

        config([
            'workflows.v2.connection' => 'delivery-test',
            'workflows.v2.queue' => 'overlap',
            'queue.default' => 'delivery-test',
            'queue.connections.delivery-test' => [
                'driver' => 'database',
                'connection' => config('database.default'),
                'table' => 'jobs',
                'queue' => 'overlap',
                'retry_after' => 60,
            ],
        ]);

        TestConcurrencyKeyActivity::$calls = [];
        TestConcurrencyKeyActivity::$key = null;
        TestConcurrencyKeyActivity::$during = null;
        TestConcurrencyKeyActivity::$fail = false;
        TestConcurrencyKeyWorkflow::$options = [
            'maxAttempts' => 1,
        ];
        Carbon::setTestNow(Carbon::parse('2026-01-15 12:00:00'));
        $this->app['events']->listen(JobFailed::class, function (JobFailed $event): void {
            $this->failures[] = $event;
        });
    }

    protected function tearDown(): void
    {
        TestConcurrencyKeyActivity::$key = null;
        TestConcurrencyKeyActivity::$during = null;
        Carbon::setTestNow();

        parent::tearDown();
    }

    // ---- S1 / C5: activity deadlines still fire while deferred ----

    public function testS1aDeferralOutlastsScheduleToStart(): void
    {
        $this->assertDeferralPastActivityDeadline([
            'maxAttempts' => 1,
            'scheduleToStartTimeout' => 10,
        ]);
    }

    public function testS1bDeferralOutlastsScheduleToClose(): void
    {
        $this->assertDeferralPastActivityDeadline([
            'maxAttempts' => 1,
            'scheduleToCloseTimeout' => 10,
        ]);
    }

    // ---- S2: cancel / terminate while deferred ----

    public function testS2aCancelWhileDeferred(): void
    {
        $this->assertControlWhileDeferred('cancel');
    }

    public function testS2bTerminateWhileDeferred(): void
    {
        $this->assertControlWhileDeferred('terminate');
    }

    // ---- S3: run execution timeout while deferred ----

    public function testS3ExecutionTimeoutWhileDeferred(): void
    {
        $holder = $this->holdLock();
        $this->useKey(releaseAfter: 5);

        $workflow = WorkflowStub::make(TestConcurrencyKeyWorkflow::class);
        $workflow->start('a', StartOptions::rejectDuplicate()->withExecutionTimeout(60));
        $this->runNextJob();
        $task = $this->activityTask();
        $this->runNextJob();
        $this->assertDeferred($task);

        Carbon::setTestNow(now()->addSeconds(61));
        $this->repairPass();
        $this->drainJobs();
        Carbon::setTestNow(now()->addSeconds(600));
        $holder->release();
        $this->repairPass();
        $this->drainJobs();

        $this->assertSame([], TestConcurrencyKeyActivity::$calls, 'Activity ran after the run timed out.');
        $this->assertNotSame(TaskStatus::Ready, $task->fresh()->status);
        $this->assertTrue($this->runClosed($workflow));
    }

    // ---- S4: the shapes stock middleware took, under the key ----

    public function testS4aKeyDefersThenCompletesWhenTheHolderReleases(): void
    {
        $holder = $this->holdLock();
        $this->useKey(releaseAfter: 5);

        $workflow = $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        $this->runNextJob();
        $this->assertDeferred($task);
        $this->assertSame(0, $task->fresh()->attempt_count);

        $holder->release();
        Carbon::setTestNow(now()->addSeconds(6));
        $this->drainJobs();

        $this->assertTrue($workflow->refresh()->completed());
        $this->assertSame(1, $task->fresh()->attempt_count);
        $this->assertTrue($this->lockIsFree(), 'Lock still held after the outcome was recorded.');
    }

    /**
     * D3 is impossible by construction: there is no middleware, so a refusal
     * has exactly one path, and it always writes the deferral and a wakeup.
     */
    public function testS4bRefusalAlwaysRecordsADeferral(): void
    {
        $this->holdLock();
        $this->useKey(releaseAfter: 5);

        $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        $this->runNextJob();

        $task->refresh();
        $this->assertSame(TaskStatus::Ready, $task->status);
        $this->assertSame(self::KEY, $task->concurrency_key);
        $this->assertTrue($task->deferred_since->equalTo(now()));
        $this->assertTrue($task->available_at->equalTo(now()->addSeconds(5)));
        $this->assertSame(1, $task->deferral_count);
        $this->assertNull($task->last_claim_error);
        $this->assertSame(1, $this->eventCount('ActivityDeferred'));
        $this->assertSame(1, DB::table('jobs')->count(), 'The deferral must queue exactly one delayed wakeup.');
        $this->assertGreaterThan(now()->getTimestamp(), DB::table('jobs')->value('available_at'));
    }

    /**
     * D5 is impossible by construction: no exception middleware exists. The
     * activity's own failures follow the retry policy and free the lock.
     */
    public function testS4cActivityFailuresFollowTheRetryPolicyAndFreeTheLock(): void
    {
        TestConcurrencyKeyWorkflow::$options = [
            'maxAttempts' => 3,
            'backoff' => [1, 1],
        ];
        TestConcurrencyKeyActivity::$fail = true;
        $this->useKey(releaseAfter: 5);

        $workflow = $this->startUntilActivityIsQueued('a');

        for ($i = 0; $i < 3; $i++) {
            $this->drainJobs();
            $this->assertTrue($this->lockIsFree(), 'Lock held after a failed attempt.');
            Carbon::setTestNow(now()->addSeconds(2));
        }
        $this->drainJobs();

        $this->assertCount(3, TestConcurrencyKeyActivity::$calls);
        $this->assertTrue($workflow->refresh()->failed());
    }

    public function testS4dNullKeyRunsWithoutALock(): void
    {
        $this->holdLock();
        $seenFree = null;
        TestConcurrencyKeyActivity::$key = static fn (): ?ConcurrencyKey => null;
        TestConcurrencyKeyActivity::$during = static function () use (&$seenFree): void {
            $seenFree = Cache::lock('other-' . self::KEY, 1)->get();
        };

        $workflow = $this->startUntilActivityIsQueued('a');
        $this->drainJobs();

        $this->assertTrue($workflow->refresh()->completed());
        $this->assertTrue($seenFree);
        $this->assertSame(0, $this->eventCount('ActivityDeferred'));
    }

    /**
     * D3: the deferred task stays visible and repair does not redeliver it
     * while it waits, so there is no invisible poll loop.
     */
    public function testS4eDeferredTaskIsNotRedeliveredByRepairWhileWaiting(): void
    {
        $this->holdLock();
        $this->useKey(releaseAfter: 30, maxWait: 600);

        $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        $this->runNextJob();

        for ($pass = 0; $pass < 5; $pass++) {
            Carbon::setTestNow(now()->addSeconds(TaskRepairPolicy::redispatchAfterSeconds() + 1));
            $this->repairPass();
        }

        $this->assertSame(1, DB::table('jobs')->count(), 'Repair redelivered a task that is waiting on purpose.');
        $this->assertSame(0, (int) $task->fresh()->repair_count);
        $this->assertSame(1, OperatorMetrics::snapshot()['tasks']['deferred']);
    }

    /**
     * D6 is impossible by construction: releaseAfter must be at least 1s.
     */
    public function testS4fReleaseAfterZeroIsRejectedAndCharged(): void
    {
        TestConcurrencyKeyWorkflow::$options = [
            'maxAttempts' => 3,
        ];
        TestConcurrencyKeyActivity::$key = static fn (): ConcurrencyKey => new ConcurrencyKey(self::KEY, 600, 0, 600);

        $workflow = $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        $this->drainJobs();

        $this->assertSame([], TestConcurrencyKeyActivity::$calls);
        $this->assertTrue($workflow->refresh()->failed());
        $this->assertSame(1, $task->fresh()->attempt_count, 'An invalid key is not retryable.');
        $this->assertStringContainsString('releaseAfter', (string) $this->activityFailureMessage());
        $this->assertTrue($this->lockIsFree());
    }

    /**
     * D2 is impossible by construction: the engine takes the key verbatim, so
     * there is no class prefix to collide on.
     */
    public function testS4gDifferentKeysDoNotCollideAcrossActivityClasses(): void
    {
        $this->holdLock();
        TestConcurrencyKeyActivity::$key = static fn (TestConcurrencyKeyActivity $activity): ConcurrencyKey => new ConcurrencyKey(
            $activity instanceof TestConcurrencyKeyOtherActivity ? 'other-resource' : self::KEY,
            600,
            5,
            600,
        );

        $workflow = WorkflowStub::make(TestConcurrencyKeyWorkflow::class);
        $workflow->start('a', 'other');
        $this->runNextJob();
        $this->drainJobs();

        $this->assertTrue($workflow->refresh()->completed(), 'A key held for one resource blocked another.');
        $this->assertSame(0, $this->eventCount('ActivityDeferred'));
    }

    // ---- S5: a lost wakeup is recovered ----

    public function testS5LostDeferralWakeupIsRecoveredByRepairAtTheDeadline(): void
    {
        $holder = $this->holdLock();
        $this->useKey(releaseAfter: 5, maxWait: 60);

        $workflow = $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        $this->runNextJob();
        DB::table('jobs')->delete();
        $holder->release();

        Carbon::setTestNow(now()->addSeconds(30));
        $this->repairPass();
        $this->assertSame(0, DB::table('jobs')->count(), 'Repair redelivered before the deferral deadline.');

        Carbon::setTestNow(now()->addSeconds(31 + TaskRepairPolicy::redispatchAfterSeconds()));
        $this->repairPass();
        $this->drainJobs();

        $this->assertTrue($workflow->refresh()->completed(), 'Repair never recovered the lost wakeup.');
        $this->assertSame(1, $task->fresh()->attempt_count);
    }

    // ---- S6: one evaluation, one attempt ----

    public function testS6aUncontendedKeyRunsOnceWithoutDeferralEvents(): void
    {
        $heldDuringRun = null;
        $this->useKey(releaseAfter: 5);
        TestConcurrencyKeyActivity::$during = function () use (&$heldDuringRun): void {
            $heldDuringRun = ! $this->lockIsFree();
        };

        $workflow = $this->startUntilActivityIsQueued('a');
        $this->drainJobs();

        $this->assertCount(1, TestConcurrencyKeyActivity::$calls);
        $this->assertSame(1, ActivityAttempt::query()->count());
        $this->assertTrue($heldDuringRun, 'The lock was not held while the activity ran.');
        $this->assertTrue($this->lockIsFree());
        $this->assertTrue($workflow->refresh()->completed());
        $this->assertSame(0, $this->eventCount('ActivityDeferred'));
        $this->assertSame(0, $this->eventCount('ActivityDeferralEnded'));
        $this->assertSame(self::KEY, ActivityAttempt::query()->sole()->concurrency_key);
    }

    public function testS6bActivityFailureIsRecordedUnderAKey(): void
    {
        TestConcurrencyKeyActivity::$fail = true;
        $this->useKey(releaseAfter: 5);

        $workflow = $this->startUntilActivityIsQueued('a');
        $this->drainJobs();

        $this->assertTrue($workflow->refresh()->failed());
        $this->assertSame(1, ActivityAttempt::query()->count());
        $this->assertTrue($this->lockIsFree());
    }

    // ---- S7: errors around the lock ----

    public function testS7aThrowingConcurrencyKeyIsChargedAndTakesNoLock(): void
    {
        TestConcurrencyKeyActivity::$key = static function (): ConcurrencyKey {
            throw new RuntimeException('Boom from concurrencyKey().');
        };

        $workflow = $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        $this->drainJobs();

        $this->assertSame([], TestConcurrencyKeyActivity::$calls);
        $this->assertTrue($workflow->refresh()->failed());
        $this->assertSame(1, $task->fresh()->attempt_count);
        $this->assertSame('Boom from concurrencyKey().', $this->activityFailureMessage());
        $this->assertTrue($this->lockIsFree());
        $this->assertCount(0, $this->failures);
    }

    public function testS7bLockIsFreeBetweenAFailedAttemptAndItsRetry(): void
    {
        TestConcurrencyKeyWorkflow::$options = [
            'maxAttempts' => 2,
            'backoff' => [1],
        ];
        $this->useKey(releaseAfter: 5);
        TestConcurrencyKeyActivity::$during = static function (): void {
            if (count(TestConcurrencyKeyActivity::$calls) === 1) {
                throw new RuntimeException('First attempt fails.');
            }
        };

        $workflow = $this->startUntilActivityIsQueued('a');
        $this->runNextJob();
        $this->assertTrue($this->lockIsFree(), 'Failed attempt kept the lock until its TTL.');

        Carbon::setTestNow(now()->addSeconds(2));
        $this->drainJobs();

        $this->assertTrue($workflow->refresh()->completed());
        $this->assertCount(2, TestConcurrencyKeyActivity::$calls);
        $this->assertSame(0, $this->eventCount('ActivityDeferred'));
    }

    // ---- S8: a duplicate delivery while the holder runs ----

    public function testS8DuplicateDeliveryWhileRunningIsANoop(): void
    {
        $this->useKey(releaseAfter: 5);
        $test = $this;
        $nested = false;
        TestConcurrencyKeyActivity::$during = static function (TestConcurrencyKeyActivity $activity) use (
            &$nested,
            $test
        ): void {
            if (! $nested) {
                $nested = true;
                $test->app->call([new RunActivityTask((string) $activity->taskId), 'handle']);
            }
        };

        $workflow = $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        $this->drainJobs();

        $this->assertCount(1, TestConcurrencyKeyActivity::$calls);
        $this->assertSame(1, ActivityAttempt::query()->count());
        $this->assertSame(1, $task->fresh()->attempt_count);
        $this->assertSame(0, $this->eventCount('ActivityDeferred'));
        $this->assertCount(0, $this->failures);
        $this->assertTrue($workflow->refresh()->completed());
    }

    // ---- S9: many laps stay bounded (D4) ----

    public function testS9FiftyDeferralLapsStayBounded(): void
    {
        $this->holdLock();
        $this->useKey(releaseAfter: 5, maxWait: 100000);

        $workflow = $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        $this->runNextJob();
        $historyBefore = $this->historyCount($workflow);

        $deliveries = 0;
        for ($lap = 0; $lap < 50; $lap++) {
            Carbon::setTestNow(now()->addSeconds(6));
            $this->repairPass();
            $deliveries += $this->drainJobs();
        }

        $task->refresh();
        $this->assertSame(50, $deliveries);
        $this->assertSame(0, $task->attempt_count);
        $this->assertSame(51, $task->deferral_count);
        $this->assertSame($historyBefore, $this->historyCount($workflow), 'History grew per lap.');
        $this->assertSame(0, ActivityAttempt::query()->count());
        $this->assertSame(0, (int) $task->repair_count, 'repair_count grows with deferral laps.');
        $this->assertSame(0, OperatorMetrics::snapshot()['tasks']['max_repair_count']);
        $this->assertSame(1, DB::table('jobs')->count());
    }

    // ---- S10: external workers (D7) ----

    public function testS10ExternalWorkerIsRefusedWhileTheKeyIsHeld(): void
    {
        $this->holdLock();
        $this->useKey(releaseAfter: 5);

        $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        DB::table('jobs')->delete();

        /** @var ActivityTaskBridge $bridge */
        $bridge = $this->app->make(ActivityTaskBridge::class);
        $status = $bridge->claimStatus($task->id, 'external-worker');

        $this->assertFalse($status['claimed'], 'An external worker claimed the task while its key was held.');
        $this->assertSame(ActivityConcurrencyGate::REASON_DEFERRED, $status['reason']);
        $this->assertDeferred($task);
        $this->assertSame(0, $task->fresh()->attempt_count);
    }

    public function testS10bExternalWorkerCannotClaimADeferredTaskEarly(): void
    {
        $this->holdLock();
        $this->useKey(releaseAfter: 5);

        $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        $this->runNextJob();
        $this->assertDeferred($task);

        $claim = $this->app->make(ActivityTaskBridge::class)
            ->claim($task->id, 'external-worker');

        $this->assertNull($claim);
    }

    // ---- S11: operator view (D1) ----

    public function testS11DeferredTaskIsCountedSeparatelyFromUnhealthy(): void
    {
        $this->holdLock();
        $this->useKey(releaseAfter: 30);

        $this->startUntilActivityIsQueued('a');
        $this->runNextJob();
        $metrics = OperatorMetrics::snapshot();

        $this->assertSame(0, $metrics['tasks']['unhealthy']);
        $this->assertSame(0, $metrics['tasks']['claim_failed']);
        $this->assertSame(1, $metrics['tasks']['deferred']);
        $this->assertSame(0, $metrics['backlog']['unhealthy_tasks']);
        $this->assertSame(1, $metrics['backlog']['deferred_tasks']);

        // In a worker backlog, past available_at, the waiting task is still not overdue.
        Carbon::setTestNow(now()->addSeconds(40));
        $metrics = OperatorMetrics::snapshot();
        $this->assertSame(0, $metrics['tasks']['dispatch_overdue']);
        $this->assertSame(0, $metrics['tasks']['unhealthy']);
    }

    // ---- C1: worker backlog while held (D8) ----

    public function testC1RepairDoesNotStackJobsForADeferredTaskInABacklog(): void
    {
        $this->holdLock();
        $this->useKey(releaseAfter: 5, maxWait: 600);

        $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        $this->runNextJob();

        for ($pass = 0; $pass < 10; $pass++) {
            Carbon::setTestNow(now()->addSeconds(5 + TaskRepairPolicy::redispatchAfterSeconds() + 1));
            $this->repairPass();
        }
        $stacked = DB::table('jobs')->count();

        $ran = $this->drainJobs(50);
        Carbon::setTestNow(now()->addSeconds(6));
        $ranAtNextDue = $this->drainJobs(50);

        $this->assertSame(1, $stacked, 'Repair stacked queue jobs for one deferred task during a backlog.');
        $this->assertSame(1, $ran);
        $this->assertSame(1, $ranAtNextDue);
        $this->assertSame(1, DB::table('jobs')->count(), 'Duplicate wakeups kept themselves alive.');
        $this->assertSame(0, $task->fresh()->attempt_count);
    }

    public function testC1bDuplicateWakeupsCollapseToOneChain(): void
    {
        $this->holdLock();
        $this->useKey(releaseAfter: 5, maxWait: 600);

        $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        Carbon::setTestNow(now()->addSeconds(1));
        // Nine extra deliveries of the same due task, as a backlog or a redelivery storm would leave.
        for ($i = 0; $i < 9; $i++) {
            dispatch(new RunActivityTask($task->id));
        }
        $this->drainJobs(50);

        $this->assertSame(1, DB::table('jobs')->count(), 'Duplicate deliveries forked extra wakeup chains.');
        $this->assertSame(1, $this->eventCount('ActivityDeferred'));
    }

    public function testC1ControlBacklogWithoutAKey(): void
    {
        $this->startUntilActivityIsQueued('a');
        $queued = [];
        for ($pass = 0; $pass < 3; $pass++) {
            Carbon::setTestNow(now()->addSeconds(TaskRepairPolicy::redispatchAfterSeconds() + 1));
            $this->repairPass();
            $queued[] = DB::table('jobs')->count();
        }

        // Pre-existing: repair re-sends an unkeyed ready task once per pass.
        $this->assertSame([2, 3, 4], $queued);
    }

    // ---- C2: due between a check and the claim (D9) ----

    /**
     * D9 is impossible by construction: the due check, the key check and the
     * claim happen in one transaction on the locked task row.
     */
    public function testC2TaskBecomingDueDuringTheClaimStillHonorsTheKey(): void
    {
        $this->holdLock();
        $this->useKey(releaseAfter: 5);

        $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        $this->runNextJob();
        $this->assertDeferred($task);
        DB::table('jobs')->delete();

        $dueAt = $task->fresh()
            ->available_at->copy();
        $reads = 0;
        WorkflowTask::retrieved(static function () use (&$reads, $dueAt): void {
            $reads++;
            Carbon::setTestNow($dueAt->copy()->addMillisecond());
        });
        Carbon::setTestNow($dueAt->copy()->subMillisecond());
        $this->app->call([new RunActivityTask($task->id), 'handle']);

        $this->assertGreaterThan(0, $reads);
        $this->assertSame([], TestConcurrencyKeyActivity::$calls, 'Activity ran while another holder had the key.');
        $this->assertSame(0, $task->fresh()->attempt_count);
        $this->assertSame(2, $task->fresh()->deferral_count);
    }

    // ---- C3: a lock that is never released (D10) ----

    public function testC3LeakedLockGivesUpAfterMaxWait(): void
    {
        $this->holdLock();
        $this->useKey(releaseAfter: 5, maxWait: 300);

        $workflow = $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        $this->runNextJob();

        $deliveries = 0;
        for ($lap = 0; $lap < 100 && ! $this->runClosed($workflow); $lap++) {
            Carbon::setTestNow(now()->addHours(1));
            $this->repairPass();
            $deliveries += $this->drainJobs();
        }

        $this->assertNotSame(TaskStatus::Ready, $task->fresh()->status, 'Task defers forever behind a leaked lock.');
        $this->assertTrue($workflow->refresh()->failed());
        $this->assertSame([], TestConcurrencyKeyActivity::$calls);
        $this->assertSame(1, ActivityAttempt::query()->count());
        $this->assertStringContainsString('Resource busy', (string) $this->activityFailureMessage());
        $this->assertSame(['gave_up'], $this->deferralOutcomes());
    }

    // ---- C4: engine read errors are not charged (D11) ----

    public function testC4TransientDbErrorBeforeTheClaimIsNotChargedToTheActivity(): void
    {
        $this->useKey(releaseAfter: 5);
        $workflow = $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();

        $armed = true;
        WorkflowTask::retrieved(static function () use (&$armed): void {
            if ($armed) {
                $armed = false;

                throw new QueryException(
                    'sqlite',
                    'select * from workflow_tasks',
                    [],
                    new PDOException('SQLSTATE[HY000]: General error: 5 database is locked'),
                );
            }
        });

        $this->runNextJob();
        $this->assertSame(0, $task->fresh()->attempt_count, 'The engine read error was charged as an attempt.');
        $this->assertCount(1, $this->failures, 'The transport failure should surface as a failed job.');
        $this->assertTrue($this->lockIsFree());

        Carbon::setTestNow(now()->addSeconds(TaskRepairPolicy::redispatchAfterSeconds() + 1));
        $this->repairPass();
        $this->drainJobs();

        $this->assertTrue($workflow->refresh()->completed());
        $this->assertSame(1, $task->fresh()->attempt_count);
    }

    // ---- the key's own rules ----

    public function testMaxWaitChargesOneResourceBusyAttemptThenTheRetryPolicyApplies(): void
    {
        TestConcurrencyKeyWorkflow::$options = [
            'maxAttempts' => 2,
            'backoff' => [1],
        ];
        $holder = $this->holdLock();
        $this->useKey(releaseAfter: 5, maxWait: 10);

        $workflow = $this->startUntilActivityIsQueued('a');
        $first = $this->activityTask();
        $this->runNextJob();

        for ($lap = 0; $lap < 3; $lap++) {
            Carbon::setTestNow(now()->addSeconds(5));
            $this->drainJobs();
        }

        $this->assertSame(TaskStatus::Completed, $first->fresh()->status);
        $this->assertSame(1, $first->fresh()->attempt_count);
        $this->assertSame([], TestConcurrencyKeyActivity::$calls);
        $this->assertSame(['gave_up'], $this->deferralOutcomes());
        $this->assertSame(1, $this->eventCount('ActivityRetryScheduled'));

        // The retry started a new deferral episode while the key was still held.
        $holder->release();
        Carbon::setTestNow(now()->addSeconds(6));
        $this->drainJobs();

        $this->assertTrue($workflow->refresh()->completed());
        $this->assertCount(1, TestConcurrencyKeyActivity::$calls);
        $this->assertSame(2, ActivityAttempt::query()->count());
        $this->assertSame(2, $this->eventCount('ActivityDeferred'));
        $this->assertSame(['gave_up', 'acquired'], $this->deferralOutcomes());
    }

    public function testMaxWaitZeroGivesUpOnTheFirstRefusal(): void
    {
        $this->holdLock();
        $this->useKey(releaseAfter: 5, maxWait: 0);

        $workflow = $this->startUntilActivityIsQueued('a');
        $this->drainJobs();

        $this->assertTrue($workflow->refresh()->failed());
        $this->assertSame(1, $this->eventCount('ActivityDeferred'));
        $this->assertSame(['gave_up'], $this->deferralOutcomes());
    }

    public function testExpiryShorterThanStartToCloseIsRejectedAndNotRetried(): void
    {
        TestConcurrencyKeyWorkflow::$options = [
            'maxAttempts' => 3,
            'startToCloseTimeout' => 120,
        ];
        $this->useKey(releaseAfter: 5, expiresAfter: 60);

        $workflow = $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        $this->drainJobs();

        $this->assertTrue($workflow->refresh()->failed());
        $this->assertSame([], TestConcurrencyKeyActivity::$calls);
        $this->assertSame(1, $task->fresh()->attempt_count);
        $this->assertStringContainsString('start-to-close', (string) $this->activityFailureMessage());
        $this->assertTrue($this->lockIsFree());
    }

    public function testExpiryEqualToStartToCloseIsAccepted(): void
    {
        TestConcurrencyKeyWorkflow::$options = [
            'maxAttempts' => 1,
            'startToCloseTimeout' => 60,
        ];
        $this->useKey(releaseAfter: 5, expiresAfter: 60);

        $workflow = $this->startUntilActivityIsQueued('a');
        $this->drainJobs();

        $this->assertTrue($workflow->refresh()->completed());
    }

    #[DataProvider('invalidKeys')]
    public function testConcurrencyKeyRejectsInvalidValues(
        string $key,
        int $expiresAfter,
        int $releaseAfter,
        int $maxWait,
        string $message
    ): void {
        $this->expectException(InvalidConcurrencyKeyException::class);
        $this->expectExceptionMessage($message);

        new ConcurrencyKey($key, $expiresAfter, $releaseAfter, $maxWait);
    }

    /**
     * @return iterable<string, array{string, int, int, int, string}>
     */
    public static function invalidKeys(): iterable
    {
        yield 'empty key' => ['  ', 60, 5, 300, 'non-empty'];
        yield 'no expiry' => [self::KEY, 0, 5, 300, 'must expire'];
        yield 'negative expiry' => [self::KEY, -1, 5, 300, 'must expire'];
        yield 'zero release' => [self::KEY, 60, 0, 300, 'releaseAfter'];
        yield 'negative max wait' => [self::KEY, 60, 5, -1, 'maxWait'];
    }

    public function testHistoryEventsAreWrittenOncePerDeferral(): void
    {
        $holder = $this->holdLock();
        $this->useKey(releaseAfter: 5);

        $workflow = $this->startUntilActivityIsQueued('a');
        $this->runNextJob();
        for ($lap = 0; $lap < 3; $lap++) {
            Carbon::setTestNow(now()->addSeconds(5));
            $this->drainJobs();
        }
        $holder->release();
        Carbon::setTestNow(now()->addSeconds(5));
        $this->drainJobs();

        $this->assertTrue($workflow->refresh()->completed());
        $types = WorkflowHistoryEvent::query()
            ->where('workflow_run_id', $workflow->runId())
            ->orderBy('sequence')
            ->pluck('event_type')
            ->map(static fn ($type): string => $type->value)
            ->all();
        $this->assertSame(1, $this->eventCount('ActivityDeferred'));
        $this->assertSame(['acquired'], $this->deferralOutcomes());
        $this->assertLessThan(
            array_search('ActivityDeferralEnded', $types, true),
            array_search('ActivityDeferred', $types, true)
        );
        $this->assertLessThan(
            array_search('ActivityStarted', $types, true),
            array_search('ActivityDeferralEnded', $types, true)
        );

        $ended = WorkflowHistoryEvent::query()->where('event_type', 'ActivityDeferralEnded')->sole();
        $this->assertSame(4, $ended->payload['deferral_count']);

        $summaries = array_column(
            HistoryTimeline::forRun(WorkflowRun::query()->findOrFail($workflow->runId())),
            'summary'
        );
        $waiting = array_values(array_filter(
            $summaries,
            static fn ($summary): bool => is_string($summary) && str_starts_with(
                $summary,
                'Waiting on probe-resource for '
            ),
        ));
        $this->assertCount(1, $waiting, 'The run timeline does not show the wait.');
    }

    public function testPollModeClaimsHonorTheKeyAndReleaseOnComplete(): void
    {
        $holder = $this->holdLock();
        $this->useKey(releaseAfter: 5);

        $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        DB::table('jobs')->delete();
        config()
            ->set('workflows.v2.task_dispatch_mode', 'poll');

        /** @var ActivityTaskBridge $bridge */
        $bridge = $this->app->make(ActivityTaskBridge::class);
        $this->assertSame([$task->id], array_column($bridge->poll(null, 'overlap'), 'task_id'));
        $refused = $bridge->claimStatus($task->id, 'poller');
        $this->assertFalse($refused['claimed']);
        $this->assertSame(ActivityConcurrencyGate::REASON_DEFERRED, $refused['reason']);
        $this->assertSame([], $bridge->poll(null, 'overlap'), 'A deferred task was offered before it was due.');
        $this->assertSame(0, DB::table('jobs')->count(), 'Poll mode must not queue a job for the deferral.');

        $holder->release();
        Carbon::setTestNow(now()->addSeconds(6));
        $claim = $bridge->claim($task->id, 'poller');
        $this->assertNotNull($claim);
        $this->assertFalse($this->lockIsFree(), 'The poll-mode claim did not take the key.');

        $bridge->complete($claim['activity_attempt_id'], 'done');

        $this->assertTrue($this->lockIsFree(), 'The key was not released after the outcome was recorded.');
        $this->assertSame(TaskStatus::Completed, $task->fresh()->status);
        $this->assertSame(['acquired'], $this->deferralOutcomes());
    }

    public function testExternalWorkerFailureReleasesTheKey(): void
    {
        $this->useKey(releaseAfter: 5);

        $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        DB::table('jobs')->delete();

        $bridge = $this->app->make(ActivityTaskBridge::class);
        $claim = $bridge->claim($task->id, 'external-worker');
        $this->assertNotNull($claim);
        $this->assertFalse($this->lockIsFree());

        $bridge->fail($claim['activity_attempt_id'], new RuntimeException('external failure'));

        $this->assertTrue($this->lockIsFree());
    }

    public function testLocalActivityWithAKeyFailsLoudly(): void
    {
        $this->useKey(releaseAfter: 5);

        $workflow = WorkflowStub::make(TestConcurrencyKeyWorkflow::class);
        $workflow->start('a', 'local');
        $this->drainJobs();

        $this->assertSame([], TestConcurrencyKeyActivity::$calls);
        $this->assertTrue($workflow->refresh()->failed());
        $this->assertStringContainsString('local activities cannot hold', (string) $this->activityFailureMessage());
    }

    // ---- helpers ----

    /**
     * @param array<string, mixed> $options
     */
    private function assertDeferralPastActivityDeadline(array $options): void
    {
        TestConcurrencyKeyWorkflow::$options = $options;
        $holder = $this->holdLock();
        $this->useKey(releaseAfter: 30);

        $workflow = $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        $this->runNextJob();
        $this->assertDeferred($task);

        Carbon::setTestNow(now()->addSeconds(11));
        $this->repairPass();
        $this->drainJobs();

        $holder->release();
        Carbon::setTestNow(now()->addSeconds(60));
        $this->repairPass();
        $this->drainJobs();

        $this->assertSame([], TestConcurrencyKeyActivity::$calls, 'Activity ran after its deadline.');
        $this->assertTrue($workflow->refresh()->failed(), 'Run did not fail on the activity deadline.');
        $this->assertNotSame(TaskStatus::Ready, $task->fresh()->status);
        $this->assertSame(1, $this->eventCount('ActivityTimedOut'));
    }

    private function assertControlWhileDeferred(string $command): void
    {
        $holder = $this->holdLock();
        $this->useKey(releaseAfter: 30);

        $workflow = $this->startUntilActivityIsQueued('a');
        $task = $this->activityTask();
        $this->runNextJob();
        $this->assertDeferred($task);

        $result = $command === 'cancel' ? $workflow->cancel('test') : $workflow->terminate('test');
        $this->assertTrue($result->accepted());
        $this->drainJobs();

        $holder->release();
        Carbon::setTestNow(now()->addSeconds(600));
        $this->repairPass();
        $this->drainJobs();
        $this->app->call([new RunActivityTask($task->id), 'handle']);

        $this->assertSame([], TestConcurrencyKeyActivity::$calls);
        $this->assertNotSame(TaskStatus::Ready, $task->fresh()->status, 'Task still open after ' . $command);
        $this->assertCount(0, $this->failures);
        $this->assertTrue($this->lockIsFree());
    }

    private function assertDeferred(WorkflowTask $task): void
    {
        $task->refresh();
        $this->assertSame(TaskStatus::Ready, $task->status);
        $this->assertNotNull($task->deferred_since, 'Task was not deferred.');
    }

    private function useKey(int $releaseAfter, int $maxWait = 600, int $expiresAfter = 600): void
    {
        TestConcurrencyKeyActivity::$key = static fn (): ConcurrencyKey => new ConcurrencyKey(
            self::KEY,
            $expiresAfter,
            $releaseAfter,
            $maxWait,
        );
    }

    private function holdLock(): Lock
    {
        $holder = Cache::lock(self::KEY, 60000);
        $this->assertTrue($holder->get());

        return $holder;
    }

    private function lockIsFree(): bool
    {
        $probe = Cache::lock(self::KEY, 1);

        if (! $probe->get()) {
            return false;
        }

        $probe->release();

        return true;
    }

    private function eventCount(string $type): int
    {
        return WorkflowHistoryEvent::query()->where('event_type', $type)->count();
    }

    /**
     * @return list<string>
     */
    private function deferralOutcomes(): array
    {
        return WorkflowHistoryEvent::query()
            ->where('event_type', 'ActivityDeferralEnded')
            ->get()
            ->map(static fn (WorkflowHistoryEvent $event): string => (string) $event->payload['outcome'])
            ->all();
    }

    private function activityFailureMessage(): ?string
    {
        $execution = ActivityExecution::query()->sole();
        $event = WorkflowHistoryEvent::query()
            ->whereIn('event_type', ['ActivityFailed', 'ActivityRetryScheduled'])
            ->orderByDesc('sequence')
            ->first();

        return $event?->payload['message'] ?? $execution->exception;
    }

    private function runClosed(WorkflowStub $workflow): bool
    {
        $status = WorkflowRun::query()->findOrFail($workflow->runId())->status->value;

        return ! in_array($status, ['pending', 'running', 'waiting'], true);
    }

    private function historyCount(WorkflowStub $workflow): int
    {
        return WorkflowHistoryEvent::query()->where('workflow_run_id', $workflow->runId())->count();
    }

    private function repairPass(): void
    {
        TaskWatchdog::runPass();
    }

    private function startUntilActivityIsQueued(string $arg): WorkflowStub
    {
        $workflow = WorkflowStub::make(TestConcurrencyKeyWorkflow::class);
        $workflow->start($arg);
        $this->runNextJob();

        return $workflow;
    }

    private function activityTask(): WorkflowTask
    {
        return WorkflowTask::query()->where('task_type', TaskType::Activity)
            ->orderBy('created_at')
            ->firstOrFail();
    }

    private function drainJobs(int $max = 20): int
    {
        $ran = 0;
        while ($ran < $max && DB::table('jobs')->whereNull('reserved_at')->where(
            'available_at',
            '<=',
            now()
                ->getTimestamp()
        )->count() > 0) {
            $this->runNextJob();
            $ran++;
        }

        return $ran;
    }

    private function runNextJob(): void
    {
        /** @var Worker $worker */
        $worker = $this->app->make('queue.worker');
        $worker->runNextJob('delivery-test', 'overlap', new WorkerOptions(sleep: 0));
    }
}
