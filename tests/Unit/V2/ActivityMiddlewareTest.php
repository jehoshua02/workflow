<?php

declare(strict_types=1);

namespace Tests\Unit\V2;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Worker;
use Illuminate\Queue\WorkerOptions;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Tests\Fixtures\V2\TestOverlappingActivity;
use Tests\Fixtures\V2\TestOverlappingWorkflow;
use Tests\TestCase;
use Workflow\V2\Enums\HistoryEventType;
use Workflow\V2\Enums\TaskStatus;
use Workflow\V2\Enums\TaskType;
use Workflow\V2\Jobs\RunActivityTask;
use Workflow\V2\Models\ActivityAttempt;
use Workflow\V2\Models\WorkflowHistoryEvent;
use Workflow\V2\Models\WorkflowTask;
use Workflow\V2\Support\TaskRepairPolicy;
use Workflow\V2\TaskWatchdog;
use Workflow\V2\WorkflowStub;

final class ActivityMiddlewareTest extends TestCase
{
    private const LOCK_KEY = 'laravel-queue-overlap:borrower-7';

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

        TestOverlappingActivity::$calls = [];
        TestOverlappingActivity::$during = null;
        TestOverlappingActivity::$middlewareFails = null;
        Carbon::setTestNow(Carbon::parse('2026-01-15 12:00:00'));
        $this->app['events']->listen(JobFailed::class, function (JobFailed $event): void {
            $this->failures[] = $event;
        });
    }

    protected function tearDown(): void
    {
        TestOverlappingActivity::$during = null;
        Carbon::setTestNow();

        parent::tearDown();
    }

    public function testRefusedLockDefersTheTaskWithoutSpendingAnAttempt(): void
    {
        $holder = Cache::lock(self::LOCK_KEY, 60);
        $this->assertTrue($holder->get());

        $workflow = $this->startUntilActivityIsQueued('product-a');
        $task = $this->activityTask();

        $this->runNextJob();

        $this->assertSame([], TestOverlappingActivity::$calls);
        $task->refresh();
        $this->assertSame(TaskStatus::Ready, $task->status);
        $this->assertSame(0, $task->attempt_count);
        $this->assertTrue($task->available_at->equalTo(now()->addSeconds(TestOverlappingActivity::RELEASE_AFTER)));
        $this->assertSame(0, ActivityAttempt::query()->count());
        $this->assertFalse($this->historyHas(HistoryEventType::ActivityStarted));
        $this->assertSame(0, DB::table('jobs')->count());
        $this->assertSame(RunActivityTask::DEFERRED_BY_MIDDLEWARE, $task->last_claim_error);
        $this->assertTrue($task->repair_available_at->equalTo($task->available_at));

        $holder->release();
        Carbon::setTestNow(now()->addSeconds(TestOverlappingActivity::RELEASE_AFTER));
        $this->repairPass();
        $this->runNextJob();
        $this->runNextJob();

        $this->assertSame(['product-a'], TestOverlappingActivity::$calls);
        $this->assertSame(1, $task->fresh()->attempt_count);
        $this->assertNull($task->fresh()->last_claim_error);
        $this->assertTrue($workflow->refresh()->completed());
        $this->assertCount(0, $this->failures);
    }

    public function testAHeldResourceKeepsOneDeliveryPerDeferral(): void
    {
        $holder = Cache::lock(self::LOCK_KEY, 600);
        $this->assertTrue($holder->get());

        $workflow = $this->startUntilActivityIsQueued('product-a');
        $task = $this->activityTask();
        $this->runNextJob();

        for ($lap = 0; $lap < 6; $lap++) {
            Carbon::setTestNow(now()->addSeconds(TestOverlappingActivity::RELEASE_AFTER - 1));
            $this->repairPass();
            $this->assertSame(0, DB::table('jobs')->count(), 'Repair redelivered a task that is not due.');

            Carbon::setTestNow(now()->addSeconds(1 + TaskRepairPolicy::redispatchAfterSeconds()));
            $this->assertFalse(TaskRepairPolicy::dispatchOverdue($task->fresh()));
            $this->repairPass();
            $this->assertSame(1, DB::table('jobs')->count(), 'Each deferral should yield exactly one delivery.');

            $this->runNextJob();
            $this->assertSame(0, DB::table('jobs')->count(), 'A refused delivery started its own chain.');
        }

        $this->assertSame([], TestOverlappingActivity::$calls);
        $this->assertSame(0, $task->fresh()->attempt_count);

        $holder->release();
        Carbon::setTestNow(now()->addSeconds(TestOverlappingActivity::RELEASE_AFTER));
        $this->repairPass();
        $this->runNextJob();
        $this->runNextJob();

        $this->assertSame(['product-a'], TestOverlappingActivity::$calls);
        $this->assertSame(1, $task->fresh()->attempt_count);
        $this->assertTrue($workflow->refresh()->completed());
    }

    public function testAnImmediateReleaseDoesNotRedeliverOnItsOwn(): void
    {
        $this->startUntilActivityIsQueued('product-a');
        $task = $this->activityTask();
        DB::table('jobs')->delete();

        (new RunActivityTask($task->id))->release(0);

        $this->assertSame(0, DB::table('jobs')->count());
        $this->assertSame(RunActivityTask::DEFERRED_BY_MIDDLEWARE, $task->fresh()->last_claim_error);
    }

    public function testAMiddlewareThatCannotBeBuiltFailsTheAttempt(): void
    {
        TestOverlappingActivity::$middlewareFails = 'middleware';

        $this->assertMiddlewareErrorFailsTheRun('Lock key could not be built.');
    }

    public function testAMiddlewareThatThrowsBeforeTheClaimFailsTheAttempt(): void
    {
        TestOverlappingActivity::$middlewareFails = 'handle';

        $this->assertMiddlewareErrorFailsTheRun('Lock store unavailable.');
    }

    public function testASecondRunForTheSameResourceIsReleasedWhileTheFirstHoldsIt(): void
    {
        WorkflowStub::make(TestOverlappingWorkflow::class)->start('borrower-7', 'product-a', false);
        WorkflowStub::make(TestOverlappingWorkflow::class)->start('borrower-7', 'product-b', false);
        $this->runNextJob();
        $this->runNextJob();
        DB::table('jobs')->delete();
        [$first, $second] = WorkflowTask::query()
            ->where('task_type', TaskType::Activity)
            ->orderBy('created_at')
            ->orderBy('id')
            ->get()
            ->all();

        TestOverlappingActivity::$during = function (string $caller) use ($second): void {
            if ($caller === 'product-a') {
                // Deliver the second run's task while the first is mid-pull.
                $this->app->call([new RunActivityTask($second->id), 'handle']);
            }
        };

        $this->app->call([new RunActivityTask($first->id), 'handle']);

        $this->assertSame(['product-a'], TestOverlappingActivity::$calls);
        $this->assertSame(1, $first->fresh()->attempt_count);
        $this->assertSame(0, $second->fresh()->attempt_count);
        $this->assertSame(TaskStatus::Ready, $second->fresh()->status);
        $this->assertSame(1, ActivityAttempt::query()->count());
        $this->assertTrue(Cache::lock(self::LOCK_KEY, 60)->get(), 'The lock outlived the activity.');
    }

    public function testTheLockIsReleasedWhenTheActivityFails(): void
    {
        $workflow = $this->startUntilActivityIsQueued('product-a', fail: true);

        $this->runNextJob();
        $this->runNextJob();

        $this->assertSame(['product-a'], TestOverlappingActivity::$calls);
        $this->assertTrue($workflow->refresh()->failed());
        $this->assertTrue(Cache::lock(self::LOCK_KEY, 60)->get(), 'A failed holder kept the lock.');
    }

    public function testAReleaseAfterTheTaskWasClaimedLeavesItAlone(): void
    {
        $this->startUntilActivityIsQueued('product-a');
        $task = $this->activityTask();
        $task->forceFill([
            'status' => TaskStatus::Leased,
        ])->save();
        $availableAt = $task->available_at;
        DB::table('jobs')->delete();

        (new RunActivityTask($task->id))->release(TestOverlappingActivity::RELEASE_AFTER);

        $this->assertEquals($availableAt, $task->fresh()->available_at);
        $this->assertNull($task->fresh()->last_claim_error);
        $this->assertSame(0, DB::table('jobs')->count());
    }

    private function assertMiddlewareErrorFailsTheRun(string $message): void
    {
        $workflow = $this->startUntilActivityIsQueued('product-a');
        $task = $this->activityTask();

        $this->runNextJob();

        $this->assertSame([], TestOverlappingActivity::$calls);
        $this->assertCount(0, $this->failures, 'The queue job failed and left the task for repair.');
        $this->assertSame(1, $task->fresh()->attempt_count);
        $this->assertNotSame(TaskStatus::Ready, $task->fresh()->status);
        $attempt = ActivityAttempt::query()->sole();
        $this->assertSame('failed', $attempt->status->value);

        $this->runNextJob();

        $this->assertTrue($workflow->refresh()->failed());
        $this->assertStringContainsString(
            $message,
            (string) WorkflowHistoryEvent::query()
                ->where('event_type', HistoryEventType::ActivityFailed->value)
                ->sole()
                ->payload['message'],
        );
    }

    private function repairPass(): void
    {
        TaskWatchdog::runPass();
    }

    private function startUntilActivityIsQueued(string $caller, bool $fail = false): WorkflowStub
    {
        $workflow = WorkflowStub::make(TestOverlappingWorkflow::class);
        $workflow->start('borrower-7', $caller, $fail);
        $this->runNextJob();

        return $workflow;
    }

    private function activityTask(): WorkflowTask
    {
        return WorkflowTask::query()->where('task_type', TaskType::Activity)->sole();
    }

    private function historyHas(HistoryEventType $type): bool
    {
        return WorkflowHistoryEvent::query()->where('event_type', $type->value)->exists();
    }

    private function runNextJob(): void
    {
        /** @var Worker $worker */
        $worker = $this->app->make('queue.worker');
        $worker->runNextJob('delivery-test', 'overlap', new WorkerOptions(sleep: 0));
    }
}
