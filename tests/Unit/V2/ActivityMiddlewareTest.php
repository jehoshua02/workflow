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
        $this->assertSame(
            now()
                ->addSeconds(TestOverlappingActivity::RELEASE_AFTER)
                ->getTimestamp(),
            (int) DB::table('jobs')->sole()->available_at,
        );

        $holder->release();
        Carbon::setTestNow(now()->addSeconds(TestOverlappingActivity::RELEASE_AFTER));
        $this->runNextJob();
        $this->runNextJob();

        $this->assertSame(['product-a'], TestOverlappingActivity::$calls);
        $this->assertSame(1, $task->fresh()->attempt_count);
        $this->assertTrue($workflow->refresh()->completed());
        $this->assertCount(0, $this->failures);
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
        $this->assertSame(0, DB::table('jobs')->count());
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
