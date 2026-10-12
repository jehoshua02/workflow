<?php

declare(strict_types=1);

namespace Tests\Unit\V2;

use DateTimeImmutable;
use DateTimeZone;
use Illuminate\Database\Events\TransactionBeginning;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Str;
use InvalidArgumentException;
use RuntimeException;
use Tests\Fixtures\V2\TestCommandTargetWorkflow;
use Tests\Fixtures\V2\TestGreetingActivity;
use Tests\Fixtures\V2\TestParentChildWorkflow;
use Tests\Fixtures\V2\TestScheduledWorkflow;
use Tests\Fixtures\V2\TestSignalWorkflow;
use Tests\Fixtures\V2\TestTimerWorkflow;
use Tests\SchemaTestCase;
use Workflow\Serializers\CodecRegistry;
use Workflow\Serializers\Serializer;
use Workflow\V2\Contracts\WorkflowTaskBridge;
use Workflow\V2\Enums\ChildCallStatus;
use Workflow\V2\Enums\MessageChannel;
use Workflow\V2\Enums\RunStatus;
use Workflow\V2\Enums\ScheduleOverlapPolicy;
use Workflow\V2\Enums\ScheduleStatus;
use Workflow\V2\Enums\SignalStatus;
use Workflow\V2\Enums\TaskStatus;
use Workflow\V2\Enums\TaskType;
use Workflow\V2\Jobs\RunWorkflowTask;
use Workflow\V2\Models\WorkflowChildCall;
use Workflow\V2\Models\WorkflowHistoryEvent;
use Workflow\V2\Models\WorkflowInstance;
use Workflow\V2\Models\WorkflowMessage;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowRunSummary;
use Workflow\V2\Models\WorkflowSignal;
use Workflow\V2\Models\WorkflowTask;
use Workflow\V2\Support\MessageService;
use Workflow\V2\Support\ScheduleManager;
use Workflow\V2\Support\StorageConnection;
use Workflow\V2\Support\TaskDispatcher;
use Workflow\V2\Support\WorkflowExecutor;
use Workflow\V2\TaskWatchdog;
use Workflow\V2\WorkflowStub;

final class StorageConnectionTransactionBoundaryTest extends SchemaTestCase
{
    private string $storageDatabase = '';

    /**
     * @var list<string>
     */
    private array $began = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->began = [];
        Event::listen(TransactionBeginning::class, function (TransactionBeginning $event): void {
            $this->began[] = $event->connectionName;
        });
    }

    protected function tearDown(): void
    {
        config()
            ->set('workflows.storage.connection', null);

        parent::tearDown();

        if ($this->storageDatabase !== '' && is_file($this->storageDatabase)) {
            @unlink($this->storageDatabase);
        }
    }

    public function testSignalActivityAndTimerRunsOpenNoTransactionOnTheDefaultConnection(): void
    {
        WorkflowStub::mock(TestGreetingActivity::class, 'Hello, Taylor!');

        $signalled = WorkflowStub::make(TestSignalWorkflow::class, 'storage-boundary-signal');
        $signalled->start();
        WorkflowStub::runReadyTasks();
        $signalled->signal('name-provided', 'Taylor');
        WorkflowStub::runReadyTasks();

        $this->freezeTime();
        $timed = WorkflowStub::make(TestTimerWorkflow::class, 'storage-boundary-timer');
        $timed->start(1);
        WorkflowStub::runReadyTasks();
        $this->travel(5)
            ->seconds();
        WorkflowStub::runReadyTasks();
        WorkflowStub::runReadyTasks();

        $this->assertTrue(WorkflowStub::load('storage-boundary-signal')->completed());
        $this->assertTrue(WorkflowStub::load('storage-boundary-timer')->completed());
        $this->assertContains('storage', $this->began);
        $this->assertNotContains((string) config('database.default'), $this->began);
    }

    public function testChildRunProjectionWritesWaitForTheStorageCommit(): void
    {
        WorkflowStub::mock(TestGreetingActivity::class, 'Hello, Taylor!');

        $levels = [];
        WorkflowChildCall::saved(static function () use (&$levels): void {
            $levels[] = StorageConnection::transactionLevel();
        });

        $parent = WorkflowStub::make(TestParentChildWorkflow::class, 'storage-boundary-parent');
        $parent->start('Taylor');
        WorkflowStub::runReadyTasks();

        $this->assertTrue(WorkflowStub::load('storage-boundary-parent')->completed());
        $this->assertSame(
            ChildCallStatus::Completed,
            WorkflowChildCall::query()->where('parent_workflow_instance_id', 'storage-boundary-parent')->sole()->status,
        );
        $this->assertNotSame([], $levels);
        $this->assertSame([0], array_values(array_unique($levels)));
        $this->assertContains('storage', $this->began);
        $this->assertNotContains((string) config('database.default'), $this->began);
    }

    public function testMessageServiceOpensItsTransactionsOnTheStorageConnection(): void
    {
        $service = new MessageService();
        $source = $this->createWorkflowTask('storage-boundary-message-source')
->run;
        $closing = $this->createWorkflowTask('storage-boundary-message-target')
->run;

        $first = $service->sendMessage($source, MessageChannel::Signal, $closing->workflow_instance_id, null, 'stream');
        $service->sendMessage($source, MessageChannel::Signal, $closing->workflow_instance_id, null, 'stream');
        $service->sendMessage($source, MessageChannel::Signal, $closing->workflow_instance_id, null, 'stream');
        $this->assertStorageTransactionsOnly();

        $inbound = $service->receiveMessages($closing, 'stream', 10);
        $this->assertCount(3, $inbound);

        $service->consumeMessage($closing, $inbound[0], 10);
        $this->assertStorageTransactionsOnly();

        $service->consumeMessages($closing, [$inbound[1]], 11);
        $this->assertStorageTransactionsOnly();

        $continued = WorkflowRun::query()->create([
            'id' => (string) Str::ulid(),
            'workflow_instance_id' => $closing->workflow_instance_id,
            'run_number' => 2,
            'workflow_class' => TestCommandTargetWorkflow::class,
            'workflow_type' => 'test-command-target-workflow',
            'status' => RunStatus::Waiting->value,
            'payload_codec' => CodecRegistry::defaultCodec(),
            'arguments' => Serializer::serializeWithCodec(CodecRegistry::defaultCodec(), []),
            'connection' => 'redis',
            'queue' => 'default',
            'started_at' => now(),
        ]);
        $service->transferMessagesToContinuedRun($closing->fresh(), $continued);
        $this->assertStorageTransactionsOnly();

        $this->assertSame(1, $first->sequence);
        $this->assertSame(2, $continued->fresh()->message_cursor_position);
        $this->assertSame(
            $continued->id,
            WorkflowMessage::query()->whereKey($inbound[2]->id)->sole()->workflow_run_id,
        );
    }

    public function testWorkflowTaskBridgeOpensItsTransactionsOnTheStorageConnection(): void
    {
        Queue::fake();

        /** @var WorkflowTaskBridge $bridge */
        $bridge = $this->app->make(WorkflowTaskBridge::class);

        $completed = $this->createWorkflowTask('storage-boundary-bridge-complete');
        $this->began = [];
        $this->assertTrue($bridge->claimStatus($completed->id, 'external-worker')['claimed']);
        $this->assertStorageTransactionsOnly();
        $this->assertTrue($bridge->heartbeat($completed->id)['renewed']);
        $this->assertStorageTransactionsOnly();
        $result = $bridge->complete($completed->id, [[
            'type' => 'complete_workflow',
            'result' => Serializer::serializeWithCodec(CodecRegistry::defaultCodec(), 'done'),
        ]]);
        $this->assertTrue($result['completed'], json_encode($result, JSON_THROW_ON_ERROR));
        $this->assertStorageTransactionsOnly();

        $failed = $this->createWorkflowTask('storage-boundary-bridge-fail');
        $this->began = [];
        $this->assertTrue($bridge->claimStatus($failed->id, 'external-worker')['claimed']);
        $this->assertTrue($bridge->fail($failed->id, 'worker crashed')['recorded']);
        $this->assertStorageTransactionsOnly();
        $this->assertSame(TaskStatus::Failed, $failed->fresh()->status);

        $executed = $this->createWorkflowTask('storage-boundary-bridge-execute');
        $this->began = [];
        $this->assertTrue($bridge->execute($executed->id)['executed']);
        $this->assertStorageTransactionsOnly();
        $this->assertSame(TaskStatus::Completed, $executed->fresh()->status);
    }

    public function testWatchdogRepairOpensNoTransactionOnTheDefaultConnection(): void
    {
        Queue::fake();

        Cache::forget(TaskWatchdog::LOOP_THROTTLE_KEY);
        $staleDispatch = now()
            ->subSeconds(20);
        $task = $this->createWorkflowTask('storage-boundary-watchdog', [
            'available_at' => $staleDispatch,
            'last_dispatched_at' => $staleDispatch,
        ]);

        $report = TaskWatchdog::runPass();

        $this->assertSame(1, $report['repaired_existing_tasks']);
        $this->assertTrue($task->fresh()->last_dispatched_at->greaterThan($staleDispatch));
        $this->assertContains('storage', $this->began);
        $this->assertNotContains((string) config('database.default'), $this->began);
    }

    public function testWatchdogMissingTaskAndDeadlineRepairsOpenNoTransactionOnTheDefaultConnection(): void
    {
        Queue::fake();
        Cache::forget(TaskWatchdog::LOOP_THROTTLE_KEY);

        $this->createRepairNeededSignalRun('storage-boundary-watchdog-missing');
        $deadline = $this->createWaitingRun('storage-boundary-watchdog-deadline');
        $deadline->forceFill([
            'run_deadline_at' => now()
                ->subMinute(),
        ])->save();
        $this->began = [];

        $report = TaskWatchdog::runPass(respectThrottle: false);

        $this->assertSame(1, $report['repaired_missing_tasks'], json_encode($report, JSON_THROW_ON_ERROR));
        $this->assertSame(1, $report['deadline_expired_tasks_created'], json_encode($report, JSON_THROW_ON_ERROR));
        $this->assertStorageTransactionsOnly();
    }

    public function testWorkflowTaskFailurePathOpensNoTransactionOnTheDefaultConnection(): void
    {
        $task = $this->createWorkflowTask('storage-boundary-task-failure');
        $failed = false;
        WorkflowHistoryEvent::creating(static function () use (&$failed): void {
            if (! $failed) {
                $failed = true;

                throw new RuntimeException('history write failed');
            }
        });
        $this->began = [];

        try {
            (new RunWorkflowTask($task->id))->handle($this->app->make(WorkflowExecutor::class));
            $this->fail('The workflow task should have thrown.');
        } catch (RuntimeException $exception) {
            $this->assertSame('history write failed', $exception->getMessage());
        }

        $this->assertTrue($failed);
        $this->assertSame(TaskStatus::Failed, $task->fresh()->status);
        $this->assertSame('history write failed', $task->fresh()->last_error);
        $this->assertStorageTransactionsOnly();
    }

    public function testScheduleTransactionsOpenOnTheStorageConnection(): void
    {
        WorkflowStub::fake();

        $schedule = ScheduleManager::create(
            scheduleId: 'storage-boundary-schedule',
            workflowClass: TestScheduledWorkflow::class,
            cronExpression: '0 * * * *',
            overlapPolicy: ScheduleOverlapPolicy::AllowAll,
        );
        $buffered = $schedule->fresh();
        $buffered->bufferAction();
        $buffered->save();
        $this->began = [];

        $this->assertSame('drained', ScheduleManager::tick()[0]['outcome']);
        $this->assertStorageTransactionsOnly();

        $this->assertNotNull(ScheduleManager::trigger($schedule->fresh()));
        $this->assertStorageTransactionsOnly();

        $schedule->fresh()
            ->forceFill([
                'next_fire_at' => now()
                    ->subMinute(),
            ])->save();
        $this->began = [];
        $this->assertSame('triggered', ScheduleManager::tick()[0]['outcome']);
        $this->assertStorageTransactionsOnly();

        $results = ScheduleManager::backfill(
            $schedule->fresh(),
            new DateTimeImmutable('2026-04-14 00:00:00', new DateTimeZone('UTC')),
            new DateTimeImmutable('2026-04-14 02:00:00', new DateTimeZone('UTC')),
        );
        $this->assertCount(2, $results);
        $this->assertStorageTransactionsOnly();

        ScheduleManager::delete($schedule->fresh());
        $this->assertStorageTransactionsOnly();

        $this->assertSame(5, (int) $schedule->fresh()->fires_count);
        $this->assertSame(ScheduleStatus::Deleted, $schedule->fresh()->status);
    }

    public function testDispatchIsDroppedWhenTheStorageTransactionRollsBack(): void
    {
        Queue::fake();

        $task = $this->createWorkflowTask('storage-boundary-rollback');

        try {
            StorageConnection::transaction(static function () use ($task): void {
                TaskDispatcher::dispatch($task);

                throw new RuntimeException('roll back');
            });
            $this->fail('The transaction should have thrown.');
        } catch (RuntimeException $exception) {
            $this->assertSame('roll back', $exception->getMessage());
        }

        Queue::assertNothingPushed();
        $this->assertNull($task->fresh()->last_dispatched_at);
    }

    public function testRolledBackSavepointDropsItsDispatchWhileTheOuterTransactionCommits(): void
    {
        Queue::fake();

        $dropped = $this->createWorkflowTask('storage-boundary-savepoint-dropped');
        $kept = $this->createWorkflowTask('storage-boundary-savepoint-kept');

        StorageConnection::transaction(function () use ($dropped, $kept): void {
            try {
                StorageConnection::transaction(static function () use ($dropped): void {
                    $dropped->forceFill([
                        'queue' => 'inner-write',
                    ])->save();
                    TaskDispatcher::dispatch($dropped);

                    throw new RuntimeException('roll back the savepoint');
                });
            } catch (RuntimeException) {
                // The outer transaction continues.
            }

            $this->assertSame(1, StorageConnection::transactionLevel());
            TaskDispatcher::dispatch($kept);
            Queue::assertNothingPushed();
        });

        Queue::assertPushed(RunWorkflowTask::class, 1);
        Queue::assertPushed(
            RunWorkflowTask::class,
            static fn (RunWorkflowTask $job): bool => $job->taskId === $kept->id
        );
        $this->assertSame('default', $dropped->fresh()->queue);
        $this->assertNull($dropped->fresh()->last_dispatched_at);
        $this->assertNotNull($kept->fresh()->last_dispatched_at);
    }

    public function testDispatchWaitsForTheApplicationsDefaultConnectionTransactionToCommit(): void
    {
        Queue::fake();

        $task = $this->createWorkflowTask('storage-boundary-app-commit');

        DB::transaction(function () use ($task): void {
            StorageConnection::transaction(static function () use ($task): void {
                TaskDispatcher::dispatch($task);
            });

            $this->assertSame(0, StorageConnection::transactionLevel());
            Queue::assertNothingPushed();
        });

        Queue::assertPushed(RunWorkflowTask::class, 1);
        Queue::assertPushed(
            RunWorkflowTask::class,
            static fn (RunWorkflowTask $job): bool => $job->taskId === $task->id
        );
        $this->assertNotNull($task->fresh()->last_dispatched_at);
    }

    public function testDispatchIsDroppedWhenTheApplicationsDefaultConnectionTransactionRollsBack(): void
    {
        Queue::fake();

        $insideStorage = $this->createWorkflowTask('storage-boundary-app-rollback-nested');
        $outsideStorage = $this->createWorkflowTask('storage-boundary-app-rollback-direct');

        try {
            DB::transaction(static function () use ($insideStorage, $outsideStorage): void {
                StorageConnection::transaction(static function () use ($insideStorage): void {
                    TaskDispatcher::dispatch($insideStorage);
                });
                TaskDispatcher::dispatch($outsideStorage);

                Queue::assertNothingPushed();

                throw new RuntimeException('application rolls back');
            });
            $this->fail('The application transaction should have thrown.');
        } catch (RuntimeException $exception) {
            $this->assertSame('application rolls back', $exception->getMessage());
        }

        Queue::assertNothingPushed();
        $this->assertNull($insideStorage->fresh()->last_dispatched_at);
        $this->assertNull($outsideStorage->fresh()->last_dispatched_at);
    }

    public function testUnknownStorageConnectionFailsLoudlyInsteadOfFallingBackToTheDefault(): void
    {
        config()
            ->set('workflows.storage.connection', 'missing_storage');

        $ran = false;

        try {
            StorageConnection::transaction(static function () use (&$ran): void {
                $ran = true;
            });
            $this->fail('An unknown storage connection should throw.');
        } catch (InvalidArgumentException $exception) {
            $this->assertStringContainsString('missing_storage', $exception->getMessage());
        }

        $this->assertFalse($ran);
        $this->assertSame([], $this->began);
    }

    public function testSingleConnectionAppsKeepTheDefaultConnectionTransactionAndDispatchTiming(): void
    {
        config()
            ->set('workflows.storage.connection', null);
        DB::purge('storage');

        $default = (string) config('database.default');

        $this->assertNull(StorageConnection::name());
        $this->assertSame(DB::connection(), StorageConnection::connection());

        // The engine tables live on the storage file in this class, so only
        // the transaction and its deferral are checked on the default here.
        $calls = [];
        DB::transaction(static function () use (&$calls): void {
            StorageConnection::afterCommit(static function () use (&$calls): void {
                $calls[] = 'after-commit';
            });
            $calls[] = 'in-transaction:' . StorageConnection::transactionLevel();
        });
        StorageConnection::transaction(static function () use (&$calls): void {
            $calls[] = 'helper:' . DB::transactionLevel();
        });
        StorageConnection::afterCommit(static function () use (&$calls): void {
            $calls[] = 'immediate';
        });

        $this->assertSame(['in-transaction:1', 'after-commit', 'helper:1', 'immediate'], $calls);
        $this->assertSame([$default, $default], $this->began);
    }

    protected function defineEnvironment($app): void
    {
        $this->storageDatabase = (string) tempnam(sys_get_temp_dir(), 'wf_storage_boundary_');

        $app['config']->set('database.connections.storage', [
            'driver' => 'sqlite',
            'database' => $this->storageDatabase,
            'prefix' => '',
            'foreign_key_constraints' => false,
        ]);
        $app['config']->set('workflows.storage.connection', 'storage');
    }

    private function assertStorageTransactionsOnly(): void
    {
        $this->assertNotSame([], $this->began);
        $this->assertSame(['storage'], array_values(array_unique($this->began)));
        $this->began = [];
    }

    private function createRepairNeededSignalRun(string $instanceId): WorkflowRun
    {
        $run = $this->createWaitingRun($instanceId);

        /** @var WorkflowSignal $signal */
        $signal = WorkflowSignal::query()->create([
            'id' => (string) Str::ulid(),
            'workflow_command_id' => (string) Str::ulid(),
            'workflow_instance_id' => $run->workflow_instance_id,
            'workflow_run_id' => $run->id,
            'target_scope' => 'instance',
            'resolved_workflow_run_id' => $run->id,
            'signal_name' => 'name-provided',
            'status' => SignalStatus::Received->value,
            'payload_codec' => CodecRegistry::defaultCodec(),
            'arguments' => Serializer::serializeWithCodec(CodecRegistry::defaultCodec(), []),
            'received_at' => now()
                ->subMinute(),
        ]);

        WorkflowRunSummary::query()->create([
            'id' => $run->id,
            'workflow_instance_id' => $run->workflow_instance_id,
            'run_number' => 1,
            'is_current_run' => true,
            'engine_source' => 'v2',
            'class' => TestCommandTargetWorkflow::class,
            'workflow_type' => 'test-command-target-workflow',
            'status' => RunStatus::Waiting->value,
            'status_bucket' => 'running',
            'connection' => 'redis',
            'queue' => 'default',
            'started_at' => $run->started_at,
            'wait_kind' => 'signal',
            'wait_reason' => 'Waiting to apply signal name-provided',
            'open_wait_id' => sprintf('signal-application:%s', $signal->id),
            'resume_source_kind' => 'workflow_signal',
            'resume_source_id' => $signal->id,
            'liveness_state' => 'repair_needed',
            'liveness_reason' => 'Accepted signal name-provided is open without an open workflow task.',
        ]);

        return $run;
    }

    private function createWaitingRun(string $instanceId): WorkflowRun
    {
        $instance = WorkflowInstance::query()->create([
            'id' => $instanceId,
            'workflow_class' => TestCommandTargetWorkflow::class,
            'workflow_type' => 'test-command-target-workflow',
            'run_count' => 1,
            'reserved_at' => now(),
            'started_at' => now(),
        ]);

        /** @var WorkflowRun $run */
        $run = WorkflowRun::query()->create([
            'id' => (string) Str::ulid(),
            'workflow_instance_id' => $instance->id,
            'run_number' => 1,
            'workflow_class' => TestCommandTargetWorkflow::class,
            'workflow_type' => 'test-command-target-workflow',
            'status' => RunStatus::Waiting->value,
            'payload_codec' => CodecRegistry::defaultCodec(),
            'arguments' => Serializer::serializeWithCodec(CodecRegistry::defaultCodec(), []),
            'connection' => 'redis',
            'queue' => 'default',
            'started_at' => now(),
        ]);

        $instance->forceFill([
            'current_run_id' => $run->id,
        ])->save();

        return $run;
    }

    /**
     * @param array<string, mixed> $attributes
     */
    private function createWorkflowTask(string $instanceId, array $attributes = []): WorkflowTask
    {
        $run = $this->createWaitingRun($instanceId);

        /** @var WorkflowTask $task */
        $task = WorkflowTask::query()->create(array_merge([
            'workflow_run_id' => $run->id,
            'task_type' => TaskType::Workflow->value,
            'status' => TaskStatus::Ready->value,
            'available_at' => now()
                ->subSecond(),
            'payload' => [],
            'connection' => 'redis',
            'queue' => 'default',
        ], $attributes));

        return $task;
    }
}
