<?php

declare(strict_types=1);

namespace Tests\Unit\V2;

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
use Tests\Fixtures\V2\TestSignalWorkflow;
use Tests\Fixtures\V2\TestTimerWorkflow;
use Tests\SchemaTestCase;
use Workflow\Serializers\CodecRegistry;
use Workflow\Serializers\Serializer;
use Workflow\V2\Enums\RunStatus;
use Workflow\V2\Enums\TaskStatus;
use Workflow\V2\Enums\TaskType;
use Workflow\V2\Jobs\RunWorkflowTask;
use Workflow\V2\Models\WorkflowInstance;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowTask;
use Workflow\V2\Support\StorageConnection;
use Workflow\V2\Support\TaskDispatcher;
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

    /**
     * @param array<string, mixed> $attributes
     */
    private function createWorkflowTask(string $instanceId, array $attributes = []): WorkflowTask
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
