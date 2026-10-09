<?php

declare(strict_types=1);

namespace Tests\Unit\V2;

use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Database\Events\TransactionBeginning;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Str;
use Tests\Fixtures\V2\TestCommandTargetWorkflow;
use Tests\Fixtures\V2\TestConcurrencyKeyActivity;
use Tests\SchemaTestCase;
use Throwable;
use Workflow\Serializers\CodecRegistry;
use Workflow\Serializers\Serializer;
use Workflow\V2\ConcurrencyKey;
use Workflow\V2\Enums\RunStatus;
use Workflow\V2\Enums\TaskStatus;
use Workflow\V2\Enums\TaskType;
use Workflow\V2\Jobs\RunWorkflowTask;
use Workflow\V2\Models\ActivityAttempt;
use Workflow\V2\Models\ActivityExecution;
use Workflow\V2\Models\WorkflowInstance;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowTask;
use Workflow\V2\Support\ActivityTaskClaimer;
use Workflow\V2\Support\StorageConnection;
use Workflow\V2\Support\TaskDispatcher;

final class StorageConnectionTransactionTest extends SchemaTestCase
{
    private string $storageDatabase = '';

    protected function tearDown(): void
    {
        TestConcurrencyKeyActivity::$key = null;
        config()
            ->set('workflows.storage.connection', null);

        parent::tearDown();

        if ($this->storageDatabase !== '' && is_file($this->storageDatabase)) {
            @unlink($this->storageDatabase);
        }
    }

    public function testHelperResolvesTheConfiguredStorageConnection(): void
    {
        $this->assertSame('storage', StorageConnection::name());
        $this->assertSame('storage', StorageConnection::connection()->getName());
        $this->assertSame('storage', WorkflowTask::query()->getModel()->getConnectionName());

        config()
            ->set('workflows.storage.connection', null);

        $this->assertNull(StorageConnection::name());
        $this->assertSame((string) config('database.default'), StorageConnection::connection()->getName());
    }

    public function testClaimLocksTheTaskInsideATransactionOnTheStorageConnection(): void
    {
        $default = (string) config('database.default');
        $this->assertNotSame('storage', $default);

        $taskId = $this->createReadyActivityTask('storage-txn-claim');

        $began = [];
        Event::listen(TransactionBeginning::class, static function (TransactionBeginning $event) use (&$began): void {
            $began[] = $event->connectionName;
        });

        $levelsAtTaskRead = null;
        Event::listen(QueryExecuted::class, static function (QueryExecuted $event) use (
            &$levelsAtTaskRead,
            $default
        ): void {
            if ($levelsAtTaskRead === null && self::isTaskRead($event)) {
                $levelsAtTaskRead = [
                    'storage' => DB::connection('storage')->transactionLevel(),
                    'default' => DB::connection($default)->transactionLevel(),
                ];
            }
        });

        $result = ActivityTaskClaimer::claimDetailed($taskId);

        $this->assertNotNull($result['claim'], (string) $result['reason']);
        $this->assertSame([
            'storage' => 1,
            'default' => 0,
        ], $levelsAtTaskRead);
        $this->assertContains('storage', $began);
        $this->assertNotContains($default, $began);
    }

    /**
     * SQLite has no row locks, so this shows serialization through its file
     * lock: the first claim's open read transaction keeps a second connection
     * from committing a claim of the same task. With the first claim's
     * transaction on the default connection both claims committed.
     */
    public function testConcurrentClaimOnASecondConnectionCannotCommitWhileTheFirstClaimIsOpen(): void
    {
        $taskId = $this->createReadyActivityTask('storage-txn-race');

        $peerOutcome = null;
        Event::listen(QueryExecuted::class, static function (QueryExecuted $event) use (&$peerOutcome, $taskId): void {
            if ($peerOutcome !== null || $event->connectionName !== 'storage' || ! self::isTaskRead($event)) {
                return;
            }

            $peerOutcome = 'pending';
            config()
                ->set('workflows.storage.connection', 'storage_peer');

            try {
                $peer = ActivityTaskClaimer::claimDetailed($taskId, 'peer-worker');
                $peerOutcome = $peer['claim'] === null ? 'refused:' . $peer['reason'] : 'claimed';
            } catch (Throwable $throwable) {
                $peerOutcome = str_contains($throwable->getMessage(), 'database is locked')
                    ? 'blocked'
                    : 'error:' . $throwable->getMessage();
            } finally {
                // A failed SQLite COMMIT leaves the peer's transaction open and its lock held.
                DB::purge('storage_peer');
                config()
                    ->set('workflows.storage.connection', 'storage');
            }
        });

        $first = ActivityTaskClaimer::claimDetailed($taskId, 'first-worker');

        $this->assertSame('blocked', $peerOutcome);
        $this->assertNotNull($first['claim'], (string) $first['reason']);
        $this->assertSame(1, ActivityAttempt::query()->where('workflow_task_id', $taskId)->count());

        /** @var WorkflowTask $task */
        $task = WorkflowTask::query()->findOrFail($taskId);
        $this->assertSame('first-worker', $task->lease_owner);
        $this->assertSame(1, (int) $task->attempt_count);
    }

    public function testTaskDispatchWaitsForTheStorageTransactionToCommit(): void
    {
        Queue::fake();

        $task = $this->createReadyWorkflowTask('storage-txn-dispatch');

        StorageConnection::transaction(static function () use ($task): void {
            TaskDispatcher::dispatch($task);

            Queue::assertNothingPushed();
        });

        Queue::assertPushed(RunWorkflowTask::class, 1);
        $this->assertNotNull($task->fresh()->last_dispatched_at);
    }

    public function testConcurrencyGateEvaluatesTheKeyInsideTheClaimTransactionOnTheStorageConnection(): void
    {
        $default = (string) config('database.default');
        $levelsAtKey = null;
        TestConcurrencyKeyActivity::$key = static function () use (&$levelsAtKey, $default): ConcurrencyKey {
            $levelsAtKey = [
                'storage' => DB::connection('storage')->transactionLevel(),
                'default' => DB::connection($default)->transactionLevel(),
            ];

            return new ConcurrencyKey('storage-gate-resource', 600, 5, 600);
        };

        $taskId = $this->createReadyActivityTask('storage-txn-gate', TestConcurrencyKeyActivity::class);

        $result = ActivityTaskClaimer::claimDetailed($taskId, 'gate-worker');

        $this->assertNotNull($result['claim'], (string) $result['reason']);
        // 1 = the claim transaction, 2 = the gate's savepoint inside it.
        $this->assertSame([
            'storage' => 2,
            'default' => 0,
        ], $levelsAtKey);
        $this->assertSame('storage-gate-resource', $result['claim']->concurrencyKey);
        $this->assertFalse(Cache::lock('storage-gate-resource', 1)->get());
    }

    protected function defineEnvironment($app): void
    {
        $this->storageDatabase = (string) tempnam(sys_get_temp_dir(), 'wf_storage_txn_');

        $connection = [
            'driver' => 'sqlite',
            'database' => $this->storageDatabase,
            'prefix' => '',
            'foreign_key_constraints' => false,
            'busy_timeout' => 200,
        ];

        // Two connection names on one file: a second worker process, as far as SQLite can tell.
        $app['config']->set('database.connections.storage', $connection);
        $app['config']->set('database.connections.storage_peer', $connection);
        $app['config']->set('workflows.storage.connection', 'storage');
    }

    private static function isTaskRead(QueryExecuted $event): bool
    {
        return str_starts_with(strtolower(ltrim($event->sql)), 'select')
            && str_contains($event->sql, 'workflow_tasks');
    }

    private function createReadyActivityTask(
        string $instanceId,
        string $activityClass = 'StorageConnectionActivity',
    ): string {
        $run = $this->createRun($instanceId);

        ActivityExecution::query()->create([
            'id' => $instanceId . '-activity',
            'workflow_run_id' => $run->id,
            'sequence' => 1,
            'activity_class' => $activityClass,
            'activity_type' => 'storage.activity',
            'status' => 'pending',
            'arguments' => serialize([]),
            'attempt_count' => 0,
        ]);

        /** @var WorkflowTask $task */
        $task = WorkflowTask::query()->create([
            'workflow_run_id' => $run->id,
            'task_type' => TaskType::Activity->value,
            'status' => TaskStatus::Ready->value,
            'available_at' => now()
                ->subSecond(),
            'payload' => [
                'activity_execution_id' => $instanceId . '-activity',
            ],
            'connection' => 'redis',
            'queue' => 'default',
        ]);

        return $task->id;
    }

    private function createReadyWorkflowTask(string $instanceId): WorkflowTask
    {
        $run = $this->createRun($instanceId);

        /** @var WorkflowTask $task */
        $task = WorkflowTask::query()->create([
            'workflow_run_id' => $run->id,
            'task_type' => TaskType::Workflow->value,
            'status' => TaskStatus::Ready->value,
            'available_at' => now()
                ->subSecond(),
            'payload' => [],
            'connection' => 'redis',
            'queue' => 'default',
        ]);

        return $task;
    }

    private function createRun(string $instanceId): WorkflowRun
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
}
