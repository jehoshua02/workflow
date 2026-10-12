<?php

declare(strict_types=1);

namespace Tests\Unit\V2;

use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use PDOException;
use PHPUnit\Framework\Attributes\DataProvider;
use Tests\TestCase;
use Throwable;
use Workflow\V2\Jobs\RunTimerTask;
use Workflow\V2\Jobs\RunWorkflowTask;
use Workflow\V2\Support\ActivityTaskClaimer;
use Workflow\V2\Support\DefaultActivityTaskBridge;
use Workflow\V2\Support\DefaultWorkflowTaskBridge;
use Workflow\V2\Support\StorageConnection;
use Workflow\V2\Support\WorkflowExecutor;

final class ClaimLockContentionTest extends TestCase
{
    private ?QueryException $armed = null;

    protected function setUp(): void
    {
        parent::setUp();

        StorageConnection::connection()->beforeExecuting(function (string $query): void {
            if ($this->armed === null || ! str_contains($query, 'workflow_tasks')) {
                return;
            }

            $exception = $this->armed;
            $this->armed = null;

            throw $exception;
        });
    }

    public function testOutermostActivityClaimReturnsTaskLocked(): void
    {
        $this->arm('HY000', 1205, 'Lock wait timeout exceeded; try restarting transaction');

        $result = ActivityTaskClaimer::claimDetailed((string) Str::ulid());

        $this->assertNull($result['claim']);
        $this->assertSame('task_locked', $result['reason']);
        $this->assertNull($result['retry_after_seconds']);
        $this->assertSame(0, StorageConnection::transactionLevel());
    }

    public function testActivityClaimInsideAnOpenStorageTransactionRethrows(): void
    {
        $this->arm('HY000', 1205, 'Lock wait timeout exceeded; try restarting transaction');

        $caught = null;

        StorageConnection::connection()->beginTransaction();

        try {
            ActivityTaskClaimer::claimDetailed((string) Str::ulid());
        } catch (Throwable $throwable) {
            $caught = $throwable;
        } finally {
            while (StorageConnection::transactionLevel() > 0) {
                StorageConnection::connection()->rollBack();
            }
        }

        $this->assertNotNull($caught, 'Contention inside an open transaction must reach the caller.');
        $this->assertSame(1205, $this->driverCodeInChain($caught));
    }

    public function testNonContentionQueryExceptionIsRethrown(): void
    {
        $this->arm('HY000', 1234, 'something else broke');

        try {
            ActivityTaskClaimer::claimDetailed((string) Str::ulid());
            $this->fail('A non-contention error must not be absorbed.');
        } catch (QueryException $exception) {
            $this->assertSame(1234, $exception->errorInfo[1]);
        }

        $this->assertSame(0, StorageConnection::transactionLevel());
    }

    public function testActivityBridgeReportsTaskLockedAsNotClaimable(): void
    {
        $this->arm('40001', 1213, 'Deadlock found when trying to get lock; try restarting transaction');

        $result = $this->app->make(DefaultActivityTaskBridge::class)->claimStatus((string) Str::ulid());

        $this->assertFalse($result['claimed']);
        $this->assertSame('task_not_claimable', $result['reason']);
        $this->assertSame('task_locked', $result['reason_detail']);
    }

    public function testWorkflowBridgeClaimReportsTaskLockedAsNotClaimable(): void
    {
        $this->arm('HY000', 1205, 'Lock wait timeout exceeded; try restarting transaction');

        $result = $this->app->make(DefaultWorkflowTaskBridge::class)->claimStatus((string) Str::ulid());

        $this->assertFalse($result['claimed']);
        $this->assertSame('task_not_claimable', $result['reason']);
        $this->assertSame('task_locked', $result['reason_detail']);
        $this->assertSame(0, StorageConnection::transactionLevel());
    }

    public function testWorkflowBridgeExecuteRefusesALockedClaim(): void
    {
        $this->arm('HY000', 1205, 'Lock wait timeout exceeded; try restarting transaction');

        $result = $this->app->make(DefaultWorkflowTaskBridge::class)->execute((string) Str::ulid());

        $this->assertFalse($result['executed']);
        $this->assertSame('claim_failed', $result['reason']);
    }

    public function testRunWorkflowTaskReturnsWithoutFailingTheJob(): void
    {
        $this->arm('HY000', 1205, 'Lock wait timeout exceeded; try restarting transaction');

        (new RunWorkflowTask((string) Str::ulid()))->handle($this->app->make(WorkflowExecutor::class));

        $this->assertNull($this->armed, 'The claim query should have hit the simulated lock.');
        $this->assertSame(0, StorageConnection::transactionLevel());
    }

    public function testRunTimerTaskReturnsWithoutFailingTheJob(): void
    {
        $this->arm('HY000', 1205, 'Lock wait timeout exceeded; try restarting transaction');

        (new RunTimerTask((string) Str::ulid()))->handle();

        $this->assertNull($this->armed, 'The claim query should have hit the simulated lock.');
        $this->assertSame(0, StorageConnection::transactionLevel());
    }

    /**
     * @return iterable<string, array{string, int, bool}>
     */
    public static function errorInfos(): iterable
    {
        yield 'mysql lock wait timeout' => ['HY000', 1205, true];
        yield 'mysql deadlock' => ['40001', 1213, true];
        yield 'mysql nowait refused' => ['HY000', 3572, true];
        yield 'postgres deadlock' => ['40P01', 7, true];
        yield 'postgres lock not available' => ['55P03', 7, true];
        yield 'mysql duplicate key' => ['23000', 1062, false];
        yield 'mysql gone away' => ['HY000', 2006, false];
        yield 'postgres serialization failure' => ['40001', 7, false];
    }

    #[DataProvider('errorInfos')]
    public function testLockContentionDetection(string $sqlState, int $driverCode, bool $expected): void
    {
        $this->assertSame(
            $expected,
            StorageConnection::isLockContention($this->queryException($sqlState, $driverCode, 'error')),
        );
    }

    private function arm(string $sqlState, int $driverCode, string $message): void
    {
        $this->armed = $this->queryException($sqlState, $driverCode, $message);
    }

    private function queryException(string $sqlState, int $driverCode, string $message): QueryException
    {
        $previous = new PDOException("SQLSTATE[{$sqlState}]: General error: {$driverCode} {$message}");
        $previous->errorInfo = [$sqlState, $driverCode, $message];

        return new QueryException(
            (string) DB::getDefaultConnection(),
            'select * from "workflow_tasks" where "id" = ? limit 1',
            [],
            $previous,
        );
    }

    private function driverCodeInChain(Throwable $throwable): ?int
    {
        for ($current = $throwable; $current !== null; $current = $current->getPrevious()) {
            if ($current instanceof QueryException) {
                return (int) ($current->errorInfo[1] ?? 0);
            }
        }

        return null;
    }
}
