<?php

declare(strict_types=1);

namespace Tests\Fixtures\V2;

use Workflow\V2\Attributes\Type;
use function Workflow\V2\localActivity;
use Workflow\V2\Workflow;

#[Type('test-local-default-connection-write-workflow')]
final class TestLocalDefaultConnectionWriteWorkflow extends Workflow
{
    public function handle(string $name): string
    {
        return localActivity(TestDefaultConnectionWriteActivity::class, $name);
    }
}
