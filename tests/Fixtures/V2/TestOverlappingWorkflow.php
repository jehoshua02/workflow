<?php

declare(strict_types=1);

namespace Tests\Fixtures\V2;

use function Workflow\V2\activity;
use Workflow\V2\Support\ActivityOptions;
use Workflow\V2\Workflow;

final class TestOverlappingWorkflow extends Workflow
{
    public function handle(string $resource, string $caller, bool $fail = false): string
    {
        return activity(
            TestOverlappingActivity::class,
            new ActivityOptions(maxAttempts: 1),
            $resource,
            $caller,
            $fail,
        );
    }
}
