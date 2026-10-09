<?php

declare(strict_types=1);

namespace Tests\Fixtures\V2;

use function Workflow\V2\activity;
use function Workflow\V2\localActivity;
use Workflow\V2\Support\ActivityOptions;
use Workflow\V2\Workflow;

final class TestConcurrencyKeyWorkflow extends Workflow
{
    /**
     * @var array<string, mixed>
     */
    public static array $options = [
        'maxAttempts' => 1,
    ];

    public function handle(string $arg, string $which = 'probe'): string
    {
        if ($which === 'local') {
            return localActivity(TestConcurrencyKeyActivity::class, $arg);
        }

        return activity(
            $which === 'other' ? TestConcurrencyKeyOtherActivity::class : TestConcurrencyKeyActivity::class,
            new ActivityOptions(...self::$options),
            $arg,
        );
    }
}
