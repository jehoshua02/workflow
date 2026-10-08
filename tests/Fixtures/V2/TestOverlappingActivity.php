<?php

declare(strict_types=1);

namespace Tests\Fixtures\V2;

use Closure;
use Illuminate\Queue\Middleware\WithoutOverlapping;
use RuntimeException;
use Workflow\V2\Activity;

final class TestOverlappingActivity extends Activity
{
    public const RELEASE_AFTER = 5;

    /**
     * @var list<string>
     */
    public static array $calls = [];

    public static ?Closure $during = null;

    /**
     * 'middleware' throws from middleware(); 'handle' throws from the middleware itself.
     */
    public static ?string $middlewareFails = null;

    public function handle(string $resource, string $caller, bool $fail = false): string
    {
        self::$calls[] = $caller;

        if (self::$during !== null) {
            (self::$during)($caller);
        }

        if ($fail) {
            throw new RuntimeException('Pull failed.');
        }

        return $caller;
    }

    public function middleware(): array
    {
        if (self::$middlewareFails === 'middleware') {
            throw new RuntimeException('Lock key could not be built.');
        }

        if (self::$middlewareFails === 'handle') {
            return [
                static function (): never {
                    throw new RuntimeException('Lock store unavailable.');
                },
            ];
        }

        [$resource] = $this->execution->activityArguments();

        return [(new WithoutOverlapping($resource, self::RELEASE_AFTER, 60))->shared()];
    }
}
