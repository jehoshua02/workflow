<?php

declare(strict_types=1);

namespace Tests\Fixtures\V2;

use Closure;
use RuntimeException;
use Workflow\V2\Activity;
use Workflow\V2\ConcurrencyKey;

class TestConcurrencyKeyActivity extends Activity
{
    /**
     * @var list<string>
     */
    public static array $calls = [];

    /**
     * @var (Closure(self): ?ConcurrencyKey)|null
     */
    public static ?Closure $key = null;

    /**
     * @var (Closure(self): void)|null
     */
    public static ?Closure $during = null;

    public static bool $fail = false;

    public function handle(string $arg): string
    {
        static::$calls[] = static::class . ':' . $arg;

        if (self::$during !== null) {
            (self::$during)($this);
        }

        if (static::$fail) {
            throw new RuntimeException('Probe activity failed.');
        }

        return $arg;
    }

    public function concurrencyKey(): ?ConcurrencyKey
    {
        return self::$key === null ? null : (self::$key)($this);
    }
}
