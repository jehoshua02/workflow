<?php

declare(strict_types=1);

namespace Tests\Fixtures\V2;

use Illuminate\Support\Facades\DB;
use Workflow\V2\Activity;

final class TestDefaultConnectionWriteActivity extends Activity
{
    public function handle(string $name): string
    {
        DB::table('local_activity_side_effects')->insert([
            'name' => $name,
        ]);

        return $name;
    }
}
