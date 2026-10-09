<?php

declare(strict_types=1);

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;
use Workflow\Support\WorkflowMigration;

return new class() extends WorkflowMigration {
    public function up(): void
    {
        Schema::table('workflow_tasks', static function (Blueprint $table): void {
            if (! Schema::hasColumn('workflow_tasks', 'concurrency_key')) {
                $table->string('concurrency_key', 255)
                    ->nullable();
            }

            if (! Schema::hasColumn('workflow_tasks', 'deferred_since')) {
                $table->timestamp('deferred_since', 6)
                    ->nullable();
            }

            if (! Schema::hasColumn('workflow_tasks', 'deferral_deadline_at')) {
                $table->timestamp('deferral_deadline_at', 6)
                    ->nullable()
                    ->index();
            }

            if (! Schema::hasColumn('workflow_tasks', 'deferral_count')) {
                $table->unsignedInteger('deferral_count')
                    ->default(0);
            }
        });

        Schema::table('activity_attempts', static function (Blueprint $table): void {
            if (! Schema::hasColumn('activity_attempts', 'concurrency_key')) {
                $table->string('concurrency_key', 255)
                    ->nullable();
            }

            if (! Schema::hasColumn('activity_attempts', 'concurrency_lock_owner')) {
                $table->string('concurrency_lock_owner', 255)
                    ->nullable();
            }
        });
    }

    public function down(): void
    {
        Schema::table('activity_attempts', static function (Blueprint $table): void {
            foreach (['concurrency_lock_owner', 'concurrency_key'] as $column) {
                if (Schema::hasColumn('activity_attempts', $column)) {
                    $table->dropColumn($column);
                }
            }
        });

        Schema::table('workflow_tasks', static function (Blueprint $table): void {
            if (Schema::hasColumn('workflow_tasks', 'deferral_deadline_at')) {
                $table->dropIndex(['deferral_deadline_at']);
            }

            foreach (['deferral_count', 'deferral_deadline_at', 'deferred_since', 'concurrency_key'] as $column) {
                if (Schema::hasColumn('workflow_tasks', $column)) {
                    $table->dropColumn($column);
                }
            }
        });
    }
};
