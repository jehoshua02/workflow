<?php

declare(strict_types=1);

namespace Workflow\V2\Exceptions;

use InvalidArgumentException;
use Workflow\Exceptions\NonRetryableExceptionContract;

/** A concurrency key that breaks the expiry or retry rules. Retrying cannot fix it. */
final class InvalidConcurrencyKeyException extends InvalidArgumentException implements NonRetryableExceptionContract
{
}
