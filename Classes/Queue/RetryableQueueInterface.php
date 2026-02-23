<?php

namespace Oniva\JobQueue\AzureQueueStorage\Queue;

/**
 * Interface for queues that support retryable messages
 */
interface RetryableQueueInterface
{
    /** Requeue all poison messages back to the original queue */
    public function retryAllFailed(array $options = []): int;

    /** Discard all poison messages */
    public function discardAllFailed(): int;

    /** Count + peek for inspection */
    public function peekFailed(int $limit = 1): array;
}
