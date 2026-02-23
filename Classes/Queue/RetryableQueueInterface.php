<?php

namespace Oniva\JobQueue\AzureQueueStorage\Queue;

/**
 * Interface for queues that support retryable messages
 */
interface RetryableQueueInterface
{
    /**
     * Requeue all poison messages back to the original queue
     * @param array $options Simple key/value array with options that can be interpreted by the concrete implementation (optional)
     * @return int The number of messages that were retried
     */
    public function retryAllFailed(array $options = []): int;

    /**
     * Discard all poison messages
     * @return int The number of messages that were discarded
     */
    public function discardAllFailed(): int;

    /**
     * Count + peek for inspection
     * @param int $limit Number of messages to peek
     * @return AzureQueueStorageMessage[] Array of failed messages, empty if there are no failed messages
     */
    public function peekFailed(int $limit = 1): array;
}
