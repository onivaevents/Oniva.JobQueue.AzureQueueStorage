<?php

declare(strict_types=1);

namespace Oniva\JobQueue\AzureQueueStorage\Queue;

use Flowpack\JobQueue\Common\Queue\Message;

/**
 * A message implementation for Azure Queue Storage
 */
class AzureQueueStorageMessage extends Message
{
    /**
     * Azure queue message ID (different from our custom message ID)
     */
    protected string $queueMessageId;

    /**
     * Pop receipt needed for message operations
     */
    protected ?string $popReceipt;

    /**
     * Blob name if this message uses claim check pattern
     */
    protected ?string $blobName;

    /**
     * Queue name where message originated
     */
    protected ?string $queueName;

    public function __construct(
        string $identifier,
        $payload,
        int $numberOfReleases,
        string $queueMessageId,
        ?string $popReceipt = null,
        ?string $blobName = null,
        ?string $queueName = null
    ) {
        parent::__construct($identifier, $payload, $numberOfReleases);
        $this->queueMessageId = $queueMessageId;
        $this->popReceipt = $popReceipt;
        $this->blobName = $blobName;
        $this->queueName = $queueName;
    }

    /**
     * Get Azure queue message ID
     */
    public function getQueueMessageId(): string
    {
        return $this->queueMessageId;
    }

    /**
     * Get pop receipt for message operations
     */
    public function getPopReceipt(): ?string
    {
        return $this->popReceipt;
    }

    /**
     * Get blob name if this message uses claim check pattern
     */
    public function getBlobName(): ?string
    {
        return $this->blobName;
    }

    /**
     * Get queue name where message originated
     */
    public function getQueueName(): ?string
    {
        return $this->queueName;
    }
}
