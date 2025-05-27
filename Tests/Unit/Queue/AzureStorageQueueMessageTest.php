<?php

declare(strict_types=1);

namespace Oniva\JobQueue\AzureQueueStorage\Tests\Unit\Queue;

use Neos\Flow\Tests\UnitTestCase;
use Oniva\JobQueue\AzureQueueStorage\Queue\AzureQueueStorageMessage;

class AzureQueueStorageMessageTest extends UnitTestCase
{
    protected AzureQueueStorageMessage $obj;

    protected function setUp(): void
    {
        parent::setUp();
        $this->obj = new AzureQueueStorageMessage(
            'id',
            'payload',
            42,
            'queueMessageId',
            'popReceipt',
            'blobName',
            'queueName',
        );
    }

    public function testGetIdReturnsId(): void
    {
        $this->assertSame('id', $this->obj->getIdentifier());
    }

    public function testGetPayloadReturnsPayload(): void
    {
        $this->assertSame('payload', $this->obj->getPayload());
    }

    public function testGetNumberOfReleasesReturnsNumberOfReleases(): void
    {
        $this->assertSame(42, $this->obj->getNumberOfReleases());
    }

    public function testGetQueueMessageIdReturnsQueueMessageId(): void
    {
        $this->assertSame('queueMessageId', $this->obj->getQueueMessageId());
    }

    public function testGetPopReceiptReturnsPopReceipt(): void
    {
        $this->assertSame('popReceipt', $this->obj->getPopReceipt());
    }

    public function testGetBlobNameReturnsBlobName(): void
    {
        $this->assertSame('blobName', $this->obj->getBlobName());
    }

    public function testGetQueueNameReturnsQueueName(): void
    {
        $this->assertSame('queueName', $this->obj->getQueueName());
    }
}
