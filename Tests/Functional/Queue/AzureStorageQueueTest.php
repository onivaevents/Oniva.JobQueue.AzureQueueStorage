<?php

declare(strict_types=1);

namespace Oniva\JobQueue\AzureQueueStorage\Tests\Functional\Queue;

use Flowpack\JobQueue\Common\Exception;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Tests\Functional\AbstractQueueTest;
use Oniva\JobQueue\AzureQueueStorage\Queue\AzureQueueStorage;

class AzureQueueStorageTest extends AbstractQueueTest
{
    /**
     * @throws Exception
     */
    protected function getQueue(): AzureQueueStorage
    {
        return new AzureQueueStorage('test-queue', $this->queueSettings);
    }
}
