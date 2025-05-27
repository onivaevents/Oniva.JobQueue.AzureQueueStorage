<?php

declare(strict_types=1);

namespace Oniva\JobQueue\AzureQueueStorage;

use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Internal\IBlob;
use MicrosoftAzure\Storage\Queue\Internal\IQueue;
use MicrosoftAzure\Storage\Queue\QueueRestProxy;
use Neos\Flow\Annotations as Flow;

#[Flow\Scope('singleton')]
class AzureStorageClientFactory
{
    /**
     * Create a Queue service client
     *
     * @param string $connectionString
     * @return IQueue
     */
    public function createQueueService(string $connectionString): IQueue
    {
        return QueueRestProxy::createQueueService($connectionString);
    }

    /**
     * Create a Blob service client
     *
     * @param string $connectionString
     * @return IBlob
     */
    public function createBlobService(string $connectionString): IBlob
    {
        return BlobRestProxy::createBlobService($connectionString);
    }
}
