<?php

namespace Oniva\JobQueue\AzureQueueStorage\Command;

use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Neos\Flow\Annotations as Flow;
use Flowpack\JobQueue\Common\Queue\QueueManager;
use Neos\Flow\Cli\CommandController;
use Oniva\JobQueue\AzureQueueStorage\Queue\AzureQueueStorageMessage;
use Oniva\JobQueue\AzureQueueStorage\Queue\RetryableQueueInterface;
use Exception;

/**
 * CLI controller to manage Azure Queue Storage message queues
 */
class RetryQueueCommandController extends CommandController
{
    /**
     * @Flow\Inject
     * @var QueueManager
     */
    protected $queueManager;

    /**
     * Show message counts for a queue including reserved and failed messages
     *
     * @param string $queue Name of the base queue
     * @return void
     */
    public function statusCommand(string $queue): void
    {
        $queueInstance = $this->getQueue($queue);

        try {
            $rows = [[
                $queueInstance->getName(),
                $queueInstance->countReady(),
                $queueInstance->countReserved(),
                $queueInstance->countFailed(),
            ]];
        } catch (Exception $e) {
            $rows = [[$queueInstance->getName(), '-', '-', '-']];
        }

        $this->output->outputTable($rows, ['Queue', '# ready', '# reserved', '# failed']);
    }

    /**
     * Retry all messages from the poison queue
     *
     * @param string $queue Name of the base queue
     * @param int $delay Optional delay in seconds before the retried messages become available again
     * @param int $limit Optional limit for the number of messages to retry, if 0 all messages will be retried
     * @return void
     * @throws \Flowpack\JobQueue\Common\Exception
     */
    public function retryAllCommand(string $queue, int $delay = 0, int $limit = 0): void
    {
        $queueInstance = $this->getQueue($queue);

        if ($queueInstance->countFailed() === 0) {
            $this->outputLine('<comment>No messages in poison queue</comment>');
            return;
        }

        $count = $queueInstance->retryAllFailed(['delay' => $delay, 'limit' => $limit]);
        $this->outputLine('Retried <success>%d</success> messages from poison queue', [$count]);
    }

    /**
     * Discard all messages from the poison queue
     *
     * @param string $queue Name of the base queue
     * @param int $limit Optional limit for the number of messages to discard, if 0 all messages will be discarded
      * @return void
      * @throws \Flowpack\JobQueue\Common\Exception
     */
    public function discardAllCommand(string $queue, int $limit = 0): void
    {
        $queueInstance = $this->getQueue($queue);

        if ($queueInstance->countFailed() === 0) {
            $this->outputLine('<comment>No messages in poison queue</comment>');
            return;
        }

        $count = $queueInstance->discardAllFailed(['limit' => $limit]);
        $this->outputLine('Discarded <success>%d</success> messages from poison queue', [$count]);
    }

    /**
     * Peek messages from the poison queue without modifying them
     *
     * @param string $queue Name of the base queue
     * @param int $limit Number of messages to peek at
     * @param int $previewLength Maximum length of the payload preview
     */
    public function peekCommand(string $queue, int $limit = 5, int $previewLength = 100): void
    {
        $queueInstance = $this->getQueue($queue);
        $messages = $queueInstance->peekFailed($limit);

        if (empty($messages)) {
            $this->outputLine('<comment>No messages in poison queue</comment>');
            return;
        }

        $rows = [];
        foreach ($messages as $message) {
            $payload = $message->getPayload();
            $payloadPreview = json_encode($payload);
            if (strlen($payloadPreview) > 100) {
                $payloadPreview = substr($payloadPreview, 0, $previewLength);
            }
            $messageId = $message->getIdentifier();
            $queueMessageId = $message instanceof AzureQueueStorageMessage ? $message->getQueueMessageId() : '-';
            $blobName = $message instanceof AzureQueueStorageMessage ? $message->getBlobName() : '-';
            $numberOfReleases = $message->getNumberOfReleases();
            $rows[] = [$messageId, $queueMessageId, $blobName, $numberOfReleases, $payloadPreview];
        }
        $this->output->outputTable($rows, ['Message ID', 'Queue Message ID', 'Blob Name', 'Releases', 'Payload']);
    }

    private function getQueue(string $queue): RetryableQueueInterface&QueueInterface
    {
        $queueInstance = $this->queueManager->getQueue($queue);
        if (!$queueInstance instanceof RetryableQueueInterface) {
            $this->outputLine('<error>Queue "%s" is not a RetryableQueueInterface queue</error>', [$queue]);
            $this->quit(1);
        }
        return $queueInstance;
    }
}
