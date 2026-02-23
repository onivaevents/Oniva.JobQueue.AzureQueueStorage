<?php

namespace Oniva\JobQueue\AzureQueueStorage\Command;

use Neos\Flow\Annotations as Flow;
use Flowpack\JobQueue\Common\Queue\QueueManager;
use Neos\Flow\Cli\CommandController;
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

    public function retryAllCommand(string $queue, int $delay = 0): void
    {
        $queueInstance = $this->getQueue($queue);

        if ($queueInstance->countFailed() === 0) {
            $this->outputLine('<comment>No messages in poison queue</comment>');
            return;
        }

        $count = $queueInstance->retryAllFailed(['delay' => $delay]);
        $this->outputLine('Retried <success>%d</success> messages from poison queue', [$count]);
    }

    /**
     * Discard all messages from the poison queue
     *
     * @param string $queue Name of the base queue
     */
    public function discardAllCommand(string $queue): void
    {
        $queueInstance = $this->getQueue($queue);

        if ($queueInstance->countFailed() === 0) {
            $this->outputLine('<comment>No messages in poison queue</comment>');
            return;
        }

        $count = $queueInstance->discardAllFailed();
        $this->outputLine('Discarded <success>%d</success> messages from poison queue', [$count]);
    }

    /**
     * Show up to $limit messages from the poison queue without consuming them
     *
     * @param string $queue Name of the base queue
     * @param int $limit Number of messages to peek at
     */
    public function peekCommand(string $queue, int $limit = 5): void
    {
        $queueInstance = $this->getQueue($queue);
        $messages = $queueInstance->peekFailed($limit);

        if (empty($messages)) {
            $this->outputLine('<comment>No messages in poison queue</comment>');
            return;
        }

        $rows = [];
        foreach ($messages as $message) {
            $rows[] = [$message->getIdentifier(), json_encode($message->getPayload())];
        }
        $this->output->outputTable($rows, ['Message ID', 'Payload']);
    }

    private function getQueue(string $queue): RetryableQueueInterface
    {
        $queueInstance = $this->queueManager->getQueue($queue);
        if (!$queueInstance instanceof RetryableQueueInterface) {
            $this->outputLine('<error>Queue "%s" is not a RetryableQueueInterface queue</error>', [$queue]);
            $this->quit(1);
        }
        return $queueInstance;
    }
}
