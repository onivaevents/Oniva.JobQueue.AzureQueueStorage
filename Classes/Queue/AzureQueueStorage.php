<?php

declare(strict_types=1);

namespace Oniva\JobQueue\AzureQueueStorage\Queue;

use Neos\Flow\Annotations as Flow;
use Flowpack\JobQueue\Common\Exception as JobQueueException;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use MicrosoftAzure\Storage\Blob\Internal\IBlob;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use MicrosoftAzure\Storage\Queue\Internal\IQueue;
use MicrosoftAzure\Storage\Queue\Models\PeekMessagesOptions;
use MicrosoftAzure\Storage\Queue\Models\QueueMessage;
use MicrosoftAzure\Storage\Queue\Models\CreateMessageOptions;
use MicrosoftAzure\Storage\Queue\Models\ListMessagesOptions;
use Exception;
use Oniva\JobQueue\AzureQueueStorage\AzureStorageClientFactory;
use Psr\Log\LoggerInterface;

/**
 * A queue implementation using Azure Storage Queue as the queue backend
 * with claim check pattern for large messages
 */
class AzureQueueStorage implements QueueInterface
{
    protected string $name;

    protected string $connectionString;

    protected string $normalPriorityQueueName;

    protected string $priorityQueueName;

    protected string $poisonQueueName;

    protected ?IQueue $queueService = null;

    protected ?IBlob $blobService = null;

    protected string $containerName;

    protected int $defaultTimeout = 30;

    /**
     * Message size threshold for claim check pattern (in bytes)
     * Azure Storage Queue messages have a max size of 64KB
     */
    protected int $claimCheckThreshold = 32768; // 32KB threshold

    /**
     * Azure's peekMessages limit
     */
    protected int $peekLimit = 32;

    /**
     * Default message TTL in seconds (7 days max for Azure Storage Queue)
     */
    protected int $defaultTtl = 604800; // 7 days

    /**
     * Default queue polling interval in milliseconds
     *
     * https://learn.microsoft.com/en-us/azure/storage/queues/storage-performance-checklist#queue-polling-interval
     */
    protected int $pollingInterval = 1000;

    /**
     * Reserved message tracking for finish/abort operations
     * Maps custom messageId to Azure queue message details
     */
    protected array $reservedMessages = [];

    /**
     * Whether to use priority queue pattern
     */
    protected bool $usePriorityQueue = false;

    /**
     * Default suffix for priority queue
     */
    protected string $prioritySuffix = '-priority';

    /**
     * Whether to use poison queue pattern
     */
    protected bool $usePoisonQueue = false;

    /**
     * Default suffix for poison queue
     */
    protected string $poisonSuffix = '-poison';

    /**
     * @Flow\Inject
     * @var AzureStorageClientFactory
     */
    protected $clientFactory;

    /**
     * @Flow\Inject
     * @var LoggerInterface
     */
    protected $systemLogger;

    /**
     * @param string $name
     * @param array $options
     * @throws JobQueueException
     */
    public function __construct(string $name, array $options = [])
    {
        $this->validateQueueName($name);
        $this->name = $name;

        $this->validateOptions($options);
        $this->connectionString = $options['connectionString'] ?? '';

        // Store other configuration
        $this->defaultTimeout = (int)($options['defaultTimeout'] ?? 30);
        $this->claimCheckThreshold = (int)($options['claimCheckThreshold'] ?? 32768);
        $this->defaultTtl = (int)($options['defaultTtl'] ?? 604800);
        $this->pollingInterval = (int)($options['pollingInterval'] ?? 1000);
        $this->containerName = $options['blobContainer'] ?? 'jobqueue-blobs';

        if (isset($options['usePriorityQueue'])) {
            $this->usePriorityQueue = (bool)$options['usePriorityQueue'];
        }

        if (isset($options['prioritySuffix'])) {
            $this->validateQueueSuffix($options['prioritySuffix']);
            $this->prioritySuffix = $options['prioritySuffix'];
        }

        if (isset($options['usePoisonQueue'])) {
            $this->usePoisonQueue = (bool)$options['usePoisonQueue'];
        }

        if (isset($options['poisonSuffix'])) {
            $this->validateQueueSuffix($options['poisonSuffix']);
            $this->poisonSuffix = $options['poisonSuffix'];
        }

        $this->normalPriorityQueueName = $name;
        $this->priorityQueueName = $name . $this->prioritySuffix;
        $this->poisonQueueName = $name . $this->poisonSuffix;
    }

    /**
     * @inheritdoc
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @inheritdoc
     */
    public function setUp(): void
    {
        try {
            // Always create the normal priority queue
            $this->getQueueService()->createQueue($this->normalPriorityQueueName);

            // Create priority queue only if priority queue feature is enabled
            if ($this->usePriorityQueue) {
                $this->getQueueService()->createQueue($this->priorityQueueName);
            }

            // Create poison queue only if poison queue feature is enabled
            if ($this->usePoisonQueue) {
                $this->getQueueService()->createQueue($this->poisonQueueName);
            }
        } catch (Exception $e) {
            if ($e->getCode() !== 409) { // 409 = Queue already exists
                throw new JobQueueException('Failed to create queue: ' . $e->getMessage(), 1234567892);
            }
        }

        try {
            // Create blob container if it doesn't exist
            $this->getBlobService()->createContainer($this->containerName);
        } catch (Exception $e) {
            if ($e->getCode() !== 409) { // 409 = Container already exists
                throw new JobQueueException('Failed to create blob container: ' . $e->getMessage(), 1234567893);
            }
        }
    }

    /**
     * @inheritdoc
     */
    public function submit($payload, array $options = []): string
    {
        $serializedPayload = json_encode($payload);
        $messageId = uniqid('msg_', true);

        $ttl = isset($options['ttl']) ? (int)$options['ttl'] : $this->defaultTtl;
        $delay = isset($options['delay']) ? (int)$options['delay'] : 0;
        $hasPriority = $this->usePriorityQueue && isset($options['priority']) && $options['priority'] === true;

        // Determine which queue to use based on priority (only if priority queue is enabled)
        $queueName = $hasPriority ? $this->priorityQueueName : $this->normalPriorityQueueName;

        try {
            $messageContent = $serializedPayload;
            $isClaimCheck = false;

            // Check if message exceeds threshold - use claim check pattern
            if (strlen($serializedPayload) > $this->claimCheckThreshold) {
                $blobName = $this->generateBlobName($messageId);

                // Store payload in blob storage
                $this->getBlobService()->createBlockBlob(
                    $this->containerName,
                    $blobName,
                    $serializedPayload
                );

                // Create claim check message
                $claimCheckData = [
                    'isClaimCheck' => true,
                    'blobName' => $blobName,
                    'originalSize' => strlen($serializedPayload),
                    'messageId' => $messageId,
                ];
                $messageContent = json_encode($claimCheckData);
                $isClaimCheck = true;
            } else {
                // For regular messages, wrap the payload with messageId
                $messageContent = json_encode([
                    'messageId' => $messageId,
                    'payload' => $payload,
                ]);
            }

            // Send message to queue
            $createMessageOptions = new CreateMessageOptions();
            $createMessageOptions->setTimeToLiveInSeconds($ttl);

            if ($delay > 0) {
                $createMessageOptions->setVisibilityTimeoutInSeconds($delay);
            }

            $this->getQueueService()->createMessage(
                $queueName,
                $messageContent,
                $createMessageOptions
            );

            return $messageId;

        } catch (Exception $e) {
            // Clean up blob if it was created but queue message failed
            if ($isClaimCheck && isset($blobName)) {
                try {
                    $this->getBlobService()->deleteBlob($this->containerName, $blobName);
                } catch (Exception $cleanupException) {
                    // Log cleanup failure but don't throw
                    $this->systemLogger->error('Failed to clean up blob after message submission failure', [
                        'blobName' => $blobName,
                        'error' => $cleanupException->getMessage(),
                    ]);
                }
            }
            throw new JobQueueException('Failed to submit message: ' . $e->getMessage(), 1234567894);
        }
    }

    /**
     * @inheritdoc
     */
    public function waitAndTake(?int $timeout = null): ?Message
    {
        $message = $this->receiveMessage($timeout);
        if ($message === null) {
            return null;
        }

        try {
            // Delete the message immediately (take pattern)
            $this->getQueueService()->deleteMessage(
                $message->getQueueName(),
                $message->getQueueMessageId(),
                $message->getPopReceipt()
            );

            // Clean up blob if it exists
            if ($message->getBlobName()) {
                try {
                    $this->getBlobService()->deleteBlob($this->containerName, $message->getBlobName());
                } catch (Exception $e) {
                    $this->systemLogger->warning('Failed to delete blob after message take', ['error' => $e->getMessage()]);
                }
            }

            return $message;
        } catch (Exception $e) {
            throw new JobQueueException('Failed to delete message: ' . $e->getMessage(), 1234567895);
        }
    }

    /**
     * @inheritdoc
     */
    public function waitAndReserve(?int $timeout = null): ?Message
    {
        $message = $this->receiveMessage($timeout);
        if ($message === null) {
            return null;
        }

        // Track reserved messages using the custom messageId
        $this->reservedMessages[$message->getIdentifier()] = [
            'queueMessageId' => $message->getQueueMessageId(),
            'popReceipt' => $message->getPopReceipt(),
            'blobName' => $message->getBlobName(),
            'queueName' => $message->getQueueName(),
        ];

        return $message;
    }

    /**
     * @inheritdoc
     */
    public function release(string $messageId, array $options = []): void
    {
        if (!isset($this->reservedMessages[$messageId])) {
            throw new JobQueueException("Message with ID {$messageId} is not reserved or has expired", 1234567896);
        }

        $messageInfo = $this->reservedMessages[$messageId];
        $delay = isset($options['delay']) ? (int)$options['delay'] : 0;

        try {
            // Update message visibility to make it available again
            $this->getQueueService()->updateMessage(
                $messageInfo['queueName'],
                $messageInfo['queueMessageId'],
                $messageInfo['popReceipt'],
                '', // Keep original content
                $delay
            );

            unset($this->reservedMessages[$messageId]);
        } catch (Exception $e) {
            throw new JobQueueException('Failed to release message: ' . $e->getMessage(), 1234567897);
        }
    }

    /**
     * @inheritdoc
     */
    public function abort(string $messageId): void
    {
        if (!isset($this->reservedMessages[$messageId])) {
            throw new JobQueueException("Message with ID {$messageId} is not reserved or has expired", 1234567898);
        }

        $messageInfo = $this->reservedMessages[$messageId];

        try {
            // Push a failed record to the poison queue
            if ($this->usePoisonQueue) {
                $failedPayload = json_encode([
                    'messageId' => $messageId,
                    'queueMessageId' => $messageInfo['queueMessageId'],
                    'originalQueue' => $messageInfo['queueName'],
                    'blobName' => $messageInfo['blobName'] ?? null,
                    'timestamp' => time(),
                ]);
                $this->getQueueService()->createMessage($this->poisonQueueName, $failedPayload);
            }

            // Delete the message from original queue
            $this->getQueueService()->deleteMessage(
                $messageInfo['queueName'],
                $messageInfo['queueMessageId'],
                $messageInfo['popReceipt']
            );

            // Clean up blob if it exists
            if (!empty($messageInfo['blobName'])) {
                try {
                    $this->getBlobService()->deleteBlob($this->containerName, $messageInfo['blobName']);
                } catch (Exception $e) {
                    // Log but don't throw - message is already aborted
                }
            }

            unset($this->reservedMessages[$messageId]);
        } catch (Exception $e) {
            throw new JobQueueException('Failed to abort message: ' . $e->getMessage(), 1234567899);
        }
    }

    /**
     * @inheritdoc
     */
    public function finish(string $messageId): bool
    {
        if (!isset($this->reservedMessages[$messageId])) {
            // Message might have been finished already or expired
            return false;
        }

        $messageInfo = $this->reservedMessages[$messageId];

        try {
            // Delete the message from queue
            $this->getQueueService()->deleteMessage(
                $messageInfo['queueName'],
                $messageInfo['queueMessageId'],
                $messageInfo['popReceipt']
            );

            // Clean up blob if it exists
            if (!empty($messageInfo['blobName'])) {
                try {
                    $this->getBlobService()->deleteBlob($this->containerName, $messageInfo['blobName']);
                } catch (Exception $e) {
                    // Log but don't throw - message is already finished
                }
            }

            unset($this->reservedMessages[$messageId]);
            return true;
        } catch (Exception $e) {
            throw new JobQueueException('Failed to finish message: ' . $e->getMessage(), 1234567900);
        }
    }

    /**
     * @inheritdoc
     */
    public function peek(int $limit = 1): array
    {
        $allMessages = [];
        $remainingLimit = $limit;

        // Peek priority queue first if enabled
        if ($this->usePriorityQueue && $remainingLimit > 0) {
            try {
                $options = new PeekMessagesOptions();
                $options->setNumberOfMessages(min($remainingLimit, $this->peekLimit));
                $result = $this->getQueueService()->peekMessages($this->priorityQueueName, $options);

                foreach ($result->getQueueMessages() as $queueMessage) {
                    $allMessages[] = $this->createMessageFromQueueMessage($queueMessage);
                    $remainingLimit--;
                    if ($remainingLimit <= 0) {
                        break;
                    }
                }
            } catch (Exception $e) {
                // Ignore if queue doesn't exist
            }
        }

        // Peek normal priority queue
        if ($remainingLimit > 0) {
            $options = new PeekMessagesOptions();
            $options->setNumberOfMessages(min($remainingLimit, $this->peekLimit));
            $result = $this->getQueueService()->peekMessages($this->normalPriorityQueueName, $options);

            foreach ($result->getQueueMessages() as $queueMessage) {
                $allMessages[] = $this->createMessageFromQueueMessage($queueMessage);
            }
        }

        return $allMessages;
    }

    /**
     * @inheritdoc
     */
    public function countReady(): int
    {
        $peekMessageOptions = new PeekMessagesOptions();
        $peekMessageOptions->setNumberOfMessages($this->peekLimit);

        try {
            $normalCount = 0;
            $priorityCount = 0;

            try {
                $messages = $this->getQueueService()->peekMessages($this->normalPriorityQueueName, $peekMessageOptions);
                $normalCount = count($messages->getQueueMessages());
                // If we hit the peek limit, fall back to approximate count
                if ($normalCount === $this->peekLimit) {
                    $normalQueueMetadata = $this->getQueueService()->getQueueMetadata($this->normalPriorityQueueName);
                    $normalCount = $normalQueueMetadata->getApproximateMessageCount();
                }

            } catch (Exception $e) {
                // Ignore if queue doesn't exist
            }

            // Only count priority queue if priority queue feature is enabled
            if ($this->usePriorityQueue) {
                try {
                    $messages = $this->getQueueService()->peekMessages($this->priorityQueueName, $peekMessageOptions);
                    $priorityCount = count($messages->getQueueMessages());
                    // If we hit the peek limit, fall back to approximate count
                    if ($priorityCount === $this->peekLimit) {
                        $priorityQueueMetadata = $this->getQueueService()->getQueueMetadata($this->priorityQueueName);
                        $priorityCount = $priorityQueueMetadata->getApproximateMessageCount();
                    }
                } catch (Exception $e) {
                    // Ignore if queue doesn't exist
                }
            }

            return $normalCount + $priorityCount;
        } catch (Exception $e) {
            return 0;
        }
    }

    /**
     * @inheritdoc
     */
    public function countReserved(): int
    {
        // Azure Storage Queue doesn't provide this information directly
        return count($this->reservedMessages);
    }

    /**
     * @inheritdoc
     */
    public function countFailed(): int
    {
        if ($this->usePoisonQueue) {
            try {
                $metadata = $this->getQueueService()->getQueueMetadata($this->poisonQueueName);
                return $metadata->getApproximateMessageCount();
            } catch (Exception $e) {
                if ($e->getCode() === 404) {
                    return 0; // queue doesn't exist yet
                }
                throw new JobQueueException('Failed to get failed message count: ' . $e->getMessage(), 1234567900);
            }
        }

        return 0;
    }

    /**
     * @inheritdoc
     */
    public function flush(): void
    {
        try {
            // Clear all messages from normal priority queue
            $this->getQueueService()->clearMessages($this->normalPriorityQueueName);

            // Clear priority queue only if priority queue feature is enabled
            if ($this->usePriorityQueue) {
                $this->getQueueService()->clearMessages($this->priorityQueueName);
            }

            // Clear poison queue only if priority queue feature is enabled
            if ($this->usePoisonQueue) {
                $this->getQueueService()->clearMessages($this->poisonQueueName);
            }

            // Clear reserved messages tracking
            $this->reservedMessages = [];

            // Optionally clean up all blobs in container
            // Note: This is aggressive and will remove ALL blobs
            $listBlobOptions = new ListBlobsOptions();
            $listBlobOptions->setPrefix(sprintf('queue-%s/', $this->name));
            $blobList = $this->getBlobService()->listBlobs($this->containerName, $listBlobOptions);
            foreach ($blobList->getBlobs() as $blob) {
                try {
                    $this->getBlobService()->deleteBlob($this->containerName, $blob->getName());
                } catch (Exception $e) {
                    // Continue with other blobs
                }
            }
        } catch (Exception $e) {
            throw new JobQueueException('Failed to flush queue: ' . $e->getMessage(), 1234567902);
        }
    }

    protected function getQueueService(): IQueue
    {
        if ($this->queueService === null) {
            $this->queueService = $this->clientFactory->createQueueService($this->connectionString);
        }
        return $this->queueService;
    }

    protected function getBlobService(): IBlob
    {
        if ($this->blobService === null) {
            $this->blobService = $this->clientFactory->createBlobService($this->connectionString);
        }
        return $this->blobService;
    }

    protected function validateQueueName(string $name): void
    {
        if (!preg_match('/^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/', $name)) {
            throw new JobQueueException(
                'Invalid queue name. Must be lowercase, alphanumeric with hyphens, 3-63 characters.',
                1234567910
            );
        }
    }

    protected function validateOptions(array $options): void
    {
        if (empty($options['connectionString'])) {
            throw new JobQueueException('Azure Storage connection string is required', 1234567890);
        }

        if (isset($options['usePriorityQueue']) && $options['usePriorityQueue']) {
            $suffix = $options['prioritySuffix'] ?? $this->prioritySuffix;
            $maxBaseNameLength = 63 - strlen($suffix);

            if (strlen($this->name) > $maxBaseNameLength) {
                throw new JobQueueException(
                    sprintf(
                        'Queue name too long for priority queue feature. Maximum %d characters allowed when using priority queue with suffix "%s".',
                        $maxBaseNameLength,
                        $suffix
                    ),
                    1234567922
                );
            }
        }

        if (isset($options['usePoisonQueue']) && $options['usePoisonQueue']) {
            $suffix = $options['poisonSuffix'] ?? $this->poisonSuffix;
            $maxBaseNameLength = 63 - strlen($suffix);

            if (strlen($this->name) > $maxBaseNameLength) {
                throw new JobQueueException(
                    sprintf(
                        'Queue name too long for poison queue feature. Maximum %d characters allowed when using poison queue with suffix "%s".',
                        $maxBaseNameLength,
                        $suffix
                    ),
                    1234567922
                );
            }
        }
    }

    protected function validateQueueSuffix(string $suffix): void
    {
        if (!preg_match('/^-[a-z0-9]+(-[a-z0-9]+)*$/', $suffix)) {
            throw new JobQueueException(
                'Invalid queue suffix. Must start with hyphen and contain only lowercase alphanumeric characters and hyphens.',
                1234567924
            );
        }
    }

    protected function receiveMessage(?int $timeout = null): ?AzureQueueStorageMessage
    {
        if ($timeout === null) {
            $timeout = $this->defaultTimeout;
        }

        $startTime = microtime(true);

        while ((microtime(true) - $startTime) < $timeout) {
            $message = $this->tryReceiveOnce();
            if ($message !== null) {
                return $message;
            }

            usleep($this->pollingInterval * 1000);
        }

        return null;
    }

    private function tryReceiveOnce(): ?AzureQueueStorageMessage
    {
        try {
            $listMessagesOptions = new ListMessagesOptions();
            $listMessagesOptions->setNumberOfMessages(1);

            /** @var $queueMessages QueueMessage[] */
            $queueMessages = [];
            $queueName = $this->normalPriorityQueueName;

            if ($this->usePriorityQueue) {
                try {
                    $result = $this->getQueueService()->listMessages($this->priorityQueueName, $listMessagesOptions);
                    $queueMessages = $result->getQueueMessages();
                    if (!empty($queueMessages)) {
                        $queueName = $this->priorityQueueName;
                    }
                } catch (Exception $e) {
                    if ($e->getCode() !== 404) {
                        throw $e;
                    }
                }
            }

            if (empty($queueMessages)) {
                $result = $this->getQueueService()->listMessages($this->normalPriorityQueueName, $listMessagesOptions);
                $queueMessages = $result->getQueueMessages();
                $queueName = $this->normalPriorityQueueName;
            }

            if (empty($queueMessages)) {
                return null;
            }

            $queueMessage = $queueMessages[0];
            $messageText = $queueMessage->getMessageText();

            $payload = $this->extractPayload($messageText);
            $messageId = $this->extractMessageId($messageText);
            $blobName = $this->extractBlobName($messageText);

            return new AzureQueueStorageMessage(
                $messageId,
                $payload,
                $queueMessage->getDequeueCount() - 1,
                $queueMessage->getMessageId(),
                $queueMessage->getPopReceipt(),
                $blobName,
                $queueName
            );
        } catch (Exception $e) {
            if ($e->getCode() === 404) {
                return null;
            }
            throw new JobQueueException('Failed to receive message: ' . $e->getMessage(), 1234567903);
        }
    }

    /**
     * Extract payload from message text, handling claim check pattern
     * @throws JobQueueException
     */
    protected function extractPayload(string $messageText)
    {
        $data = json_decode($messageText, true);

        if (is_array($data) && isset($data['isClaimCheck']) && $data['isClaimCheck'] === true) {
            // This is a claim check message - retrieve actual payload from blob
            try {
                $blobResult = $this->getBlobService()->getBlob($this->containerName, $data['blobName']);
                $blobContent = stream_get_contents($blobResult->getContentStream());
                if ($blobContent === false) {
                    throw new JobQueueException('Failed to read blob content', 1234567911);
                }
                $payload = json_decode($blobContent, true);
                if (json_last_error() !== JSON_ERROR_NONE) {
                    throw new JobQueueException('Invalid JSON in blob: ' . json_last_error_msg(), 1234567912);
                }
                return $payload;
            } catch (Exception $e) {
                throw new JobQueueException('Failed to retrieve claim check blob: ' . $e->getMessage(), 1234567904);
            }
        }

        // Check if this is a regular message with messageId wrapper
        if (is_array($data) && isset($data['payload']) && isset($data['messageId'])) {
            return $data['payload'];
        }

        // Fallback - return whatever json_decode produced
        return $data;
    }

    /**
     * Extract message ID from message text
     */
    protected function extractMessageId(string $messageText): string
    {
        $data = json_decode($messageText, true);

        if (is_array($data) && isset($data['messageId'])) {
            return $data['messageId'];
        }

        // Fallback to generating ID if not found
        return uniqid('msg_', true);
    }

    /**
     * Extract blob name from message text (if claim check message)
     */
    protected function extractBlobName(string $messageText): ?string
    {
        $data = json_decode($messageText, true);

        if (is_array($data) && isset($data['isClaimCheck']) && $data['isClaimCheck'] === true) {
            return $data['blobName'];
        }

        return null;
    }

    /**
     * Generate a unique blob name for claim check
     */
    protected function generateBlobName(string $messageId): string
    {
        return sprintf(
            'queue-%s/claim-check/%s-%s.json',
            $this->name,
            $messageId,
            uniqid()
        );
    }

    /**
     * @throws JobQueueException
     */
    protected function createMessageFromQueueMessage(QueueMessage $queueMessage): AzureQueueStorageMessage
    {
        $payload = $this->extractPayload($queueMessage->getMessageText());
        return new AzureQueueStorageMessage(
            $this->extractMessageId($queueMessage->getMessageText()),
            $payload,
            0, // numberOfReleases not available in peek
            $queueMessage->getMessageId(),
            null, // No pop receipt in peek
            $this->extractBlobName($queueMessage->getMessageText())
        );
    }
}
