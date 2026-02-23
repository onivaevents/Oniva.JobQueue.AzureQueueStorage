<?php

declare(strict_types=1);

namespace Oniva\JobQueue\AzureQueueStorage\Tests\Unit\Queue;

use MicrosoftAzure\Storage\Blob\Internal\IBlob;
use MicrosoftAzure\Storage\Blob\Models\GetBlobResult;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsResult;
use MicrosoftAzure\Storage\Queue\Internal\IQueue;
use MicrosoftAzure\Storage\Queue\Models\CreateMessageResult;
use MicrosoftAzure\Storage\Queue\Models\GetQueueMetadataResult;
use MicrosoftAzure\Storage\Queue\Models\ListMessagesOptions;
use MicrosoftAzure\Storage\Queue\Models\ListMessagesResult;
use MicrosoftAzure\Storage\Queue\Models\PeekMessagesOptions;
use MicrosoftAzure\Storage\Queue\Models\PeekMessagesResult;
use MicrosoftAzure\Storage\Queue\Models\QueueMessage;
use Neos\Flow\Tests\UnitTestCase;
use Oniva\JobQueue\AzureQueueStorage\AzureStorageClientFactory;
use Oniva\JobQueue\AzureQueueStorage\Queue\AzureQueueStorage;
use Flowpack\JobQueue\Common\Exception as JobQueueException;
use Oniva\JobQueue\AzureQueueStorage\Queue\AzureQueueStorageMessage;
use PHPUnit\Framework\MockObject\MockObject;
use Psr\Log\LoggerInterface;
use Exception;
use ReflectionClass;

class AzureStorageQueueTest extends UnitTestCase
{
    protected AzureQueueStorage $obj;
    protected AzureStorageClientFactory&MockObject $clientFactory;
    protected IQueue&MockObject $queueService;
    protected IBlob&MockObject $blobService;
    protected LoggerInterface $logger;

    protected function setUp(): void
    {
        parent::setUp();
        $this->clientFactory = $this->createMock(AzureStorageClientFactory::class);
        $this->queueService = $this->createMock(IQueue::class);
        $this->blobService = $this->createMock(IBlob::class);
        $this->logger = $this->createMock(LoggerInterface::class);
        $this->clientFactory->method('createQueueService')
            ->willReturn($this->queueService);
        $this->clientFactory->method('createBlobService')
            ->willReturn($this->blobService);
        $this->obj = new AzureQueueStorage('test-queue', [
            'connectionString' => 'DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test==',
        ]);
        $this->inject($this->obj, 'clientFactory', $this->clientFactory);
        $this->inject($this->obj, 'systemLogger', $this->logger);
    }

    /**
     * Helper to create a queue instance with injected dependencies
     */
    protected function createQueue(string $name = 'test-queue', array $options = []): AzureQueueStorage
    {
        if (!isset($options['connectionString'])) {
            $options['connectionString'] = 'DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test==';
        }

        $queue = new AzureQueueStorage($name, $options);
        $this->inject($queue, 'clientFactory', $this->clientFactory);
        $this->inject($queue, 'systemLogger', $this->logger);

        return $queue;
    }

    /**
     * @test
     */
    public function constructorValidatesQueueName(): void
    {
        $this->expectException(JobQueueException::class);
        $this->expectExceptionCode(1234567910);
        $this->expectExceptionMessage('Invalid queue name');

        new AzureQueueStorage('Invalid-Queue-Name', [
            'connectionString' => 'test',
        ]);
    }

    /**
     * @test
     */
    public function constructorValidatesPriorityQueueNameLength(): void
    {
        $this->expectException(JobQueueException::class);
        $this->expectExceptionCode(1234567922);
        $this->expectExceptionMessage('Queue name too long for priority queue feature');

        // Queue name that would exceed 63 chars with priority suffix
        $longName = str_repeat('a', 60);
        new AzureQueueStorage($longName, [
            'connectionString' => 'test',
            'usePriorityQueue' => true,
        ]);
    }

    /**
     * @test
     */
    public function constructorValidatesPoisonQueueNameLength(): void
    {
        $this->expectException(JobQueueException::class);
        $this->expectExceptionCode(1234567922);
        $this->expectExceptionMessage('Queue name too long for poison queue feature');

        // Queue name that would exceed 63 chars with poison suffix
        $longName = str_repeat('a', 60);
        new AzureQueueStorage($longName, [
            'connectionString' => 'test',
            'usePoisonQueue' => true,
        ]);
    }

    /**
     * @test
     */
    public function constructorRequiresConnectionString(): void
    {
        $this->expectException(JobQueueException::class);
        $this->expectExceptionCode(1234567890);
        $this->expectExceptionMessage('Azure Storage connection string is required');

        new AzureQueueStorage('test-queue', []);
    }

    /**
     * @test
     */
    public function constructorValidatesPrioritySuffix(): void
    {
        $this->expectException(JobQueueException::class);
        $this->expectExceptionCode(1234567924);
        $this->expectExceptionMessage('Invalid queue suffix');

        new AzureQueueStorage('test-queue', [
            'connectionString' => 'test',
            'prioritySuffix' => 'invalid_suffix', // Should start with hyphen
        ]);
    }

    /**
     * @test
     */
    public function constructorValidatesPoisonSuffix(): void
    {
        $this->expectException(JobQueueException::class);
        $this->expectExceptionCode(1234567924);
        $this->expectExceptionMessage('Invalid queue suffix');

        new AzureQueueStorage('test-queue', [
            'connectionString' => 'test',
            'poisonSuffix' => 'invalid_suffix', // Should start with hyphen
        ]);
    }

    /**
     * @test
     */
    public function getNameReturnsQueueName(): void
    {
        $queue = $this->createQueue('my-queue');
        $this->assertSame('my-queue', $queue->getName());
    }

    /**
     * @test
     */
    public function setUpCreatesQueuesAndContainer(): void
    {
        $queue = $this->createQueue();

        $this->queueService->expects($this->once())
            ->method('createQueue')
            ->with('test-queue');

        $this->blobService->expects($this->once())
            ->method('createContainer')
            ->with('jobqueue-blobs');

        $queue->setUp();
    }

    /**
     * @test
     */
    public function setUpCreatesPriorityQueueWhenEnabled(): void
    {
        $queue = $this->createQueue('test-queue', ['usePriorityQueue' => true]);

        $matcher = $this->exactly(2);
        $this->queueService->expects($matcher)
            ->method('createQueue')
            ->willReturnCallback(function (string $queueName) use ($matcher) {
                match ($matcher->numberOfInvocations()) {
                    1 =>  $this->assertEquals($queueName, 'test-queue'),
                    2 =>  $this->assertEquals($queueName, 'test-queue-priority'),
                };
            });

        $queue->setUp();
    }

    /**
     * @test
     */
    public function setUpIgnoresAlreadyExistsErrors(): void
    {
        $queue = $this->createQueue();

        $exception = new Exception('Queue already exists', 409);

        $this->queueService->expects($this->once())
            ->method('createQueue')
            ->willThrowException($exception);

        $this->blobService->expects($this->once())
            ->method('createContainer')
            ->willThrowException($exception);

        // Should not throw exception for 409 errors
        $queue->setUp();
    }

    /**
     * @test
     */
    public function submitCreatesMessageForSmallPayload(): void
    {
        $queue = $this->createQueue();
        $payload = ['test' => 'data', 'number' => 123];

        // The implementation now wraps the payload with messageId
        $this->queueService->expects($this->once())
            ->method('createMessage')
            ->with(
                'test-queue',
                $this->callback(function ($messageContent) use ($payload) {
                    $decodedContent = json_decode($messageContent, true);

                    // Verify the structure matches what the implementation creates
                    return is_array($decodedContent)
                        && isset($decodedContent['messageId'])
                        && isset($decodedContent['payload'])
                        && $decodedContent['payload'] === $payload
                        && is_string($decodedContent['messageId'])
                        && strpos($decodedContent['messageId'], 'msg_') === 0;
                }),
                $this->isInstanceOf(\MicrosoftAzure\Storage\Queue\Models\CreateMessageOptions::class)
            )
            ->willReturn($this->createMock(CreateMessageResult::class));

        $messageId = $queue->submit($payload);

        $this->assertStringStartsWith('msg_', $messageId);
    }

    /**
     * @test
     */
    public function submitUsesClaimCheckPatternForLargePayload(): void
    {
        $queue = $this->createQueue();

        // Create a large payload that exceeds the threshold
        $largeData = str_repeat('x', 40000); // 40KB
        $payload = ['data' => $largeData];
        $serializedPayload = json_encode($payload);

        $this->blobService->expects($this->once())
            ->method('createBlockBlob')
            ->with(
                'jobqueue-blobs',
                $this->stringContains('queue-test-queue/claim-check/'),
                $serializedPayload
            );

        $this->queueService->expects($this->once())
            ->method('createMessage')
            ->with(
                'test-queue',
                $this->callback(function ($messageContent) {
                    $data = json_decode($messageContent, true);
                    return $data['isClaimCheck'] === true &&
                        isset($data['blobName']) &&
                        isset($data['originalSize']) &&
                        isset($data['messageId']);
                }),
                $this->anything()
            )
            ->willReturn($this->createMock(CreateMessageResult::class));

        $queue->submit($payload);
    }

    /**
     * @test
     */
    public function submitToPriorityQueueWhenEnabled(): void
    {
        $queue = $this->createQueue('test-queue', ['usePriorityQueue' => true]);

        $payload = ['test' => 'data'];

        $this->queueService->expects($this->once())
            ->method('createMessage')
            ->with(
                'test-queue-priority',
                $this->anything(),
                $this->anything()
            )
            ->willReturn($this->createMock(CreateMessageResult::class));

        $queue->submit($payload, ['priority' => true]);
    }

    /**
     * @test
     */
    public function submitWithDelayAndTtl(): void
    {
        $queue = $this->createQueue();

        $this->queueService->expects($this->once())
            ->method('createMessage')
            ->with(
                'test-queue',
                $this->anything(),
                $this->callback(function ($options) {
                    return $options->getTimeToLiveInSeconds() === 3600 &&
                        $options->getVisibilityTimeoutInSeconds() === 60;
                })
            )
            ->willReturn($this->createMock(CreateMessageResult::class));

        $queue->submit(['test' => 'data'], ['ttl' => 3600, 'delay' => 60]);
    }

    /**
     * @test
     */
    public function submitCleansUpBlobOnQueueFailure(): void
    {
        $queue = $this->createQueue();

        $largeData = str_repeat('x', 40000);
        $payload = ['data' => $largeData];

        $this->blobService->expects($this->once())
            ->method('createBlockBlob');

        $exception = new Exception('', 500);
        $this->queueService->expects($this->once())
            ->method('createMessage')
            ->willThrowException($exception);

        $this->blobService->expects($this->once())
            ->method('deleteBlob')
            ->with('jobqueue-blobs', $this->anything());

        $this->expectException(JobQueueException::class);
        $this->expectExceptionCode(1234567894);

        $queue->submit($payload);
    }

    /**
     * @test
     */
    public function waitAndTakeReceivesAndDeletesMessage(): void
    {
        $queue = $this->createQueue();

        $queueMessage = $this->createMockQueueMessage('{"test":"data"}', 'azure-msg-id', 'pop-receipt');

        $listResult = $this->createMock(ListMessagesResult::class);
        $listResult->method('getQueueMessages')->willReturn([$queueMessage]);

        $this->queueService->expects($this->once())
            ->method('listMessages')
            ->with('test-queue', $this->anything())
            ->willReturn($listResult);

        $this->queueService->expects($this->once())
            ->method('deleteMessage')
            ->with('test-queue', 'azure-msg-id', 'pop-receipt');

        $message = $queue->waitAndTake();

        $this->assertInstanceOf(AzureQueueStorageMessage::class, $message);
        $this->assertEquals(['test' => 'data'], $message->getPayload());
    }

    /**
     * @test
     */
    public function waitAndTakeHandlesClaimCheckMessage(): void
    {
        $queue = $this->createQueue();

        $claimCheckData = [
            'isClaimCheck' => true,
            'blobName' => 'test-blob',
            'originalSize' => 50000,
            'messageId' => 'msg_123',
        ];

        $queueMessage = $this->createMockQueueMessage(json_encode($claimCheckData), 'azure-msg-id', 'pop-receipt');

        $listResult = $this->createMock(ListMessagesResult::class);
        $listResult->method('getQueueMessages')->willReturn([$queueMessage]);

        $this->queueService->expects($this->once())
            ->method('listMessages')
            ->willReturn($listResult);

        // Mock blob retrieval
        $blobResult = $this->createMock(GetBlobResult::class);
        $stream = fopen('php://memory', 'r+');
        fwrite($stream, '{"actual":"payload"}');
        rewind($stream);
        $blobResult->method('getContentStream')->willReturn($stream);

        $this->blobService->expects($this->once())
            ->method('getBlob')
            ->with('jobqueue-blobs', 'test-blob')
            ->willReturn($blobResult);

        $this->queueService->expects($this->once())
            ->method('deleteMessage');

        $this->blobService->expects($this->once())
            ->method('deleteBlob')
            ->with('jobqueue-blobs', 'test-blob');

        $message = $queue->waitAndTake();

        $this->assertEquals(['actual' => 'payload'], $message->getPayload());
    }

    /**
     * @test
     */
    public function waitAndTakeLogsWarningOnBlobDeleteFailure(): void
    {
        $queue = $this->createQueue();

        $claimCheckData = [
            'isClaimCheck' => true,
            'blobName' => 'test-blob',
            'messageId' => 'msg_123',
        ];

        $queueMessage = $this->createMockQueueMessage(json_encode($claimCheckData), 'azure-msg-id', 'pop-receipt');

        $listResult = $this->createMock(ListMessagesResult::class);
        $listResult->method('getQueueMessages')->willReturn([$queueMessage]);

        $this->queueService->method('listMessages')->willReturn($listResult);
        $this->queueService->method('deleteMessage');

        // Mock blob retrieval
        $blobResult = $this->createMock(GetBlobResult::class);
        $stream = fopen('php://memory', 'r+');
        fwrite($stream, '{"test":"data"}');
        rewind($stream);
        $blobResult->method('getContentStream')->willReturn($stream);

        $this->blobService->method('getBlob')->willReturn($blobResult);

        // Blob deletion fails
        $exception = new Exception('Failed to delete blob', 500);
        $this->blobService->expects($this->once())
            ->method('deleteBlob')
            ->willThrowException($exception);

        $this->logger->expects($this->once())
            ->method('warning')
            ->with(
                'Failed to delete blob after message take',
                ['error' => 'Failed to delete blob']
            );

        $message = $queue->waitAndTake();
        $this->assertNotNull($message);
    }

    /**
     * @test
     */
    public function waitAndReserveTracksReservedMessages(): void
    {
        $queue = $this->createQueue();

        $queueMessage = $this->createMockQueueMessage('{"test":"data"}', 'azure-msg-id', 'pop-receipt');

        $listResult = $this->createMock(ListMessagesResult::class);
        $listResult->method('getQueueMessages')->willReturn([$queueMessage]);

        $this->queueService->expects($this->once())
            ->method('listMessages')
            ->willReturn($listResult);

        $message = $queue->waitAndReserve();

        $this->assertInstanceOf(AzureQueueStorageMessage::class, $message);

        // Verify message is tracked internally
        $reflection = new ReflectionClass($queue);
        $property = $reflection->getProperty('reservedMessages');
        $property->setAccessible(true);
        $reservedMessages = $property->getValue($queue);

        $this->assertArrayHasKey($message->getIdentifier(), $reservedMessages);
    }

    /**
     * @test
     */
    public function releaseUpdatesMessageVisibility(): void
    {
        $queue = $this->createQueue();

        // First reserve a message
        $this->setupReservedMessage($queue, 'msg_123');

        $this->queueService->expects($this->once())
            ->method('updateMessage')
            ->with(
                'test-queue',
                'azure-msg-id',
                'pop-receipt',
                '',
                0
            );

        $queue->release('msg_123');
    }

    /**
     * @test
     */
    public function releaseWithDelay(): void
    {
        $queue = $this->createQueue();

        $this->setupReservedMessage($queue, 'msg_123');

        $this->queueService->expects($this->once())
            ->method('updateMessage')
            ->with(
                'test-queue',
                'azure-msg-id',
                'pop-receipt',
                '',
                300 // 5 minute delay
            );

        $queue->release('msg_123', ['delay' => 300]);
    }

    /**
     * @test
     */
    public function releaseThrowsExceptionForUnknownMessage(): void
    {
        $queue = $this->createQueue();

        $this->expectException(JobQueueException::class);
        $this->expectExceptionCode(1234567896);

        $queue->release('unknown-message');
    }

    /**
     * @test
     */
    public function abortDeletesMessageAndBlob(): void
    {
        $queue = $this->createQueue();

        $this->setupReservedMessage($queue, 'msg_123', 'test-blob');

        $this->queueService->expects($this->once())
            ->method('deleteMessage')
            ->with('test-queue', 'azure-msg-id', 'pop-receipt');

        $this->blobService->expects($this->once())
            ->method('deleteBlob')
            ->with('jobqueue-blobs', 'test-blob');

        $queue->abort('msg_123');
    }

    /**
     * @test
     */
    public function abortPushesToPoisonQueueIfEnabled(): void
    {
        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        $this->setupReservedMessage($queue, 'msg_123', 'test-blob');

        // Expect a poison queue message
        $this->queueService->expects($this->once())
            ->method('createMessage')
            ->with(
                'test-queue-poison',
                $this->callback(function ($payload) {
                    $data = json_decode($payload, true);
                    return isset($data['messageId'], $data['queueMessageId'], $data['originalQueue'], $data['timestamp']);
                })
            );

        // Expect original message deletion
        $this->queueService->expects($this->once())
            ->method('deleteMessage')
            ->with('test-queue', 'azure-msg-id', 'pop-receipt');

        // Expect blob deletion
        $this->blobService->expects($this->once())
            ->method('deleteBlob')
            ->with('jobqueue-blobs', 'test-blob');

        $queue->abort('msg_123');
    }

    /**
     * @test
     */
    public function finishDeletesMessageAndCleansBlobIfExists(): void
    {
        $queue = $this->createQueue();

        $this->setupReservedMessage($queue, 'msg_123', 'test-blob');

        $this->queueService->expects($this->once())
            ->method('deleteMessage')
            ->with('test-queue', 'azure-msg-id', 'pop-receipt');

        $this->blobService->expects($this->once())
            ->method('deleteBlob')
            ->with('jobqueue-blobs', 'test-blob');

        $result = $queue->finish('msg_123');

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function finishReturnsFalseForUnknownMessage(): void
    {
        $queue = $this->createQueue();

        $result = $queue->finish('unknown-message');

        $this->assertFalse($result);
    }

    /**
     * @test
     */
    public function peekReturnsMessagesFromBothQueues(): void
    {
        $queue = $this->createQueue('test-queue', ['usePriorityQueue' => true]);

        // Mock priority messages
        $highPriorityMessage = $this->createMock(QueueMessage::class);
        $highPriorityMessage->method('getMessageText')->willReturn('{"priority":"high"}');
        $highPriorityMessage->method('getMessageId')->willReturn('high-msg-id');

        $highPriorityResult = $this->createMock(PeekMessagesResult::class);
        $highPriorityResult->method('getQueueMessages')->willReturn([$highPriorityMessage]);

        // Mock normal priority messages
        $normalMessage = $this->createMock(QueueMessage::class);
        $normalMessage->method('getMessageText')->willReturn('{"priority":"normal"}');
        $normalMessage->method('getMessageId')->willReturn('normal-msg-id');

        $normalResult = $this->createMock(PeekMessagesResult::class);
        $normalResult->method('getQueueMessages')->willReturn([$normalMessage]);

        $this->queueService->expects($this->exactly(2))
            ->method('peekMessages')
            ->willReturnCallback(
                fn (string $queueName, PeekMessagesOptions $options) =>
                match ($queueName) {
                    'test-queue-priority' => $highPriorityResult,
                    'test-queue' => $normalResult,
                }
            );

        $messages = $queue->peek(2);

        $this->assertCount(2, $messages);
        $this->assertEquals(['priority' => 'high'], $messages[0]->getPayload());
        $this->assertEquals(['priority' => 'normal'], $messages[1]->getPayload());
    }

    /**
     * @test
     */
    public function countReadyReturnsApproximateMessageCount(): void
    {
        $queue = $this->createQueue();

        $messages = $this->createMock(PeekMessagesResult::class);
        $messages->method('getQueueMessages')->willReturn(range(1, 32)); // Simulate hitting peek limit

        $metadata = $this->createMock(GetQueueMetadataResult::class);
        $metadata->method('getApproximateMessageCount')->willReturn(42);

        $this->queueService->expects($this->once())
            ->method('peekMessages')
            ->with('test-queue')
            ->willReturn($messages);

        $this->queueService->expects($this->once())
            ->method('getQueueMetadata')
            ->with('test-queue')
            ->willReturn($metadata);

        $count = $queue->countReady();

        $this->assertEquals(42, $count);
    }

    /**
     * @test
     */
    public function countReadySumsBothQueuesWhenPriorityEnabled(): void
    {
        $queue = $this->createQueue('test-queue', ['usePriorityQueue' => true]);

        $normalMessages = $this->createMock(PeekMessagesResult::class);
        $normalMessages->method('getQueueMessages')->willReturn(range(1, 32)); // Simulate hitting peek limit

        $priorityMessages = $this->createMock(PeekMessagesResult::class);
        $priorityMessages->method('getQueueMessages')->willReturn(range(1, 32)); // Simulate hitting peek limit

        $normalMetadata = $this->createMock(GetQueueMetadataResult::class);
        $normalMetadata->method('getApproximateMessageCount')->willReturn(30);

        $priorityMetadata = $this->createMock(GetQueueMetadataResult::class);
        $priorityMetadata->method('getApproximateMessageCount')->willReturn(12);

        $this->queueService->expects($this->exactly(2))
            ->method('peekMessages')
        ->willReturnCallback(
            fn ($queueName) =>
    match($queueName) {
        'test-queue-priority' => $priorityMessages,
        'test-queue' => $normalMessages
    }
        );

        $this->queueService->expects($this->exactly(2))
            ->method('getQueueMetadata')
        ->willReturnCallback(
            fn ($queueName) =>
    match($queueName) {
        'test-queue-priority' => $priorityMetadata,
        'test-queue' => $normalMetadata
    }
        );

        $count = $queue->countReady();

        $this->assertEquals(42, $count);
    }

    /**
     * @test
     */
    public function countReservedReturnsTrackedCount(): void
    {
        $queue = $this->createQueue();

        // Setup some reserved messages
        $this->setupReservedMessage($queue, 'msg_1');
        $this->setupReservedMessage($queue, 'msg_2');
        $this->setupReservedMessage($queue, 'msg_3');

        $this->assertEquals(3, $queue->countReserved());
    }

    /**
     * @test
     */
    public function countFailedReturnsZeroWhenPoisonQueueDisabled(): void
    {
        $queue = $this->createQueue();

        $this->assertEquals(0, $queue->countFailed());
    }

    /**
     * @test
     */
    public function countFailedReturnsCountWhenPoisonQueueEnabled(): void
    {
        $metadata = $this->createMock(GetQueueMetadataResult::class);
        $metadata->method('getApproximateMessageCount')->willReturn(5);

        $this->queueService->expects($this->once())
            ->method('getQueueMetadata')
            ->with('test-queue-poison')
            ->willReturn($metadata);

        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        $this->assertEquals(5, $queue->countFailed());
    }

    /**
     * @test
     */
    public function countFailedReturnsZeroWhenPoisonQueueDoesNotExist(): void
    {
        $this->queueService->expects($this->once())
            ->method('getQueueMetadata')
            ->willThrowException(new Exception('Queue not found', 404));

        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        $this->assertEquals(0, $queue->countFailed());
    }

    /**
     * @test
     */
    public function flushClearsQueuesAndBlobs(): void
    {
        $queue = $this->createQueue();

        $this->queueService->expects($this->once())
            ->method('clearMessages')
            ->with('test-queue');

        $listBlobsResult = $this->createMock(ListBlobsResult::class);
        $listBlobsResult->method('getBlobs')->willReturn([]);

        $this->blobService->expects($this->once())
            ->method('listBlobs')
            ->willReturn($listBlobsResult);

        $queue->flush();
    }

    /**
     * @test
     */
    public function flushClearsBothQueuesWhenPriorityEnabled(): void
    {
        $queue = $this->createQueue('test-queue', ['usePriorityQueue' => true]);

        $matcher = $this->exactly(2);
        $this->queueService->expects($matcher)
            ->method('clearMessages')
        ->willReturnCallback(function (string $queueName) use ($matcher) {
            match ($matcher->numberOfInvocations()) {
                1 =>  $this->assertEquals($queueName, 'test-queue'),
                2 =>  $this->assertEquals($queueName, 'test-queue-priority'),
            };
        });

        $listBlobsResult = $this->createMock(ListBlobsResult::class);
        $listBlobsResult->method('getBlobs')->willReturn([]);

        $this->blobService->expects($this->once())
            ->method('listBlobs')
            ->willReturn($listBlobsResult);

        $queue->flush();
    }

    /**
     * @test
     */
    public function flushClearsPoisonQueueWhenPoisonEnabled(): void
    {
        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        $matcher = $this->exactly(2);
        $this->queueService->expects($matcher)
            ->method('clearMessages')
            ->willReturnCallback(function (string $queueName) use ($matcher) {
                match ($matcher->numberOfInvocations()) {
                    1 =>  $this->assertEquals($queueName, 'test-queue'),
                    2 =>  $this->assertEquals($queueName, 'test-queue-poison'),
                };
            });

        $listBlobsResult = $this->createMock(ListBlobsResult::class);
        $listBlobsResult->method('getBlobs')->willReturn([]);

        $this->blobService->expects($this->once())
            ->method('listBlobs')
            ->willReturn($listBlobsResult);

        $queue->flush();
    }

    /**
     * @test
     */
    public function receiveMessageChecksHighPriorityFirst(): void
    {
        $queue = $this->createQueue('test-queue', ['usePriorityQueue' => true]);

        $highPriorityMessage = $this->createMockQueueMessage('{"priority":"high"}', 'high-id', 'high-receipt');

        $highResult = $this->createMock(ListMessagesResult::class);
        $highResult->method('getQueueMessages')->willReturn([$highPriorityMessage]);

        $emptyResult = $this->createMock(ListMessagesResult::class);
        $emptyResult->method('getQueueMessages')->willReturn([]);

        // Should check high priority queue first
        $this->queueService->expects($this->once())
            ->method('listMessages')
            ->with('test-queue-priority', $this->anything())
            ->willReturn($highResult);

        $message = $queue->waitAndTake();

        $this->assertEquals(['priority' => 'high'], $message->getPayload());
    }

    /**
     * Helper method to create a mock queue message
     */
    protected function createMockQueueMessage(string $messageText, string $messageId, string $popReceipt): QueueMessage&MockObject
    {
        $queueMessage = $this->createMock(QueueMessage::class);
        $queueMessage->method('getMessageText')->willReturn($messageText);
        $queueMessage->method('getMessageId')->willReturn($messageId);
        $queueMessage->method('getPopReceipt')->willReturn($popReceipt);
        $queueMessage->method('getDequeueCount')->willReturn(1);

        return $queueMessage;
    }

    /**
     * @test
     */
    public function dequeueSetsVisibilityTimeout(): void
    {
        $queue = $this->createQueue('test-queue', ['visibilityTimeout' => 120]);

        $listResult = $this->createMock(ListMessagesResult::class);
        $listResult->method('getQueueMessages')->willReturn([]);

        $this->queueService->expects($this->once())
            ->method('listMessages')
            ->with(
                'test-queue',
                $this->callback(function (ListMessagesOptions $options) {
                    return $options->getVisibilityTimeoutInSeconds() === 120;
                })
            )
            ->willReturn($listResult);

        $queue->waitAndReserve(0);
    }

    /**
     * Helper method to setup a reserved message in the queue's internal tracking
     */
    protected function setupReservedMessage(AzureQueueStorage $queue, string $messageId, ?string $blobName = null): void
    {
        $reservedMessages = [
            $messageId => [
                'queueMessageId' => 'azure-msg-id',
                'popReceipt' => 'pop-receipt',
                'blobName' => $blobName,
                'queueName' => 'test-queue',
            ],
        ];

        $reflection = new ReflectionClass($queue);
        $property = $reflection->getProperty('reservedMessages');
        $property->setAccessible(true);
        $existingMessages = $property->getValue($queue);
        $property->setValue($queue, array_merge($existingMessages, $reservedMessages));
    }
}
