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
                'Failed to delete blob',
                ['error' => 'Failed to delete blob', 'blobName' => 'test-blob']
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
    public function releaseHandles404Gracefully(): void
    {
        $queue = $this->createQueue();
        $this->setupReservedMessage($queue, 'msg_123');

        $this->queueService->expects($this->once())
            ->method('updateMessage')
            ->willThrowException(new Exception('Not found', 404));

        $this->logger->expects($this->once())
            ->method('warning')
            ->with(
                'Message no longer available during release(), likely taken by another worker',
                ['messageId' => 'msg_123']
            );

        // Should not throw
        $queue->release('msg_123');

        $this->assertEquals(0, $queue->countReserved());
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
    public function abortOnPoisonQueueSkipsDeleteAndRepoison(): void
    {
        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        // Manually set up a reserved message that came from the poison queue
        $reflection = new ReflectionClass($queue);
        $property = $reflection->getProperty('reservedMessages');
        $property->setAccessible(true);
        $property->setValue($queue, [
            'msg_123' => [
                'queueMessageId' => 'azure-msg-id',
                'popReceipt'     => 'pop-receipt',
                'blobName'       => null,
                'queueName'      => 'test-queue-poison',
                'payload'        => ['test' => 'data'],
            ],
        ]);

        // Neither createMessage nor deleteMessage should be called
        $this->queueService->expects($this->never())->method('createMessage');
        $this->queueService->expects($this->never())->method('deleteMessage');

        $queue->abort('msg_123');

        // Message should still be untracked locally after abort
        $this->assertEquals(0, $queue->countReserved());
    }

    /**
     * @test
     */
    public function abortHandles404Gracefully(): void
    {
        $queue = $this->createQueue();
        $this->setupReservedMessage($queue, 'msg_123');

        $this->queueService->expects($this->once())
            ->method('deleteMessage')
            ->willThrowException(new Exception('Not found', 404));

        $this->logger->expects($this->once())
            ->method('warning')
            ->with(
                'Message no longer available during abort(), likely taken by another worker',
                ['messageId' => 'msg_123']
            );

        $queue->abort('msg_123');

        $this->assertEquals(0, $queue->countReserved());
    }

    /**
     * @test
     */
    public function abortPreservesPoisonPayloadInline(): void
    {
        $queue = $this->createQueue('test-queue', [
            'usePoisonQueue'       => true,
            'preservePoisonPayload' => true,
        ]);

        $this->setupReservedMessage($queue, 'msg_123');

        // Manually inject payload into reserved message
        $reflection = new ReflectionClass($queue);
        $property = $reflection->getProperty('reservedMessages');
        $property->setAccessible(true);
        $messages = $property->getValue($queue);
        $messages['msg_123']['payload'] = ['important' => 'data'];
        $property->setValue($queue, $messages);

        $this->queueService->expects($this->once())
            ->method('createMessage')
            ->with(
                'test-queue-poison',
                $this->callback(function ($envelope) {
                    $data = json_decode($envelope, true);
                    return isset($data['payload']) && $data['payload'] === ['important' => 'data'];
                })
            );

        $this->queueService->method('deleteMessage');

        $queue->abort('msg_123');
    }

    /**
     * @test
     */
    public function abortPreservesPoisonPayloadViaClaimCheckForLargePayload(): void
    {
        $queue = $this->createQueue('test-queue', [
            'usePoisonQueue'        => true,
            'preservePoisonPayload' => true,
            'claimCheckThreshold'   => 100,
        ]);

        $this->setupReservedMessage($queue, 'msg_123');

        $reflection = new ReflectionClass($queue);
        $property = $reflection->getProperty('reservedMessages');
        $property->setAccessible(true);
        $messages = $property->getValue($queue);
        $messages['msg_123']['payload'] = ['data' => str_repeat('x', 200)];
        $property->setValue($queue, $messages);

        $this->blobService->expects($this->once())
            ->method('createBlockBlob')
            ->with('jobqueue-blobs', $this->stringContains('poison-msg_123'));

        $this->queueService->expects($this->once())
            ->method('createMessage')
            ->with(
                'test-queue-poison',
                $this->callback(function ($envelope) {
                    $data = json_decode($envelope, true);
                    return ($data['isClaimCheck'] ?? false) === true && isset($data['blobName']);
                })
            );

        $this->queueService->method('deleteMessage');

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
    public function finishReturnsTrueWhenMessageAlreadyDeletedByAnotherWorker(): void
    {
        $queue = $this->createQueue();
        $this->setupReservedMessage($queue, 'msg_123');

        $this->queueService->expects($this->once())
            ->method('deleteMessage')
            ->willThrowException(new Exception('Not found', 404));

        $this->logger->expects($this->once())
            ->method('info')
            ->with('Message already deleted by another worker during finish', ['messageId' => 'msg_123']);

        $result = $queue->finish('msg_123');

        $this->assertTrue($result);
        $this->assertEquals(0, $queue->countReserved());
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
     * @test
     */
    public function peekFailedReturnsEmptyArrayWhenPoisonQueueDisabled(): void
    {
        $queue = $this->createQueue();

        $this->queueService->expects($this->never())->method('peekMessages');

        $result = $queue->peekFailed(5);

        $this->assertSame([], $result);
    }

    /**
     * @test
     */
    public function peekFailedReturnsPoisonMessages(): void
    {
        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        $envelope = [
            'messageId'    => 'msg_123',
            'originalQueue' => 'test-queue',
            'timestamp'    => time(),
        ];
        $queueMessage = $this->createMock(QueueMessage::class);
        $queueMessage->method('getMessageText')->willReturn(json_encode($envelope));
        $queueMessage->method('getMessageId')->willReturn('azure-msg-id');
        $queueMessage->method('getPopReceipt')->willReturn(null);
        $queueMessage->method('getDequeueCount')->willReturn(0);

        $peekResult = $this->createMock(PeekMessagesResult::class);
        $peekResult->method('getQueueMessages')->willReturn([$queueMessage]);

        $this->queueService->expects($this->once())
            ->method('peekMessages')
            ->with('test-queue-poison', $this->callback(function (PeekMessagesOptions $o) {
                return $o->getNumberOfMessages() === 1;
            }))
            ->willReturn($peekResult);

        $messages = $queue->peekFailed(1);

        $this->assertCount(1, $messages);
        $this->assertInstanceOf(AzureQueueStorageMessage::class, $messages[0]);
        // Poison envelope should be the payload
        $payload = $messages[0]->getPayload();
        $this->assertEquals('test-queue', $payload['originalQueue']);
    }

    /**
     * @test
     */
    public function peekFailedRespectsLimit(): void
    {
        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        $peekResult = $this->createMock(PeekMessagesResult::class);
        $peekResult->method('getQueueMessages')->willReturn([]);

        $this->queueService->expects($this->once())
            ->method('peekMessages')
            ->with('test-queue-poison', $this->callback(function (PeekMessagesOptions $o) {
                return $o->getNumberOfMessages() === 10;
            }))
            ->willReturn($peekResult);

        $queue->peekFailed(10);
    }

    /**
     * @test
     */
    public function peekFailedCapsAtPeekLimit(): void
    {
        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        $peekResult = $this->createMock(PeekMessagesResult::class);
        $peekResult->method('getQueueMessages')->willReturn([]);

        $this->queueService->expects($this->once())
            ->method('peekMessages')
            ->with('test-queue-poison', $this->callback(function (PeekMessagesOptions $o) {
                return $o->getNumberOfMessages() === 32; // peekLimit
            }))
            ->willReturn($peekResult);

        $queue->peekFailed(100); // request more than peekLimit
    }

    /**
     * @test
     */
    public function peekFailedReturnsEmptyWhenPoisonQueueDoesNotExist(): void
    {
        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        $this->queueService->expects($this->once())
            ->method('peekMessages')
            ->willThrowException(new Exception('Queue not found', 404));

        $result = $queue->peekFailed(5);

        $this->assertSame([], $result);
    }

    /**
     * @test
     */
    public function peekFailedThrowsOnUnexpectedError(): void
    {
        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        $this->queueService->expects($this->once())
            ->method('peekMessages')
            ->willThrowException(new Exception('Service unavailable', 503));

        $this->expectException(JobQueueException::class);
        $this->expectExceptionCode(1234567930);

        $queue->peekFailed(1);
    }

    /**
     * @test
     */
    public function peekFailedResolvesClaimCheckBlobForPoisonMessage(): void
    {
        $queue = $this->createQueue('test-queue', [
            'usePoisonQueue'        => true,
            'preservePoisonPayload' => true,
        ]);

        $envelope = [
            'messageId'    => 'msg_123',
            'originalQueue' => 'test-queue',
            'timestamp'    => time(),
            'isClaimCheck' => true,
            'blobName'     => 'poison-blob',
        ];
        $queueMessage = $this->createMock(QueueMessage::class);
        $queueMessage->method('getMessageText')->willReturn(json_encode($envelope));
        $queueMessage->method('getMessageId')->willReturn('azure-msg-id');
        $queueMessage->method('getPopReceipt')->willReturn(null);
        $queueMessage->method('getDequeueCount')->willReturn(0);

        $peekResult = $this->createMock(PeekMessagesResult::class);
        $peekResult->method('getQueueMessages')->willReturn([$queueMessage]);

        $this->queueService->expects($this->once())
            ->method('peekMessages')
            ->willReturn($peekResult);

        $stream = fopen('php://memory', 'r+');
        fwrite($stream, json_encode(['original' => 'payload']));
        rewind($stream);

        $blobResult = $this->createMock(GetBlobResult::class);
        $blobResult->method('getContentStream')->willReturn($stream);

        $this->blobService->expects($this->once())
            ->method('getBlob')
            ->with('jobqueue-blobs', 'poison-blob')
            ->willReturn($blobResult);

        $messages = $queue->peekFailed(1);

        $this->assertCount(1, $messages);
        $payload = $messages[0]->getPayload();
        $this->assertEquals(['original' => 'payload'], $payload['payload']);
        $this->assertArrayNotHasKey('isClaimCheck', $payload);
    }

    /**
     * @test
     */
    public function retryAllFailedReturnsZeroWhenPoisonQueueDisabled(): void
    {
        $queue = $this->createQueue();

        $this->queueService->expects($this->never())->method('listMessages');

        $count = $queue->retryAllFailed();

        $this->assertSame(0, $count);
    }

    /**
     * @test
     */
    public function retryAllFailedReturnsZeroWhenPoisonQueueEmpty(): void
    {
        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        $emptyResult = $this->createMock(ListMessagesResult::class);
        $emptyResult->method('getQueueMessages')->willReturn([]);

        $this->queueService->expects($this->once())
            ->method('listMessages')
            ->with('test-queue-poison', $this->anything())
            ->willReturn($emptyResult);

        $count = $queue->retryAllFailed();

        $this->assertSame(0, $count);
    }

    /**
     * @test
     */
    public function retryAllFailedRequeuesMessageToOriginalQueue(): void
    {
        $queue = $this->createQueue('test-queue', [
            'usePoisonQueue'        => true,
            'preservePoisonPayload' => true,
        ]);

        $envelope = [
            'messageId'    => 'msg_123',
            'originalQueue' => 'test-queue',
            'timestamp'    => time(),
            'payload'      => ['retry' => 'me'],
        ];

        $queueMessage = $this->createMockQueueMessage(json_encode($envelope), 'azure-msg-id', 'pop-receipt');

        $listResult = $this->createMock(ListMessagesResult::class);
        $emptyResult = $this->createMock(ListMessagesResult::class);
        $emptyResult->method('getQueueMessages')->willReturn([]);

        $this->queueService->expects($this->exactly(2))
            ->method('listMessages')
            ->willReturnOnConsecutiveCalls($listResult, $emptyResult);

        $listResult->method('getQueueMessages')->willReturn([$queueMessage]);

        // Should requeue to original queue
        $this->queueService->expects($this->once())
            ->method('createMessage')
            ->with(
                'test-queue',
                $this->callback(function ($content) {
                    $data = json_decode($content, true);
                    return isset($data['messageId'])
                        && isset($data['payload'])
                        && $data['payload'] === ['retry' => 'me'];
                }),
                $this->anything()
            );

        // Should delete from poison queue
        $this->queueService->expects($this->once())
            ->method('deleteMessage')
            ->with('test-queue-poison', 'azure-msg-id', 'pop-receipt');

        $count = $queue->retryAllFailed();

        $this->assertSame(1, $count);
    }

    /**
     * @test
     */
    public function retryAllFailedRequeuesMultipleMessages(): void
    {
        $queue = $this->createQueue('test-queue', [
            'usePoisonQueue'        => true,
            'preservePoisonPayload' => true,
        ]);

        $makeEnvelope = fn (int $i) => json_encode([
            'messageId'    => "msg_$i",
            'originalQueue' => 'test-queue',
            'timestamp'    => time(),
            'payload'      => ['index' => $i],
        ]);

        $msg1 = $this->createMockQueueMessage($makeEnvelope(1), 'azure-id-1', 'receipt-1');
        $msg2 = $this->createMockQueueMessage($makeEnvelope(2), 'azure-id-2', 'receipt-2');

        $result1 = $this->createMock(ListMessagesResult::class);
        $result1->method('getQueueMessages')->willReturn([$msg1]);

        $result2 = $this->createMock(ListMessagesResult::class);
        $result2->method('getQueueMessages')->willReturn([$msg2]);

        $emptyResult = $this->createMock(ListMessagesResult::class);
        $emptyResult->method('getQueueMessages')->willReturn([]);

        $this->queueService->expects($this->exactly(3))
            ->method('listMessages')
            ->willReturnOnConsecutiveCalls($result1, $result2, $emptyResult);

        $this->queueService->expects($this->exactly(2))->method('createMessage');
        $this->queueService->expects($this->exactly(2))->method('deleteMessage');

        $count = $queue->retryAllFailed();

        $this->assertSame(2, $count);
    }

    /**
     * @test
     */
    public function retryAllFailedReturnsZeroWhenPoisonQueueDoesNotExist(): void
    {
        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        $this->queueService->expects($this->once())
            ->method('listMessages')
            ->willThrowException(new Exception('Queue not found', 404));

        $count = $queue->retryAllFailed();

        $this->assertSame(0, $count);
    }

    /**
     * @test
     */
    public function retryAllFailedThrowsAndReleasesOnRequeueError(): void
    {
        $queue = $this->createQueue('test-queue', [
            'usePoisonQueue'        => true,
            'preservePoisonPayload' => true,
        ]);

        $envelope = [
            'messageId'     => 'msg_123',
            'originalQueue' => 'test-queue',
            'timestamp'     => time(),
            'payload'       => ['data' => 'x'],
        ];

        $queueMessage = $this->createMockQueueMessage(json_encode($envelope), 'azure-msg-id', 'pop-receipt');

        $listResult = $this->createMock(ListMessagesResult::class);
        $listResult->method('getQueueMessages')->willReturn([$queueMessage]);

        $this->queueService->expects($this->once())
            ->method('listMessages')
            ->willReturn($listResult);

        // createMessage (requeue) fails
        $this->queueService->expects($this->once())
            ->method('createMessage')
            ->willThrowException(new Exception('Service error', 500));

        // Should try to release the poison message back
        $this->queueService->expects($this->once())
            ->method('updateMessage')
            ->with('test-queue-poison', 'azure-msg-id', 'pop-receipt', '', 0);

        $this->expectException(JobQueueException::class);
        $this->expectExceptionCode(1234567932);

        $queue->retryAllFailed();
    }

    /**
     * @test
     */
    public function retryAllFailedLogsErrorWhenReleaseAlsoFails(): void
    {
        $queue = $this->createQueue('test-queue', [
            'usePoisonQueue'        => true,
            'preservePoisonPayload' => true,
        ]);

        $envelope = [
            'messageId'     => 'msg_123',
            'originalQueue' => 'test-queue',
            'timestamp'     => time(),
            'payload'       => ['data' => 'x'],
        ];

        $queueMessage = $this->createMockQueueMessage(json_encode($envelope), 'azure-msg-id', 'pop-receipt');

        $listResult = $this->createMock(ListMessagesResult::class);
        $listResult->method('getQueueMessages')->willReturn([$queueMessage]);

        $this->queueService->method('listMessages')->willReturn($listResult);
        $this->queueService->method('createMessage')
            ->willThrowException(new Exception('Service error', 500));
        $this->queueService->method('updateMessage')
            ->willThrowException(new Exception('Also failed', 500));

        $this->logger->expects($this->once())
            ->method('error')
            ->with('Failed to release poison message after retry error', $this->arrayHasKey('messageId'));

        $this->expectException(JobQueueException::class);

        $queue->retryAllFailed();
    }

    /**
     * @test
     */
    public function retryAllFailedWithDelayPassesDelayOption(): void
    {
        $queue = $this->createQueue('test-queue', [
            'usePoisonQueue'        => true,
            'preservePoisonPayload' => true,
        ]);

        $envelope = [
            'messageId'     => 'msg_123',
            'originalQueue' => 'test-queue',
            'timestamp'     => time(),
            'payload'       => ['data' => 'x'],
        ];

        $queueMessage = $this->createMockQueueMessage(json_encode($envelope), 'azure-msg-id', 'pop-receipt');

        $listResult = $this->createMock(ListMessagesResult::class);
        $emptyResult = $this->createMock(ListMessagesResult::class);
        $listResult->method('getQueueMessages')->willReturn([$queueMessage]);
        $emptyResult->method('getQueueMessages')->willReturn([]);

        $this->queueService->expects($this->exactly(2))
            ->method('listMessages')
            ->willReturnOnConsecutiveCalls($listResult, $emptyResult);

        $this->queueService->expects($this->once())
            ->method('createMessage')
            ->with(
                'test-queue',
                $this->anything(),
                $this->callback(function ($options) {
                    return $options->getVisibilityTimeoutInSeconds() === 60;
                })
            );

        $this->queueService->method('deleteMessage');

        $queue->retryAllFailed(['delay' => 60]);
    }

    /**
     * @test
     */
    public function discardAllFailedReturnsZeroWhenPoisonQueueDisabled(): void
    {
        $queue = $this->createQueue();

        $this->queueService->expects($this->never())->method('listMessages');

        $count = $queue->discardAllFailed();

        $this->assertSame(0, $count);
    }

    /**
     * @test
     */
    public function discardAllFailedReturnsZeroWhenPoisonQueueEmpty(): void
    {
        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        $emptyResult = $this->createMock(ListMessagesResult::class);
        $emptyResult->method('getQueueMessages')->willReturn([]);

        $this->queueService->expects($this->once())
            ->method('listMessages')
            ->with('test-queue-poison', $this->anything())
            ->willReturn($emptyResult);

        $count = $queue->discardAllFailed();

        $this->assertSame(0, $count);
    }

    /**
     * @test
     */
    public function discardAllFailedDeletesMessagesAndBlobs(): void
    {
        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        $envelope = json_encode([
            'messageId'    => 'msg_123',
            'originalQueue' => 'test-queue',
            'timestamp'    => time(),
            'isClaimCheck' => true,
            'blobName'     => 'poison-blob',
        ]);

        $queueMessage = $this->createMockQueueMessage($envelope, 'azure-msg-id', 'pop-receipt');

        $listResult = $this->createMock(ListMessagesResult::class);
        $emptyResult = $this->createMock(ListMessagesResult::class);
        $listResult->method('getQueueMessages')->willReturn([$queueMessage]);
        $emptyResult->method('getQueueMessages')->willReturn([]);

        $this->queueService->expects($this->exactly(2))
            ->method('listMessages')
            ->willReturnOnConsecutiveCalls($listResult, $emptyResult);

        $this->blobService->expects($this->once())
            ->method('deleteBlob')
            ->with('jobqueue-blobs', 'poison-blob');

        $this->queueService->expects($this->once())
            ->method('deleteMessage')
            ->with('test-queue-poison', 'azure-msg-id', 'pop-receipt');

        $count = $queue->discardAllFailed();

        $this->assertSame(1, $count);
    }

    /**
     * @test
     */
    public function discardAllFailedDeletesMultipleMessages(): void
    {
        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        $makeMsg = fn (int $i) => $this->createMockQueueMessage(
            json_encode(['messageId' => "msg_$i", 'originalQueue' => 'test-queue', 'timestamp' => time()]),
            "azure-id-$i",
            "receipt-$i"
        );

        $result1 = $this->createMock(ListMessagesResult::class);
        $result1->method('getQueueMessages')->willReturn([$makeMsg(1)]);

        $result2 = $this->createMock(ListMessagesResult::class);
        $result2->method('getQueueMessages')->willReturn([$makeMsg(2)]);

        $emptyResult = $this->createMock(ListMessagesResult::class);
        $emptyResult->method('getQueueMessages')->willReturn([]);

        $this->queueService->expects($this->exactly(3))
            ->method('listMessages')
            ->willReturnOnConsecutiveCalls($result1, $result2, $emptyResult);

        $this->queueService->expects($this->exactly(2))->method('deleteMessage');

        $count = $queue->discardAllFailed();

        $this->assertSame(2, $count);
    }

    /**
     * @test
     */
    public function discardAllFailedSkipsBlobDeletionWhenNoBlobName(): void
    {
        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        $envelope = json_encode([
            'messageId'    => 'msg_123',
            'originalQueue' => 'test-queue',
            'timestamp'    => time(),
        ]);

        $queueMessage = $this->createMockQueueMessage($envelope, 'azure-msg-id', 'pop-receipt');

        $listResult = $this->createMock(ListMessagesResult::class);
        $emptyResult = $this->createMock(ListMessagesResult::class);
        $listResult->method('getQueueMessages')->willReturn([$queueMessage]);
        $emptyResult->method('getQueueMessages')->willReturn([]);

        $this->queueService->expects($this->exactly(2))
            ->method('listMessages')
            ->willReturnOnConsecutiveCalls($listResult, $emptyResult);

        $this->blobService->expects($this->never())->method('deleteBlob');
        $this->queueService->expects($this->once())->method('deleteMessage');

        $count = $queue->discardAllFailed();

        $this->assertSame(1, $count);
    }

    /**
     * @test
     */
    public function discardAllFailedReturnsZeroWhenPoisonQueueDoesNotExist(): void
    {
        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        $this->queueService->expects($this->once())
            ->method('listMessages')
            ->willThrowException(new Exception('Queue not found', 404));

        $count = $queue->discardAllFailed();

        $this->assertSame(0, $count);
    }

    /**
     * @test
     */
    public function discardAllFailedThrowsOnUnexpectedListError(): void
    {
        $queue = $this->createQueue('test-queue', ['usePoisonQueue' => true]);

        $this->queueService->expects($this->once())
            ->method('listMessages')
            ->willThrowException(new Exception('Service unavailable', 503));

        $this->expectException(JobQueueException::class);
        $this->expectExceptionCode(1234567933);

        $queue->discardAllFailed();
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
                'payload' => ['test' => 'data'],
            ],
        ];

        $reflection = new ReflectionClass($queue);
        $property = $reflection->getProperty('reservedMessages');
        $property->setAccessible(true);
        $existingMessages = $property->getValue($queue);
        $property->setValue($queue, array_merge($existingMessages, $reservedMessages));
    }
}
