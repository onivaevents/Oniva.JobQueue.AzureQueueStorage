# Oniva.JobQueue.AzureQueueStorage

A job queue backend for the [Flowpack.JobQueue.Common](https://github.com/Flowpack/jobqueue-common) package 
based on [Azure Queue Storage](https://learn.microsoft.com/en-us/azure/storage/queues/storage-queues-introduction).

## Features
* Azure Queue Storage Integration - Built on Azure's reliable queue service
* Claim Check Pattern - Automatically stores large job payloads (>32KB) in Azure Blob Storage to bypass queue message size limits
* Priority Queue - Optionally use a separate queue for high-priority jobs that will always be processed first
* Poison Queue - Optionally move failed jobs to a dead-letter queue for debugging and manual intervention
* Configurable Options - Customize timeouts, polling intervals, and more via settings

## Installation
```bash
composer require oniva/jobqueue-azurequeuestorage
```

## Configuration

Add the following to your Flow application's Settings.yaml:

```yaml
Flowpack:
  JobQueue:
    Common:
      queues:
        'my-azure-storage-queue':
          className: 'Oniva\JobQueue\AzureQueueStorage\Queue\AzureQueueStorage'
          options:
            connectionString: DefaultEndpointsProtocol=https;AccountName=myaccountname;AccountKey=myaccountkey;EndpointSuffix=core.windows.net
```

Full example configuration with all options:
```yaml
Flowpack:
  JobQueue:
    Common:
      queues:
        'my-azure-storage-queue':
          className: 'Oniva\JobQueue\AzureQueueStorage\Queue\AzureQueueStorage'
          maximumNumberOfReleases: 3
          executeIsolated: true
          options:
            connectionString: DefaultEndpointsProtocol=https;AccountName=myaccountname;AccountKey=myaccountkey;EndpointSuffix=core.windows.net
            defaultTimeout: 30 # Default polling timeout in seconds (how long to wait for a message)
            visibilityTimeout: 300 # Default visibility timeout in seconds (how long message stays hidden during processing), note that this must exceed the maximum expected job duration
            claimCheckThreshold: 32768 # Message size threshold for claim check pattern (in bytes)
            defaultTtl: 604800 # Default message TTL in seconds (7 days max for Azure Storage Queue)
            pollingInterval: 1000 # Polling interval in milliseconds
            blobContainer: jobqueue-blobs # Blob container name for claim check messages
            usePriorityQueue: true # Enable priority queueing
            usePoisonQueue: true # Enable poison queue for failed jobs
            preservePoisonPayload: true # Preserve the original payload for failed jobs in the poison queue
            prioritySuffix: '-priority' # Suffix for priority queue
            poisonSuffix: '-poison' # Suffix for poison queue
```

## Priority Queueing
To enable priority queueing, set `usePriorityQueue` to `true`. 
This allows you to submit high-priority jobs that will be processed before regular jobs.

```php
$queue->submit($payload, ['priority' => true]);
```

## Poison Queue

To enable the poison queue, set `usePoisonQueue` to `true`. Failed jobs (those that exceed
`maximumNumberOfReleases`) are automatically moved to a dead-letter queue for inspection.

By default, only metadata is stored in the poison queue. To preserve the original payload for
retry or debugging, also set `preservePoisonPayload: true`.

To retry failed jobs, run a worker directly against the poison queue:
```bash
./flow flowpack.jobqueue.common:job:work my-azure-storage-queue-poison
```

Note: jobs being processed from the poison queue will not be re-poisoned on failure — they
are left visible in the queue for manual inspection instead.

## Caveats
* 7-day message limit - Azure Queue Storage automatically deletes messages after 7 days maximum, even if unprocessed
* 64KB queue message size - While the claim check pattern handles larger payloads, it adds latency and blob storage costs
* No native queue priorities - Priority queues are simulated by polling multiple queues, which increases API calls
* Approximate counts only - Queue metrics like countReady() are estimates, not exact counts, due to Azure's distributed nature
* No message ordering guarantee - Azure Queue Storage doesn't guarantee FIFO ordering, messages may be processed out of sequence
* Poison queue retry is manual - Failed jobs are moved to a dead-letter queue but not automatically retried.