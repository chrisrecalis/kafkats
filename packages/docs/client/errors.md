# Error Handling

kafkats provides specific error types for different failure scenarios, making it easy to handle errors appropriately.

## Error Hierarchy

```
KafkaError (base: errorCode, retriable)
├── KafkaProtocolError (an error code the broker answered)
├── ConnectionError (network failures)
├── TimeoutError (request timeouts)
├── GroupError (groupId)
│   ├── RebalanceInProgressError
│   ├── UnknownMemberIdError
│   ├── IllegalGenerationError
│   └── CommitFailedError
├── OffsetOutOfRangeError
├── NoOffsetForPartitionError
├── ProducerFencedError
├── InvalidTxnStateError
├── TransactionAbortedError (outcome)
└── Specific errors...
```

## Common Errors

### ConnectionError

Network-level connection failures:

```typescript
import { ConnectionError } from '@kafkats/client'

try {
	await producer.send('events', [{ value: 'payload' }])
} catch (error) {
	if (error instanceof ConnectionError) {
		console.log('Failed to connect:', error.message)
		// Retry or fail gracefully
	}
}
```

### TimeoutError

Request took too long:

```typescript
import { TimeoutError } from '@kafkats/client'

try {
	await consumer.runEach('events', async () => {
		// ...
	})
} catch (error) {
	if (error instanceof TimeoutError) {
		console.log('Request timed out')
		// Increase timeout or retry
	}
}
```

### Produce Rejections

A send that fails rejects with a `KafkaError`. When the broker rejected the records for good, it is a
`KafkaProtocolError` carrying the broker's `errorCode` (`retriable: false`). When the producer stopped retrying, it is a
`KafkaError` with `retriable: true` whose `cause` is the last failure; the records may or may not have been written.

```typescript
import { ErrorCode, KafkaError, KafkaProtocolError } from '@kafkats/client'

try {
	await producer.send('events', { value: hugePayload })
} catch (error) {
	if (error instanceof KafkaProtocolError && error.errorCode === ErrorCode.MessageTooLarge) {
		console.log('Message too large:', error.message)
		// Split the message or increase broker limits
	} else if (error instanceof KafkaError && error.retriable) {
		console.log('Retries exhausted:', error.cause)
		// Message may or may not have been delivered
	}
}
```

## Broker Errors

### LeaderNotAvailableError

Partition leader is unavailable:

```typescript
import { LeaderNotAvailableError } from '@kafkats/client'

// Usually retriable - kafkats handles this automatically
```

### CoordinatorNotAvailableError

Group coordinator is unavailable:

```typescript
import { CoordinatorNotAvailableError } from '@kafkats/client'

// Consumer will retry finding the coordinator
```

### UnknownTopicOrPartitionError

Topic doesn't exist:

```typescript
import { UnknownTopicOrPartitionError } from '@kafkats/client'

try {
	await producer.send('nonexistent', { value: 'data' })
} catch (error) {
	if (error instanceof UnknownTopicOrPartitionError) {
		console.log('Topic not found:', error.message)
	}
}
```

## Consumer Group Errors

### RebalanceInProgressError

Consumer group is rebalancing:

```typescript
import { RebalanceInProgressError } from '@kafkats/client'

// Handled automatically - wait for rebalance to complete
```

`consumer.commit()` rejects with it while the group rebalances: commit again once the rebalance is done, or commit in `onPartitionsRevoked`.

### UnknownMemberIdError

Consumer was removed from group:

```typescript
import { UnknownMemberIdError } from '@kafkats/client'

// Consumer will rejoin the group
```

### IllegalGenerationError

Consumer has stale generation:

```typescript
import { IllegalGenerationError } from '@kafkats/client'

// Consumer will rejoin with new generation
```

### CommitFailedError

`consumer.commit()` was rejected because this member's generation is gone (the broker answered `IllegalGeneration`,
`UnknownMemberId` or `FencedInstanceId`, available as `cause`). Nothing was committed and the consumer rejoins the
group; the partitions may have moved to another member, which will process their records again from the last
committed offsets. A commit that was still being retried when the consumer rejoined, or when a partition of it was
revoked or lost, fails the same way (`cause`: `IllegalGeneration`) without being sent again. A transaction's
`sendOffsets()` raises it too when given the metadata of a member that has not joined.

```typescript
import { CommitFailedError } from '@kafkats/client'

try {
	await consumer.commit(offsets)
} catch (error) {
	if (error instanceof CommitFailedError) {
		// Do not retry: these offsets belong to a generation that is gone
	}
}
```

## Offset Reset Errors

### NoOffsetForPartitionError

`autoOffsetReset: 'none'` and an assigned partition has no committed offset: the run (`runEach()`, `runBatch()`,
`stream()`) rejects with it. `topic` and `partition` name the partition.

### OffsetOutOfRangeError

A partition's fetch position is outside the offsets the broker holds (`topic`, `partition`, `offset`). The consumer
does not throw it: under `'none'` it pauses the partition and emits `offsetOutOfRange` (see
[Offset Reset](/client/consumer#offset-reset)). A `FetchManager` used directly without an `onOffsetOutOfRange` callback
throws it from `poll()`.

## Transaction Errors

| Error                     | Raised when                                                                                                                                                                                                   |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `TransactionAbortedError` | A transaction did not commit. `outcome: 'aborted'`: it did not; `'unknown'`: EndTxn(commit) was sent but not confirmed, the broker may have committed. `cause` is why.                                        |
| `InvalidTxnStateError`    | A call does not fit the transaction's state: a handle used after it ended, or a transaction begun on a producer in the `'error'` state.                                                                       |
| `ProducerFencedError`     | A newer producer with the same transactional ID (or the transaction timeout) took the producer's epoch: it rejects every later call; replace it. Also an idempotent producer's send answered PRODUCER_FENCED. |

See [Transactions: Error Handling](/client/transactions#error-handling).

## Checking Retriability

Use `isRetriable()` to check if an error can be retried:

```typescript
import { isRetriable } from '@kafkats/client'

const topic = 'events'
const messages = [{ value: 'data' }]

try {
	await producer.send(topic, messages)
} catch (error) {
	if (isRetriable(error)) {
		// Error is transient, retry might succeed
		await delay(1000)
		await producer.send(topic, messages)
	} else {
		// Permanent error, don't retry
		throw error
	}
}
```

## Error Utilities

### isKafkaError

Check if an error is from kafkats:

```typescript
import { isKafkaError } from '@kafkats/client'

const topic = 'events'
const messages = [{ value: 'data' }]

try {
	await producer.send(topic, messages)
} catch (error) {
	if (isKafkaError(error)) {
		console.log('Kafka error:', error.errorCode, error.message)
	} else {
		console.log('Other error:', error)
	}
}
```

### shouldRefreshMetadata

Check if metadata should be refreshed:

```typescript
import { shouldRefreshMetadata } from '@kafkats/client'

const topic = 'events'
const messages = [{ value: 'data' }]

try {
	await producer.send(topic, messages)
} catch (error) {
	if (shouldRefreshMetadata(error)) {
		// Metadata might be stale, kafkats refreshes automatically
	}
}
```

## Producer Error Events

Listen for producer errors:

```typescript
producer.on('error', error => {
	console.error('Producer error:', error)
})
```

## Consumer Error Events

Listen for consumer errors:

```typescript
consumer.on('error', error => {
	console.error('Consumer error:', error)
})

// Session lost - partitions are no longer owned
consumer.on('partitionsLost', partitions => {
	console.log('Lost partitions:', partitions)
	// Cannot commit offsets for these partitions
})
```

## Error Handling Patterns

### Retry with Backoff

```typescript
async function sendWithRetry(producer: Producer, topic: string, messages: ProducerMessage[], maxRetries = 3) {
	for (let attempt = 0; attempt < maxRetries; attempt++) {
		try {
			return await producer.send(topic, messages)
		} catch (error) {
			if (!isRetriable(error) || attempt === maxRetries - 1) {
				throw error
			}
			await delay(100 * Math.pow(2, attempt))
		}
	}
}
```

### Dead Letter Queue

```typescript
await consumer.runEach('my-topic', async (message, ctx) => {
	try {
		await processMessage(message)
	} catch (error) {
		// Send failed messages to DLQ
		await dlqProducer.send('my-topic-dlq', [
			{
				key: message.key,
				value: message.value,
				headers: {
					error: error instanceof Error ? error.message : String(error),
					originalTopic: ctx.topic,
					originalPartition: String(ctx.partition),
				},
			},
		])
	}
})
```

### Circuit Breaker

```typescript
class CircuitBreaker {
	private failures = 0
	private lastFailure = 0
	private readonly threshold = 5
	private readonly resetMs = 30000

	async call<T>(fn: () => Promise<T>): Promise<T> {
		if (this.isOpen()) {
			throw new Error('Circuit breaker is open')
		}

		try {
			const result = await fn()
			this.failures = 0
			return result
		} catch (error) {
			this.failures++
			this.lastFailure = Date.now()
			throw error
		}
	}

	private isOpen(): boolean {
		if (this.failures < this.threshold) return false
		return Date.now() - this.lastFailure < this.resetMs
	}
}
```
