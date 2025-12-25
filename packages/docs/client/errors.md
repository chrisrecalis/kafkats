# Error Handling

kafkats provides specific error types for different failure scenarios, making it easy to handle errors appropriately.

## Error Hierarchy

```
KafkaError (base)
├── KafkaProtocolError (protocol-level errors)
├── ConnectionError (network failures)
├── TimeoutError (request timeouts)
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
	consumer.subscribe('events')
	await consumer.runEach(async () => {
		// ...
	})
} catch (error) {
	if (error instanceof TimeoutError) {
		console.log('Request timed out')
		// Increase timeout or retry
	}
}
```

### SendTimeoutError

Producer send timed out:

```typescript
import { SendTimeoutError } from '@kafkats/client'

try {
	await producer.send('events', { value: 'data' })
} catch (error) {
	if (error instanceof SendTimeoutError) {
		console.log('Send timed out after retries')
		// Message may or may not have been delivered
	}
}
```

### RecordTooLargeError

Message exceeds broker limits:

```typescript
import { RecordTooLargeError } from '@kafkats/client'

try {
	await producer.send('events', { value: hugePayload })
} catch (error) {
	if (error instanceof RecordTooLargeError) {
		console.log('Message too large:', error.message)
		// Split the message or increase broker limits
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
		console.log('Kafka error:', error.code, error.message)
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
consumer.subscribe('my-topic')
await consumer.runEach(async (message, ctx) => {
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
