# Consumer Group

Read messages from Kafka using a consumer group.

## Code

```typescript
import { KafkaClient, topic, string, json } from '@kafkats/client'

interface UserEvent {
	userId: string
	action: string
	timestamp: number
}

const userEvents = topic('user-events', {
	key: string(),
	value: json<UserEvent>(),
})

async function main() {
	const client = new KafkaClient({
		clientId: 'consumer-example',
		brokers: ['localhost:9092'],
	})

	const consumer = client.consumer({
		groupId: 'user-events-processor',
		autoOffsetReset: 'earliest', // Start from beginning
	})

	// Handle shutdown
	const controller = new AbortController()

	process.on('SIGINT', () => {
		console.log('\nShutting down...')
		controller.abort()
	})

	process.on('SIGTERM', () => {
		console.log('\nShutting down...')
		controller.abort()
	})

	try {
		console.log('Waiting for messages...')

		// Process messages
		consumer.subscribe(userEvents)
		await consumer.runEach(
			async (message, ctx) => {
				console.log(`[${ctx.topic}:${ctx.partition}] offset=${ctx.offset}`)
				console.log(`  Key: ${message.key}`)
				console.log(`  Value: ${JSON.stringify(message.value)}`)
				console.log()
			},
			{
				signal: controller.signal,
			}
		)
	} finally {
		consumer.stop()
		console.log('Consumer closed')
	}
}

main().catch(console.error)
```

## Output

```
Waiting for messages...
[user-events:0] offset=0
  Key: user-1
  Value: {"userId":"user-1","action":"login","timestamp":1703001234567}

[user-events:0] offset=1
  Key: user-2
  Value: {"userId":"user-2","action":"signup","timestamp":1703001234567}

[user-events:0] offset=2
  Key: user-1
  Value: {"userId":"user-1","action":"logout","timestamp":1703001235567}

^C
Shutting down...
Consumer closed
```

## Key Points

1. **Consumer group** - Multiple consumers share the workload
2. **Auto offset reset** - Start from `earliest` or `latest`
3. **Typed messages** - Using topic definition with codecs
4. **Graceful shutdown** - Handle SIGINT/SIGTERM
5. **Context** - Access topic, partition, offset metadata

## Variations

### Batch Processing

Process multiple messages at once:

```typescript
consumer.subscribe(userEvents)
await consumer.runBatch(
	async (messages, ctx) => {
		console.log(`Received ${messages.length} messages`)

		for (const message of messages) {
			await processMessage(message)
		}
	},
	{
		maxBatchSize: 100,
		maxBatchWaitMs: 50,
	}
)
```

### Parallel Partition Processing

Process multiple partitions concurrently:

```typescript
consumer.subscribe(userEvents)
await consumer.runEach(handler, {
	partitionConcurrency: 4, // Process 4 partitions in parallel
})
```

### Multiple Topics

Subscribe to multiple topics:

```typescript
consumer.subscribe(['events', 'logs', 'metrics'])
await consumer.runEach(handler)
```
