# Consumer API

The consumer reads records from Kafka topics using consumer groups for automatic partition assignment and offset commits.

## Creating a Consumer

```typescript
import { KafkaClient } from '@kafkats/client'

const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['localhost:9092'],
})

const consumer = client.consumer({
	groupId: 'my-consumer-group',
	autoOffsetReset: 'earliest',
})
```

## Subscribing to Topics

Before consuming messages, you must subscribe to topics using `subscribe()` or manually assign partitions using `assign()`.

### subscribe()

Use `subscribe()` for group-managed partition assignment. The group coordinator automatically assigns partitions and handles rebalancing when consumers join or leave.

```typescript
// Subscribe to a single topic
consumer.subscribe('events')

// Subscribe to multiple topics
consumer.subscribe(['events', 'logs'])

// Subscribe with a typed topic definition
consumer.subscribe(userEvents)
```

| Subscription               | What it does                       | Example                                       |
| -------------------------- | ---------------------------------- | --------------------------------------------- |
| Topic name                 | Consume raw `Buffer` key/value     | `consumer.subscribe('events')`                |
| Multiple topics            | Consume multiple topics at once    | `consumer.subscribe(['events', 'logs'])`      |
| Typed topic (`topic(...)`) | Decode with codecs and infer types | `consumer.subscribe(userEvents)`              |
| Custom subscription        | Provide explicit decoders          | `consumer.subscribe({ topic: 'events', decoder })` |

### assign()

Use `assign()` for manual partition assignment. You directly control which partitions to consume without group coordination.

```typescript
// Manually assign specific partitions
consumer.assign([
	{ topic: 'events', partition: 0, offset: 0n },
	{ topic: 'events', partition: 1, offset: 0n },
])

// Use -1n to use autoOffsetReset setting
consumer.assign([
	{ topic: 'events', partition: 0, offset: -1n },
])

// With typed decoders
consumer.assign(
	[{ topic: 'events', partition: 0, offset: 0n }],
	{ subscription: userEvents }
)
```

| Feature | `subscribe()` | `assign()` |
|---------|---------------|-----------|
| Partition assignment | Automatic (group coordinator) | Manual (you specify) |
| Rebalancing | Yes - partitions reassigned on member changes | No - static assignment |
| Consumer group | Required | Optional |
| Use case | Horizontal scaling, failover | Replay, testing, manual control |

## Processing Messages

### Single Message Mode (runEach)

Process messages one at a time:

```typescript
consumer.subscribe('events')

await consumer.runEach(async (message, ctx) => {
	console.log({
		topic: ctx.topic,
		partition: ctx.partition,
		offset: ctx.offset,
		key: message.key?.toString(),
		value: message.value?.toString(),
	})
})
```

### Batch Mode (runBatch)

Process messages in batches for higher throughput:

```typescript
consumer.subscribe('events')

await consumer.runBatch(
	async (messages, ctx) => {
		console.log(`Received ${messages.length} messages from ${ctx.topic}[${ctx.partition}]`)

		for (const message of messages) {
			await processMessage(message)
		}
	},
	{
		maxBatchSize: 100, // Max messages per batch
		maxBatchWaitMs: 50, // Max wait time to fill batch
	}
)
```

### Async Iterator Mode (stream)

Consume messages via `for await ... of`:

```typescript
consumer.subscribe('events')

for await (const { message, ctx } of consumer.stream()) {
	console.log(ctx.topic, ctx.partition, ctx.offset, message.value)
}
```

## Message Structure

```typescript
interface Message<V = Buffer, K = Buffer> {
	topic: string // Source topic
	partition: number // Source partition
	offset: bigint // Message offset
	timestamp: bigint // Message timestamp (ms)
	key: K | null // Message key
	value: V // Message value
	headers: Record<string, Buffer> // Message headers
}
```

## Consume Context

```typescript
interface ConsumeContext {
	signal: AbortSignal // Aborted when consumer shuts down
	topic: string // Current message topic
	partition: number // Current message partition
	offset: bigint // Current message offset
}
```

Use the signal to cancel long-running operations:

```typescript
consumer.subscribe('events')

await consumer.runEach(async (message, ctx) => {
	const response = await fetch(url, { signal: ctx.signal })
	// ...
})
```

## Typed Consumers

Use typed topics for automatic deserialization:

```typescript
import { topic, string, json } from '@kafkats/client'

interface UserEvent {
	userId: string
	action: string
}

const userEvents = topic('user-events', {
	key: string(),
	value: json<UserEvent>(),
})

consumer.subscribe(userEvents)

await consumer.runEach(async message => {
	// message.key: string
	// message.value: UserEvent
	console.log(`User ${message.value.userId}: ${message.value.action}`)
})
```

## Run Options

### runEach Options

| Option                 | Type          | Default | Description                                  |
| ---------------------- | ------------- | ------- | -------------------------------------------- |
| `partitionConcurrency` | `number`      | `1`     | How many partitions process concurrently     |
| `autoCommit`           | `boolean`     | `true`  | Enable periodic commits                      |
| `commitOffsets`        | `boolean`     | `true`  | Track consumed offsets for committing        |
| `autoCommitIntervalMs` | `number`      | `5000`  | Commit interval when `autoCommit` is enabled |
| `signal`               | `AbortSignal` | -       | Abort to stop the consumer                   |

### runBatch Options

| Option                 | Type          | Default | Description                                  |
| ---------------------- | ------------- | ------- | -------------------------------------------- |
| `partitionConcurrency` | `number`      | `1`     | How many partitions process concurrently     |
| `autoCommit`           | `boolean`     | `true`  | Enable periodic commits                      |
| `commitOffsets`        | `boolean`     | `true`  | Track consumed offsets for committing        |
| `autoCommitIntervalMs` | `number`      | `5000`  | Commit interval when `autoCommit` is enabled |
| `signal`               | `AbortSignal` | -       | Abort to stop the consumer                   |
| `maxBatchSize`         | `number`      | `100`   | Maximum messages per partition-batch         |
| `maxBatchWaitMs`       | `number`      | `50`    | Max time to wait before flushing a batch     |

## Partition Concurrency

Control how many partitions are processed concurrently:

```typescript
consumer.subscribe('events')

// Process up to 4 partitions at the same time
await consumer.runEach(handler, {
	partitionConcurrency: 4,
})
```

::: warning
Higher concurrency increases throughput but may cause out-of-order processing across partitions. Within a partition, order is always preserved.
:::

## Offset Management

When `autoCommit: true` and `commitOffsets: true`, the consumer:

- Marks offsets as "consumed" after your handler completes successfully.
- Commits pending offsets periodically (`autoCommitIntervalMs`) and once more during shutdown (unless the session is lost).

If you set `autoCommit: false` (or `commitOffsets: false`), the consumer will not commit offsets. On restart it will resume from the last committed offsets (or apply `autoOffsetReset` if none exist).

::: tip Manual commits
The public consumer API currently focuses on automatic commits. If you need explicit offset commits / seeking, use `pause()`/`resume()` for backpressure and consider managing offsets externally until manual commit APIs are exposed.
:::

## Backpressure: Pause and Resume

Pause fetching from specific partitions while you're overloaded:

```typescript
consumer.pause([{ topic: 'events', partition: 0 }])
// ... catch up ...
consumer.resume([{ topic: 'events', partition: 0 }])
```

## Consumer Events

Listen to consumer lifecycle events:

```typescript
consumer.on('running', () => {
	console.log('Consumer started')
})

consumer.on('stopped', () => {
	console.log('Consumer stopped')
})

consumer.on('partitionsAssigned', partitions => {
	console.log('Assigned:', partitions)
})

consumer.on('partitionsRevoked', partitions => {
	console.log('Revoked:', partitions)
})

consumer.on('error', error => {
	console.error('Consumer error:', error)
})
```

## Static Membership

Use static membership to avoid rebalances during restarts:

```typescript
const consumer = client.consumer({
	groupId: 'my-group',
	groupInstanceId: 'instance-1', // Unique per consumer
	sessionTimeoutMs: 30000,
})
```

With static membership:

- Consumer keeps its partition assignment on restart
- No rebalance triggered if consumer rejoins within session timeout

## Graceful Shutdown

Stop the consumer gracefully:

```typescript
consumer.subscribe('events')
const run = consumer.runEach(handler)

// Later: stop consuming
consumer.stop()
await run
```

With abort signal:

```typescript
const controller = new AbortController()

consumer.subscribe('events')

// Start consuming
const run = consumer.runEach(handler, { signal: controller.signal })

// Later: stop gracefully
controller.abort()
await run
```

## Partition Assignment Strategies

```typescript
const consumer = client.consumer({
	groupId: 'my-group',
	partitionAssignmentStrategy: 'cooperative-sticky', // Default
})
```

| Strategy               | Description                                            |
| ---------------------- | ------------------------------------------------------ |
| `'cooperative-sticky'` | Incremental rebalance (Kafka 2.4+), minimizes movement |
| `'sticky'`             | Minimize movement, eager rebalance                     |
| `'range'`              | Simple per-topic assignment                            |

## Isolation Level

Control visibility of transactional messages:

```typescript
const consumer = client.consumer({
	groupId: 'my-group',
	isolationLevel: 'read_committed', // Default - only committed transactions
	// isolationLevel: 'read_uncommitted', // See all messages
})
```

## Next Steps

- [Error Handling](/client/errors) - Error types and recovery
- [Codecs](/client/codecs) - Custom serialization
- [Transactions](/client/transactions) - Exactly-once consume-transform-produce
