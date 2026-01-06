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

## Subscriptions

`runEach()`, `runBatch()`, and `stream()` take a `subscription` argument.

| Subscription               | What it does                       | Example                                                   |
| -------------------------- | ---------------------------------- | --------------------------------------------------------- |
| Topic name                 | Consume raw `Buffer` key/value     | `consumer.runEach('events', handler)`                     |
| Multiple topics            | Consume multiple topics at once    | `consumer.runEach(['events', 'logs'], handler)`           |
| Typed topic (`topic(...)`) | Decode with codecs and infer types | `consumer.runEach(userEvents, handler)`                   |
| Custom subscription        | Provide explicit decoders          | `consumer.runEach({ topic: 'events', decoder }, handler)` |

## Processing Messages

### Single Message Mode (runEach)

Process messages one at a time:

```typescript
await consumer.runEach('events', async (message, ctx) => {
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
await consumer.runBatch(
	'events',
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
for await (const { message, ctx } of consumer.stream('events')) {
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
await consumer.runEach('events', async (message, ctx) => {
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

await consumer.runEach(userEvents, async message => {
	// message.key: string
	// message.value: UserEvent
	console.log(`User ${message.value.userId}: ${message.value.action}`)
})
```

## Run Options

### runEach Options

| Option                 | Type                 | Default | Description                                         |
| ---------------------- | -------------------- | ------- | --------------------------------------------------- |
| `partitionConcurrency` | `number`             | `1`     | How many partitions process concurrently            |
| `autoCommit`           | `boolean`            | `true`  | Enable periodic commits                             |
| `commitOffsets`        | `boolean`            | `true`  | Track consumed offsets for committing               |
| `autoCommitIntervalMs` | `number`             | `5000`  | Commit interval when `autoCommit` is enabled        |
| `signal`               | `AbortSignal`        | -       | Abort to stop the consumer                          |
| `assignment`           | `ManualAssignment[]` | -       | Manually assign partitions instead of joining group |

### runBatch Options

| Option                 | Type                 | Default | Description                                         |
| ---------------------- | -------------------- | ------- | --------------------------------------------------- |
| `partitionConcurrency` | `number`             | `1`     | How many partitions process concurrently            |
| `autoCommit`           | `boolean`            | `true`  | Enable periodic commits                             |
| `commitOffsets`        | `boolean`            | `true`  | Track consumed offsets for committing               |
| `autoCommitIntervalMs` | `number`             | `5000`  | Commit interval when `autoCommit` is enabled        |
| `signal`               | `AbortSignal`        | -       | Abort to stop the consumer                          |
| `maxBatchSize`         | `number`             | `100`   | Maximum messages per partition-batch                |
| `maxBatchWaitMs`       | `number`             | `50`    | Max time to wait before flushing a batch            |
| `assignment`           | `ManualAssignment[]` | -       | Manually assign partitions instead of joining group |

## Partition Concurrency

Control how many partitions are processed concurrently:

```typescript
// Process up to 4 partitions at the same time
await consumer.runEach('events', handler, {
	partitionConcurrency: 4,
})
```

::: warning
Higher concurrency increases throughput but may cause out-of-order processing across partitions. Within a partition, order is always preserved.
:::

## Offset Management

When `autoCommit: true` and `commitOffsets: true`, the consumer:

- Marks offsets as “consumed” after your handler completes successfully.
- Commits pending offsets periodically (`autoCommitIntervalMs`) and once more during shutdown (unless the session is lost).

If you set `autoCommit: false` (or `commitOffsets: false`), the consumer will not commit offsets. On restart it will resume from the last committed offsets (or apply `autoOffsetReset` if none exist).

::: tip Manual commits
The public consumer API currently focuses on automatic commits. If you need explicit offset commits, consider managing offsets externally until manual commit APIs are exposed.
:::

## Backpressure: Pause and Resume

Pause fetching from specific partitions while you're overloaded:

```typescript
consumer.pause([{ topic: 'events', partition: 0 }])
// ... catch up ...
consumer.resume([{ topic: 'events', partition: 0 }])
```

## Seeking to a Specific Offset

Reposition the consumer to a specific offset for a partition:

```typescript
// Seek to a specific offset
consumer.seek('events', 0, 100n)

// Seek to the beginning
consumer.seek('events', 0, 0n)
```

The seek only affects the **next fetch** - messages already fetched will still be delivered. For controlled seeking, combine with pause/resume:

```typescript
await consumer.runEach('events', async (message, ctx) => {
	if (shouldReplay(message)) {
		// Pause, seek, resume pattern for controlled repositioning
		consumer.pause([{ topic: ctx.topic, partition: ctx.partition }])
		consumer.seek(ctx.topic, ctx.partition, 0n) // Seek to beginning
		consumer.resume([{ topic: ctx.topic, partition: ctx.partition }])
	}
})
```

::: warning
Seeking does not affect committed offsets. If you want to persist the new position, you'll need to commit after seeking. On restart, the consumer will resume from the last committed offset.
:::

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

## Manual Topic Assignment

Bypass the consumer group protocol entirely and manually assign specific partitions:

```typescript
interface ManualAssignment {
	topic: string
	partition: number
	offset?: bigint // Optional starting offset
}
```

### Basic Usage

```typescript
await consumer.runEach(
	'events',
	async message => {
		console.log(message.value)
	},
	{
		assignment: [
			{ topic: 'events', partition: 0 },
			{ topic: 'events', partition: 1 },
		],
	}
)
```

### With Explicit Starting Offset

```typescript
await consumer.runEach(
	'events',
	async message => {
		console.log(message.value)
	},
	{
		assignment: [{ topic: 'events', partition: 0, offset: 100n }],
	}
)
```

### How It Works

When using manual assignment:

- **No group coordination** - Skips JoinGroup/SyncGroup protocol exchanges
- **No rebalancing** - Partitions remain statically assigned
- **Offset resolution** - If `offset` is omitted, the consumer fetches committed offsets (if present) or falls back to `autoOffsetReset`
- **Offset commits** - Still commits offsets under `groupId` (if `commitOffsets: true`), but without group generation/member metadata

### Use Cases

- **Static partition mapping** - When you need deterministic partition-to-consumer mapping
- **Multiple consumers on same partition** - Multiple consumers with the same `groupId` can consume the same partition independently
- **Testing** - Simplified testing without rebalance complexity
- **State restoration** - Read specific partitions for state recovery

::: warning
Manual assignment does not emit `partitionsRevoked` events since there is no rebalancing. Ensure your application handles partition ownership appropriately.
:::

## Graceful Shutdown

Stop the consumer gracefully:

```typescript
const run = consumer.runEach('events', handler)

// Later: stop consuming
consumer.stop()
await run
```

With abort signal:

```typescript
const controller = new AbortController()

// Start consuming
const run = consumer.runEach('events', handler, { signal: controller.signal })

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
