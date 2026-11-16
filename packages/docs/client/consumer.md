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
await consumer.runBatch('events', async (messages, ctx) => {
	console.log(`Received ${messages.length} messages from ${ctx.topic}[${ctx.partition}]`)

	for (const message of messages) {
		await processMessage(message)
	}
})
```

Each batch holds records from one partition, up to the consumer's `maxBytesPerPartition` (estimated from key and
value sizes). Records fetched ahead while a handler is busy stay buffered for the next batch, so a slow handler never
receives more than one partition's byte budget at once. A record larger than the limit is delivered on its own.
Lower `maxBytesPerPartition` when per-record work is slow: a rebalance waits for in-flight batches, so smaller batches
keep rebalances and topic priority responsive (smaller fetches are the trade-off).

Offsets of a batch are marked as consumed when the handler returns. A handler that works through a large batch can
mark its progress as it goes with `ctx.resolveOffset(offset)`, so that what it has finished is committed even if it
fails or the consumer stops before the end of the batch:

```typescript
await consumer.runBatch('events', async (messages, ctx) => {
	for (const message of messages) {
		await processMessage(message)
		ctx.resolveOffset(message.offset) // everything up to this record can be committed
	}
})
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
	groupId: string // Consumer group whose offsets are being tracked
	consumerGroupMetadata?: Readonly<{
		groupId: string
		generationId: number
		memberId: string
		groupInstanceId?: string | null
	}> // Present for consumer-group delivery; captured when the record or batch is delivered
}
```

Pass the context to `transaction.sendOffsets(ctx)` when atomically committing consumed offsets with produced
records. Its consumer-group metadata is a delivery-time snapshot, so a transaction committed after a rebalance is
fenced using the membership that actually delivered the records. The fields are public and survive object spread.

Manually assigned consumers have a `groupId` but no `consumerGroupMetadata`, because they do not join the group.

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
| `priority`             | `PriorityStrategy`   | -       | Prioritize topics when fetching and delivering      |

### runBatch Options

| Option                 | Type                 | Default | Description                                         |
| ---------------------- | -------------------- | ------- | --------------------------------------------------- |
| `partitionConcurrency` | `number`             | `1`     | How many partitions process concurrently            |
| `autoCommit`           | `boolean`            | `true`  | Enable periodic commits                             |
| `commitOffsets`        | `boolean`            | `true`  | Track consumed offsets for committing               |
| `autoCommitIntervalMs` | `number`             | `5000`  | Commit interval when `autoCommit` is enabled        |
| `signal`               | `AbortSignal`        | -       | Abort to stop the consumer                          |
| `assignment`           | `ManualAssignment[]` | -       | Manually assign partitions instead of joining group |
| `priority`             | `PriorityStrategy`   | -       | Prioritize topics when fetching and delivering      |

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

## Topic Priority

When one consumer handles latency-sensitive and backlog topics, use a priority strategy to control both which
buffered records are delivered and which partitions the background fetcher requests.

```typescript
import { strict, weighted } from '@kafkats/client'

const live = topic('live')
const bulk = topic('bulk')

await consumer.runEach([live, bulk], handler, {
	partitionConcurrency: 4,
	priority: strict({ order: ['live', 'bulk'] }),
})

// Reserve an approximate 80/20 share while allowing either topic to use spare capacity.
await consumer.runEach([live, bulk], handler, {
	priority: weighted({ shares: { live: 8, bulk: 2 } }),
})
```

`strict` can starve lower-priority topics while higher-priority traffic remains available. Use `weighted` when every
topic must continue making progress. Topics omitted from a strict order share the final tier; topics omitted from
weighted shares receive a share of `1`.

Custom strategies implement `PriorityStrategy` and return ordered `drain` entries, each capped at `maxBytes` out of
the poll's `byteBudget` (the consumer's `maxBytesPerPartition`), plus the partitions currently allowed to `fetch`. Buffered partitions omitted from `drain` are still delivered FIFO after the planned entries within
the remaining poll budget, so hold a topic back through `fetch`, not `drain`. Derive fairness from the supplied
scheduler state: the fetch loop may call `schedule()` without a drain, and a drain decision may be only partially
applied.

Priority applies at poll granularity. A handler batch already admitted is never preempted; lower `maxBytesPerPartition` for
finer-grained scheduling when `partitionConcurrency` is small. Literal topic names are checked against the
subscription. One TypeScript limitation is that `topic<Order>('orders')` leaves the name typed as `string` because
generic arguments cannot be partially inferred; codec-driven inference preserves the literal name.

## Offset Management

When `autoCommit: true` and `commitOffsets: true`, the consumer:

- Marks offsets as “consumed” after your handler completes successfully.
- Commits pending offsets periodically (`autoCommitIntervalMs`), when partitions are revoked, and once more during shutdown (unless the session is lost).

If you set `autoCommit: false` (or `commitOffsets: false`), the consumer will not commit offsets on its own. On restart it will resume from the last committed offsets (or apply `autoOffsetReset` if none exist). In `runBatch`, `ctx.resolveOffset()` controls how far automatic commits may go.

### Committing Offsets Yourself

`consumer.commit()` commits offsets of assigned partitions when you choose: from a handler, from a timer, or from `onPartitionsRevoked`. Each offset is the **next** offset to read (the last processed offset + 1) and can carry a metadata string, stored and read back with it:

```typescript
await consumer.commit([{ topic: 'events', partition: 0, offset: lastProcessed + 1n, metadata: 'checkpoint-42' }], {
	timeoutMs: 10_000, // default: defaultApiTimeoutMs
})
```

- It works with a group or a manual assignment, also with `commitOffsets: false`. Offsets of partitions not assigned to this consumer are skipped. It throws when the consumer is not running.
- A moved, loading or slow coordinator is retried until `timeoutMs` passed; then it rejects with `TimeoutError`. Aborting `signal` rejects it with the signal's reason.
- It rejects with `RebalanceInProgressError` while the group rebalances (commit again once it is done, or in `onPartitionsRevoked`), and with `CommitFailedError` when this member's generation is gone: nothing was committed and the consumer rejoins the group.

### Reading Committed Offsets

```typescript
const committed = await consumer.committed([{ topic: 'events', partition: 0 }])
const entry = committed.get('events:0') // { offset, metadata, leaderEpoch? }, or null without a committed offset
```

The map is keyed by `topic:partition`. Offsets of an open transaction are waited for, up to `timeoutMs`. Each assignment also carries the committed offsets of its partitions (see [Rebalance Hooks](#rebalance-hooks)).

### Position, Next Offset and Lag

| Method                         | Returns                                                                                                                                                                                                                                                                  |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `position(topic, partition)`   | The offset the next fetch starts at. Fetching runs ahead of the handlers, so this is past records still buffered.                                                                                                                                                        |
| `nextOffset(topic, partition)` | The offset of the next record to be delivered to a handler: the earliest record fetched but not delivered yet, else the fetch position (which moves past transaction markers and aborted records). Once everything delivered is processed, this is the offset to commit. |
| `currentLag(topic, partition)` | How many records can be read beyond `nextOffset`: the partition's end as of its last fetch response (the last stable offset under `read_committed`, else the high watermark) minus `nextOffset`. `undefined` before the first fetch response and while paused.           |

All three return `undefined` for a partition that is not assigned to the consumer. `consumer.metrics()` reports the same per partition (`position`, `nextOffset`, `endOffset`, `bufferedBytes`, `bufferedRecords`, `paused`).

`consumer.commitStartPosition([{ topic, partition }])` commits the position a partition starts from. It is for a partition that has no committed offset yet: committing where it starts makes a restart resume from there instead of applying `autoOffsetReset` again (with `'latest'`, that would skip whatever arrived in between). Call it from `onPartitionsAssigned` or a `partitionsAssigned` listener, optionally after `seek()`. It throws once records of the partition have been fetched, because the position is then past records no handler has seen.

### Offset Reset

`autoOffsetReset` decides where a partition starts when it has no committed offset, and where it goes on when its position is out of range (the records were deleted by retention or `admin.deleteRecords()`). It takes a policy, or a function returning each partition's policy:

```typescript
const consumer = client.consumer({
	groupId: 'my-group',
	autoOffsetReset: (topic, partition) => (topic.startsWith('audit-') ? 'none' : 'earliest'),
})

consumer.on('offsetReset', ({ topic, partition, from, to, reason }) => {
	console.log(`${topic}-${partition} moved from ${from} to ${to} (${reason})`) // reason: 'no-commit' | 'out-of-range'
})

consumer.on('offsetOutOfRange', ({ topic, partition, offset }) => {
	// Only under 'none': the partition is paused. Decide where it goes on, then resume it.
	consumer.seek(topic, partition, restartOffsetFor(topic, partition))
	consumer.resume([{ topic, partition }])
})
```

| Policy               | No committed offset                            | Position out of range                                                      |
| -------------------- | ---------------------------------------------- | -------------------------------------------------------------------------- |
| `'earliest'`         | Earliest available offset; `offsetReset`       | Earliest available offset; `offsetReset`                                   |
| `'latest'` (default) | End of the partition; `offsetReset`            | End of the partition; `offsetReset`                                        |
| `'none'`             | The run fails with `NoOffsetForPartitionError` | The partition is paused and `offsetOutOfRange` is emitted; the run goes on |

Under `read_committed` (the default), the end of a partition is its last stable offset: `'latest'` does not skip records of transactions still open.

### Connections

Every consumer fetches on its own connection to each broker. A broker answers one request per connection at a time and holds a fetch for up to `maxWaitMs` when there is no data, so consumers that shared a connection would wait for each other's long polls.

## Backpressure: Pause and Resume

Pause fetching from specific partitions while you're overloaded:

```typescript
consumer.pause([{ topic: 'events', partition: 0 }])
// ... catch up ...
consumer.resume([{ topic: 'events', partition: 0 }])
```

Once `pause()` returns, no record of the partition fetched before is delivered, except to a handler of that partition that is already running; after `resume()` they are fetched and delivered again, none lost and none twice. A pause does not outlive the partition's assignment: under the eager protocol every rebalance assigns partitions anew, unpaused. Pause them in `onPartitionsAssigned` to keep them paused.

## Seeking to a Specific Offset

Reposition the consumer to a specific offset for a partition:

```typescript
// Seek to a specific offset
consumer.seek('events', 0, 100n)

// Seek to the beginning
consumer.seek('events', 0, 0n)
```

Records of the partition fetched but not delivered yet are dropped: once `seek()` returns, delivery restarts at the new offset. Only a handler of that partition that is already running is not interrupted: called from inside `runEach`'s handler, the remaining records of the batch being processed are still delivered before the seek takes effect. To set where a partition starts before any of its records is fetched, seek it in `onPartitionsAssigned`.

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

| Event                | Payload                                       | Emitted when                                                                                        |
| -------------------- | --------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `running`            | -                                             | The consumer joined (or took its manual assignment) and `onPartitionsAssigned` returned             |
| `stopped`            | -                                             | The run ended                                                                                       |
| `error`              | `Error`                                       | A failure inside the consumer (a fetch error, an unreachable cluster, ...)                          |
| `rebalancePending`   | -                                             | The group asked for a rebalance; no new handler starts, running ones finish                         |
| `rebalance`          | -                                             | The rebalance starts, after `onBeforeRebalance` returned                                            |
| `partitionsRevoked`  | `TopicPartition[]`                            | Partitions were taken away, after `onPartitionsRevoked` and their final commit                      |
| `partitionsAssigned` | `AssignedPartition[]`                         | Partitions were assigned, after `onPartitionsAssigned` returned                                     |
| `partitionsLost`     | `TopicPartition[]`                            | The session was lost with these partitions; their offsets can no longer be committed                |
| `rebalanceComplete`  | -                                             | The rebalance finished (the member rejoined and took its partitions), unless another one is pending |
| `offsetReset`        | `{ topic, partition, from, to, reason }`      | A partition's position was set by `'earliest'` or `'latest'` (see [Offset Reset](#offset-reset))    |
| `offsetOutOfRange`   | `{ topic, partition, offset }`                | A partition's position is out of range under `'none'`: it is paused                                 |
| `fetchPosition`      | `{ topic, partition, offset }`, `recordCount` | Fetching advanced; `offset` is the next fetch position                                              |

`consumer.rebalancePending` is `true` from `rebalancePending` until `rebalanceComplete`.

## Rebalance Hooks

Three hooks of the consumer config let the application act when partitions move. The consumer awaits them inside the rebalance, unlike the events above:

```typescript
const consumer = client.consumer({
	groupId: 'my-group',
	// Taken away: their handlers have returned, their offsets can still be committed.
	onPartitionsRevoked: async partitions => {
		await consumer.commit(checkpointsOf(partitions))
	},
	// Assigned: none of them is fetched before this returns.
	onPartitionsAssigned: async partitions => {
		for (const { topic, partition, offset, committed } of partitions) {
			console.log(`${topic}-${partition} starts at ${offset}, committed metadata: ${committed?.metadata}`)
		}
	},
	// Lost with the session: their offsets can no longer be committed.
	onPartitionsLost: async partitions => {
		discardWorkOf(partitions)
	},
})
```

| Hook                   | Called                                                                                                                                                                                                            | What it can rely on                                                                                                                                                                                                                                                  |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `onPartitionsRevoked`  | When a rebalance takes partitions away: all of them under the eager protocol, the revoked ones under the cooperative protocol                                                                                     | Their handlers have returned. It runs before the consumer commits their offsets (with `commitOffsets`) and stops fetching them; `commit()` here commits under the generation that owned them. Not called for lost partitions.                                        |
| `onPartitionsAssigned` | When partitions are assigned: on the first join (before `running`), by each rebalance (every partition under the eager protocol, the added ones under the cooperative protocol), and once for a manual assignment | None of them is fetched before it returns, so `pause()` and `seek()` here take effect before their first record; partitions paused here stay paused. Each entry carries `offset` (where it starts) and `committed` (`{ offset, metadata, leaderEpoch? }` or `null`). |
| `onPartitionsLost`     | When the group expired or fenced this member                                                                                                                                                                      | It is invoked synchronously as the loss is detected, so its synchronous part runs before any other code (handlers of these partitions may still be running). The member rejoins only once it resolved.                                                               |

A hook that throws fails the rebalance and the run (the `runEach()`/`runBatch()` promise rejects), like `onBeforeRebalance`.

An eager rebalance goes: `rebalancePending` → running handlers finish → `onBeforeRebalance` → `rebalance` → `onPartitionsRevoked` → final commit → `partitionsRevoked` → rejoin → `onPartitionsAssigned` → `partitionsAssigned` → `rebalanceComplete`. A cooperative rebalance calls the same hooks after the rejoin, with the revoked and the added partitions.

### Group Metadata

`consumer.groupMetadata()` returns the group metadata of the consumer's last completed join (`groupId`, `generationId`, `memberId`, `groupInstanceId`): what offsets committed in a transaction for its assignment must carry (`tx.sendOffsets({ consumerGroupMetadata, offsets })`, see [Transactions](/client/transactions)). In `onPartitionsAssigned` it is already that of the new assignment. It is `undefined` with a manual assignment, before the first join, and from the moment the group rejected this member's session or generation until the next join completed.

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
