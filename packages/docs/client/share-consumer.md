# ShareConsumer API

The share consumer reads records from Kafka topics using Kafka **Share Groups** (KIP-932) for queue-like consumption with per-record acknowledgements.

`ShareConsumer` requires the **production-ready Kafka 4.2** Share Group APIs: ShareFetch and ShareAcknowledge v2.
These APIs include the KIP-1206 acquire modes and KIP-1222 acquisition-lock renewal. Kafka 4.2.1+ is recommended
because it fixes a critical Share Group broker deadlock. The preview APIs from Kafka 4.0 and 4.1 are not supported.

## Requirements

- Kafka 4.2+ with ShareFetch and ShareAcknowledge v2 is required
- Kafka 4.2.1+ is recommended
- Clusters running the latest production metadata version enable `share.version=1` by default

If the broker does not support Share APIs, `ShareConsumer` throws `KafkaFeatureUnsupportedError('share-groups')`.
If Share Groups are supported but disabled, it throws `KafkaFeatureDisabledError('share-groups')`.

## Creating a ShareConsumer

```typescript
import { KafkaClient } from '@kafkats/client'

const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['localhost:9092'],
})

const shareConsumer = client.shareConsumer({
	groupId: 'my-share-group',
})
```

## Subscriptions

`runEach()` and `stream()` take a `subscription` argument.

| Subscription               | What it does                       | Example                                                        |
| -------------------------- | ---------------------------------- | -------------------------------------------------------------- |
| Topic name                 | Consume raw `Buffer` key/value     | `shareConsumer.runEach('events', handler)`                     |
| Multiple topics            | Consume multiple topics at once    | `shareConsumer.runEach(['events', 'logs'], handler)`           |
| Typed topic (`topic(...)`) | Decode with codecs and infer types | `shareConsumer.runEach(userEvents, handler)`                   |
| Custom subscription        | Provide explicit decoders          | `shareConsumer.runEach({ topic: 'events', decoder }, handler)` |

## Processing Messages

### Single Message Mode (runEach)

```typescript
await shareConsumer.runEach('events', async (message, ctx) => {
	console.log({
		topic: ctx.topic,
		partition: ctx.partition,
		offset: ctx.offset,
		value: message.value?.toString('utf-8'),
	})
})
```

### Acknowledgements

`ShareConsumer` uses per-record acknowledgements:

- `message.ack()` (ACCEPT) — finalize successful processing
- `message.release()` (RELEASE) — return to the queue for redelivery
- `message.reject()` (REJECT) — drop without redelivery
- `message.renew()` (RENEW) — extend the acquisition lock without finalizing

`ack`, `release`, and `reject` each finalize the record; calling more than one for the same message throws. `renew` does **not** finalize — call it any number of times to keep the lock alive while a slow handler runs, then call one of the finalizing methods (or rely on `runEach()`'s implicit ACCEPT on success).

If your `runEach()` handler completes successfully and you did not call any of these methods, the record is **implicitly acknowledged** (ACCEPT).

In `stream()` mode, the record is **implicitly acknowledged** (ACCEPT) when you advance the iterator (or when the stream closes) unless you called `release()`/`reject()`.

```typescript
await shareConsumer.runEach('events', async message => {
	try {
		if (message.value !== null) {
			await process(message.value.toString('utf-8'))
		}
		await message.ack()
	} catch {
		await message.release()
	}
})
```

#### Renewing the acquisition lock (KIP-1222)

For long-running handlers that need more time than the broker's `group.share.record.lock.duration.ms`, call `message.renew()` to extend the lock:

```typescript
await shareConsumer.runEach('events', async (message, ctx) => {
	const heartbeat = setInterval(() => {
		message.renew().catch(err => ctx.signal.aborted || console.error(err))
	}, 20_000)
	try {
		if (message.value !== null) {
			await longRunningProcessing(message.value)
		}
	} finally {
		clearInterval(heartbeat)
	}
})
```

### Async Iterator Mode (stream)

Consume messages via `for await ... of`:

```typescript
for await (const { message, ctx } of shareConsumer.stream('events')) {
	console.log(ctx.topic, ctx.partition, ctx.offset, message.value?.toString('utf-8'))
	// Optionally:
	// await message.release()
	// await message.reject()
}
```

## Message Structure

```typescript
interface ShareMessage<V = Buffer, K = Buffer> {
	topic: string
	partition: number
	offset: bigint
	timestamp: bigint
	key: K | null
	value: V | null
	headers: Record<string, Buffer>
	deliveryCount?: number
	ack(): Promise<void>
	release(): Promise<void>
	reject(): Promise<void>
	renew(): Promise<void>
}
```

## Consume Context

```typescript
interface ConsumeContext {
	signal: AbortSignal
	topic: string
	partition: number
	offset: bigint
}
```

Use the signal to cancel long-running operations:

```typescript
await shareConsumer.runEach('events', async (message, ctx) => {
	const response = await fetch(url, { signal: ctx.signal })
	// ...
})
```

## Typed ShareConsumers

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

await shareConsumer.runEach(userEvents, async message => {
	// message.key: string
	// message.value: UserEvent
	if (message.value !== null) {
		console.log(`User ${message.value.userId}: ${message.value.action}`)
	}
})
```

## Consumer Config

| Option        | Type                                  | Default             | Description                                                                                                            |
| ------------- | ------------------------------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `groupId`     | `string`                              | required            | Share group identifier                                                                                                 |
| `rackId`      | `string`                              | -                   | Rack ID hint for rack-aware assignment                                                                                 |
| `maxWaitMs`   | `number`                              | `5000`              | `ShareFetch.max_wait_ms`                                                                                               |
| `minBytes`    | `number`                              | `1`                 | `ShareFetch.min_bytes`                                                                                                 |
| `maxBytes`    | `number`                              | `1048576`           | `ShareFetch.max_bytes`                                                                                                 |
| `maxRecords`  | `number`                              | `500`               | Max records the broker should return per fetch                                                                         |
| `batchSize`   | `number`                              | `100`               | Suggested batch size for acquired records and acknowledgements                                                         |
| `acquireMode` | `'batch_optimized' \| 'record_limit'` | `'batch_optimized'` | KIP-1206. `record_limit` strictly caps each fetch at `maxRecords`; `batch_optimized` may exceed it for batch alignment |

Against brokers without ShareFetch or ShareAcknowledge v2, the consumer throws `KafkaFeatureUnsupportedError`
before acquiring records.

## Run Options

### runEach Options

| Option          | Type          | Default | Description                                       |
| --------------- | ------------- | ------- | ------------------------------------------------- |
| `concurrency`   | `number`      | `10`    | Max records processed simultaneously              |
| `ackBatchSize`  | `number`      | `1000`  | Flush acknowledgements when this many are pending |
| `idleBackoffMs` | `number`      | `200`   | Backoff when no records are returned              |
| `signal`        | `AbortSignal` | -       | Abort to stop the consumer                        |

::: details ackBatchSize tuning
Acknowledgements are batched and coalesced to reduce network overhead. Consecutive offsets with the same acknowledgement type are combined into ranges before sending.

- **Lower values** (e.g., 100): Acks sent sooner, reducing risk of lock timeout for slow handlers, but more network requests
- **Higher values** (e.g., 2000): Fewer requests, but acks delayed longer

The default (1000) works well for most workloads.
:::

## Concurrency

Control how many records are processed concurrently:

```typescript
await shareConsumer.runEach('events', handler, { concurrency: 10 })
```

::: warning
Share Groups are queue-like: ordering is not guaranteed, and records from a single partition may be delivered/processed out of order (for example when redeliveries occur).
:::

## Graceful Shutdown

Stop the consumer gracefully:

```typescript
const run = shareConsumer.runEach('events', handler)

// Later: stop consuming
await shareConsumer.stop()
await run
```

`stop()` waits for shutdown cleanup to complete, including flushing any pending acknowledgements and leaving the share group.

With abort signal:

```typescript
const controller = new AbortController()
const run = shareConsumer.runEach('events', handler, { signal: controller.signal })

// Later: stop consuming
controller.abort()
await run
```

## Starting Position (latest)

Share Groups are queue-like: when a member starts fetching, it effectively begins from "now" (the current end offset).

If you want to ensure you only process records produced after your consumer is ready, wait for `partitionsAssigned` before producing:

```typescript
shareConsumer.on('partitionsAssigned', () => {
	// Safe point to produce messages you expect this ShareConsumer to receive.
})
```

## ShareConsumer Events

Listen to share consumer lifecycle events:

```typescript
shareConsumer.on('running', () => {
	console.log('ShareConsumer started')
})

shareConsumer.on('stopped', () => {
	console.log('ShareConsumer stopped')
})

shareConsumer.on('partitionsAssigned', partitions => {
	console.log('Assigned:', partitions)
})

shareConsumer.on('partitionsRevoked', partitions => {
	console.log('Revoked:', partitions)
})

shareConsumer.on('error', error => {
	console.error('ShareConsumer error:', error)
})
```

## When to Use Share Groups

Share Groups decouple parallelism from partition count. They excel when:

- **Message processing takes time** (10ms+): With regular consumer groups, concurrency is limited to partition count. Share Groups can process many messages concurrently regardless of partitions.
- **You need queue-like semantics**: Messages are delivered to any available consumer, not tied to specific partitions.
- **Ordering is not required**: Share Groups do not guarantee order.

### Performance Comparison

With 3 partitions, 20ms processing time per message, and 20 concurrent handlers:

| Consumer Type    | Throughput | Why                                         |
| ---------------- | ---------- | ------------------------------------------- |
| Regular Consumer | ~50 msg/s  | Limited to 3 concurrent (one per partition) |
| Share Consumer   | ~650 msg/s | 20 concurrent handlers                      |

For trivial handlers (0ms processing), regular consumers may be faster due to lower protocol overhead.

## Limitations

- No dead-letter queue / retry policy helpers (KIP-1191 lands in Kafka 4.4 and is not yet implemented)
- Share Groups do not support static membership
- Requires Kafka 4.2+ ShareFetch and ShareAcknowledge v2
