# ShareConsumer API

The share consumer reads records from Kafka topics using Kafka **Share Groups** (KIP-932) for queue-like consumption with per-record acknowledgements.

::: warning Experimental
`ShareConsumer` is experimental and requires Kafka 4.1+ Share Groups.
:::

## Requirements

- Kafka brokers must support Share APIs (Kafka 4.1+)
- Share Groups must be enabled on the cluster (for example `share.version=1`)

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

| Subscription               | What it does                                  | Example                                                        |
| -------------------------- | --------------------------------------------- | -------------------------------------------------------------- |
| Topic name                 | Consume raw `Buffer` key/value                | `shareConsumer.runEach('events', handler)`                     |
| Multiple topics            | Consume multiple topics at once               | `shareConsumer.runEach(['events', 'logs'], handler)`           |
| Typed topic (`topic(...)`) | Decode with codecs and infer types            | `shareConsumer.runEach(userEvents, handler)`                   |
| Custom subscription        | Provide explicit decoders                     | `shareConsumer.runEach({ topic: 'events', decoder }, handler)` |

## Processing Messages

### Single Message Mode (runEach)

```typescript
await shareConsumer.runEach('events', async (message, ctx) => {
	console.log({
		topic: ctx.topic,
		partition: ctx.partition,
		offset: ctx.offset,
		value: message.value.toString('utf-8'),
	})
})
```

### Acknowledgements

`ShareConsumer` uses per-record acknowledgements:

- `message.ack()` (ACCEPT)
- `message.release()` (RELEASE)
- `message.reject()` (REJECT)

If your `runEach()` handler completes successfully and you did not call any of these methods, the record is **implicitly acknowledged** (ACCEPT).

In `stream()` mode, the record is **implicitly acknowledged** (ACCEPT) when you advance the iterator (or when the stream closes) unless you called `release()`/`reject()`.

```typescript
await shareConsumer.runEach('events', async message => {
	try {
		await process(message.value.toString('utf-8'))
		await message.ack()
	} catch {
		await message.release()
	}
})
```

### Async Iterator Mode (stream)

Consume messages via `for await ... of`:

```typescript
for await (const { message, ctx } of shareConsumer.stream('events')) {
	console.log(ctx.topic, ctx.partition, ctx.offset, message.value.toString('utf-8'))
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
	value: V
	headers: Record<string, Buffer>
	deliveryCount?: number
	ack(): Promise<void>
	release(): Promise<void>
	reject(): Promise<void>
}
```

::: tip Tombstones
Kafka tombstones (`value = null`) are supported, but the current types do not model nullable values.
:::

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
	console.log(`User ${message.value.userId}: ${message.value.action}`)
})
```

## Run Options

### runEach Options

| Option         | Type          | Default | Description                                       |
| -------------- | ------------- | ------- | ------------------------------------------------- |
| `concurrency`  | `number`      | `10`    | Max records processed simultaneously              |
| `ackBatchSize` | `number`      | `100`   | Flush acknowledgements when this many are pending |
| `idleBackoffMs`| `number`      | `200`   | Backoff when no records are returned              |
| `signal`       | `AbortSignal` | -       | Abort to stop the consumer                        |

::: details ackBatchSize tuning
Acknowledgements are batched to reduce network overhead. With concurrent processing, fast handlers queue acks while slow handlers are still running. `ackBatchSize` controls when to flush pending acks without waiting for all handlers to complete.

- **Lower values** (e.g., 20): Acks sent sooner, reducing risk of lock timeout for slow handlers, but more network requests
- **Higher values** (e.g., 500): Fewer requests, but acks delayed longer

The default (100) works well for most workloads.
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
shareConsumer.stop()
await run
```

With abort signal:

```typescript
const controller = new AbortController()
const run = shareConsumer.runEach('events', handler, { signal: controller.signal })

// Later: stop consuming
controller.abort()
await run
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

## Limitations

- No dead-letter queue / retry policy helpers
- Share Groups do not support static membership
