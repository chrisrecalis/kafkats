# Producer API

The producer handles sending messages to Kafka topics with automatic batching, compression, and retries.

Unlike a “send one request per call” API, `producer.send()` is **queue-based**: messages are first queued in an in-memory accumulator, then flushed to Kafka as partition batches based on your batching settings.

## Creating a Producer

```typescript
import { KafkaClient } from '@kafkats/client'

const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['localhost:9092'],
})

const producer = client.producer({
	acks: 'all', // Wait for all replicas
	compression: 'gzip', // Snappy/LZ4/Zstd require codec registration
	lingerMs: 5, // Batch for 5ms
})
```

## Sending Messages

### Basic Usage

```typescript
// Send a single message
await producer.send('my-topic', [{ value: 'Hello, Kafka!' }])

// Send multiple messages
await producer.send('events', [
	{ key: 'user-1', value: 'event-1' },
	{ key: 'user-2', value: 'event-2' },
])

// Send to a different topic with a separate call
await producer.send('logs', [{ value: 'log message' }])
```

### How `send()` Works (Queue + Batches)

When you call `producer.send(...)`:

1. Messages are **encoded** (codecs / strings / buffers), assigned a **partition**, and appended to an in-memory **per-topic-partition batch**.
2. Batches are flushed to the broker when they become “ready” (see triggers below).
3. The returned promise resolves when the broker acknowledges the produced records (based on `acks`).

This design makes `send()` fast under load, but it also means you should think about **backpressure**: if you produce faster than Kafka can accept, the in-memory queue can grow.

#### Flush Triggers

| Trigger             | Controlled by                                | What happens                                                    |
| ------------------- | -------------------------------------------- | --------------------------------------------------------------- |
| Time-based batching | `lingerMs`                                   | Flush the current batch for a partition after the timer expires |
| Size-based batching | `maxBatchBytes`                              | Flush the current batch when it reaches the size threshold      |
| Explicit flush      | `producer.flush()` / `producer.disconnect()` | Flush all batches immediately and wait for acknowledgments      |

::: tip Fire-and-forget
You can call `producer.send(...)` without awaiting it, but you must eventually call `await producer.flush()` or `await producer.disconnect()` (or keep the process alive) to ensure queued records are actually delivered.
:::

### Message Structure

```typescript
interface ProducerMessage<V = Buffer, K = Buffer | string> {
	key?: K | null // Message key (determines partition)
	value: V // Message value
	headers?: Record<string, string | Buffer> // Optional headers
	partition?: number // Explicit partition (bypasses partitioner)
	timestamp?: Date // Message timestamp (defaults to now)
}
```

### Send Result

```typescript
const result = await producer.send('my-topic', { value: 'Hello!' })

console.log({
	topic: result.topic, // 'my-topic'
	partition: result.partition, // 0
	offset: result.offset, // 42n (bigint)
	timestamp: result.timestamp, // Date
})
```

## Typed Topics

Use the `topic()` helper for type-safe producers:

```typescript
import { topic, string, json } from '@kafkats/client'

interface Order {
	id: string
	items: string[]
	total: number
}

const ordersTopic = topic('orders', {
	key: string(),
	value: json<Order>(),
})

// Type-checked at compile time
await producer.send(ordersTopic, [
	{
		key: 'order-123',
		value: { id: 'order-123', items: ['item-a'], total: 99.99 },
	},
])
```

## Message Keys and Partitioning

Keys determine which partition a message goes to. Messages with the same key always go to the same partition:

```typescript
// All messages for user-1 go to the same partition (ordered)
await producer.send('events', [
	{ key: 'user-1', value: 'login' },
	{ key: 'user-1', value: 'click' },
	{ key: 'user-1', value: 'logout' },
])
```

### Partitioner Strategies

```typescript
// Default: murmur2 (consistent hashing)
const producer = client.producer({
	partitioner: 'murmur2',
})

// Round-robin (even distribution)
const producer = client.producer({
	partitioner: 'round-robin',
})

// Custom partitioner
const producer = client.producer({
	partitioner: (topic, key, value, partitionCount) => {
		if (key === null) return -1 // Use sticky partitioner
		const hash = customHash(key)
		return Math.abs(hash) % partitionCount
	},
})
```

## Batching

The producer batches messages for efficiency. Configure batching behavior:

```typescript
const producer = client.producer({
	lingerMs: 5, // Wait up to 5ms to batch messages
	maxBatchBytes: 16384, // Flush when batch reaches 16KB
})
```

### Flushing

Force all pending messages to be sent:

```typescript
// Flush and wait for all acknowledgments
await producer.flush()
```

## Compression

Enable compression to reduce network bandwidth:

```typescript
import { CompressionType, compressionCodecs, createLz4Codec, createSnappyCodec, createZstdCodec } from '@kafkats/client'
import snappy from 'snappy' // or '@kafkajs/snappy'
import lz4 from 'lz4' // or 'lz4-napi'
import { compress, decompress } from '@mongodb-js/zstd' // or '@kafkajs/zstd'

// Register codecs once during startup (gzip is built-in)
compressionCodecs.register(CompressionType.Snappy, createSnappyCodec(snappy))
compressionCodecs.register(CompressionType.Lz4, createLz4Codec(lz4))
compressionCodecs.register(CompressionType.Zstd, createZstdCodec({ compress, decompress }))

// Assuming `client` is already created
const producer = client.producer({
	compression: 'snappy', // Also: 'gzip', 'lz4', 'zstd', 'none'
})
```

Supported compression libraries:

- **Snappy** — `snappy` (callback-based) or `@kafkajs/snappy` (N-API)
- **LZ4** — `lz4` (sync) or `lz4-napi` (native/N-API)
- **Zstd** — `@mongodb-js/zstd` (WASM) or `@kafkajs/zstd` (N-API)

| Type       | Speed     | Compression Ratio |
| ---------- | --------- | ----------------- |
| `'none'`   | Fastest   | 1:1               |
| `'snappy'` | Fast      | Good              |
| `'lz4'`    | Very fast | Good              |
| `'gzip'`   | Slow      | Best              |
| `'zstd'`   | Medium    | Best              |

## Error Handling and Retries

The producer automatically retries on retriable errors:

```typescript
const producer = client.producer({
	retries: 3, // Retry up to 3 times
	retryBackoffMs: 100, // Start with 100ms backoff
	maxRetryBackoffMs: 1000, // Max 1s backoff
})
```

Handle errors:

```typescript
import { SendTimeoutError, RecordTooLargeError } from '@kafkats/client'

try {
	await producer.send('my-topic', { value: largePayload })
} catch (error) {
	if (error instanceof RecordTooLargeError) {
		console.log('Message too large for broker')
	} else if (error instanceof SendTimeoutError) {
		console.log('Send timed out')
	}
}
```

## Idempotent Producer

Enable exactly-once delivery semantics:

```typescript
const producer = client.producer({
	idempotent: true,
	acks: 'all', // Required
})
```

With idempotent mode:

- Broker assigns a unique producer ID
- Per-partition sequence numbers detect duplicates
- Retries are safe and won't create duplicates

## Headers

Attach metadata to messages:

```typescript
await producer.send('events', [
	{
		value: 'event data',
		headers: {
			'correlation-id': '12345',
			source: 'web-app',
			timestamp: Date.now().toString(),
		},
	},
])
```

## Closing the Producer

Always close the producer when done:

```typescript
// Flush pending messages and close
await producer.disconnect()
```

## Producer Options

This is a quick reference for `client.producer({...})`. For the complete configuration (including consumer + client options), see [Configuration](/client/configuration).

| Option                 | Type                                                                          | Default     | Notes                                                  |
| ---------------------- | ----------------------------------------------------------------------------- | ----------- | ------------------------------------------------------ |
| `acks`                 | `'all' \| 'leader' \| 'none'`                                                 | `'all'`     | Durability vs latency tradeoff                         |
| `compression`          | `'none' \| 'gzip' \| 'snappy' \| 'lz4' \| 'zstd'`                             | `'none'`    | Applied to record batches                              |
| `lingerMs`             | `number`                                                                      | `5`         | Time-based batching                                    |
| `maxBatchBytes`        | `number`                                                                      | `16384`     | Size-based batching                                    |
| `retries`              | `number`                                                                      | `3`         | Retries on retriable errors                            |
| `retryBackoffMs`       | `number`                                                                      | `100`       | Backoff start                                          |
| `maxRetryBackoffMs`    | `number`                                                                      | `1000`      | Backoff cap                                            |
| `partitioner`          | `'murmur2' \| 'round-robin' \| (topic, key, value, partitionCount) => number` | `'murmur2'` | `-1` means “sticky” for keyless records                |
| `requestTimeoutMs`     | `number`                                                                      | `30000`     | Produce request timeout                                |
| `maxInFlight`          | `number`                                                                      | `5`         | Limits concurrent in-flight produce requests           |
| `idempotent`           | `boolean`                                                                     | `false`     | Safe retries + duplicate detection                     |
| `transactionalId`      | `string`                                                                      | -           | Enables transactions (use `producer.transaction(...)`) |
| `transactionTimeoutMs` | `number`                                                                      | `60000`     | Transaction timeout (broker + client)                  |

## Next Steps

- [Transactions](/client/transactions) - Exactly-once semantics
- [Codecs](/client/codecs) - Custom serialization
- [Error Handling](/client/errors) - Error types and handling
