# Migrating from KafkaJS

This guide helps you migrate from [KafkaJS](https://kafka.js.org/) to `@kafkats/client`. While both libraries serve the same purpose, there are important API differences to be aware of.

## Why Migrate?

`@kafkats/client` offers several advantages over KafkaJS:

- **Pure TypeScript** - Written from scratch in TypeScript with full type safety
- **Type-safe topics** - Define topics with codecs for compile-time type checking
- **Modern API** - Async-first APIs like `runEach()`/`runBatch()`/`transaction()`
- **Active development** - Regular updates with modern Kafka protocol support
- **Share Groups** - Support for KIP-932 Share Groups
- **Transactions** - Full exactly-once semantics with transactional producer

## Quick Comparison

| Feature           | KafkaJS                     | @kafkats/client              |
| ----------------- | --------------------------- | ---------------------------- |
| Client creation   | `new Kafka({...})`          | `new KafkaClient({...})`     |
| Producer creation | `kafka.producer()`          | `client.producer()`          |
| Consumer creation | `kafka.consumer({groupId})` | `client.consumer({groupId})` |
| Admin creation    | `kafka.admin()`             | `client.admin()`             |
| Message handler   | `eachMessage` callback      | `runEach()` method           |
| Batch handler     | `eachBatch` callback        | `runBatch()` method          |
| Type safety       | Runtime only                | Compile-time with codecs     |

## Client Configuration

### KafkaJS

```typescript
const { Kafka, logLevel } = require('kafkajs')

const kafka = new Kafka({
	clientId: 'my-app',
	brokers: ['kafka1:9092', 'kafka2:9092'],
	connectionTimeout: 3000,
	requestTimeout: 30000,
	ssl: true,
	sasl: {
		mechanism: 'scram-sha-256',
		username: 'user',
		password: 'pass',
	},
	retry: {
		initialRetryTime: 100,
		retries: 8,
	},
	logLevel: logLevel.INFO,
})
```

### @kafkats/client

```typescript
import { KafkaClient } from '@kafkats/client'

const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['kafka1:9092', 'kafka2:9092'],
	connectionTimeoutMs: 3000,
	requestTimeoutMs: 30000,
	tls: { enabled: true },
	sasl: {
		mechanism: 'SCRAM-SHA-256',
		username: 'user',
		password: 'pass',
	},
	logLevel: 'info',
})
```

### Configuration Mapping

| KafkaJS             | @kafkats/client                                            | Notes                                                        |
| ------------------- | ---------------------------------------------------------- | ------------------------------------------------------------ |
| `clientId`          | `clientId`                                                 | Same                                                         |
| `brokers`           | `brokers`                                                  | Same                                                         |
| `connectionTimeout` | `connectionTimeoutMs`                                      | Explicit `Ms` suffix                                         |
| `requestTimeout`    | `requestTimeoutMs`                                         | Explicit `Ms` suffix                                         |
| `ssl: true`         | `tls: { enabled: true }`                                   | Renamed to `tls`                                             |
| `ssl: {...}`        | `tls: { enabled: true, ... }`                              | Same options as Node.js `tls.connect()`                      |
| `sasl`              | `sasl`                                                     | Same structure, mechanism: `'PLAIN'`, `'SCRAM-SHA-256'`, etc |
| `retry`             | `producer({ retries, retryBackoffMs, maxRetryBackoffMs })` | Retry is configured per producer                             |
| `logLevel`          | `logLevel`                                                 | String values: `'debug'`, `'info'`, `'warn'`, `'error'`      |

## Producer Migration

### KafkaJS

```typescript
const { Partitioners, CompressionTypes } = require('kafkajs')

const producer = kafka.producer({
	createPartitioner: Partitioners.DefaultPartitioner,
	allowAutoTopicCreation: true,
	idempotent: true,
	maxInFlightRequests: 5,
})

await producer.connect()

await producer.send({
	topic: 'my-topic',
	messages: [
		{ key: 'key1', value: 'value1' },
		{ key: 'key2', value: JSON.stringify({ foo: 'bar' }), headers: { source: 'app' } },
	],
	acks: -1,
	timeout: 30000,
	compression: CompressionTypes.GZIP,
})

await producer.disconnect()
```

### @kafkats/client

```typescript
const producer = client.producer({
	partitioner: 'murmur2', // or 'round-robin' or custom function
	idempotent: true,
	maxInFlight: 5,
	acks: 'all', // 'all' | 'leader' | 'none'
	// 'gzip' works out of the box; for 'snappy'/'lz4'/'zstd' you must register a codec first
	compression: 'gzip', // 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd'
	lingerMs: 5,
	maxBatchBytes: 16384,
	retries: 3,
})

// No explicit connect() needed - connects automatically on first send

const result = await producer.send('my-topic', [
	{ key: 'key1', value: Buffer.from('value1') },
	{ key: 'key2', value: Buffer.from(JSON.stringify({ foo: 'bar' })), headers: { source: 'app' } },
])

await producer.disconnect()
```

::: tip Transactional producers
If you configure `transactionalId`, you must use `producer.transaction(...)` and cannot call `producer.send(...)` directly.
:::

### Type-Safe Producer with Codecs

```typescript
import { topic, string, json } from '@kafkats/client'

interface OrderEvent {
	orderId: string
	amount: number
	status: string
}

const orderTopic = topic('orders', {
	key: string(),
	value: json<OrderEvent>(),
})

// Type-checked at compile time
await producer.send(orderTopic, {
	key: 'order-123',
	value: { orderId: 'order-123', amount: 99.99, status: 'created' },
})
```

### Producer Configuration Mapping

| KafkaJS               | @kafkats/client    | Notes                                                                                                                         |
| --------------------- | ------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `createPartitioner`   | `partitioner`      | `'murmur2'`, `'round-robin'`, or function                                                                                     |
| `acks: -1`            | `acks: 'all'`      | String values: `'all'`, `'leader'`, `'none'`                                                                                  |
| `acks: 1`             | `acks: 'leader'`   |                                                                                                                               |
| `acks: 0`             | `acks: 'none'`     |                                                                                                                               |
| `timeout`             | `requestTimeoutMs` | Configured on the producer (not per-send)                                                                                     |
| `compression`         | `compression`      | `gzip` is built-in; `snappy`/`lz4`/`zstd` require `npm install` + codec registration (see [Compression](/client/compression)) |
| `idempotent`          | `idempotent`       | Same                                                                                                                          |
| `transactionalId`     | `transactionalId`  | Use `producer.transaction(...)` for sends                                                                                     |
| `maxInFlightRequests` | `maxInFlight`      | Shorter name                                                                                                                  |
| N/A                   | `lingerMs`         | Batching delay (new feature)                                                                                                  |
| N/A                   | `maxBatchBytes`    | Batch size limit (new feature)                                                                                                |

### Batch Sending

**KafkaJS:**

```typescript
await producer.sendBatch({
	topicMessages: [
		{ topic: 'topic-a', messages: [{ value: 'msg1' }] },
		{ topic: 'topic-b', messages: [{ value: 'msg2' }] },
	],
})
```

**@kafkats/client:**

```typescript
// Send to multiple topics with separate calls (batched internally)
await Promise.all([
	producer.send('topic-a', [{ value: Buffer.from('msg1') }]),
	producer.send('topic-b', [{ value: Buffer.from('msg2') }]),
])
```

## Consumer Migration

### KafkaJS

```typescript
const consumer = kafka.consumer({
	groupId: 'my-group',
	sessionTimeout: 30000,
	rebalanceTimeout: 60000,
	heartbeatInterval: 3000,
	maxBytesPerPartition: 1048576,
	minBytes: 1,
	maxBytes: 10485760,
	maxWaitTimeInMs: 5000,
	readUncommitted: false,
})

await consumer.connect()
await consumer.subscribe({ topics: ['my-topic'], fromBeginning: true })

await consumer.run({
	autoCommit: true,
	autoCommitInterval: 5000,
	autoCommitThreshold: null,
	eachMessage: async ({ topic, partition, message }) => {
		console.log({
			topic,
			partition,
			offset: message.offset,
			key: message.key?.toString(),
			value: message.value?.toString(),
			headers: message.headers,
		})
	},
})
```

### @kafkats/client

```typescript
const consumer = client.consumer({
	groupId: 'my-group',
	sessionTimeoutMs: 30000,
	rebalanceTimeoutMs: 60000,
	heartbeatIntervalMs: 3000,
	maxBytesPerPartition: 1048576,
	minBytes: 1,
	maxWaitMs: 5000,
	autoOffsetReset: 'earliest', // 'earliest' | 'latest' | 'none'
	isolationLevel: 'read_committed', // 'read_committed' | 'read_uncommitted'
})

// No explicit connect() or subscribe() - all handled by runEach()

await consumer.runEach(
	'my-topic',
	async (message, ctx) => {
		console.log({
			topic: message.topic,
			partition: message.partition,
			offset: message.offset,
			key: message.key?.toString(),
			value: message.value?.toString(),
			headers: message.headers,
		})
	},
	{
		autoCommit: true,
		autoCommitIntervalMs: 5000,
	}
)
```

### Consumer Configuration Mapping

| KafkaJS                 | @kafkats/client                      | Notes                                         |
| ----------------------- | ------------------------------------ | --------------------------------------------- |
| `groupId`               | `groupId`                            | Same                                          |
| `sessionTimeout`        | `sessionTimeoutMs`                   | Explicit `Ms` suffix                          |
| `rebalanceTimeout`      | `rebalanceTimeoutMs`                 | Explicit `Ms` suffix                          |
| `heartbeatInterval`     | `heartbeatIntervalMs`                | Explicit `Ms` suffix                          |
| `maxBytesPerPartition`  | `maxBytesPerPartition`               | Same                                          |
| `minBytes`              | `minBytes`                           | Same                                          |
| `maxWaitTimeInMs`       | `maxWaitMs`                          | Shorter name                                  |
| `readUncommitted: true` | `isolationLevel: 'read_uncommitted'` | More explicit                                 |
| `fromBeginning: true`   | `autoOffsetReset: 'earliest'`        | Per-consumer config                           |
| N/A                     | `partitionAssignmentStrategy`        | `'cooperative-sticky'`, `'sticky'`, `'range'` |

### Subscribe vs Direct Topic

**KafkaJS** requires explicit subscribe:

```typescript
await consumer.subscribe({ topics: ['topic-a', 'topic-b'] })
await consumer.run({ eachMessage: handler })
```

**@kafkats/client** combines subscribe and run:

```typescript
// Single topic
await consumer.runEach('my-topic', handler)

// Multiple topics
await consumer.runEach(['topic-a', 'topic-b'], handler)
```

### Batch Processing

**KafkaJS:**

```typescript
await consumer.run({
	eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning }) => {
		for (const message of batch.messages) {
			await processMessage(message)
			resolveOffset(message.offset)
			await heartbeat()
		}
	},
})
```

**@kafkats/client:**

```typescript
await consumer.runBatch(
	'my-topic',
	async (messages, ctx) => {
		for (const message of messages) {
			await processMessage(message)
			ctx.markConsumed(message.offset) // Mark offset for commit
		}
	},
	{
		autoCommit: true,
		autoCommitIntervalMs: 5000,
	}
)
```

::: tip Partial batch commits
The `ctx.markConsumed(offset)` function marks individual message offsets for commit. If your handler crashes partway through a batch, only the marked offsets will be committed on the next auto-commit cycle, preventing reprocessing of already-handled messages.

If you don't call `markConsumed()`, all offsets are committed when the batch handler completes successfully (backward compatible behavior).
:::

### Type-Safe Consumer

```typescript
import { topic, string, json } from '@kafkats/client'

interface OrderEvent {
	orderId: string
	amount: number
}

const orderTopic = topic('orders', {
	key: string(),
	value: json<OrderEvent>(),
})

// message.key is string, message.value is OrderEvent
await consumer.runEach(orderTopic, async message => {
	console.log(`Order ${message.value.orderId}: $${message.value.amount}`)
})
```

### Consumer Events

**KafkaJS:**

```typescript
const { GROUP_JOIN, REBALANCING } = consumer.events

consumer.on(GROUP_JOIN, e => {
	console.log('Joined group', e.payload.groupId)
})

consumer.on(REBALANCING, e => {
	console.log('Rebalancing...', e.payload.groupId)
})
```

**@kafkats/client:**

```typescript
consumer.on('running', () => {
	console.log('Consumer started')
})

consumer.on('partitionsAssigned', partitions => {
	console.log('Assigned partitions:', partitions)
})

consumer.on('partitionsRevoked', partitions => {
	console.log('Revoked partitions:', partitions)
})

consumer.on('stopped', () => {
	console.log('Consumer stopped')
})
```

### Pause/Resume

**KafkaJS:**

```typescript
consumer.pause([{ topic: 'my-topic', partitions: [0, 1] }])
consumer.resume([{ topic: 'my-topic', partitions: [0, 1] }])
```

**@kafkats/client:**

```typescript
consumer.pause([
	{ topic: 'my-topic', partition: 0 },
	{ topic: 'my-topic', partition: 1 },
])
consumer.resume([
	{ topic: 'my-topic', partition: 0 },
	{ topic: 'my-topic', partition: 1 },
])
```

### Stopping the Consumer

**KafkaJS:**

```typescript
await consumer.disconnect()
```

**@kafkats/client:**

```typescript
consumer.stop() // Signals graceful shutdown
// The runEach/runBatch promise resolves when stopped
```

## Transactions

### KafkaJS

```typescript
const producer = kafka.producer({
	transactionalId: 'my-txn-producer',
	maxInFlightRequests: 1,
	idempotent: true,
})

await producer.connect()

const transaction = await producer.transaction()

try {
	await transaction.send({ topic: 'topic-a', messages: [{ value: 'msg' }] })

	await transaction.sendOffsets({
		consumerGroupId: 'my-group',
		topics: [{ topic: 'input-topic', partitions: [{ partition: 0, offset: '100' }] }],
	})

	await transaction.commit()
} catch (e) {
	await transaction.abort()
	throw e
}
```

### @kafkats/client

```typescript
const producer = client.producer({
	transactionalId: 'my-txn-producer',
	maxInFlight: 1,
	idempotent: true,
})

// Automatic commit/abort based on callback success/failure
await producer.transaction(async tx => {
	await tx.send('topic-a', [{ value: Buffer.from('msg') }])

	await tx.sendOffsets({
		groupId: 'my-group',
		offsets: [{ topic: 'input-topic', partition: 0, offset: 100n }],
	})
})
```

## Admin API

### KafkaJS

```typescript
const admin = kafka.admin()
await admin.connect()

// List topics
const topics = await admin.listTopics()

// Create topics
await admin.createTopics({
	topics: [{ topic: 'new-topic', numPartitions: 3, replicationFactor: 2 }],
})

// Delete topics
await admin.deleteTopics({ topics: ['old-topic'] })

// Describe groups
const groups = await admin.describeGroups(['my-group'])

// Delete groups
await admin.deleteGroups(['old-group'])

await admin.disconnect()
```

### @kafkats/client

```typescript
const admin = client.admin()

// List topics
const topics = await admin.listTopics()

// Create topics
await admin.createTopics([{ name: 'new-topic', numPartitions: 3, replicationFactor: 2 }])

// Delete topics
await admin.deleteTopics(['old-topic'])

// Describe groups
const groups = await admin.describeGroups(['my-group'])

// Delete groups
await admin.deleteGroups(['old-group'])

// No `admin.disconnect()`; call `client.disconnect()` when shutting down
```

### Admin API Mapping

| KafkaJS                        | @kafkats/client              | Notes                  |
| ------------------------------ | ---------------------------- | ---------------------- |
| `admin.connect()`              | N/A                          | No explicit connect    |
| `admin.disconnect()`           | N/A                          | No explicit disconnect |
| `admin.listTopics()`           | `admin.listTopics()`         | Same                   |
| `admin.createTopics({topics})` | `admin.createTopics(topics)` | Direct array           |
| `admin.deleteTopics({topics})` | `admin.deleteTopics(topics)` | Direct array           |
| `admin.fetchTopicMetadata()`   | `admin.describeTopics()`     | Renamed                |
| `admin.listGroups()`           | `admin.listGroups()`         | Same                   |
| `admin.describeGroups()`       | `admin.describeGroups()`     | Same                   |
| `admin.deleteGroups()`         | `admin.deleteGroups()`       | Same                   |
| `admin.describeCluster()`      | `admin.describeCluster()`    | Same                   |
| `admin.fetchTopicOffsets()`    | `admin.fetchTopicOffsets()`  | Similar                |
| `admin.describeAcls()`         | `admin.describeAcls()`       | Same                   |
| `admin.createAcls()`           | `admin.createAcls()`         | Same                   |
| `admin.deleteAcls()`           | `admin.deleteAcls()`         | Same                   |

## Compression

### KafkaJS

```typescript
const { CompressionTypes, CompressionCodecs } = require('kafkajs')
const SnappyCodec = require('kafkajs-snappy')

CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec

await producer.send({
	topic: 'my-topic',
	compression: CompressionTypes.GZIP,
	messages: [{ value: 'data' }],
})
```

### @kafkats/client

```typescript
import snappy from 'snappy'
import { CompressionType, compressionCodecs, createSnappyCodec } from '@kafkats/client'

// GZIP works out of the box; Snappy/LZ4/Zstd require registering a codec.
compressionCodecs.register(CompressionType.Snappy, createSnappyCodec(snappy))

const producer = client.producer({
	compression: 'snappy', // 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd'
})

await producer.send('my-topic', [{ value: Buffer.from('data') }])
```

See the [Compression docs](/client/compression) for more details and supported libraries.

## Error Handling

### KafkaJS

```typescript
const { KafkaJSError, KafkaJSConnectionError } = require('kafkajs')

try {
	await producer.send({ topic: 'my-topic', messages: [] })
} catch (error) {
	if (error instanceof KafkaJSConnectionError) {
		console.log('Connection error')
	} else if (error.retriable) {
		console.log('Retriable error')
	}
}
```

### @kafkats/client

```typescript
import { KafkaError, KafkaProtocolError, ConnectionError, TimeoutError, isRetriable } from '@kafkats/client'

try {
	await producer.send('my-topic', [{ value: Buffer.from('data') }])
} catch (error) {
	if (error instanceof ConnectionError) {
		console.log('Connection error')
	} else if (error instanceof TimeoutError) {
		console.log('Request timed out')
	} else if (error instanceof KafkaProtocolError) {
		console.log('Protocol error:', error.errorCode)
	} else if (isRetriable(error)) {
		console.log('Retriable error')
	}
}
```

## Graceful Shutdown

### KafkaJS

```typescript
const shutdown = async () => {
	await consumer.disconnect()
	await producer.disconnect()
	await admin.disconnect()
}

process.on('SIGTERM', shutdown)
process.on('SIGINT', shutdown)
```

### @kafkats/client

```typescript
const consumerRun = consumer.runEach('my-topic', async message => {
	// ...
})

const shutdown = async () => {
	consumer.stop() // Graceful stop
	await consumerRun.catch(() => {}) // Optional: wait for runEach/runBatch to exit
	await producer.disconnect()
	await client.disconnect() // Close all broker connections
}

process.on('SIGTERM', shutdown)
process.on('SIGINT', shutdown)
```

## Migration Checklist

1. **Update imports**
    - `const { Kafka } = require('kafkajs')` → `import { KafkaClient } from '@kafkats/client'`

2. **Update client configuration**
    - `new Kafka({...})` → `new KafkaClient({...})`
    - Add `Ms` suffix to timeout options
    - Change `ssl` to `tls`

3. **Update producer code**
    - Remove `await producer.connect()`
    - Change message values to `Buffer` (or use typed topics)
    - Update `acks` to string values
    - Update compression to string values (`'gzip'` works out of the box; for `'snappy'`/`'lz4'`/`'zstd'` you must `npm install` a library and register the codec)

4. **Update consumer code**
    - Replace `subscribe()` + `run()` with `runEach()` or `runBatch()`
    - Change `fromBeginning` to `autoOffsetReset: 'earliest'`
    - Update event listeners

5. **Update admin code**
    - Remove `connect()` and `disconnect()` calls
    - Simplify method arguments (direct arrays instead of objects)

6. **Update error handling**
    - Import new error types
    - Update `instanceof` checks

7. **Update compression setup**
    - GZIP works without any packages
    - For Snappy/LZ4/Zstd: install a compression library (e.g. `npm install snappy`) and register via `compressionCodecs.register(...)` — see [Compression docs](/client/compression)

## Feature Differences

### Features in @kafkats/client not in KafkaJS

- **Type-safe topics** with compile-time checking
- **Share Groups** (KIP-932) for queue-like consumption
- **Cooperative sticky assignor** by default
- **Stream mode** with async iterators

### Features in KafkaJS not yet in @kafkats/client

- `consumer.commitOffsets()` for manual offset commits outside handlers (use `ctx.markConsumed()` inside batch handlers)
- Admin partition reassignment (`alterPartitionReassignments`)
- Advanced retry options (e.g., `multiplier`, `factor` for exponential backoff)

## Getting Help

If you encounter issues during migration:

- Check the [full documentation](/client/)
- Review the [examples](/examples/)
- Open an issue on [GitHub](https://github.com/chrisrecalis/kafkats/issues)
