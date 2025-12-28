# Getting Started with @kafkats/flow

## Installation

```bash
pnpm add @kafkats/flow
```

## Creating a Flow Application

```typescript
import { flow, topic } from '@kafkats/flow'
import { string, json } from '@kafkats/client'

const app = flow({
	applicationId: 'my-stream-app',
	client: {
		clientId: 'my-stream-app',
		brokers: ['localhost:9092'],
	},
})
```

### Configuration Options

| Option                | Type                                | Default           | Description                                                                                   |
| --------------------- | ----------------------------------- | ----------------- | --------------------------------------------------------------------------------------------- |
| `applicationId`       | `string`                            | -                 | Required. Also used as the consumer group id                                                  |
| `client`              | `KafkaClient \| KafkaClientConfig`  | -                 | Required. Pass an existing client or a config object                                          |
| `numStreamThreads`    | `number`                            | `1`               | Number of parallel stream threads (each has its own producer/consumer)                        |
| `processingGuarantee` | `'at_least_once' \| 'exactly_once'` | `'at_least_once'` | Enables transactional processing when `'exactly_once'`                                        |
| `commitIntervalMs`    | `number`                            | `100`             | Transaction commit interval in milliseconds (only applies to `exactly_once`)                  |
| `stateDir`            | `string`                            | -                 | State directory (used by some store providers)                                                |
| `stateStoreProvider`  | `StateStoreProvider`                | in-memory         | State store backend (in-memory by default)                                                    |
| `changelog`           | `object`                            | -                 | Changelog topic settings: `replicationFactor`, `topicConfigs`, `autoCreate`                   |
| `consumer`            | `Omit<ConsumerConfig, 'groupId'>`   | -                 | Consumer overrides (Flow sets `groupId` to `applicationId`)                                   |
| `producer`            | `ProducerConfig`                    | -                 | Producer overrides                                                                            |
| `runEach`             | `RunEachOptions`                    | -                 | Consumer run-loop options (e.g. `partitionConcurrency`, `autoCommitIntervalMs`, `assignment`) |

#### Client Config (KafkaClientConfig)

If you pass a config object as `client`, it uses the same options as `new KafkaClient({...})` in `@kafkats/client`:

| Option     | Type         | Notes                                               |
| ---------- | ------------ | --------------------------------------------------- |
| `brokers`  | `string[]`   | Required                                            |
| `clientId` | `string`     | Optional (Flow may set a default)                   |
| `tls`      | `TlsConfig`  | Omit for plaintext; use `{ enabled: true }` for TLS |
| `sasl`     | `SaslConfig` | SASL authentication                                 |

## Processing Guarantees

Flow supports two processing guarantees:

### At-Least-Once (Default)

Messages are processed at least once. In case of failures, some messages may be reprocessed.

```typescript
const app = flow({
	applicationId: 'my-app',
	client: { brokers: ['localhost:9092'] },
	processingGuarantee: 'at_least_once', // default
})
```

Consumer offsets are committed periodically (controlled by `autoCommitIntervalMs` in `runEach` options).

### Exactly-Once

Messages are processed exactly once using Kafka transactions. Output messages and consumer offset commits are atomic.

```typescript
const app = flow({
	applicationId: 'my-app',
	client: { brokers: ['localhost:9092'] },
	processingGuarantee: 'exactly_once',
	commitIntervalMs: 100, // optional, default 100ms
})
```

#### How Exactly-Once Works

Flow implements exactly-once semantics using batch-based transaction commits, similar to Kafka Streams:

1. **Batch Processing** - Multiple input messages are processed within a single transaction batch
2. **Periodic Commits** - Transactions commit at regular intervals (controlled by `commitIntervalMs`)
3. **Atomic Commits** - Each commit atomically writes output messages and commits consumer offsets

Transactions are also committed:

- When the application shuts down via `close()`
- When a consumer rebalance occurs (partitions are revoked)

#### Commit Interval Tuning

The `commitIntervalMs` setting controls the trade-off between latency and throughput:

| Value            | Latency | Throughput | Use Case                 |
| ---------------- | ------- | ---------- | ------------------------ |
| Lower (50-100ms) | Lower   | Lower      | Real-time processing     |
| Higher (500ms+)  | Higher  | Higher     | Batch-oriented workloads |

::: tip
The default of 100ms provides a good balance for most use cases. Kafka Streams uses a default of 30 seconds, but Flow uses a lower default for more responsive processing.
:::

#### Consumer Configuration

When using exactly-once, downstream consumers should use `read_committed` isolation to only see committed messages:

```typescript
const consumer = client.consumer({
	groupId: 'downstream-consumer',
	isolationLevel: 'read_committed',
})
```

## Defining Topics

Define typed topics for input and output:

```typescript
import { topic } from '@kafkats/flow'
import { string, json } from '@kafkats/client'

interface UserEvent {
	userId: string
	action: string
	timestamp: number
}

const events = topic('user-events', {
	key: string(),
	value: json<UserEvent>(),
})
```

## Building a Topology

### Stream Processing

```typescript
// Create a stream from a topic
app.stream(events)
	.filter((key, value) => value.action === 'purchase')
	.mapValues(value => ({ ...value, processed: true }))
	.to(outputTopic)
```

### Table Processing

```typescript
// Create a table (changelog) from a topic
const usersTable = app.table(usersTopic)

// Transform table values
usersTable
	.mapValues(user => ({ name: user.name, email: user.email }))
	.toStream()
	.to(userProfilesTopic)
```

### Aggregations

```typescript
app.stream(events).groupByKey().count().toStream().to(countsTopic)
```

## Starting and Stopping

```typescript
// Start the application
await app.start()

// Check state
console.log(app.state()) // 'RUNNING'

// Stop gracefully
await app.close()
```

### Application States

| State         | Description                      |
| ------------- | -------------------------------- |
| `CREATED`     | Application created, not started |
| `RUNNING`     | Processing messages              |
| `REBALANCING` | Consumer group rebalancing       |
| `ERROR`       | Fatal error occurred             |
| `STOPPED`     | Gracefully stopped               |

## Complete Example

```typescript
import { flow, topic, TimeWindows } from '@kafkats/flow'
import { string, json } from '@kafkats/client'

interface PageView {
	userId: string
	page: string
	timestamp: number
}

interface PageViewCount {
	page: string
	count: number
	windowStart: number
	windowEnd: number
}

// Define topics
const pageViews = topic('page-views', {
	key: string(),
	value: json<PageView>(),
})

const pageViewCounts = topic('page-view-counts', {
	key: string(),
	value: json<PageViewCount>(),
})

// Create app
const app = flow({
	applicationId: 'page-view-counter',
	client: { clientId: 'page-view-counter', brokers: ['localhost:9092'] },
})

// Build topology
app.stream(pageViews)
	// Rekey by page
	.selectKey((_, value) => value.page)
	// Group and window
	.groupByKey()
	.windowedBy(TimeWindows.of('1h'))
	.count()
	// Transform output
	.toStream()
	.map((windowedKey, count) => ({
		key: windowedKey.key,
		value: {
			page: windowedKey.key,
			count,
			windowStart: windowedKey.window.start,
			windowEnd: windowedKey.window.end,
		},
	}))
	.to(pageViewCounts)

// Handle shutdown
process.on('SIGTERM', async () => {
	await app.close()
})

// Start
await app.start()
console.log('Stream processing started')
```

## Next Steps

- [KStream Operations](/flow/streams) - Stream transformations
- [KTable Operations](/flow/tables) - Table transformations
- [Aggregations](/flow/aggregations) - Counting and reducing
- [Windowing](/flow/windowing) - Time-based processing
