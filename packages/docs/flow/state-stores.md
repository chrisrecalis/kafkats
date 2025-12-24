# State Stores

State stores maintain the state needed for stateful operations like aggregations, joins, and windowing.

## State Store Types

| Store Type    | Use Case                                        |
| ------------- | ----------------------------------------------- |
| KeyValueStore | Simple key-value lookups (tables, aggregations) |
| WindowStore   | Time-windowed state (windowed aggregations)     |
| SessionStore  | Session-windowed state (session aggregations)   |

## Built-in: In-Memory Store

Default store for development and testing:

```typescript
import { flow, topic, inMemory } from '@kafkats/flow'
import { string, json } from '@kafkats/client'

const app = flow({
	applicationId: 'my-app',
	client: { clientId: 'my-app', brokers: ['localhost:9092'] },
	stateStoreProvider: inMemory(), // Default
})
```

::: warning
In-memory stores are lost on restart. Use LMDB for persistence.
:::

## Persistent: LMDB Store

For production use, install the LMDB provider:

```bash
pnpm add @kafkats/flow-state-lmdb
```

```typescript
import { flow, topic } from '@kafkats/flow'
import { string, json } from '@kafkats/client'
import { lmdb } from '@kafkats/flow-state-lmdb'

const app = flow({
	applicationId: 'my-app',
	client: { clientId: 'my-app', brokers: ['localhost:9092'] },
	stateStoreProvider: lmdb({
		stateDir: './state',
		mapSize: 1024 * 1024 * 1024, // 1GB max size
	}),
})
```

## Store Interfaces

### KeyValueStore

```typescript
interface KeyValueStore<K, V> {
	get(key: K): Promise<V | undefined>
	put(key: K, value: V): Promise<void>
	delete(key: K): Promise<void>
	all(): AsyncIterable<{ key: K; value: V }>
	range(from: K, to: K): AsyncIterable<{ key: K; value: V }>
}
```

### WindowStore

```typescript
interface WindowStore<K, V> {
	get(key: K, windowStart: number): Promise<V | undefined>
	put(key: K, value: V, windowStart: number): Promise<void>
	fetch(key: K, from: number, to: number): AsyncIterable<{ windowStart: number; value: V }>
	fetchAll(from: number, to: number): AsyncIterable<{ key: K; windowStart: number; value: V }>
}
```

### SessionStore

```typescript
interface SessionStore<K, V> {
	get(key: K, sessionStart: number, sessionEnd: number): Promise<V | undefined>
	put(key: K, value: V, sessionStart: number, sessionEnd: number): Promise<void>
	findSessions(
		key: K,
		from: number,
		to: number
	): AsyncIterable<{
		sessionStart: number
		sessionEnd: number
		value: V
	}>
}
```

## Materialization

Name stores for later querying:

```typescript
// Named store for counts
const counts = stream.groupByKey().count({
	materialized: { storeName: 'user-counts' },
})

// Query the store
const store = app.getStore<string, number>('user-counts')
const count = await store.get('user-123')
```

## Custom Store Provider

Implement your own store provider:

```typescript
interface StateStoreProvider {
	createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>): KeyValueStore<K, V>

	createWindowStore<K, V>(name: string, options: WindowStoreOptions<K, V>): WindowStore<K, V>

	createSessionStore<K, V>(name: string, options: SessionStoreOptions<K, V>): SessionStore<K, V>

	close(): Promise<void>
}
```

Example Redis provider skeleton:

```typescript
import { StateStoreProvider, KeyValueStore } from '@kafkats/flow'
import Redis from 'ioredis'

class RedisStateStoreProvider implements StateStoreProvider {
	private redis: Redis

	constructor(options: { url: string }) {
		this.redis = new Redis(options.url)
	}

	createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>) {
		return new RedisKeyValueStore<K, V>(this.redis, name, options)
	}

	// ... implement other methods

	async close() {
		await this.redis.quit()
	}
}
```

## Store Options

### KeyValueStoreOptions

| Option       | Type       | Required | Description                                |
| ------------ | ---------- | -------- | ------------------------------------------ |
| `keyCodec`   | `Codec<K>` | Yes      | Codec for serializing/deserializing keys   |
| `valueCodec` | `Codec<V>` | Yes      | Codec for serializing/deserializing values |

### WindowStoreOptions

| Option         | Type       | Required | Description                                |
| -------------- | ---------- | -------- | ------------------------------------------ |
| `keyCodec`     | `Codec<K>` | Yes      | Codec for keys                             |
| `valueCodec`   | `Codec<V>` | Yes      | Codec for values                           |
| `windowSizeMs` | `number`   | Yes      | Window size in milliseconds                |
| `retentionMs`  | `number`   | Yes      | How long to retain windows in milliseconds |

### SessionStoreOptions

| Option        | Type       | Required | Description                                 |
| ------------- | ---------- | -------- | ------------------------------------------- |
| `keyCodec`    | `Codec<K>` | Yes      | Codec for keys                              |
| `valueCodec`  | `Codec<V>` | Yes      | Codec for values                            |
| `retentionMs` | `number`   | Yes      | How long to retain sessions in milliseconds |

## Interactive Queries

Query state stores while the application runs:

```typescript
// Materialize the aggregation
const userCounts = stream.groupByKey().count({
	materialized: { storeName: 'user-counts' },
})

// Start the app
await app.start()

// Query the store
const store = app.getStore<string, number>('user-counts')

// HTTP endpoint example
app.get('/users/:id/count', async (req, res) => {
	const count = await store.get(req.params.id)
	res.json({ userId: req.params.id, count: count ?? 0 })
})
```

## Changelog Topics

State stores are backed by **changelog topics** for fault tolerance. Every state mutation is written to a Kafka topic, enabling state restoration after restarts or failures.

### Topic Naming

Changelog topics follow this naming convention:

```
{applicationId}-{storeName}-changelog
```

For example, an app with `applicationId: 'my-app'` and a store named `user-counts` creates:

```
my-app-user-counts-changelog
```

### Partition Count Inference

Changelog topics must have the **same number of partitions** as the source topic(s) to maintain data locality. When Task N processes partition N of the source topic, it must write to partition N of the changelog topic.

The partition count is automatically inferred:

```typescript
// Source topic has 8 partitions
// → Changelog topic created with 8 partitions
app.stream('orders', { key: codec.string(), value: codec.json<Order>() })
	.groupByKey()
	.count() // Changelog: my-app-count-store-0-changelog (8 partitions)
```

For merged streams, the **maximum** partition count is used:

```typescript
const stream1 = app.stream('topic-a') // 4 partitions
const stream2 = app.stream('topic-b') // 8 partitions

stream1
	.merge(stream2)
	.groupByKey()
	.count() // Changelog created with 8 partitions (max)
```

### Validation

On startup, existing changelog topics are validated:

- If the changelog exists with the **correct** partition count → processing continues
- If the changelog exists with the **wrong** partition count → throws `ChangelogPartitionMismatchError`
- If the changelog doesn't exist → created automatically (unless `autoCreate: false`)

```typescript
import { ChangelogPartitionMismatchError, SourceTopicNotFoundError } from '@kafkats/flow'

try {
	await app.start()
} catch (err) {
	if (err instanceof ChangelogPartitionMismatchError) {
		console.error(`Changelog ${err.changelogTopic} has ${err.actualPartitions} partitions`)
		console.error(`Expected ${err.expectedPartitions} based on source topics: ${err.sourceTopics}`)
		// Fix: Delete the changelog topic and restart, or recreate with correct partition count
	}
	if (err instanceof SourceTopicNotFoundError) {
		console.error(`Source topic ${err.topic} doesn't exist for store ${err.storeName}`)
		// Fix: Create the source topic first
	}
}
```

### Global Configuration

Configure changelog behavior for all state stores:

```typescript
const app = flow({
	applicationId: 'my-app',
	client: { brokers: ['localhost:9092'] },
	changelog: {
		// Replication factor for all changelog topics
		replicationFactor: 3,

		// Additional topic configs applied to all changelogs
		topicConfigs: {
			'min.insync.replicas': '2',
			'segment.bytes': '104857600', // 100MB
		},

		// Set to false to skip auto-creation (production safety)
		autoCreate: false,
	},
})
```

::: tip Production Recommendation
Set `autoCreate: false` in production and pre-create changelog topics with your infrastructure tooling. This prevents accidental topic creation with incorrect settings.
:::

### Per-Store Configuration

Configure changelog settings for individual state stores:

```typescript
stream.groupByKey().count({
	storeName: 'user-counts',
	changelog: {
		// Custom topic name (default: {appId}-{storeName}-changelog)
		topicName: 'my-custom-changelog',

		// Replication factor for this store
		replicationFactor: 2,

		// Custom topic configs
		topicConfigs: {
			'retention.ms': '604800000', // 7 days
		},

		// Skip restoration on startup (not recommended)
		skipRestoration: false,
	},
})
```

### Disabling Changelogs

For ephemeral state that doesn't need persistence:

```typescript
stream.groupByKey().count({
	changelog: false, // No changelog topic created
})
```

::: warning
Without a changelog, state is lost on restart. Only disable for truly ephemeral computations.
:::

### Default Topic Configs

Changelog topics are created with these defaults (optimized for state stores):

| Config           | Value     | Description                        |
| ---------------- | --------- | ---------------------------------- |
| `cleanup.policy` | `compact` | Keep only latest value per key     |
| `retention.ms`   | `-1`      | Infinite retention                 |
| `segment.bytes`  | `50MB`    | Smaller segments for faster replay |

### State Restoration

On restart:

1. State store is restored from changelog topic
2. Consumer reads from beginning to end of changelog
3. Each record updates the local state store
4. Processing resumes from last committed offset

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ Changelog Topic │────▶│ State Restoration│────▶│ Local Store     │
│ (Kafka)         │     │ Consumer         │     │ (Memory/LMDB)   │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

## Store Cleanup

Clean up state when processing:

```typescript
// Tombstone (null value) deletes from store
await producer.send(topic, [{ key: 'user-123', value: null }])
```

For windowed stores, old windows are automatically cleaned based on retention.

## Best Practices

1. **Name stores** - Use meaningful names for queryability
2. **Use LMDB in production** - For persistence and performance
3. **Configure retention** - Don't keep state forever
4. **Monitor size** - Watch state store disk usage
5. **Backup state** - Regularly back up LMDB directories
6. **Pre-create changelogs in production** - Use `autoCreate: false` and create topics via infrastructure tooling
7. **Match partition counts** - Ensure changelog partitions match source topic partitions
8. **Set replication factor** - Use `replicationFactor: 3` for production durability
9. **Handle startup errors** - Catch `ChangelogPartitionMismatchError` and `SourceTopicNotFoundError`
