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
In-memory stores are lost on restart and rebuilt from the changelog. Use LMDB or RocksDB for persistence.
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
		maxDbs: 4096, // max number of named databases (default 4096)
	}),
})
```

Every partition of every store is a separate LMDB database (see [Partitioned State](#partitioned-state)), so `maxDbs` needs to cover the number of stores times the number of partitions.

## Persistent: RocksDB Store

```bash
pnpm add @kafkats/flow-state-rocksdb
```

```typescript
import { rocksdb } from '@kafkats/flow-state-rocksdb'

const app = flow({
	applicationId: 'my-app',
	client: { clientId: 'my-app', brokers: ['localhost:9092'] },
	stateStoreProvider: rocksdb({ stateDir: './state' }),
})
```

RocksDB appends writes to a log and an in-memory table instead of waiting for the disk on every write, and keeps state in compressed, sorted files. Prefer it for write-heavy topologies (aggregations under `at_least_once` in particular) and for state larger than memory. See [@kafkats/flow-state-rocksdb](/flow-state-rocksdb/).

## Store Interfaces

### KeyValueStore

```typescript
interface KeyValueStore<K, V> {
	readonly name: string
	get(key: K): Promise<V | undefined>
	put(key: K, value: V): Promise<void>
	delete(key: K): Promise<void>
	all(): AsyncIterable<[K, V]>
	range(from: K, to: K): AsyncIterable<[K, V]>
	approximateNumEntries(): Promise<number>
	init(): Promise<void>
	flush(): Promise<void>
	close(): Promise<void>
}
```

### WindowStore

Window and session stores are key-value stores keyed by `WindowedKey<K>`:

```typescript
interface WindowedKey<K> {
	key: K
	windowStart: number
	windowEnd: number
}

interface WindowStore<K, V> extends KeyValueStore<WindowedKey<K>, V> {
	fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]>
	fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]>
	fetchRange(keyFrom: K, keyTo: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]>
	expireOldWindows(currentTime: number): Promise<number>
}
```

### SessionStore

```typescript
interface SessionStore<K, V> extends KeyValueStore<WindowedKey<K>, V> {
	findSessions(key: K, earliestStart: number, latestEnd: number): AsyncIterable<[WindowedKey<K>, V]>
	remove(key: K): Promise<void>
	expireOldSessions(currentTime: number): Promise<WindowedKey<K>[]>
}
```

## Materialization

Name stores so that their changelog topics keep stable names:

```typescript
// Named store for counts (changelog: <applicationId>-user-counts-changelog)
const counts = stream.groupByKey().count({
	storeName: 'user-counts',
})
```

## Custom Store Provider

Implement your own store provider:

```typescript
interface StateStoreProvider {
	readonly name: string

	// Optional: durable checkpoints so changelog restoration can resume
	getChangelogCheckpointStore?(): ChangelogCheckpointStore | undefined

	createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>): KeyValueStore<K, V>

	createWindowStore<K, V>(name: string, options: WindowStoreOptions<K, V>): WindowStore<K, V>

	createSessionStore<K, V>(name: string, options: SessionStoreOptions<K, V>): SessionStore<K, V>

	close(): Promise<void>
}
```

Example Redis provider skeleton:

```typescript
import type { StateStoreProvider, KeyValueStore, KeyValueStoreOptions } from '@kafkats/flow'
import Redis from 'ioredis'

class RedisStateStoreProvider implements StateStoreProvider {
	readonly name = 'redis'
	private redis: Redis

	constructor(options: { url: string }) {
		this.redis = new Redis(options.url)
	}

	createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>): KeyValueStore<K, V> {
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

Interactive queries are not supported: `FlowApp` does not expose its state stores. To serve aggregation results, write them to a topic and read them from there:

```typescript
stream.groupByKey().count({ storeName: 'user-counts' }).toStream().to('user-counts')
```

## Partitioned State

State is partitioned like the input. Each source partition has its **own slice of every state store**: state partition `N` is written to changelog partition `N` and restored from it. When partitions move between instances, their state moves with them.

Provider-level store names include the application id and the partition. A store named `user-counts` in the application `my-app` that has seen partitions 0 and 3 is two stores in the provider:

```
my-app.user-counts.p0
my-app.user-counts.p3
```

This matters if you run a custom provider or inspect an LMDB directory. With LMDB, each of these is a named database, which counts towards `maxDbs` (default `4096`).

### Co-partitioning

Because state is per partition, all topics that feed one stateful operation must have the **same number of partitions**. That includes:

- streams combined with `merge()` before an aggregation
- both sides of a stream-stream or table-table join
- the stream and the table of a stream-table join

Records with the same key must also be in the same partition index in each topic, which means the topics must be written with the same partitioner.

If the partition counts differ, `start()` throws `CoPartitioningError`:

```typescript
import { CoPartitioningError } from '@kafkats/flow'

try {
	await app.start()
} catch (err) {
	if (err instanceof CoPartitioningError) {
		console.error(err.partitionCounts) // Map { 'orders' => 6, 'users' => 3 }
	}
}
```

Fix it by repartitioning one side with [`through()`](/flow/streams#through) into a topic with the right partition count, or use a [global table](/flow/tables#global-tables) for lookups, which has no co-partitioning requirement.

Streams that were re-keyed with `selectKey()`, `map()` or `groupBy()` are repartitioned automatically. See [Repartitioning](/flow/streams#repartitioning).

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

Changelog topics must have the **same number of partitions** as the source topic(s) to maintain data locality. State partition N (from source partition N) is written to partition N of the changelog topic, whatever the record key hashes to.

The partition count is automatically inferred:

```typescript
// Source topic has 8 partitions
// → Changelog topic created with 8 partitions
app.stream('orders', { key: codec.string(), value: codec.json<Order>() }).groupByKey().count() // Changelog: my-app-count-store-0-changelog (8 partitions)
```

Merged streams feeding one stateful operation must have the same partition count (see [Co-partitioning](#co-partitioning)):

```typescript
const stream1 = app.stream('topic-a') // 4 partitions
const stream2 = app.stream('topic-b') // 8 partitions

stream1.merge(stream2).groupByKey().count() // start() throws CoPartitioningError
```

### Validation

On startup, existing changelog topics are validated:

- If the changelog exists with the **correct** partition count → processing continues
- If the changelog exists with the **wrong** partition count → throws `ChangelogPartitionMismatchError`
- If the changelog doesn't exist → created automatically (unless `autoCreate: false`)
- If topics that must be co-partitioned have different partition counts → throws `CoPartitioningError`

With `autoCreate: false`, Flow does not create changelog topics, and [repartition topics](/flow/streams#repartitioning) must be created up front. A missing repartition topic or `through()` topic fails `start()` with `SourceTopicNotFoundError`.

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
	client: { clientId: 'my-app', brokers: ['localhost:9092'] },
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

On every partition assignment (startup, rebalance, restart after a failure):

1. The newly assigned partitions are paused
2. The state of each partition is restored from the changelog partition with the same index, up to the end offset at the time the restore started
3. Each record updates the local state store
4. The partitions resume from the last committed offset

If the store is durable (LMDB), a checkpoint lets the restore resume where it stopped. A restore that makes no progress for `changelog.restoration.idleTimeoutMs` (default 30 seconds) fails instead of completing with partial state. See [Fault Tolerance](/flow/fault-tolerance#state-restoration) for details.

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

1. **Name stores** - Use meaningful, stable names for stores and their changelog topics
2. **Use LMDB in production** - For persistence and performance
3. **Configure retention** - Don't keep state forever
4. **Monitor size** - Watch state store disk usage
5. **Backup state** - Regularly back up LMDB directories
6. **Pre-create changelogs in production** - Use `autoCreate: false` and create topics via infrastructure tooling
7. **Match partition counts** - Ensure changelog partitions match source topic partitions, and that co-partitioned inputs have the same partition count
8. **Set replication factor** - Use `replicationFactor: 3` for production durability
9. **Handle startup errors** - Catch `ChangelogPartitionMismatchError`, `CoPartitioningError` and `SourceTopicNotFoundError`
10. **Size `maxDbs` for LMDB** - Each store has one database per partition
