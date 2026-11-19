# State Stores

Stateful operations keep their state in state stores: tables, aggregations, join buffers, suppression buffers, and the stores of your own processors. Every task has its own instance of each store of its subtopology, for its partition; a store is backed by a changelog topic, from which the instance is restored wherever the task runs next.

## Providers

A state store provider decides where stores live. One provider serves the whole app:

| Provider                                        | Package                       | Durable across restarts | Use                                      |
| ----------------------------------------------- | ----------------------------- | ----------------------- | ---------------------------------------- |
| `inMemory()` (default)                          | `@kafkats/flow`               | no                      | Tests, small state, fast restore         |
| [`lmdb({ stateDir })`](/flow-state-lmdb/)       | `@kafkats/flow-state-lmdb`    | yes                     | Read-heavy state, memory-mapped          |
| [`rocksdb({ stateDir })`](/flow-state-rocksdb/) | `@kafkats/flow-state-rocksdb` | yes                     | Write-heavy state, state larger than RAM |

```typescript
import { lmdb } from '@kafkats/flow-state-lmdb'

const app = flow({
	applicationId: 'my-app',
	client: { clientId: 'my-app', brokers: ['localhost:9092'] },
	stateStoreProvider: lmdb({ stateDir: '/var/lib/my-app' }),
})
```

With `inMemory()`, a restarted process restores all its state from the changelogs. A persistent provider also stores **checkpoints** (how far each store reflects its changelog), so a restarted instance only reads the changelog records written since. Do not let two running instances share one state directory.

Provider-level store names are `<applicationId>.<storeName>.p<partition>`: one store per store of the topology and task partition (so an LMDB environment needs `maxDbs` above the number of stores times the partitions an instance holds or held, plus one for checkpoints and one per global table; the default is 4096).

## Store Types

| Type                                     | Holds                                     | Used by                                               |
| ---------------------------------------- | ----------------------------------------- | ----------------------------------------------------- |
| `KeyValueStore`                          | A value per key                           | Your processors, suppression buffers                  |
| `TimestampedKeyValueStore`               | A value and its record timestamp per key  | Tables, aggregations, your processors                 |
| `WindowStore` / `TimestampedWindowStore` | Values per key and window `[start, end)`  | Time window aggregations, your processors             |
| `SessionStore`                           | Values per key and session `[start, end]` | Session and sliding window aggregations, join buffers |

Stores of tables and aggregations are timestamped: their values carry the timestamp of the record that last updated them, encoded `[int64 BE timestamp][value]`, as Kafka Streams' timestamped stores.

```typescript
// Every store also has the StateStore members: name, init(), flush(), close()
interface KeyValueStore<K, V> {
	get(key: K): Promise<V | undefined>
	put(key: K, value: V): Promise<void>
	delete(key: K): Promise<void>
	all(): AsyncIterable<[K, V]>
	range(from: K, to: K): AsyncIterable<[K, V]> // inclusive, by encoded key bytes
	approximateNumEntries(): Promise<number>
}

interface WindowStore<K, V> extends KeyValueStore<WindowedKey<K>, V> {
	fetch(key: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]> // windows by start, in time order
	fetchAll(timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]>
	fetchRange(keyFrom: K, keyTo: K, timeFrom: number, timeTo: number): AsyncIterable<[WindowedKey<K>, V]>
	expireOldWindows(currentTime: number): Promise<number>
}

interface SessionStore<K, V> extends KeyValueStore<WindowedKey<K>, V> {
	// The sessions that end at or after earliestSessionEnd and start at or before latestSessionStart.
	findSessions(key: K, earliestSessionEnd: number, latestSessionStart: number): AsyncIterable<[WindowedKey<K>, V]>
	remove(key: K): Promise<void>
	expireOldSessions(currentTime: number): Promise<WindowedKey<K>[]>
}

interface WindowedKey<K> {
	key: K
	windowStart: number
	windowEnd: number
}
```

Your own processors create stores with `Stores` and use them through their context; see the [Processor API](/flow/processor-api#state-stores).

## Changelog Topics

Each store is backed by a changelog topic: every write of a task's store instance is also sent to partition N of `<applicationId>-<storeName>-changelog` (N being the task's partition), in the same transaction as the task's outputs under exactly-once. Restoring a store replays its changelog partition.

### Topic configs

| Store                     | `cleanup.policy` | `retention.ms`          |
| ------------------------- | ---------------- | ----------------------- |
| Key-value stores          | `compact`        | `-1`                    |
| Window and session stores | `delete,compact` | store retention + 1 day |

Both get `segment.bytes=52428800`. Window stores expire old windows locally without writing tombstones, as Kafka Streams does; the changelog's time retention drops them on the broker. The configs are, in order of precedence: the store's own `topicConfigs`, then `FlowConfig.changelog.topicConfigs`, then these defaults, except the `retention.ms` of a window or session store, which only its own `topicConfigs` replace (as in Kafka Streams): an application-wide `retention.ms`, set to bound repartition topics, never cuts a window's changelog below the store's retention.

### Partition counts

A changelog topic has as many partitions as its subtopology has tasks: the largest partition count of the subtopology's input topics. It cannot be configured. An existing changelog or repartition topic with another count fails `start()` with `ChangelogPartitionMismatchError`.

### Per-store settings

```typescript
stream.groupByKey().count({
	storeName: 'clicks-per-user',
	changelog: {
		topicName: 'clicks-per-user-changelog', // default <applicationId>-<storeName>-changelog
		replicationFactor: 3,
		topicConfigs: { 'min.insync.replicas': '2' },
	},
})
```

| `ChangelogConfig`   | Description                                                                   |
| ------------------- | ----------------------------------------------------------------------------- |
| `enabled`           | `false` disables the changelog (as `changelog: false`).                       |
| `topicName`         | Another topic name.                                                           |
| `replicationFactor` | Default: `FlowConfig.changelog.replicationFactor`, else the broker's default. |
| `topicConfigs`      | Merged over the defaults and the app's `changelog.topicConfigs`.              |
| `skipRestoration`   | Do not restore the store from its changelog (it is still written).            |

A store without a changelog is not restored: wherever its task runs next, the store starts from what the local provider holds (nothing, with `inMemory()`).

### Creation and validation

`start()` creates missing changelog and repartition topics, with `FlowConfig.changelog.replicationFactor` and the configs above. In production, where topics are created ahead, set `changelog.autoCreate: false`: `start()` then only checks that every internal topic exists with its partition count, and fails with `InternalTopicNotFoundError` naming the missing one, its kind (`'changelog'` or `'repartition'`) and the partition count it needs.

```typescript
flow({
	applicationId: 'my-app',
	client,
	changelog: { replicationFactor: 3, topicConfigs: { 'min.insync.replicas': '2' }, autoCreate: false },
})
```

## Restoration

When a task with state is assigned, its partitions stay paused while its stores are restored from their changelogs, then it starts processing. `start()` resolves once the tasks of the first assignment are restored and running; the app is `REBALANCING` while any assigned task restores.

- A restore reads each changelog partition, with `read_committed`, up to the end (the high watermark) it had when the restore began, so exactly-once state is restored up to the last committed transaction. The records of a transaction left open are waited out until the broker aborts it; under exactly-once an instance's own producer aborts the one its previous incarnation (same transactional id) left open before it restores anything.
- A checkpoint is trusted only when it is within the changelog's offsets and the local store is not empty. Otherwise the store is wiped and rebuilt from the beginning of the changelog. Restores write progress checkpoints as they go.
- A restore that makes no progress for `changelog.restoration.idleTimeoutMs` (default 30 s) fails: during `start()`, `start()` rejects with it; later, the instance rejoins the group and tries again (see [Fault Tolerance](/flow/fault-tolerance#failure-handling)). A changelog whose records were deleted below the restore's position (out of range) makes the task wipe its stores and rebuild them.
- At most `changelog.restoration.maxConcurrentTasks` tasks (default 4) restore at once; tasks that are running keep processing meanwhile.

A task that comes back to an instance after a rebalance finds its store instance as it left it (stores stay open across rebalances in one process), and only catches up from its checkpoint. With `inMemory()`, the store instances of partitions another instance owns now are cleared 30 seconds after every assigned task is running. Persistent providers keep them for `stateCleanupDelayMs` (default 10 minutes, Kafka Streams' `state.cleanup.delay.ms`) after their task left, then clear them and invalidate their checkpoints. The state an earlier start left of partitions no task of this start took is dropped after the same delay: stores without a changelog or with `skipRestoration` are cleared, and the other logged stores, left unopened, have their checkpoints invalidated and are recorded as dropped, so that their next owner clears them first, also when their changelog settings changed meanwhile. A task that comes back later starts from empty stores and rebuilds the logged ones from the changelog: an older checkpoint could be trusted over a compacted changelog, whose replay would miss the deletes compaction removed. A task assigned while its store is being cleared does not wait for the clear: its restore finishes it before the task processes anything. A clear that `close()` or a crash stopped (a cleanup's, or a restore's before it rebuilds the store) is finished the same way by the store's next owner after a restart, also when the store's changelog settings changed meanwhile: a persistent provider's checkpoint store records the clear until it is done.

## Exactly-Once State

Under `exactly_once`, store writes of a task are buffered until its transaction commits, then applied to the store. An aborted transaction (or a crash) never leaves uncommitted updates in a store, also with persistent providers. Reads see the task's own buffered writes.

## Co-partitioning

Processors that share a store run in one task per partition, so the topics feeding a stateful subtopology must have the same partition count; `start()` throws `CoPartitioningError` otherwise. Streams re-keyed before a stateful operation are repartitioned to the right count automatically. See [Topology and Naming](/flow/topology).

## Interactive Queries

Interactive queries are not available: an app does not expose its stores to other code. To serve results, write them to a topic, or read them from the `TestDriver` in tests (`ctx.getStateStore()`).

## Custom Providers

A provider implements `StateStoreProvider`:

```typescript
interface StateStoreProvider {
	readonly name: string
	/** Durable checkpoints, so restoration resumes where the local state is. */
	getChangelogCheckpointStore?(): ChangelogCheckpointStore | undefined
	createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>): KeyValueStore<K, V>
	createWindowStore<K, V>(name: string, options: WindowStoreOptions<K, V>): WindowStore<K, V>
	createSessionStore<K, V>(name: string, options: SessionStoreOptions<K, V>): SessionStore<K, V>
	close(): Promise<void>
}
```

For an engine that maps byte keys to byte values in byte order (as LMDB and RocksDB do), `@kafkats/flow/state` provides the stores on top of an `OrderedByteStore`: `OrderedKeyValueStore`, `OrderedWindowStore`, `OrderedSessionStore` and `OrderedChangelogCheckpointStore`. The LMDB and RocksDB providers are built from them. The shared test suites the providers pass are in the repository's `packages/flow-state-testkit`.

## Best Practices

1. **Name your stores** (`storeName`, `Stores` definitions): state and changelogs are keyed by store names, and generated names shift when the topology changes.
2. **Use a persistent provider in production** for large state, to restart without replaying whole changelogs.
3. **Pre-create internal topics** and set `changelog.autoCreate: false` where topic creation is controlled.
4. **Set `replicationFactor` and `min.insync.replicas`** for changelogs as for any topic you cannot lose.
5. **Bound windowed state** with grace periods and the new window factories; the deprecated ones keep a day of windows.
