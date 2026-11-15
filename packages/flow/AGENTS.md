# @kafkats/flow

Kafka Streams-like flow APIs built on top of @kafkats/client.

## Commands

```bash
pnpm build      # Build the package
pnpm test       # Run tests
pnpm typecheck  # Type checking
```

## Architecture

```
src/
├── flow.ts        # flow() / FlowAppImpl: DSL entry points, start/close lifecycle, wiring of runtime/
├── types.ts       # Public types (FlowApp, KStream, KTable, FlowConfig, ...)
├── changelog.ts   # Stable import path re-exporting changelog/
├── changelog/
│   ├── config.ts    # Changelog config, restoration options, topic naming
│   ├── errors.ts    # ChangelogPartitionMismatchError, SourceTopicNotFoundError, CoPartitioningError
│   ├── writer.ts    # ChangelogWriter, changelog key format for windowed stores
│   └── restorer.ts  # ChangelogRestorer: checkpoint trust, replay to the log end, stall detection
├── codec.ts       # Serialization codecs (string, json, buffer)
├── state.ts       # State store interfaces
├── state/
│   ├── memory.ts       # In-memory state store implementation
│   ├── partitioned.ts  # Per-partition slices of a store (<applicationId>.<storeName>.p<partition>)
│   ├── ordered.ts      # Stores over any byte-ordered engine (OrderedByteStore); used by the LMDB and RocksDB providers
│   ├── ordered-keys.ts # Key layouts of window and session stores in such an engine, and the ranges their queries scan
│   ├── buffered.ts     # Transactional write buffer (exactly_once)
│   └── changelog.ts    # Changelog-backed store wrappers
├── streams/       # KStream / KTable / grouped stream implementations (the DSL)
│   └── topology-builder.ts # TopologyBuilder: the only part of the app the DSL may use (stores, topology, output)
├── processors/    # Processor nodes (aggregations, joins, table nodes)
├── runtime/
│   ├── context.ts         # Record context (worker + source partition) via AsyncLocalStorage
│   ├── status.ts          # App state, last error, closing/started flags, shutdown signal
│   ├── errors.ts          # Failure classification (fatal vs recoverable), restart backoff
│   ├── topology.ts        # What the DSL registered (sources, internal topics, co-partition groups, global tables)
│   ├── store-registry.ts  # State stores: per-partition stacks, provider names, changelog specs/writers
│   ├── checkpoints.ts     # Changelog checkpoints (durable or in-process); pending checkpoints of an open transaction
│   ├── internal-topics.ts # Co-partitioning checks, creation of changelog/repartition topics
│   ├── restoration.ts     # Restore queue, app-/partition-scoped restore, release/prune of unowned state
│   ├── start-positions.ts # Start position of partitions without a committed offset
│   ├── transactions.ts    # Exactly-once batch of one worker (begin/commit/abort, commit timer)
│   ├── worker-pool.ts     # WorkerPool: starts/stops the stream workers, commit on close
│   ├── stream-worker.ts   # StreamWorker: run/restart loop, message handling, rebalance events
│   ├── worker-config.ts   # Per-worker producer/consumer/run config (transactional id, group instance id, timeouts)
│   ├── global-tables.ts   # Loading and following global tables
│   └── output.ts          # RecordOutput: encoding, partitioner, transactional send
├── testing.ts     # TestDriver, MockProducer/MockConsumer (`@kafkats/flow/testing`)
├── state-kit.ts   # What state store providers are built from (`@kafkats/flow/state`)
└── index.ts       # Public exports
```

### Key Abstractions

- **flow()**: Creates a FlowApp for building streaming topologies
- **KStream**: Unbounded stream of key-value records
- **KTable**: Changelog stream representing a table
- **KGroupedStream**: Stream grouped by key for aggregations
- **Windowing**: TimeWindows, SessionWindows (SlidingWindows is not implemented and throws)

### Runtime Model

- **Partitioned state**: every source partition has its own slice of every state store. State partition N is logged to changelog partition N and restored from it. Provider-level store names are `<applicationId>.<storeName>.p<partition>` (default LMDB `maxDbs` is 4096 for that reason).
- **Co-partitioning**: all topics feeding one stateful operation must have the same partition count, otherwise `start()` throws `CoPartitioningError`. Consumers always use the `range` assignor so partition N of every co-partitioned topic lands on the same worker.
- **Repartitioning**: `groupBy()`, and `selectKey()`/`map()` followed by `groupByKey()`, a join or `toTable()`, write through an internal topic `<applicationId>-<name | repartition-N>-repartition` and read it back. `through(topic)` does the same with a user topic. Both are read from the beginning when the group has no committed offset. Needs key and value codecs.
- **Global tables**: `globalTable()` is read in full by every instance outside the consumer group, loaded before processing starts, has no changelog and is only usable as the lookup side of a stream join.
- **Exactly-once**: offsets, outputs and changelog records commit in one transaction. Store writes are buffered (`state/buffered.ts`) and applied only after the commit; an abort or crash never leaves uncommitted updates in a store.
- **Fail-stop workers**: a worker that fails never continues past the failure. Recoverable failures (Kafka/infrastructure errors) abort the transaction, drop buffered state, rejoin and resume from committed offsets with backoff (0.5s to 30s), unless `restartOnError: false`. A broker-aborted transaction (fenced producer after `transactionTimeoutMs`) is recoverable too. User-code errors and fatal Kafka errors set `state()` to `'ERROR'`; see `app.getError()`.
- **Restoration**: newly assigned partitions are paused, restored from the changelog, then resumed. A restore with no progress for `changelog.restoration.idleTimeoutMs` (default 30000) fails. Checkpoints are ignored (wipe + rebuild) when outside the changelog offset range or when the local store is empty.
- **Stream time** is tracked per source partition (late-record dropping, window expiry). Clocks are in memory and are re-seeded from the restored window/session store after a restart, rebalance or abort.
- **Start position**: source partitions read from `'latest'` with no committed offset are pinned to the position the process first started at until the first commit. The pinned position is also committed (best effort) so a process restart resumes from it.
- **Deferred acknowledgements**: output and changelog sends are not awaited per record. Each worker collects them in `PendingSends` (`runtime/pending-sends.ts`) and flushes before committing: at the end of each consumed batch under at-least-once, before `sendOffsets` under exactly-once. A failed send fails the worker; an aborted transaction discards its pending sends (late acknowledgements are ignored).
- **Known limitations**: stream-stream left/outer joins emit the unmatched result eagerly; derived tables (`filter`/`mapValues`/joins/windowed results) are not materialized and cannot be a join lookup side.

User-facing documentation: `packages/docs/flow/fault-tolerance.md`.

### Codecs

```typescript
import { codec, string, json, buffer, type Codec } from '@kafkats/flow'

// Built-in codec factories (re-exported from @kafkats/client)
string()      // UTF-8 string
json<T>()     // JSON serialization
buffer()      // Raw Buffer passthrough
codec.json<T>() // `codec` is a namespace of the same factories

// Custom codec: any object implementing Codec<T>
const myCodec: Codec<MyType> = {
  encode: (value) => Buffer.from(...),
  decode: (buffer) => ...,
}
```

## Example

```typescript
import { flow, topic, string, json } from '@kafkats/flow'

const app = flow({
	applicationId: 'my-app',
	client: { clientId: 'my-app', brokers: ['localhost:9092'] },
})

const input = topic('input', { key: string(), value: json<Event>() })
const output = topic('output', { key: string(), value: json<Result>() })

app.stream(input)
	.filter((k, v) => v.type === 'click')
	.mapValues(v => ({ count: 1, ...v }))
	.to(output.name, output) // to() takes the topic name; codecs via options

await app.start()
```

## State Stores

Pluggable state stores for stateful stream processing (aggregations, joins, windowing).

### Interfaces

- **KeyValueStore<K, V>** - Basic key-value store
- **WindowStore<K, V>** - Time-windowed state
- **SessionStore<K, V>** - Session window state
- **StateStoreProvider** - Factory for creating stores

### Built-in Provider

```typescript
import { codec, inMemory } from '@kafkats/flow'

const provider = inMemory()
const store = provider.createKeyValueStore('my-store', {
	keyCodec: codec.string(),
	valueCodec: codec.json<MyValue>(),
})
```

### LMDB Provider (Persistent)

```typescript
import { lmdb } from '@kafkats/flow-state-lmdb'

const provider = lmdb({ stateDir: './state' })
```

## Changelog Topics

State stores are backed by changelog topics for fault tolerance. Key features:

### Partition Count Inference

Changelog topics are automatically created with the same partition count as source topics:

```typescript
// Source "orders" has 8 partitions → changelog gets 8 partitions
app.stream('orders', { key: codec.string(), value: codec.json<Order>() }).groupByKey().count()
```

Topics that feed the same stateful operation (merged streams, join sides) must have the same partition count; a mismatch throws `CoPartitioningError` from `start()`. Repartition topics are created with the largest partition count of the topics feeding them.

### Configuration

Global config in `FlowConfig`:

```typescript
flow({
	applicationId: 'my-app',
	client: { clientId: 'my-app', brokers: ['localhost:9092'] },
	changelog: {
		replicationFactor: 3,
		topicConfigs: { 'min.insync.replicas': '2' },
		autoCreate: false, // Production: pre-create topics
	},
})
```

Per-store config in `Materialized`:

```typescript
stream.groupByKey().count({
	changelog: {
		topicName: 'custom-changelog',
		replicationFactor: 2,
	},
})
```

### Error Handling

- **ChangelogPartitionMismatchError** - Existing changelog has wrong partition count
- **SourceTopicNotFoundError** - Source topic doesn't exist, can't infer partitions

### Key Files

- `src/changelog/` - ChangelogConfig, error classes, ChangelogWriter/Restorer
- `src/runtime/internal-topics.ts` - validateAndCreateInternalTopics(); `src/runtime/restoration.ts` - restoration; `src/runtime/stream-worker.ts` - worker lifecycle
- `src/state/partitioned.ts`, `src/state/buffered.ts` - per-partition stores and the transactional write buffer

## Testing Infrastructure

The package provides a testing module at `@kafkats/flow/testing` for writing tests with minimal boilerplate.

### TestDriver

The main test utility that mocks Kafka and manages lifecycle:

```typescript
import { TestDriver, ResultCollector } from '@kafkats/flow/testing'
import { codec } from '@kafkats/flow'

const driver = new TestDriver()

// Build topology
driver
	.input('orders', { value: codec.json<Order>() })
	.mapValues(order => ({ ...order, processed: true }))
	.filter((_, v) => v.total > 100)
	.to('large-orders', { value: codec.json() })

// Run test with automatic lifecycle management
await driver.run(async ({ send, output }) => {
	await send('orders', { id: 'o1', total: 250 })
	await send('orders', { id: 'o2', total: 50 })

	const results = output('large-orders', { value: codec.json() })
	expect(results).toHaveLength(1)
})
```

### ResultCollector

For capturing stream output without sending to a topic:

```typescript
const results = new ResultCollector<string, Order>()

driver
	.input('orders', { key: codec.string(), value: codec.json<Order>() })
	.filter((_, v) => v.amount > 100)
	.peek(results.collector())

await driver.run(async ({ send }) => {
	await send('orders', { id: '1', amount: 150 }, { key: Buffer.from('k1') })

	expect(results.values).toHaveLength(1)
	expect(results.first?.value.amount).toBe(150)
})
```

### Utilities

- **Loopback**: records written to a topic the topology also reads (`through()`, repartition topics) are fed back into the topology automatically
- **MockProducer** records `timestamp` and `headers` of produced messages
- **testRecord(topic, key, value, options?)**: Create a test record for processor testing
- **testRecordSequence(topic, items, options?)**: Create a sequence of records with incrementing timestamps (`{ startTimestamp, timestampIncrement, partition }`, bigint)
- **createFactory(generator)**: Create test data factories (`create()`, `createMany()`, `reset()`)
- **timestamps(baseTime?)**: Create timestamp helpers for time-based testing (`at()`, `after()`, `base`, `advance()`; bigint ms)
- **quickTest(fn, config?)**: Minimal setup for simple tests

`send()` takes `Buffer` keys (or `keyValue` + `keyCodec`) and `bigint` timestamps, and JSON-encodes values unless a `codec` is given.

### Example: Stream-Table Join

```typescript
const driver = new TestDriver()
const results = new ResultCollector<string, EnrichedEvent>()

const users = driver.table('users', { key: codec.string(), value: codec.json<User>() })

driver
	.input('events', { key: codec.string(), value: codec.json<Event>() })
	.join(users, (event, user) => ({ ...event, userName: user.name }))
	.peek(results.collector())

await driver.run(async ({ send }) => {
	// Populate table
	await send('users', { name: 'Alice' }, { key: Buffer.from('user1') })

	// Send event
	await send('events', { action: 'click' }, { key: Buffer.from('user1') })

	expect(results.first?.value.userName).toBe('Alice')
})
```
