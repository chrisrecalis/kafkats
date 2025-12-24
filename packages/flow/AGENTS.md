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
├── flow.ts     # Core streaming DSL (KStream, KTable, windowing)
├── codec.ts    # Serialization codecs (string, json, buffer)
├── state.ts    # State store interfaces
├── state/
│   └── memory.ts  # In-memory state store implementation
└── index.ts    # Public exports
```

### Key Abstractions

- **flow()**: Creates a FlowApp for building streaming topologies
- **KStream**: Unbounded stream of key-value records
- **KTable**: Changelog stream representing a table
- **KGroupedStream**: Stream grouped by key for aggregations
- **Windowing**: TimeWindows, SessionWindows, SlidingWindows

### Codecs

```typescript
import { codec, string, json, buffer } from '@kafkats/flow'

// Built-in codecs
string    // UTF-8 string
json      // JSON serialization
buffer    // Raw Buffer passthrough

// Custom codec
const myCodec = codec<MyType>(
  (value) => Buffer.from(...),  // encode
  (buffer) => ...               // decode
)
```

## Example

```typescript
import { flow, topic, string, json } from '@kafkats/flow'

const app = flow({ brokers: ['localhost:9092'] })

const input = topic('input', string, json<Event>())
const output = topic('output', string, json<Result>())

app.stream(input)
	.filter((k, v) => v.type === 'click')
	.mapValues(v => ({ count: 1, ...v }))
	.to(output)

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
import { inMemory } from '@kafkats/flow'

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
app.stream('orders', { key: codec.string(), value: codec.json<Order>() })
	.groupByKey()
	.count()
```

For merged streams, uses the maximum partition count from all sources.

### Configuration

Global config in `FlowConfig`:

```typescript
flow({
	applicationId: 'my-app',
	client: { brokers: ['localhost:9092'] },
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

- `src/changelog.ts` - ChangelogConfig, error classes, ChangelogWriter/Restorer
- `src/flow.ts` - validateAndCreateChangelogTopics(), source topic tracking

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

- **testRecord(topic, key, value, options?)**: Create a test record for processor testing
- **testRecordSequence(topic, items, options?)**: Create a sequence of records with incrementing timestamps
- **createFactory(generator)**: Create test data factories
- **timestamps(baseTime?)**: Create timestamp helpers for time-based testing
- **quickTest(fn)**: Minimal setup for simple tests

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
