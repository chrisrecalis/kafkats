# Testing

`@kafkats/flow/testing` runs a topology without Kafka. Its `TestDriver` is flow's counterpart of Kafka Streams' `TopologyTestDriver`: it runs the production task runtime (tasks, state stores, commits, the exception handlers) over in-memory ports, deterministically, and with a mock wall clock. The utilities do not depend on a test framework.

```typescript
import { TestDriver, ResultCollector } from '@kafkats/flow/testing'
```

## TestDriver

```typescript
import { string, json } from '@kafkats/flow'
import { TestDriver } from '@kafkats/flow/testing'

const driver = new TestDriver()

driver
	.input('orders', { key: string(), value: json<Order>() })
	.filter((_, order) => order !== null && order.total > 100)
	.mapValues(order => ({ ...order!, processed: true }))
	.to('large-orders', { key: string(), value: json() })

await driver.run(async ({ send, output }) => {
	await send('orders', { id: '1', total: 150 }, { keyValue: 'o1', keyCodec: string() })
	await send('orders', { id: '2', total: 50 }, { keyValue: 'o2', keyCodec: string() })

	const results = output('large-orders', { key: string(), value: json<Order>() })
	expect(results).toHaveLength(1)
	expect(results[0]?.value?.id).toBe('1')
})
```

- `driver.input(topic, consumed)`, `driver.table(topic, options)` and `driver.globalTable(topic, options)` start a topology, as `app.stream()`, `app.table()` and `app.globalTable()` do; `driver.flow` is the underlying app (for `addStateStore()`, `describe()`, or several inputs).
- `run(fn)` starts the driver, runs `fn` with the test context, then stops it. `start()` and `stop()` do the same in two steps.
- **`send()` resolves once the record and everything it led to were processed**: records written to topics the topology reads (repartition topics, `through()` topics, a topic it writes with `to()` and reads with `stream()`) are fed back in, partitioned as a producer would partition them. There is nothing to wait for.
- Offsets are committed after every record by default, as `TopologyTestDriver` does (`commitEvery`).

### Sending records

```typescript
await send('orders', order, {
	keyValue: 'o1', // a key, encoded with keyCodec
	keyCodec: string(),
	// or key: Buffer.from('o1')
	codec: json<Order>(), // value codec; without one, non-Buffer values are sent as JSON
	partition: 0, // default 0
	timestamp: 1_700_000_000_000, // number or bigint; default: the mock wall clock
	headers: { source: Buffer.from('test') },
})
await send('users', null, { keyValue: 'u1', keyCodec: string() }) // a tombstone
await sendAll('orders', [order1, order2], { keyValue: 'o1', keyCodec: string() })
```

A topic the topology reads gets as many partitions as the records sent to it need, unless `partitions` in the driver config says otherwise.

### Reading output

```typescript
const records = output('large-orders', { key: string(), value: json<Order>() }) // [{ topic, key, value, partition? }]
producer.messagesFor('large-orders') // raw: Buffer key and value, partition, timestamp (bigint), headers
clear() // forget what was produced so far
```

`partition` is set only when the sink chose one (a `partitioner`, or the base key's partition of a windowed key); otherwise the record went to the producer's default partitioning. `producer` (a `MockProducer`) records every send, including repartition and changelog records, and under exactly-once the committed transactions (`producer.transactions`). `consumer.committed` holds the committed offsets by `topic:partition`, with their commit metadata.

## Time

Stream time moves with the records' timestamps; the wall clock is a mock that moves only when the test moves it.

```typescript
const driver = new TestDriver({ initialWallClockTimeMs: 0 })

await driver.run(async ({ send, advanceWallClockTime }) => {
	await send('events', 'x', { timestamp: 1_000 })
	await advanceWallClockTime(60_000) // wall-clock punctuators due by then run, then whatever became processable
})
```

Windows close, suppressed results are released and unmatched left/outer join results are emitted only when stream time passes them: send a later record (of any key in the same task) to move it. The driver's throttle for emit-on-window-close and outer-join results is 0 by default (`emitIntervalMs`), so they appear as soon as stream time allows.

## State Stores

```typescript
await driver.run(async ({ send, getStateStore }) => {
	await send('clicks', 'x', { keyValue: 'u1', keyCodec: string() })

	const counts = getStateStore<TimestampedKeyValueStore<string, number>>('clicks-per-user') // the task of partition 0
	expect((await counts.get('u1'))?.value).toBe(1)

	const typed = getStateStore(lastSeen) // the Stores definition of a processor's store gives it its type
	const other = getStateStore('clicks-per-user', 1) // the task of partition 1
})
```

`getStateStore(name, partition?)` returns the store instance of the task of `partition` (default 0), or a global table's store. The stores of tables and aggregations are timestamped: their values are `{ value, timestamp }`.

## Configuration

`new TestDriver(config)` takes every `FlowConfig` option except `client` (`applicationId` defaults to `'test-app'`), so tests can set `processingGuarantee`, handlers, `maxTaskIdleMs` or a state store provider, plus:

| Option                   | Default      | Description                                                                                        |
| ------------------------ | ------------ | -------------------------------------------------------------------------------------------------- |
| `initialWallClockTimeMs` | `Date.now()` | Start of the mock wall clock                                                                       |
| `partitions`             | -            | Partition counts by topic, as partitioners and co-partitioning see them                            |
| `commitEvery`            | `'record'`   | Commit after every record, only when the driver stops (`'stop'`), or every N ms of mock wall clock |
| `emitIntervalMs`         | `0`          | Throttle of emit-on-window-close and outer-join results, on the mock wall clock                    |

## Failures

A failure stops the driver: the `send()` that caused it rejects with the error, and so does every later call. A fatal error is also given to the uncaught exception handler, but the driver neither restarts nor revives tasks: there is no log to replay from. Exception handlers answering `'continue'` work as in production, which makes them easy to test:

```typescript
const driver = new TestDriver({ deserializationExceptionHandler: 'continue' })
driver.input('in', { value: json<{ n: number }>() }).to('out', { value: json() })

await driver.run(async ({ send, output }) => {
	await send('in', Buffer.from('not json'))
	await send('in', { n: 1 })
	expect(output('out')).toHaveLength(1)
})
```

## ResultCollector

Captures records with `peek()` instead of reading a topic:

```typescript
const results = new ResultCollector<string, Order>()

driver
	.input('orders', { key: string(), value: json<Order>() })
	.filter((_, order) => order !== null && order.total > 100)
	.peek(results.collector())

await driver.run(async ({ send }) => {
	await send('orders', { id: '1', total: 150 }, { key: Buffer.from('order-1') })

	expect(results.values).toHaveLength(1)
	expect(results.first?.value.total).toBe(150)
	expect(results.first?.key).toBe('order-1')
})
```

```typescript
class ResultCollector<K, V> {
	readonly records: ReadonlyArray<{ key: K; value: V }>
	readonly values: V[]
	readonly keys: K[]
	readonly first: { key: K; value: V } | undefined
	readonly last: { key: K; value: V } | undefined
	get(index: number): { key: K; value: V } // throws if there is none
	readonly length: number
	readonly isEmpty: boolean
	filter(predicate: (key: K, value: V) => boolean): Array<{ key: K; value: V }>
	some(predicate: (key: K, value: V) => boolean): boolean
	every(predicate: (key: K, value: V) => boolean): boolean
	clear(): void
	collector(): (key: K | null, value: V | null) => void // throws on a null key or value
}
```

## Testing Tables and Joins

```typescript
const driver = new TestDriver()
const results = new ResultCollector<string, { action: string; userName: string }>()

const users = driver.table('users', { key: string(), value: json<User>() })

driver
	.input('events', { key: string(), value: json<{ action: string }>() })
	.join(users, (event, user) => ({ ...event, userName: user.name }))
	.peek(results.collector())

await driver.run(async ({ send }) => {
	await send('users', { name: 'Alice' }, { keyValue: 'u1', keyCodec: string() })
	await send('events', { action: 'click' }, { keyValue: 'u1', keyCodec: string() })

	expect(results.first?.value.userName).toBe('Alice')
})
```

## Testing Windowed Aggregations

```typescript
import { TimeWindows, type Windowed } from '@kafkats/flow'
import { TestDriver, ResultCollector, timestamps } from '@kafkats/flow/testing'

const driver = new TestDriver()
const results = new ResultCollector<Windowed<string>, number>()

driver
	.input('clicks', { key: string(), value: json() })
	.groupByKey()
	.windowedBy(TimeWindows.ofSizeWithNoGrace('1h'))
	.count({ emit: 'on-window-close' })
	.toStream()
	.peek(results.collector())

await driver.run(async ({ send }) => {
	const ts = timestamps(BigInt(Date.parse('2024-01-01T10:00:00Z')))
	const key = { keyValue: 'user1', keyCodec: string() }

	await send('clicks', {}, { ...key, timestamp: ts.at(0) })
	await send('clicks', {}, { ...key, timestamp: ts.at('1s') })
	expect(results.isEmpty).toBe(true) // the window is still open

	await send('clicks', {}, { ...key, timestamp: ts.at('1h') }) // stream time reaches the window's end
	expect(results.first?.key.window.start).toBe(Number(ts.base))
	expect(results.first?.value).toBe(2)
})
```

## Checking a Topology

`driver.flow.describe()` (or `app.describe()`) builds the topology without running it, and the description shows the subtopologies, stores and topics. A mistake throws `TopologyError`: a missing codec or an illegal or duplicate name from the DSL call that makes it, a conflict between parts of the topology from `describe()`. Defining the topology in a test and calling `describe()` checks both. See [describe()](/flow/topology#describe).

## Test Utilities

### timestamps

All values are `bigint` milliseconds, ready for the `timestamp` option of `send()`. Durations are milliseconds or strings like `'500ms'`, `'5s'`, `'1m'`, `'2h'`, `'1d'`:

```typescript
import { timestamps } from '@kafkats/flow/testing'

const ts = timestamps(1_700_000_000_000n) // base time, default 0n

ts.base // 1700000000000n
ts.at('5m') // 5 minutes after the base time
ts.after('1h') // alias of at()
ts.advance('10s').base // 1700000010000n
```

### testRecord, testRecordSequence

Records shaped like consumed messages, for testing functions directly:

```typescript
import { testRecord, testRecordSequence } from '@kafkats/flow/testing'

const record = testRecord('my-topic', 'key', { data: 'value' }, { timestamp: 1000n, headers: {} })

const records = testRecordSequence(
	'my-topic',
	[
		{ key: 'k1', value: { n: 1 } },
		{ key: 'k2', value: { n: 2 } },
	],
	{ startTimestamp: 1000n, timestampIncrement: 1000n } // defaults 0n and 1n
)
records[1]?.offset // 1n
```

### createFactory

```typescript
import { createFactory } from '@kafkats/flow/testing'

const orders = createFactory(i => ({ id: `o${i}`, total: 10 * i }))
orders.create() // { id: 'o0', total: 0 }
orders.createMany(2, { total: 5 }) // o1 and o2, total 5
orders.reset()
```

### quickTest

Builds and exercises a topology in one function. The driver starts on the first `send()`, `sendAll()` or `waitForOutput()` and stops when the function returns:

```typescript
import { quickTest } from '@kafkats/flow/testing'

await quickTest(async ({ input, send, output }) => {
	input('numbers', { value: json<number>() })
		.mapValues(n => (n ?? 0) * 2)
		.to('doubled', { value: json<number>() })

	await send('numbers', 5)
	expect(output('doubled', { value: json<number>() })[0]?.value).toBe(10)
})
```
