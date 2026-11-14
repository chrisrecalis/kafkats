# Testing

@kafkats/flow provides a testing module for writing tests without a real Kafka broker.

## Installation

The testing utilities are included in @kafkats/flow. They mock the Kafka client with Vitest, so use them from Vitest tests:

```typescript
import { TestDriver, ResultCollector } from '@kafkats/flow/testing'
```

## TestDriver

The main testing utility that mocks Kafka:

```typescript
import { TestDriver, ResultCollector } from '@kafkats/flow/testing'
import { string, json } from '@kafkats/client'

const driver = new TestDriver()

// Build topology
driver
	.input('orders', { key: string(), value: json<Order>() })
	.filter((_, order) => order !== null && order.total > 100)
	.mapValues(order => ({ ...order, processed: true }))
	.to('large-orders', { value: json() })

// Run test
await driver.run(async ({ send, output }) => {
	await send('orders', { id: '1', total: 150 })
	await send('orders', { id: '2', total: 50 })

	const results = output('large-orders', { value: json<Order>() })
	expect(results).toHaveLength(1)
	expect(results[0]?.value?.id).toBe('1')
})
```

`send()` encodes values as JSON unless you pass a `codec` (a `Buffer` is sent as is). Keys are `Buffer`s (`{ key: Buffer.from('k1') }`), or a value plus its codec (`{ keyValue: 'k1', keyCodec: string() }`). Timestamps are `bigint` milliseconds and default to the current time.

### Topics the topology also reads

When a topology writes to a topic that it also reads, `TestDriver` feeds the record back into the topology automatically, like a real consumer would. This covers `through()` and the internal repartition topics created by `groupBy()` or by `selectKey()` followed by `groupByKey()`, a join or `toTable()`. The record is delivered once the current record has been processed, with its key, partition, timestamp and headers.

```typescript
driver
	.input('orders', { key: string(), value: json<Order>() })
	.groupBy((_, order) => order?.userId ?? 'unknown', { key: string(), name: 'orders-by-user' })
	.count()
	.toStream()
	.peek(results.collector())

await driver.run(async ({ send }) => {
	await send('orders', { userId: 'u1', total: 10 }, { key: Buffer.from('o1') })

	// The record went through the repartition topic and was counted
	expect(results.last?.value).toBe(1)
})
```

### Timestamps and headers

`MockProducer` records the `timestamp` and `headers` of every message it receives, next to `topic`, `key`, `value` and `partition`, so you can assert on them. Records written with `to()` or `through()` keep the input record's timestamp:

```typescript
driver.input('in').to('out')

await driver.run(async ({ send, producer }) => {
	await send('in', Buffer.from('v'), { timestamp: 1000n, headers: { 'trace-id': Buffer.from('abc') } })

	const [message] = producer.messagesFor('out')
	expect(message?.timestamp).toBe(1000n)
	expect(message?.headers).toEqual({ 'trace-id': Buffer.from('abc') })
})
```

## ResultCollector

Capture stream output without writing to a topic:

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

### ResultCollector API

```typescript
class ResultCollector<K, V> {
	// Get all collected records
	readonly records: ReadonlyArray<{ key: K; value: V }>

	// Get just values
	readonly values: V[]

	// Get just keys
	readonly keys: K[]

	// First/last record
	readonly first: { key: K; value: V } | undefined
	readonly last: { key: K; value: V } | undefined

	// Record at an index (throws if there is none), count, emptiness
	get(index: number): { key: K; value: V }
	readonly length: number
	readonly isEmpty: boolean

	// Query collected records
	filter(predicate: (key: K, value: V) => boolean): Array<{ key: K; value: V }>
	some(predicate: (key: K, value: V) => boolean): boolean
	every(predicate: (key: K, value: V) => boolean): boolean

	// Clear collected records
	clear(): void

	// Get collector function for peek() (throws on a null key or value)
	collector(): (key: K | null, value: V | null) => void
}
```

## Testing Tables

```typescript
const driver = new TestDriver()
const results = new ResultCollector<string, EnrichedEvent>()

// Create a table
const users = driver.table('users', {
	key: string(),
	value: json<User>(),
})

// Join stream with table
driver
	.input('events', { key: string(), value: json<Event>() })
	.join(users, (event, user) => ({
		...event,
		userName: user.name,
	}))
	.peek(results.collector())

await driver.run(async ({ send }) => {
	// Populate table first
	await send('users', { id: 'u1', name: 'Alice' }, { key: Buffer.from('u1') })

	// Then send event
	await send('events', { action: 'click' }, { key: Buffer.from('u1') })

	expect(results.first?.value.userName).toBe('Alice')
})
```

## Testing Windowed Aggregations

```typescript
import { TimeWindows, type Windowed } from '@kafkats/flow'
import { timestamps } from '@kafkats/flow/testing'

const driver = new TestDriver()
const results = new ResultCollector<Windowed<string>, number>()

driver
	.input('clicks', { key: string(), value: json() })
	.groupByKey()
	.windowedBy(TimeWindows.of('1h'))
	.count()
	.toStream()
	.peek(results.collector())

await driver.run(async ({ send }) => {
	const ts = timestamps(BigInt(Date.parse('2024-01-01T10:00:00Z')))

	await send('clicks', {}, { key: Buffer.from('user1'), timestamp: ts.at(0) })
	await send('clicks', {}, { key: Buffer.from('user1'), timestamp: ts.at('1s') })
	await send('clicks', {}, { key: Buffer.from('user1'), timestamp: ts.at('2s') })

	expect(results.last?.key.key).toBe('user1')
	expect(results.last?.value).toBe(3)
})
```

## Test Utilities

### testRecord

Create a test record:

```typescript
import { testRecord } from '@kafkats/flow/testing'

const record = testRecord(
	'my-topic',
	'key',
	{ data: 'value' },
	{
		timestamp: 1000n,
		headers: { 'trace-id': Buffer.from('abc123') },
	}
)
```

### testRecordSequence

Create multiple records with consecutive offsets and incrementing timestamps:

```typescript
import { testRecordSequence } from '@kafkats/flow/testing'

const records = testRecordSequence(
	'my-topic',
	[
		{ key: 'k1', value: { n: 1 } },
		{ key: 'k2', value: { n: 2 } },
		{ key: 'k3', value: { n: 3 } },
	],
	{ startTimestamp: 1000n, timestampIncrement: 1000n } // defaults: 0n and 1n
)

records[2]?.timestamp // 3000n
records[2]?.offset // 2n
```

### timestamps

Helper for time-based testing. All values are `bigint` milliseconds, ready for the `timestamp` option of `send()`. Durations are milliseconds or strings like `'500ms'`, `'5s'`, `'1m'`, `'2h'`, `'1d'`:

```typescript
import { timestamps } from '@kafkats/flow/testing'

const ts = timestamps(1_700_000_000_000n) // base time, default 0n

ts.base // 1700000000000n
ts.at(0) // the base time
ts.at('5m') // 5 minutes after the base time
ts.after('1h') // 1 hour after the base time (alias for at())

const later = ts.advance('10s') // new helper whose base is 10 seconds later
later.base // 1700000010000n
```

### quickTest

Minimal setup for simple tests. Build the topology first; the driver starts on the first `send()`, `sendAll()` or `waitForOutput()` and is stopped when the function returns:

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

## Complete Example

```typescript
import { describe, it, expect } from 'vitest'
import { TestDriver, ResultCollector, timestamps } from '@kafkats/flow/testing'
import { TimeWindows, type Windowed } from '@kafkats/flow'
import { string, json } from '@kafkats/client'

interface ClickEvent {
	userId: string
	page: string
}

describe('Click Analytics', () => {
	it('counts page views per user in 1-hour windows', async () => {
		const driver = new TestDriver()
		const results = new ResultCollector<Windowed<string>, number>()

		// Build topology
		driver
			.input('clicks', {
				key: string(),
				value: json<ClickEvent>(),
			})
			.groupByKey()
			.windowedBy(TimeWindows.of('1h'))
			.count()
			.toStream()
			.peek(results.collector())

		await driver.run(async ({ send }) => {
			const ts = timestamps(BigInt(Date.parse('2024-01-01T10:00:00Z')))
			const key = Buffer.from('u1')

			// User clicks
			await send('clicks', { userId: 'u1', page: '/home' }, { key, timestamp: ts.at(0) })
			await send('clicks', { userId: 'u1', page: '/products' }, { key, timestamp: ts.at('1m') })
			await send('clicks', { userId: 'u1', page: '/checkout' }, { key, timestamp: ts.at('2m') })

			// Check results
			expect(results.last?.value).toBe(3)
			expect(results.last?.key.key).toBe('u1')
			expect(results.last?.key.window.start).toBe(Number(ts.base))
		})
	})

	it('handles multiple users', async () => {
		const driver = new TestDriver()
		const results = new ResultCollector<string, number>()

		driver
			.input('clicks', { key: string(), value: json() })
			.groupByKey()
			.count()
			.toStream()
			.peek(results.collector())

		await driver.run(async ({ send }) => {
			await send('clicks', {}, { key: Buffer.from('user1') })
			await send('clicks', {}, { key: Buffer.from('user2') })
			await send('clicks', {}, { key: Buffer.from('user1') })

			// Last update should show user1 with 2 clicks
			const user1Records = results.filter(key => key === 'user1')
			expect(user1Records[user1Records.length - 1]?.value).toBe(2)
		})
	})
})
```
