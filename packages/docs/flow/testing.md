# Testing

@kafkats/flow provides a testing module for writing tests without a real Kafka broker.

## Installation

The testing utilities are included in @kafkats/flow:

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
	.filter((_, order) => order.total > 100)
	.mapValues(order => ({ ...order, processed: true }))
	.to('large-orders', { value: json() })

// Run test
await driver.run(async ({ send, output }) => {
	await send('orders', { id: '1', total: 150 })
	await send('orders', { id: '2', total: 50 })

	const results = output('large-orders', { value: json() })
	expect(results).toHaveLength(1)
	expect(results[0].value.id).toBe('1')
})
```

## ResultCollector

Capture stream output without writing to a topic:

```typescript
const results = new ResultCollector<string, Order>()

driver
	.input('orders', { key: string(), value: json<Order>() })
	.filter((_, order) => order.total > 100)
	.peek(results.collector())

await driver.run(async ({ send }) => {
	await send('orders', { id: '1', total: 150 }, { key: 'order-1' })

	expect(results.values).toHaveLength(1)
	expect(results.first?.value.total).toBe(150)
	expect(results.first?.key).toBe('order-1')
})
```

### ResultCollector API

```typescript
interface ResultCollector<K, V> {
	// Get all collected records
	readonly records: Array<{ key: K; value: V }>

	// Get just values
	readonly values: V[]

	// Get just keys
	readonly keys: K[]

	// First/last record
	readonly first: { key: K; value: V } | undefined
	readonly last: { key: K; value: V } | undefined

	// Clear collected records
	clear(): void

	// Get collector function for peek()
	collector(): (key: K, value: V) => void
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
	await send('users', { id: 'u1', name: 'Alice' }, { key: 'u1' })

	// Then send event
	await send('events', { action: 'click' }, { key: 'u1' })

	expect(results.first?.value.userName).toBe('Alice')
})
```

## Testing Windowed Aggregations

```typescript
import { TimeWindows } from '@kafkats/flow'

const driver = new TestDriver()
const results = new ResultCollector<string, number>()

driver
	.input('clicks', { key: string(), value: json() })
	.groupByKey()
	.windowedBy(TimeWindows.of('1h'))
	.count()
	.toStream()
	.peek((windowedKey, count) => {
		results.collector()(windowedKey.key, count)
	})

await driver.run(async ({ send }) => {
	const baseTime = Date.now()

	await send('clicks', {}, { key: 'user1', timestamp: baseTime })
	await send('clicks', {}, { key: 'user1', timestamp: baseTime + 1000 })
	await send('clicks', {}, { key: 'user1', timestamp: baseTime + 2000 })

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
		timestamp: Date.now(),
		headers: { 'trace-id': 'abc123' },
	}
)
```

### testRecordSequence

Create multiple records with incrementing timestamps:

```typescript
import { testRecordSequence } from '@kafkats/flow/testing'

const records = testRecordSequence(
	'my-topic',
	[
		{ key: 'k1', value: { n: 1 } },
		{ key: 'k2', value: { n: 2 } },
		{ key: 'k3', value: { n: 3 } },
	],
	{ baseTime: Date.now(), interval: 1000 }
)
```

### timestamps

Helper for time-based testing:

```typescript
import { timestamps } from '@kafkats/flow/testing'

const ts = timestamps(Date.now())

ts.now() // Current time
ts.plus('5m') // 5 minutes later
ts.minus('1h') // 1 hour earlier
ts.advance('10s') // Move forward 10 seconds
```

### quickTest

Minimal setup for simple tests:

```typescript
import { quickTest } from '@kafkats/flow/testing'

await quickTest(async ({ input, output }) => {
	input('numbers')
		.mapValues((n: number) => n * 2)
		.to('doubled')

	await input.send('numbers', 5)
	expect(output('doubled')[0]).toBe(10)
})
```

## Complete Example

```typescript
import { describe, it, expect } from 'vitest'
import { TestDriver, ResultCollector } from '@kafkats/flow/testing'
import { TimeWindows } from '@kafkats/flow'
import { string, json } from '@kafkats/client'

interface ClickEvent {
	userId: string
	page: string
	timestamp: number
}

interface ClickStats {
	userId: string
	pageViews: number
	windowStart: number
}

describe('Click Analytics', () => {
	it('counts page views per user in 1-hour windows', async () => {
		const driver = new TestDriver()
		const results = new ResultCollector<string, ClickStats>()

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
			.mapValues((count, windowedKey) => ({
				userId: windowedKey.key,
				pageViews: count,
				windowStart: windowedKey.window.start,
			}))
			.peek(results.collector())

		await driver.run(async ({ send }) => {
			const baseTime = new Date('2024-01-01T10:00:00Z').getTime()

			// User clicks
			await send('clicks', { userId: 'u1', page: '/home', timestamp: baseTime }, { key: 'u1' })
			await send('clicks', { userId: 'u1', page: '/products', timestamp: baseTime + 60000 }, { key: 'u1' })
			await send('clicks', { userId: 'u1', page: '/checkout', timestamp: baseTime + 120000 }, { key: 'u1' })

			// Check results
			expect(results.last?.value.pageViews).toBe(3)
			expect(results.last?.value.userId).toBe('u1')
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
			await send('clicks', {}, { key: 'user1' })
			await send('clicks', {}, { key: 'user2' })
			await send('clicks', {}, { key: 'user1' })

			// Last update should show user1 with 2 clicks
			const user1Records = results.records.filter(r => r.key === 'user1')
			expect(user1Records[user1Records.length - 1]?.value).toBe(2)
		})
	})
})
```
