/**
 * Example tests demonstrating the new testing infrastructure
 *
 * These examples show how to write tests with minimal boilerplate
 * compared to the original approach.
 */

import { describe, expect, it, afterEach, vi } from 'vitest'
import { codec, TimeWindows } from '../../src/index.js'
import {
	TestDriver,
	ResultCollector,
	testRecordSequence,
	createFactory,
	timestamps,
	quickTest,
} from '../../src/testing.js'

afterEach(() => {
	vi.restoreAllMocks()
})

// ============================================================================
// Basic Usage Examples
// ============================================================================

describe('TestDriver basics', () => {
	it('processes a simple map/filter pipeline', async () => {
		const driver = new TestDriver()

		type Order = { id: string; total: number }

		// Define topology - much cleaner than the old way
		driver
			.input('orders', { value: codec.json<Order>() })
			.mapValues(order => ({ ...order, large: order.total > 100 }))
			.filter((_, value) => value.large)
			.to('large-orders', { value: codec.json() })

		// Run test with automatic lifecycle management
		await driver.run(async ({ send, output }) => {
			await send('orders', { id: 'o1', total: 250 })
			await send('orders', { id: 'o2', total: 50 })

			const results = output('large-orders', { value: codec.json() })
			expect(results).toHaveLength(1)
			expect(results[0]!.value).toEqual({ id: 'o1', total: 250, large: true })
		})
	})

	it('uses ResultCollector for stateful operations', async () => {
		const driver = new TestDriver()
		const results = new ResultCollector<string, number>()

		driver
			.input('events', { key: codec.string(), value: codec.json<{ type: string }>() })
			.groupByKey()
			.count()
			.toStream()
			.peek(results.collector())

		await driver.run(async ({ send }) => {
			await send('events', { type: 'click' }, { key: Buffer.from('user1') })
			await send('events', { type: 'click' }, { key: Buffer.from('user1') })
			await send('events', { type: 'click' }, { key: Buffer.from('user2') })

			expect(results.length).toBe(3)
			expect(results.values).toEqual([1, 2, 1])
			expect(results.last?.value).toBe(1)
		})
	})
})

// ============================================================================
// Comparison: Old vs New Approach
// ============================================================================

describe('before and after comparison', () => {
	/**
	 * OLD APPROACH (what we had before):
	 *
	 * it('processes map/filter/to pipelines', async () => {
	 *   const { app, consumers, producers } = createTestApp()
	 *
	 *   type Order = { id: string; total: number }
	 *   type Enriched = { id: string; total: number; large: boolean }
	 *
	 *   app.stream('orders', { value: codec.json<Order>() })
	 *     .mapValues(order => ({ ...order, large: order.total > 100 }) satisfies Enriched)
	 *     .filter((_key, value) => value.large)
	 *     .to('large-orders', { value: codec.json<Enriched>() })
	 *
	 *   await app.start()
	 *   const consumer = consumers[0]!
	 *   const producer = producers[0]!
	 *
	 *   await consumer.emitMessage('orders', Buffer.from(JSON.stringify({ id: 'o1', total: 250 })))
	 *   await consumer.emitMessage('orders', Buffer.from(JSON.stringify({ id: 'o2', total: 50 })))
	 *
	 *   expect(producer.messages).toHaveLength(1)
	 *   const payload = JSON.parse(producer.messages[0]!.value.toString()) as Enriched
	 *   expect(payload).toEqual({ id: 'o1', total: 250, large: true })
	 *
	 *   await app.close()
	 * })
	 */

	// NEW APPROACH - Same test, much cleaner:
	it('processes map/filter/to pipelines (new way)', async () => {
		const driver = new TestDriver()

		type Order = { id: string; total: number }

		driver
			.input('orders', { value: codec.json<Order>() })
			.mapValues(order => ({ ...order, large: order.total > 100 }))
			.filter((_, value) => value.large)
			.to('large-orders', { value: codec.json() })

		await driver.run(async ({ send, output }) => {
			await send('orders', { id: 'o1', total: 250 })
			await send('orders', { id: 'o2', total: 50 })

			const results = output('large-orders', { value: codec.json() })
			expect(results).toHaveLength(1)
			expect(results[0]!.value).toEqual({ id: 'o1', total: 250, large: true })
		})
	})
})

// ============================================================================
// Stream-Table Join Examples
// ============================================================================

describe('stream-table joins with TestDriver', () => {
	it('joins stream records with table lookups', async () => {
		const driver = new TestDriver()
		const results = new ResultCollector<string, { userId: string; action: string; userName: string }>()

		type Event = { userId: string; action: string }
		type User = { name: string }

		const users = driver.table('users', { key: codec.string(), value: codec.json<User>() })

		driver
			.input('events', { key: codec.string(), value: codec.json<Event>() })
			.join(users, (event, user) => ({
				userId: event.userId,
				action: event.action,
				userName: user.name,
			}))
			.peek(results.collector())

		await driver.run(async ({ send }) => {
			// Populate table
			await send('users', { name: 'Alice' }, { key: Buffer.from('user1') })
			await send('users', { name: 'Bob' }, { key: Buffer.from('user2') })

			// Send events
			await send('events', { userId: 'user1', action: 'click' }, { key: Buffer.from('user1') })
			await send('events', { userId: 'user3', action: 'buy' }, { key: Buffer.from('user3') })

			// Only user1 event should join (inner join)
			expect(results.length).toBe(1)
			expect(results.first?.value).toEqual({
				userId: 'user1',
				action: 'click',
				userName: 'Alice',
			})
		})
	})
})

// ============================================================================
// Stream-Stream Join with Time Windows
// ============================================================================

describe('windowed joins', () => {
	it('joins streams within a time window', async () => {
		const driver = new TestDriver()
		const results = new ResultCollector<string, { page: string; ad: string }>()

		type Click = { page: string }
		type Impression = { ad: string }

		const clicks = driver.input('clicks', { key: codec.string(), value: codec.json<Click>() })
		const impressions = driver.input('impressions', { key: codec.string(), value: codec.json<Impression>() })

		clicks
			.join(
				impressions,
				(click, impression) => ({
					page: click.page,
					ad: impression.ad,
				}),
				{ within: TimeWindows.of('5s') }
			)
			.peek(results.collector())

		await driver.run(async ({ send }) => {
			const ts = timestamps()

			// Impression at t=1000ms
			await send('impressions', { ad: 'banner1' }, { key: Buffer.from('user1'), timestamp: ts.at('1s') })

			// Click at t=2000ms (within 5s window)
			await send('clicks', { page: 'home' }, { key: Buffer.from('user1'), timestamp: ts.at('2s') })

			// Click at t=10000ms (outside 5s window)
			await send('clicks', { page: 'about' }, { key: Buffer.from('user1'), timestamp: ts.at('10s') })

			expect(results.some((_, v) => v.page === 'home' && v.ad === 'banner1')).toBe(true)
		})
	})
})

// ============================================================================
// Test Data Factory Examples
// ============================================================================

describe('factories and sequences', () => {
	it('uses createFactory for test data generation', async () => {
		type Order = { id: string; amount: number; customer: string }

		const orderFactory = createFactory<Order>(index => ({
			id: `order-${index}`,
			amount: (index + 1) * 100,
			customer: `customer-${index % 3}`,
		}))

		const driver = new TestDriver()
		const results = new ResultCollector<string, number>()

		driver
			.input('orders', { key: codec.string(), value: codec.json<Order>() })
			.groupBy((_, v) => v.customer, { key: codec.string() })
			.count()
			.toStream()
			.peek(results.collector())

		await driver.run(async ({ send }) => {
			// Generate and send 6 orders
			const orders = orderFactory.createMany(6)
			for (const order of orders) {
				await send('orders', order, { key: Buffer.from(order.id) })
			}

			// Should have counts for 3 customers (2 orders each)
			expect(results.length).toBe(6)
		})
	})

	it('uses testRecordSequence for processor testing', () => {
		type Event = { type: string }

		const records = testRecordSequence<string, Event>(
			'events',
			[
				{ key: 'k1', value: { type: 'a' } },
				{ key: 'k2', value: { type: 'b' } },
				{ key: 'k1', value: { type: 'c' } },
			],
			{ startTimestamp: 1000n, timestampIncrement: 100n }
		)

		expect(records).toHaveLength(3)
		expect(records[0]!.timestamp).toBe(1000n)
		expect(records[1]!.timestamp).toBe(1100n)
		expect(records[2]!.timestamp).toBe(1200n)
		expect(records[0]!.offset).toBe(0n)
		expect(records[1]!.offset).toBe(1n)
	})
})

// ============================================================================
// QuickTest - Minimal Boilerplate
// ============================================================================

describe('quickTest helper', () => {
	it('provides the simplest possible test setup', async () => {
		await quickTest(async ({ input, send, output }) => {
			type Event = { type: string }

			input('in', { value: codec.json<Event>() })
				.mapValues(e => ({ ...e, processed: true }))
				.to('out', { value: codec.json() })

			await send('in', { type: 'click' })

			const results = output('out')
			expect(results).toHaveLength(1)
		})
	})
})

// ============================================================================
// Exactly-Once Semantics
// ============================================================================

describe('exactly_once processing', () => {
	it('verifies transaction usage', async () => {
		const driver = new TestDriver({ processingGuarantee: 'exactly_once' })

		driver
			.input('events', { value: codec.json<{ id: string }>() })
			.mapValues(v => ({ ...v, handled: true }))
			.to('out', { value: codec.json() })

		await driver.run(async ({ send }) => {
			await send('events', { id: 't1' })
		})

		// With batched commits, transactions are committed on close()
		const producer = driver.producers[0]!
		expect(producer.transactions).toHaveLength(1)
		// Transaction should contain offset commits
		expect(producer.transactions[0]!.offsets).toHaveLength(1)
		expect(producer.transactions[0]!.offsets[0]!.topic).toBe('events')
		expect(producer.transactions[0]!.offsets[0]!.partition).toBe(0)
	})
})

// ============================================================================
// Multiple Stream Threads
// ============================================================================

describe('multi-threaded topology', () => {
	it('spawns multiple workers', async () => {
		const driver = new TestDriver({ numStreamThreads: 2 })

		driver.input('events')

		await driver.run(async () => {
			expect(driver.consumers).toHaveLength(2)
			expect(driver.producers).toHaveLength(2)
		})
	})
})

// ============================================================================
// ResultCollector Advanced Usage
// ============================================================================

describe('ResultCollector features', () => {
	it('provides filtering and querying capabilities', async () => {
		const driver = new TestDriver()
		const results = new ResultCollector<string, { amount: number; category: string }>()

		driver
			.input('orders', { key: codec.string(), value: codec.json<{ amount: number; category: string }>() })
			.peek(results.collector())

		await driver.run(async ({ send }) => {
			await send('orders', { amount: 100, category: 'electronics' }, { key: Buffer.from('o1') })
			await send('orders', { amount: 50, category: 'books' }, { key: Buffer.from('o2') })
			await send('orders', { amount: 200, category: 'electronics' }, { key: Buffer.from('o3') })

			// Filter by predicate
			const electronics = results.filter((_, v) => v.category === 'electronics')
			expect(electronics).toHaveLength(2)

			// Check conditions
			expect(results.some((_, v) => v.amount > 150)).toBe(true)
			expect(results.every((_, v) => v.amount > 0)).toBe(true)

			// Access by position
			expect(results.first?.value.amount).toBe(100)
			expect(results.last?.value.amount).toBe(200)
			expect(results.get(1).value.category).toBe('books')

			// Get all values
			expect(results.values.map(v => v.amount)).toEqual([100, 50, 200])
		})
	})
})

// ============================================================================
// Branching
// ============================================================================

describe('branching with TestDriver', () => {
	it('routes to first matching branch', async () => {
		const driver = new TestDriver()

		type Event = { type: string }

		const branches = driver.input('events', { value: codec.json<Event>() }).branch(
			(_, v) => v.type === 'a',
			(_, v) => v.type === 'b'
		)

		branches[0]!.to('topic-a', { value: codec.json() })
		branches[1]!.to('topic-b', { value: codec.json() })

		await driver.run(async ({ send, output }) => {
			await send('events', { type: 'b' })

			expect(output('topic-a')).toHaveLength(0)
			expect(output('topic-b')).toHaveLength(1)
		})
	})
})

// ============================================================================
// Merging Streams
// ============================================================================

describe('merging streams', () => {
	it('combines records from multiple topics', async () => {
		const driver = new TestDriver()

		type Item = { id: string }

		const left = driver.input('left', { value: codec.json<Item>() })
		const right = driver.input('right', { value: codec.json<Item>() })

		left.merge(right)
			.mapValues(v => ({ ...v, merged: true }))
			.to('merged', { value: codec.json() })

		await driver.run(async ({ send, output }) => {
			await send('left', { id: 'l1' })
			await send('right', { id: 'r1' })

			const results = output('merged', { value: codec.json<{ id: string; merged: boolean }>() })
			expect(results).toHaveLength(2)
			expect(results.map(r => r.value.id)).toEqual(['l1', 'r1'])
		})
	})
})
