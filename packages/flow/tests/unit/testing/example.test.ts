/**
 * The stateless cases of testing-example.test.ts against the new TestDriver (the legacy copy keeps running until the
 * public switch). Adapted: the multi-threaded case (one consumer and one producer per app now).
 */
import { describe, expect, it } from 'vitest'
import { FlowConfigError } from '@/api/errors.js'
import { codec } from '@/codec.js'
import { TestDriver } from '@/testing/driver.js'
import { quickTest, ResultCollector, testRecordSequence } from '@/testing/helpers.js'

describe('TestDriver basics', () => {
	it('processes a simple map/filter pipeline', async () => {
		const driver = new TestDriver()

		type Order = { id: string; total: number }

		driver
			.input('orders', { value: codec.json<Order>() })
			.mapValues(order => {
				if (order === null) {
					throw new Error('unexpected tombstone in mapValues() test')
				}
				return { ...order, large: order.total > 100 }
			})
			.filter((_, value) => Boolean(value?.large))
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

describe('factories and sequences', () => {
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

describe('quickTest helper', () => {
	it('provides the simplest possible test setup', async () => {
		await quickTest(async ({ input, send, output }) => {
			type Event = { type: string }

			input('in', { value: codec.json<Event>() })
				.mapValues(e => {
					if (e === null) {
						throw new Error('unexpected tombstone in quickTest mapValues()')
					}
					return { ...e, processed: true }
				})
				.to('out', { value: codec.json() })

			expect(output('out')).toHaveLength(0)
			await send('in', { type: 'click' })

			const results = output('out')
			expect(results).toHaveLength(1)
		})
	})
})

describe('exactly_once processing', () => {
	it('verifies transaction usage', async () => {
		const driver = new TestDriver({ processingGuarantee: 'exactly_once', commitEvery: 'stop' })

		driver
			.input('events', { value: codec.json<{ id: string }>() })
			.mapValues(v => {
				if (v === null) {
					throw new Error('unexpected tombstone in mapValues() test')
				}
				return { ...v, handled: true }
			})
			.to('out', { value: codec.json() })

		await driver.run(async ({ send }) => {
			await send('events', { id: 't1' })
		})

		// With commitEvery: 'stop', the transaction is committed on stop()
		const producer = driver.producers[0]!
		expect(producer.transactions).toHaveLength(1)
		// Transaction should contain offset commits
		expect(producer.transactions[0]!.offsets).toHaveLength(1)
		expect(producer.transactions[0]!.offsets[0]!.topic).toBe('events')
		expect(producer.transactions[0]!.offsets[0]!.partition).toBe(0)
	})
})

describe('one consumer and one producer', () => {
	it('runs every task of the app on one consumer and one producer', async () => {
		const driver = new TestDriver()

		driver.input('events')

		await driver.run(async () => {
			expect(driver.consumers).toHaveLength(1)
			expect(driver.producers).toHaveLength(1)
		})
	})

	it('rejects numStreamThreads', () => {
		const driver = new TestDriver({ numStreamThreads: 2 } as never)
		expect(() => driver.flow).toThrow(FlowConfigError)
	})
})

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

describe('branching with TestDriver', () => {
	it('routes to first matching branch', async () => {
		const driver = new TestDriver()

		type Event = { type: string }

		const branches = driver.input('events', { value: codec.json<Event>() }).branch(
			(_, v) => v !== null && v.type === 'a',
			(_, v) => v !== null && v.type === 'b'
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

describe('merging streams', () => {
	it('combines records from multiple topics', async () => {
		const driver = new TestDriver()

		type Item = { id: string }

		const left = driver.input('left', { value: codec.json<Item>() })
		const right = driver.input('right', { value: codec.json<Item>() })

		left.merge(right)
			.mapValues(v => {
				if (v === null) {
					throw new Error('Unexpected tombstone in merge()')
				}
				return { ...v, merged: true }
			})
			.to('merged', { value: codec.json() })

		await driver.run(async ({ send, output }) => {
			await send('left', { id: 'l1' })
			await send('right', { id: 'r1' })

			const results = output('merged', { value: codec.json<{ id: string; merged: boolean }>() })
			expect(results).toHaveLength(2)
			expect(
				results.map(r => {
					if (r.value === null) {
						throw new Error('Unexpected tombstone in output')
					}
					return r.value.id
				})
			).toEqual(['l1', 'r1'])
		})
	})
})
