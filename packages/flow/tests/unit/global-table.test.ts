import { EventEmitter } from 'node:events'
import { describe, expect, it } from 'vitest'
import type { KafkaClient } from '@kafkats/client'
import { codec, GlobalTableTailer, InMemoryKeyValueStore, type KeyValueStore } from '../../src/index.js'
import { TestDriver, ResultCollector } from '../../src/testing.js'

type AppInternals = {
	sourcesByTopic: Map<string, unknown[]>
	changelogTopics: Map<string, unknown>
	globalTables: Array<{ topic: string }>
	stateStores: Map<string, KeyValueStore<unknown, unknown>>
}

function internals(app: unknown): AppInternals {
	return app as AppInternals
}

type User = { name: string }

describe('globalTable() wiring', () => {
	it('does not register the topic on the group consumer and creates no changelog', () => {
		const driver = new TestDriver()
		const table = driver.flow.globalTable('users', {
			key: codec.string(),
			value: codec.json<User>(),
			materialized: { storeName: 'users-global' },
		})
		expect(table).toBeDefined()

		const app = internals(driver.flow)
		// Group consumption would split partitions across instances - global tables must not use it.
		expect(app.sourcesByTopic.has('users')).toBe(false)
		// The source topic IS the changelog for a global store: no changelog topic, no wrapper.
		expect(app.changelogTopics.size).toBe(0)
		expect(app.globalTables).toHaveLength(1)
		expect(app.globalTables[0]!.topic).toBe('users')
	})

	it('joins streams against the globally materialized store', async () => {
		const driver = new TestDriver()
		type Event = { action: string }
		const results = new ResultCollector<string, { action: string; name: string }>()

		const table = driver.flow.globalTable('users', {
			key: codec.string(),
			value: codec.json<User>(),
			materialized: { storeName: 'users-global' },
		})
		driver
			.input('events', { key: codec.string(), value: codec.json<Event>() })
			.join(table, (event, user) => ({ action: event.action, name: user.name }))
			.peek(results.collector())

		await driver.run(async ({ send }) => {
			// Simulate the global tailer materializing a user (possibly from a partition the group
			// consumer would never be assigned). Must not require a worker/changelog context.
			const store = internals(driver.flow).stateStores.get('users-global') as unknown as KeyValueStore<
				string,
				User
			>
			await store.put('u1', { name: 'Alice' })

			await send('events', { action: 'click' }, { key: codec.string().encode('u1') })
			expect(results.records).toEqual([{ key: 'u1', value: { action: 'click', name: 'Alice' } }])
		})
	})
})

describe('GlobalTableTailer', () => {
	type Delivered = {
		topic: string
		partition: number
		offset: bigint
		timestamp: bigint
		key: Buffer | null
		value: Buffer | null
		headers: Record<string, Buffer>
	}

	class StubConsumer extends EventEmitter {
		handler: ((message: Delivered) => Promise<void>) | null = null
		options: { assignment?: Array<{ topic: string; partition: number; offset: bigint }> } | null = null
		private stopResolve: (() => void) | null = null

		runEach(
			_topics: unknown,
			handler: (message: Delivered) => Promise<void>,
			options: { assignment?: Array<{ topic: string; partition: number; offset: bigint }> }
		): Promise<void> {
			this.handler = handler
			this.options = options
			this.emit('running')
			return new Promise(resolve => {
				this.stopResolve = resolve
			})
		}

		stop(): void {
			this.stopResolve?.()
			this.stopResolve = null
		}

		async deliver(partition: number, offset: bigint, key: string | null, value: string | null): Promise<void> {
			await this.handler!({
				topic: 'users',
				partition,
				offset,
				timestamp: 0n,
				key: key === null ? null : Buffer.from(key),
				value: value === null ? null : Buffer.from(value),
				headers: {},
			})
		}
	}

	function stubClient(consumer: StubConsumer, endOffsets: Map<number, bigint>): KafkaClient {
		return {
			getMetadata: async (_topics: string[]) => ({
				topics: new Map([
					[
						'users',
						{
							partitions: new Map(
								[...endOffsets.keys()].map(p => [p, { partitionIndex: p, leaderId: 0 }])
							),
						},
					],
				]),
			}),
			admin: () => ({
				fetchTopicOffsets: async (_topic: string, partitions: number[], which: 'earliest' | 'latest') =>
					new Map(partitions.map(p => [p, which === 'earliest' ? 0n : endOffsets.get(p)!])),
			}),
			consumer: () => consumer,
		} as unknown as KafkaClient
	}

	async function waitForHandler(consumer: StubConsumer): Promise<void> {
		while (!consumer.handler) {
			await new Promise(resolve => setImmediate(resolve))
		}
	}

	it('materializes every partition from earliest, blocks until caught up, then keeps tailing', async () => {
		const consumer = new StubConsumer()
		const endOffsets = new Map<number, bigint>([
			[0, 2n],
			[1, 1n],
			[2, 0n],
		])
		const store = new InMemoryKeyValueStore<string, string>('global', {
			keyCodec: codec.string(),
			valueCodec: codec.string(),
		})
		await store.init()

		const tailer = new GlobalTableTailer<string, string>(
			stubClient(consumer, endOffsets),
			'users',
			{ keyCodec: codec.string(), valueCodec: codec.string() },
			store
		)

		let started = false
		const startPromise = tailer.start().then(() => {
			started = true
		})
		await waitForHandler(consumer)

		// All partitions are assigned from earliest - no consumer group involved.
		expect(consumer.options?.assignment).toEqual([
			{ topic: 'users', partition: 0, offset: 0n },
			{ topic: 'users', partition: 1, offset: 0n },
			{ topic: 'users', partition: 2, offset: 0n },
		])

		await consumer.deliver(0, 0n, 'u0', 'v0')
		await new Promise(resolve => setImmediate(resolve))
		// Still catching up: p0 has one more record and p1 is untouched.
		expect(started).toBe(false)

		await consumer.deliver(0, 1n, 'u1', 'v1')
		await new Promise(resolve => setImmediate(resolve))
		expect(started).toBe(false)

		await consumer.deliver(1, 0n, 'u2', 'v2')
		await startPromise
		expect(started).toBe(true)

		expect(await store.get('u0')).toBe('v0')
		expect(await store.get('u1')).toBe('v1')
		expect(await store.get('u2')).toBe('v2')

		// Keeps tailing after catch-up: updates and tombstones apply continuously.
		await consumer.deliver(1, 1n, 'u2', 'v2-updated')
		expect(await store.get('u2')).toBe('v2-updated')
		await consumer.deliver(0, 2n, 'u0', null)
		expect(await store.get('u0')).toBeUndefined()

		await tailer.stop()
	})

	it('resolves immediately when all partitions are empty', async () => {
		const consumer = new StubConsumer()
		const endOffsets = new Map<number, bigint>([
			[0, 0n],
			[1, 0n],
		])
		const store = new InMemoryKeyValueStore<string, string>('global-empty', {
			keyCodec: codec.string(),
			valueCodec: codec.string(),
		})
		await store.init()

		const tailer = new GlobalTableTailer<string, string>(
			stubClient(consumer, endOffsets),
			'users',
			{ keyCodec: codec.string(), valueCodec: codec.string() },
			store
		)
		await tailer.start()
		await tailer.stop()
	})
})
