import { EventEmitter } from 'node:events'
import { describe, expect, it, vi, afterEach } from 'vitest'
import { KafkaClient, type Message } from '@kafkats/client'

import { codec, flow } from '../../src/index.js'

type TestMessage = Omit<Message<Buffer>, 'value'> & { value: Buffer | null }
type TestContext = { signal: AbortSignal; topic: string; partition: number; offset: bigint }
type TestHandler = (message: TestMessage, ctx: TestContext) => Promise<void>

class TestConsumer extends EventEmitter {
	private handler: TestHandler | null = null
	private stopResolve: (() => void) | null = null
	consumerGroup = { currentMemberId: 'member-1', currentGenerationId: 1 }

	async runEach(_subscription: string[], handler: TestHandler): Promise<void> {
		this.handler = handler
		this.emit('running')
		return new Promise(resolve => {
			this.stopResolve = () => {
				this.emit('stopped')
				resolve()
			}
		})
	}

	stop(): void {
		this.stopResolve?.()
		this.stopResolve = null
	}

	async emitMessage(topic: string, value: Buffer | null, key?: Buffer | null, timestamp?: bigint): Promise<void> {
		if (!this.handler) {
			throw new Error('consumer not started')
		}
		const message: TestMessage = {
			topic,
			partition: 0,
			offset: 0n,
			timestamp: timestamp ?? 0n,
			key: key ?? null,
			value,
			headers: {},
		}
		const ctx = { signal: new AbortController().signal, topic, partition: 0, offset: 0n }
		await this.handler(message, ctx)
	}
}

class TestProducer {
	messages: Array<{ topic: string; key?: Buffer | null; value: Buffer | null; partition?: number }> = []

	async send(
		topic: string,
		message: { key?: Buffer | null; value: Buffer | null; partition?: number }
	): Promise<void> {
		this.messages.push({ topic, key: message.key, value: message.value, partition: message.partition })
	}

	async transaction(
		fn: (tx: {
			send: (
				topic: string,
				message: { key?: Buffer | null; value: Buffer | null; partition?: number }
			) => Promise<void>
			sendOffsets: (params: {
				offsets: Array<{ topic: string; partition: number; offset: bigint }>
			}) => Promise<void>
		}) => Promise<void>
	): Promise<void> {
		await fn({
			send: async (topic, message) => {
				await this.send(topic, message)
			},
			sendOffsets: async () => {},
		})
	}

	async disconnect(): Promise<void> {
		return
	}
}

function createTestApp() {
	const client = new KafkaClient({ clientId: 'test-app', brokers: ['localhost:9092'] })
	const consumers: TestConsumer[] = []
	const producers: TestProducer[] = []

	vi.spyOn(client, 'connect').mockResolvedValue()
	vi.spyOn(client, 'disconnect').mockResolvedValue()
	vi.spyOn(client, 'consumer').mockImplementation(() => {
		const consumer = new TestConsumer()
		consumers.push(consumer)
		return consumer as unknown as never
	})
	vi.spyOn(client, 'producer').mockImplementation(() => {
		const producer = new TestProducer()
		producers.push(producer)
		return producer as unknown as never
	})

	const app = flow({ applicationId: 'test-app', client })
	return { app, consumers, producers }
}

afterEach(() => {
	vi.restoreAllMocks()
})

type User = { active: boolean; name: string }

describe('KTable.filter table semantics', () => {
	it('forwards a tombstone when the predicate stops matching (retraction, not a drop)', async () => {
		const { app, consumers } = createTestApp()

		const emitted: Array<{ key: string | null; value: User | null }> = []

		app.table('users', { key: codec.string(), value: codec.json<User>() })
			.filter((_key, value) => value?.active === true)
			.toStream()
			.peek((key, value) => emitted.push({ key, value }))

		await app.start()
		const consumer = consumers[0]!

		await consumer.emitMessage(
			'users',
			Buffer.from(JSON.stringify({ active: true, name: 'alice' })),
			Buffer.from('u1')
		)
		await consumer.emitMessage(
			'users',
			Buffer.from(JSON.stringify({ active: false, name: 'alice' })),
			Buffer.from('u1')
		)

		// Kafka Streams KTableFilter: predicate failure retracts the row downstream via a tombstone.
		expect(emitted).toEqual([
			{ key: 'u1', value: { active: true, name: 'alice' } },
			{ key: 'u1', value: null },
		])

		await app.close()
	})

	it('passes tombstones through without invoking the predicate', async () => {
		const { app, consumers } = createTestApp()

		const emitted: Array<{ key: string | null; value: User | null }> = []
		const seenValues: Array<User | null> = []

		app.table('users', { key: codec.string(), value: codec.json<User>() })
			.filter((_key, value) => {
				seenValues.push(value)
				return value!.active
			})
			.toStream()
			.peek((key, value) => emitted.push({ key, value }))

		await app.start()
		const consumer = consumers[0]!

		await consumer.emitMessage(
			'users',
			Buffer.from(JSON.stringify({ active: true, name: 'bob' })),
			Buffer.from('u2')
		)
		// Delete the row: the tombstone must be forwarded as-is, without evaluating the predicate.
		await consumer.emitMessage('users', null, Buffer.from('u2'))

		expect(seenValues).not.toContain(null)
		expect(emitted).toEqual([
			{ key: 'u2', value: { active: true, name: 'bob' } },
			{ key: 'u2', value: null },
		])

		await app.close()
	})
})

describe('KTable.mapValues table semantics', () => {
	it('never invokes the mapper on tombstones and forwards null unchanged', async () => {
		const { app, consumers } = createTestApp()

		type Mapped = { name: string; extra: boolean }
		const emitted: Array<{ key: string | null; value: Mapped | null }> = []
		const mapperInputs: Array<unknown> = []

		app.table('users', { key: codec.string(), value: codec.json<User>() })
			.mapValues(value => {
				mapperInputs.push(value)
				return { ...(value as User), extra: true }
			})
			.toStream()
			.peek((key, value) => emitted.push({ key, value: value as Mapped | null }))

		await app.start()
		const consumer = consumers[0]!

		await consumer.emitMessage(
			'users',
			Buffer.from(JSON.stringify({ active: true, name: 'carol' })),
			Buffer.from('u3')
		)
		await consumer.emitMessage('users', null, Buffer.from('u3'))

		// The mapper must never see the tombstone; downstream must receive null, not `{ extra: true }`.
		expect(mapperInputs).not.toContain(null)
		expect(emitted).toEqual([
			{ key: 'u3', value: { active: true, name: 'carol', extra: true } },
			{ key: 'u3', value: null },
		])

		await app.close()
	})
})
