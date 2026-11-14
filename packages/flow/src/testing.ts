/**
 * @kafkats/flow testing utilities: run a topology against an in-memory Kafka.
 *
 * `TestDriver` replaces the client's consumers and producers with `MockConsumer` / `MockProducer`.
 * Records sent through the test context are delivered straight to the topology; everything the
 * topology produces is captured by the producer, and records written to a topic the topology also
 * reads (`through()`, repartition topics) are fed back into it.
 *
 * @example
 * ```typescript
 * import { TestDriver } from '@kafkats/flow/testing'
 *
 * const driver = new TestDriver()
 *
 * driver.input('orders', { value: codec.json<Order>() })
 *   .mapValues(order => ({ ...order, processed: true }))
 *   .to('processed-orders', { value: codec.json() })
 *
 * await driver.run(async ({ send, output }) => {
 *   await send('orders', { id: '1', amount: 100 })
 *   expect(output('processed-orders')).toHaveLength(1)
 * })
 * ```
 */

import { EventEmitter } from 'node:events'
import { vi } from 'vitest'
import { KafkaClient, type ConsumeContext, type Message, type RunEachOptions } from '@kafkats/client'
import type { Codec } from '@/codec.js'
import { flow, type FlowApp, type FlowConfig, type Consumed } from '@/flow.js'

// ============================================================================
// Types
// ============================================================================

export type TestMessage<K = Buffer, V = Buffer> = Message<V> & { key: K | null }

export interface TestRecord<K = unknown, V = unknown> {
	topic: string
	key: K | null
	value: V | null
	partition?: number
	timestamp?: bigint
	headers?: Record<string, Buffer>
}

export interface SendOptions {
	key?: Buffer | null
	partition?: number
	timestamp?: bigint
	headers?: Record<string, Buffer>
}

export interface TestDriverConfig extends Partial<Omit<FlowConfig, 'client' | 'applicationId'>> {
	applicationId?: string
}

type TopicPartitionOffset = { topic: string; partition: number; offset: bigint }

// ============================================================================
// Mock consumer and producer
// ============================================================================

/**
 * Stands in for a client consumer. `runEach()` stores the topology's handler and blocks until
 * `stop()`; `sendMessage()` delivers a record to that handler.
 */
export class MockConsumer extends EventEmitter {
	/** Topics this consumer subscribed to. */
	subscription: string[] = []

	private handler: ((message: Message<Buffer>, ctx: ConsumeContext) => Promise<void>) | null = null
	private stopResolve: (() => void) | null = null
	private nextOffset = 0n
	/** Records produced to a subscribed topic while another record is being processed. */
	private readonly pending: Array<{ topic: string; value: Buffer | null; options: SendOptions }> = []
	private delivering = false

	async runEach(
		subscription: string[],
		handler: (message: Message<Buffer>, ctx: ConsumeContext) => Promise<void>,
		_options?: RunEachOptions
	): Promise<void> {
		this.subscription = subscription
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

	/** Whether the topology reads the given topic. */
	subscribes(topic: string): boolean {
		return this.subscription.includes(topic)
	}

	/**
	 * Deliver a record to the topology. A record sent while another one is being processed
	 * (the topology writing to a topic it reads) is delivered once the current one is done,
	 * like a real consumer would.
	 */
	async sendMessage(topic: string, value: Buffer | null, options: SendOptions = {}): Promise<void> {
		if (!this.handler) {
			throw new Error('Consumer not started - call driver.start() first')
		}
		if (this.delivering) {
			this.pending.push({ topic, value, options })
			return
		}

		this.delivering = true
		try {
			await this.deliver(topic, value, options)
			for (let next = this.pending.shift(); next; next = this.pending.shift()) {
				await this.deliver(next.topic, next.value, next.options)
			}
		} finally {
			this.delivering = false
			this.pending.length = 0
		}
	}

	private async deliver(topic: string, value: Buffer | null, options: SendOptions): Promise<void> {
		const offset = this.nextOffset++
		const partition = options.partition ?? 0
		await this.handler!(
			{
				topic,
				partition,
				offset,
				timestamp: options.timestamp ?? BigInt(Date.now()),
				key: options.key ?? null,
				value: value as Buffer,
				headers: options.headers ?? {},
			},
			{ signal: new AbortController().signal, topic, partition, offset, groupId: 'test-group' }
		)
	}
}

/** Stands in for a client producer and captures every message and transaction. */
export class MockProducer {
	readonly messages: Array<TestRecord<Buffer, Buffer>> = []
	readonly transactions: Array<{
		messages: Array<TestRecord<Buffer, Buffer>>
		offsets: TopicPartitionOffset[]
	}> = []

	/** Set by the TestDriver: feeds records written to a topic the topology also reads back into it. */
	loopback: ((record: TestRecord<Buffer, Buffer>) => Promise<void>) | null = null

	async send(
		topic: string,
		message: {
			key?: Buffer | null
			value: Buffer | null
			partition?: number
			timestamp?: Date
			headers?: Record<string, Buffer>
		}
	): Promise<void> {
		const record: TestRecord<Buffer, Buffer> = {
			topic,
			key: message.key ?? null,
			value: message.value,
			partition: message.partition,
			timestamp: message.timestamp ? BigInt(message.timestamp.getTime()) : undefined,
			headers: message.headers,
		}
		this.messages.push(record)
		await this.loopback?.(record)
	}

	async transaction(
		fn: (tx: {
			send: (
				topic: string,
				message: { key?: Buffer | null; value: Buffer | null; partition?: number }
			) => Promise<void>
			sendOffsets: (context: ConsumeContext, offsets: TopicPartitionOffset[]) => Promise<void>
		}) => Promise<void>
	): Promise<void> {
		const firstMessage = this.messages.length
		const offsets: TopicPartitionOffset[] = []

		await fn({
			send: (topic, message) => this.send(topic, message),
			sendOffsets: async (_context, offsetsToCommit) => {
				offsets.push(...offsetsToCommit)
			},
		})

		this.transactions.push({ messages: this.messages.slice(firstMessage), offsets })
	}

	async disconnect(): Promise<void> {}

	/** Messages produced to a topic. */
	messagesFor(topic: string): Array<TestRecord<Buffer, Buffer>> {
		return this.messages.filter(m => m.topic === topic)
	}

	/** Forget all captured messages and transactions. */
	clear(): void {
		this.messages.length = 0
		this.transactions.length = 0
	}
}

// ============================================================================
// Test driver
// ============================================================================

/**
 * Test harness for flow topologies: mocks the Kafka client, manages the app lifecycle and
 * captures output.
 *
 * @example
 * ```typescript
 * const driver = new TestDriver()
 *
 * driver.input('events', { value: codec.json<Event>() })
 *   .filter((_, v) => v.type === 'click')
 *   .to('clicks', { value: codec.json() })
 *
 * await driver.run(async ({ send, output }) => {
 *   await send('events', { type: 'click', target: 'button' })
 *   await send('events', { type: 'view', page: 'home' })
 *
 *   const clicks = output('clicks', { value: codec.json<Event>() })
 *   expect(clicks).toHaveLength(1)
 * })
 * ```
 */
export class TestDriver {
	private readonly config: TestDriverConfig
	private readonly client: KafkaClient
	private app: FlowApp | null = null

	readonly consumers: MockConsumer[] = []
	readonly producers: MockProducer[] = []

	constructor(config: TestDriverConfig = {}) {
		this.config = config
		this.client = new KafkaClient({ clientId: config.applicationId ?? 'test-app', brokers: ['localhost:9092'] })

		vi.spyOn(this.client, 'connect').mockResolvedValue()
		vi.spyOn(this.client, 'disconnect').mockResolvedValue()
		vi.spyOn(this.client, 'consumer').mockImplementation(() => {
			const consumer = new MockConsumer()
			this.consumers.push(consumer)
			return consumer as unknown as never
		})
		vi.spyOn(this.client, 'producer').mockImplementation(() => {
			const producer = new MockProducer()
			producer.loopback = async record => {
				await this.consumers
					.find(c => c.subscribes(record.topic))
					?.sendMessage(record.topic, record.value, record)
			}
			this.producers.push(producer)
			return producer as unknown as never
		})
	}

	/** The FlowApp under test, created on first access. */
	get flow(): FlowApp {
		this.app ??= flow({
			applicationId: this.config.applicationId ?? 'test-app',
			client: this.client,
			...this.config,
		})
		return this.app
	}

	/** Shorthand for `flow.stream()`. */
	input<K = Buffer, V = Buffer>(topic: string, options?: Consumed<K, V>) {
		return this.flow.stream(topic, options)
	}

	/** Shorthand for `flow.table()`. */
	table<K = Buffer, V = Buffer>(topic: string, options?: Consumed<K, V>) {
		return this.flow.table(topic, options)
	}

	/** Start the app and return the test context. */
	async start(): Promise<TestContextApi> {
		await this.flow.start()
		return new TestContextImpl(this)
	}

	async stop(): Promise<void> {
		await this.app?.close()
	}

	/**
	 * Start the app, run `fn`, then stop the app.
	 *
	 * @example
	 * ```typescript
	 * await driver.run(async ({ send, output }) => {
	 *   await send('input', { data: 'test' })
	 *   expect(output('output')).toHaveLength(1)
	 * })
	 * ```
	 */
	async run(fn: (ctx: TestContextApi) => Promise<void>): Promise<void> {
		const ctx = await this.start()
		try {
			await fn(ctx)
		} finally {
			await this.stop()
		}
	}
}

// ============================================================================
// Test context
// ============================================================================

/** Sends records to a running topology and reads its output. Methods may be destructured. */
export interface TestContextApi {
	/**
	 * Send a record to a topic. Buffers are sent as is; other values are encoded with `codec`,
	 * or as JSON when no codec is given.
	 *
	 * @example
	 * ```typescript
	 * await ctx.send('orders', { id: '1', amount: 100 }, { codec: codec.json() })
	 * await ctx.send('raw', Buffer.from('data'))
	 * await ctx.send('keyed', { data: 1 }, { key: Buffer.from('k1') })
	 * ```
	 */
	send<V>(topic: string, value: V, options?: SendMessageOptions<V>): Promise<void>

	/** Send several records to a topic, in order. */
	sendAll<V>(topic: string, values: V[], options?: SendMessageOptions<V>): Promise<void>

	/**
	 * Records produced to a topic, decoded with the given codecs (raw buffers otherwise).
	 *
	 * @example
	 * ```typescript
	 * const orders = ctx.output('orders', { value: codec.json<Order>() })
	 * ```
	 */
	output<K = Buffer, V = Buffer>(topic: string, options?: OutputOptions<K, V>): DecodedRecord<K, V>[]

	/**
	 * Poll until at least `count` records were produced to a topic.
	 *
	 * @example
	 * ```typescript
	 * const messages = await ctx.waitForOutput('out', 3, { timeout: 5000 })
	 * ```
	 */
	waitForOutput<K = Buffer, V = Buffer>(
		topic: string,
		count: number,
		options?: WaitOptions<K, V>
	): Promise<DecodedRecord<K, V>[]>

	/** The first mock producer, for advanced assertions. */
	readonly producer: MockProducer

	/** The first mock consumer, for advanced control. */
	readonly consumer: MockConsumer

	/** Clear all captured output. */
	clear(): void
}

export interface SendMessageOptions<V> {
	/** Codec to encode the value */
	codec?: Codec<V>
	/** Message key (raw buffer) */
	key?: Buffer | null
	/** Message key (will be encoded with keyCodec) */
	keyValue?: unknown
	/** Key codec for encoding keyValue */
	keyCodec?: Codec<unknown>
	/** Partition to send to */
	partition?: number
	/** Message timestamp */
	timestamp?: bigint
	/** Message headers */
	headers?: Record<string, Buffer>
}

export interface OutputOptions<K, V> {
	/** Codec to decode keys */
	key?: Codec<K>
	/** Codec to decode values */
	value?: Codec<V>
}

export interface WaitOptions<K, V> extends OutputOptions<K, V> {
	/** Timeout in milliseconds */
	timeout?: number
	/** Poll interval in milliseconds */
	interval?: number
}

export interface DecodedRecord<K, V> {
	topic: string
	key: K | null
	value: V | null
	partition?: number
}

function decodeRecord<K, V>(
	record: TestRecord<Buffer, Buffer>,
	options: OutputOptions<K, V> = {}
): DecodedRecord<K, V> {
	const decode = <T>(buffer: Buffer | null, codec?: Codec<T>): T | null =>
		buffer === null ? null : codec ? codec.decode(buffer) : (buffer as unknown as T)
	return {
		topic: record.topic,
		key: decode(record.key, options.key),
		value: decode(record.value, options.value),
		partition: record.partition,
	}
}

class TestContextImpl implements TestContextApi {
	constructor(private readonly driver: TestDriver) {}

	get consumer(): MockConsumer {
		const consumer = this.driver.consumers[0]
		if (!consumer) {
			throw new Error('No consumer created - ensure you have defined a topology')
		}
		return consumer
	}

	get producer(): MockProducer {
		const producer = this.driver.producers[0]
		if (!producer) {
			throw new Error('No producer created - ensure you have defined a topology with output')
		}
		return producer
	}

	// Arrow functions so the methods can be destructured from the context.
	send = async <V>(topic: string, value: V, options: SendMessageOptions<V> = {}): Promise<void> => {
		const encoded = Buffer.isBuffer(value)
			? value
			: options.codec
				? options.codec.encode(value)
				: Buffer.from(JSON.stringify(value))
		const key =
			options.keyValue !== undefined && options.keyCodec
				? options.keyCodec.encode(options.keyValue)
				: (options.key ?? null)

		await this.consumer.sendMessage(topic, encoded, {
			key,
			partition: options.partition,
			timestamp: options.timestamp,
			headers: options.headers,
		})
	}

	sendAll = async <V>(topic: string, values: V[], options: SendMessageOptions<V> = {}): Promise<void> => {
		for (const value of values) {
			await this.send(topic, value, options)
		}
	}

	output = <K = Buffer, V = Buffer>(topic: string, options: OutputOptions<K, V> = {}): DecodedRecord<K, V>[] =>
		this.producer.messagesFor(topic).map(record => decodeRecord(record, options))

	waitForOutput = async <K = Buffer, V = Buffer>(
		topic: string,
		count: number,
		options: WaitOptions<K, V> = {}
	): Promise<DecodedRecord<K, V>[]> => {
		const { timeout = 5000, interval = 10 } = options
		const deadline = Date.now() + timeout
		while (Date.now() < deadline) {
			const output = this.output(topic, options)
			if (output.length >= count) {
				return output
			}
			await new Promise(resolve => setTimeout(resolve, interval))
		}
		const current = this.producer.messagesFor(topic).length
		throw new Error(
			`Timeout waiting for ${count} messages on topic "${topic}". ` +
				`Only received ${current} messages after ${timeout}ms.`
		)
	}

	clear = (): void => {
		this.producer.clear()
	}
}

// ============================================================================
// Result collector
// ============================================================================

/**
 * Captures stream output without writing it to a topic.
 *
 * @example
 * ```typescript
 * const results = new ResultCollector<string, Order>()
 *
 * driver.input('orders', { key: codec.string(), value: codec.json<Order>() })
 *   .filter((_, v) => v.amount > 100)
 *   .peek(results.collector())
 *
 * await driver.run(async ({ send }) => {
 *   await send('orders', { id: '1', amount: 150 }, { key: Buffer.from('k1') })
 *   expect(results.get(0).value.amount).toBe(150)
 * })
 * ```
 */
export class ResultCollector<K, V> {
	private readonly items: Array<{ key: K; value: V }> = []

	/** A callback for `.peek()`. Throws on a null key or value. */
	collector(): (key: K | null, value: V | null) => void {
		return (key, value) => {
			if (key === null || value === null) {
				throw new Error(
					'ResultCollector received a null key/value. Use ResultCollector<K | null, V | null> if desired.'
				)
			}
			this.items.push({ key, value })
		}
	}

	get records(): ReadonlyArray<{ key: K; value: V }> {
		return this.items
	}

	get values(): V[] {
		return this.items.map(item => item.value)
	}

	get keys(): K[] {
		return this.items.map(item => item.key)
	}

	/** The record at `index`; throws when there is none. */
	get(index: number): { key: K; value: V } {
		const item = this.items[index]
		if (!item) {
			throw new Error(`No record at index ${index}. Collection has ${this.items.length} items.`)
		}
		return item
	}

	get first(): { key: K; value: V } | undefined {
		return this.items[0]
	}

	get last(): { key: K; value: V } | undefined {
		return this.items[this.items.length - 1]
	}

	get length(): number {
		return this.items.length
	}

	get isEmpty(): boolean {
		return this.items.length === 0
	}

	clear(): void {
		this.items.length = 0
	}

	filter(predicate: (key: K, value: V) => boolean): Array<{ key: K; value: V }> {
		return this.items.filter(item => predicate(item.key, item.value))
	}

	some(predicate: (key: K, value: V) => boolean): boolean {
		return this.items.some(item => predicate(item.key, item.value))
	}

	every(predicate: (key: K, value: V) => boolean): boolean {
		return this.items.every(item => predicate(item.key, item.value))
	}
}

// ============================================================================
// Records, factories and timestamps
// ============================================================================

interface ProcessorTestRecord<K, V> {
	topic: string
	key: K | null
	value: V
	partition: number
	offset: bigint
	timestamp: bigint
	headers: Record<string, Buffer>
}

/**
 * Create a record for testing a processor directly.
 *
 * @example
 * ```typescript
 * const record = testRecord('orders', 'key1', { id: '1', amount: 100 })
 * ```
 */
export function testRecord<K, V>(
	topic: string,
	key: K | null,
	value: V,
	options: { partition?: number; offset?: bigint; timestamp?: bigint; headers?: Record<string, Buffer> } = {}
): ProcessorTestRecord<K, V> {
	return {
		topic,
		key,
		value,
		partition: options.partition ?? 0,
		offset: options.offset ?? 0n,
		timestamp: options.timestamp ?? BigInt(Date.now()),
		headers: options.headers ?? {},
	}
}

/**
 * Create records with consecutive offsets and evenly spaced timestamps.
 *
 * @example
 * ```typescript
 * const records = testRecordSequence('events', [
 *   { key: 'k1', value: { type: 'a' } },
 *   { key: 'k2', value: { type: 'b' } },
 * ], { startTimestamp: 1000n, timestampIncrement: 100n })
 * ```
 */
export function testRecordSequence<K, V>(
	topic: string,
	items: Array<{ key: K | null; value: V }>,
	options: { startTimestamp?: bigint; timestampIncrement?: bigint; partition?: number } = {}
): Array<ProcessorTestRecord<K, V>> {
	const { startTimestamp = 0n, timestampIncrement = 1n, partition = 0 } = options
	return items.map((item, index) =>
		testRecord(topic, item.key, item.value, {
			partition,
			offset: BigInt(index),
			timestamp: startTimestamp + timestampIncrement * BigInt(index),
		})
	)
}

/**
 * Create a test data factory. `generator` receives a counter that increments on every `create()`.
 *
 * @example
 * ```typescript
 * const orderFactory = createFactory<Order>(index => ({ id: `order-${index}`, amount: 10 }))
 * const orders = orderFactory.createMany(100)
 * const bigOrder = orderFactory.create({ amount: 10000 })
 * ```
 */
export function createFactory<T>(generator: (index: number) => T) {
	let counter = 0
	return {
		/** Create one instance, with optional overrides. */
		create(overrides: Partial<T> = {}): T {
			return { ...generator(counter++), ...overrides }
		},
		/** Create `count` instances, with optional overrides. */
		createMany(count: number, overrides: Partial<T> = {}): T[] {
			return Array.from({ length: count }, () => this.create(overrides))
		},
		/** Restart the counter at 0. */
		reset(): void {
			counter = 0
		},
	}
}

const DURATION_UNIT_MS: Record<string, bigint> = { ms: 1n, s: 1000n, m: 60_000n, h: 3_600_000n, d: 86_400_000n }

function parseDurationMs(duration: string | number): bigint {
	if (typeof duration === 'number') return BigInt(duration)
	const match = duration.match(/^(\d+)(ms|s|m|h|d)$/)
	if (!match) throw new Error(`Invalid duration: ${duration}`)
	return BigInt(match[1]!) * DURATION_UNIT_MS[match[2]!]!
}

/**
 * Timestamps relative to a base time. Durations are milliseconds or strings like `'500ms'`,
 * `'5s'`, `'1m'`, `'2h'`, `'1d'`.
 *
 * @example
 * ```typescript
 * const ts = timestamps()
 * await ctx.send('events', data, { timestamp: ts.at(0) })      // base time
 * await ctx.send('events', data, { timestamp: ts.after('5s') }) // 5 seconds after base
 * ```
 */
export function timestamps(baseTime: bigint = 0n) {
	return {
		/** Base time plus `offset`. */
		at(offset: string | number): bigint {
			return baseTime + parseDurationMs(offset)
		},
		/** Alias for `at()`. */
		after(duration: string | number): bigint {
			return this.at(duration)
		},
		get base(): bigint {
			return baseTime
		},
		/** A new helper whose base is `duration` later. */
		advance(duration: string | number) {
			return timestamps(baseTime + parseDurationMs(duration))
		},
	}
}

// ============================================================================
// quickTest
// ============================================================================

/**
 * Build and exercise a topology in one function. The driver starts on the first `send`,
 * `sendAll` or `waitForOutput`, and is stopped (and all mocks restored) afterwards.
 *
 * @example
 * ```typescript
 * await quickTest(async ({ input, output, send }) => {
 *   input('in', { value: codec.json<Event>() })
 *     .mapValues(e => ({ ...e, processed: true }))
 *     .to('out', { value: codec.json() })
 *
 *   await send('in', { type: 'click' })
 *   expect(output('out')).toHaveLength(1)
 * })
 * ```
 */
export async function quickTest(
	fn: (helpers: {
		driver: TestDriver
		input: TestDriver['input']
		table: TestDriver['table']
		flow: FlowApp
		send: TestContextApi['send']
		sendAll: TestContextApi['sendAll']
		output: TestContextApi['output']
		waitForOutput: TestContextApi['waitForOutput']
		clear: TestContextApi['clear']
		producer: MockProducer
		consumer: MockConsumer
	}) => Promise<void>,
	config: TestDriverConfig = {}
): Promise<void> {
	const driver = new TestDriver(config)
	let started: Promise<TestContextApi> | undefined
	const context = () => (started ??= driver.start())

	try {
		await fn({
			driver,
			input: driver.input.bind(driver),
			table: driver.table.bind(driver),
			get flow() {
				return driver.flow
			},
			send: async (topic, value, options) => (await context()).send(topic, value, options),
			sendAll: async (topic, values, options) => (await context()).sendAll(topic, values, options),
			waitForOutput: async (topic, count, options) => (await context()).waitForOutput(topic, count, options),
			// Readable before the driver starts: no producer yet means no output.
			output: (topic, options) =>
				driver.producers[0]?.messagesFor(topic).map(record => decodeRecord(record, options)) ?? [],
			clear: () => driver.producers[0]?.clear(),
			get producer() {
				return driver.producers[0]!
			},
			get consumer() {
				return driver.consumers[0]!
			},
		})
	} finally {
		await driver.stop()
		vi.restoreAllMocks()
	}
}

export { vi } from 'vitest'
