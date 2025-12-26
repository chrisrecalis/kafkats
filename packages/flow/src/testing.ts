/**
 * @kafkats/flow Testing Infrastructure
 *
 * This module provides utilities for testing flow processors and topologies
 * with minimal boilerplate. It's designed to make writing tests easy for
 * both library developers and users implementing custom processors.
 *
 * @example Basic usage
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
import { KafkaClient, type Message } from '@kafkats/client'
import type { Codec } from '@/codec.js'
import { flow, type FlowApp, type FlowConfig, type Consumed } from '@/flow.js'

// ============================================================================
// Types
// ============================================================================

export type TestMessage<K = Buffer, V = Buffer> = Message<V> & { key: K | null }

export interface TestRecord<K = unknown, V = unknown> {
	topic: string
	key: K | null
	value: V
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

// ============================================================================
// Mock Implementations
// ============================================================================

type TestHandler = (message: Message<Buffer>, ctx: TestContext) => Promise<void>
type TestContext = { signal: AbortSignal; topic: string; partition: number; offset: bigint }

/**
 * Mock consumer that allows programmatic message emission
 */
export class MockConsumer extends EventEmitter {
	private handler: TestHandler | null = null
	private stopResolve: (() => void) | null = null
	private offsetCounter = 0n

	consumerGroup = { currentMemberId: 'member-1', currentGenerationId: 1 }

	subscribe(_topics: string | string[]): void {
		// Mock: just accept the subscription
	}

	async runEach(handler: TestHandler): Promise<void> {
		this.handler = handler
		super.emit('running')
		return new Promise(resolve => {
			this.stopResolve = () => {
				super.emit('stopped')
				resolve()
			}
		})
	}

	stop(): void {
		this.stopResolve?.()
		this.stopResolve = null
	}

	/**
	 * Send a message to be processed by the topology
	 */
	async sendMessage(topic: string, value: Buffer, options: SendOptions = {}): Promise<void> {
		if (!this.handler) {
			throw new Error('Consumer not started - call driver.start() first')
		}

		const offset = this.offsetCounter++
		const message: Message<Buffer> = {
			topic,
			partition: options.partition ?? 0,
			offset,
			timestamp: options.timestamp ?? BigInt(Date.now()),
			key: options.key ?? null,
			value,
			headers: options.headers ?? {},
		}

		const ctx: TestContext = {
			signal: new AbortController().signal,
			topic,
			partition: options.partition ?? 0,
			offset,
		}

		await this.handler(message, ctx)
	}
}

/**
 * Mock producer that captures all produced messages
 */
export class MockProducer {
	readonly messages: TestRecord<Buffer, Buffer>[] = []
	readonly transactions: Array<{
		messages: TestRecord<Buffer, Buffer>[]
		offsets: Array<{ topic: string; partition: number; offset: bigint }>
	}> = []

	private currentTxMessages: TestRecord<Buffer, Buffer>[] = []

	send(topic: string, message: { key?: Buffer | null; value: Buffer; partition?: number }): Promise<void> {
		const record: TestRecord<Buffer, Buffer> = {
			topic,
			key: message.key ?? null,
			value: message.value,
			partition: message.partition,
		}
		this.messages.push(record)
		this.currentTxMessages.push(record)
		return Promise.resolve()
	}

	async transaction(
		fn: (tx: {
			send: (topic: string, message: { key?: Buffer | null; value: Buffer; partition?: number }) => Promise<void>
			sendOffsets: (params: {
				groupId?: string
				consumerGroupMetadata?: {
					groupId: string
					generationId: number
					memberId: string
					groupInstanceId?: string | null
				}
				offsets: Array<{ topic: string; partition: number; offset: bigint }>
			}) => Promise<void>
		}) => Promise<void>
	): Promise<void> {
		this.currentTxMessages = []
		const offsets: Array<{ topic: string; partition: number; offset: bigint }> = []

		await fn({
			send: (topic, message) => this.send(topic, message),
			sendOffsets: params => {
				offsets.push(...params.offsets)
				return Promise.resolve()
			},
		})

		this.transactions.push({ messages: [...this.currentTxMessages], offsets })
	}

	async disconnect(): Promise<void> {}

	/**
	 * Get messages for a specific topic
	 */
	messagesFor(topic: string): TestRecord<Buffer, Buffer>[] {
		return this.messages.filter(m => m.topic === topic)
	}

	/**
	 * Clear all captured messages
	 */
	clear(): void {
		this.messages.length = 0
		this.transactions.length = 0
	}
}

// ============================================================================
// Test Driver
// ============================================================================

/**
 * TestDriver provides a complete test harness for flow topologies.
 *
 * It handles:
 * - Mocking the Kafka client
 * - Managing app lifecycle
 * - Sending test messages
 * - Capturing output
 * - Type-safe assertions
 *
 * @example
 * ```typescript
 * const driver = new TestDriver()
 *
 * // Build topology
 * driver.input('events', { value: codec.json<Event>() })
 *   .filter((_, v) => v.type === 'click')
 *   .to('clicks', { value: codec.json() })
 *
 * // Run test
 * await driver.run(async ({ send, output }) => {
 *   await send('events', { type: 'click', target: 'button' })
 *   await send('events', { type: 'view', page: 'home' })
 *
 *   const clicks = output('clicks', codec.json<Event>())
 *   expect(clicks).toHaveLength(1)
 *   expect(clicks[0].value.target).toBe('button')
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
		this.client = new KafkaClient({
			clientId: config.applicationId ?? 'test-app',
			brokers: ['localhost:9092'],
		})

		this.setupMocks()
	}

	private setupMocks(): void {
		vi.spyOn(this.client, 'connect').mockResolvedValue()
		vi.spyOn(this.client, 'disconnect').mockResolvedValue()

		vi.spyOn(this.client, 'consumer').mockImplementation(() => {
			const consumer = new MockConsumer()
			this.consumers.push(consumer)
			return consumer as unknown as never
		})

		vi.spyOn(this.client, 'producer').mockImplementation(() => {
			const producer = new MockProducer()
			this.producers.push(producer)
			return producer as unknown as never
		})
	}

	/**
	 * Get the FlowApp instance for building topologies
	 */
	get flow(): FlowApp {
		if (!this.app) {
			this.app = flow({
				applicationId: this.config.applicationId ?? 'test-app',
				client: this.client,
				...this.config,
			})
		}
		return this.app
	}

	/**
	 * Create a stream from a topic - shorthand for flow.stream()
	 */
	input<K = Buffer, V = Buffer>(topic: string, options?: Consumed<K, V>) {
		return this.flow.stream(topic, options)
	}

	/**
	 * Create a table from a topic - shorthand for flow.table()
	 */
	table<K = Buffer, V = Buffer>(topic: string, options?: Consumed<K, V>) {
		return this.flow.table(topic, options)
	}

	/**
	 * Start the test driver and return test context
	 */
	async start(): Promise<TestContextApi> {
		await this.flow.start()
		return this.createContext()
	}

	/**
	 * Stop the test driver
	 */
	async stop(): Promise<void> {
		await this.app?.close()
	}

	/**
	 * Run a test with automatic lifecycle management
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

	private createContext(): TestContextApi {
		return new TestContextImpl(
			() => {
				const consumer = this.consumers[0]
				if (!consumer) {
					throw new Error('No consumer created - ensure you have defined a topology')
				}
				return consumer
			},
			() => {
				const producer = this.producers[0]
				if (!producer) {
					throw new Error('No producer created - ensure you have defined a topology with output')
				}
				return producer
			}
		)
	}
}

/**
 * Test context providing methods for sending messages and reading output
 */
export interface TestContextApi {
	/**
	 * Send a message to a topic
	 *
	 * @example
	 * ```typescript
	 * // With codec - value will be encoded automatically
	 * await ctx.send('orders', { id: '1', amount: 100 }, { codec: codec.json() })
	 *
	 * // With raw buffer
	 * await ctx.send('raw', Buffer.from('data'))
	 *
	 * // With key
	 * await ctx.send('keyed', { data: 1 }, { key: 'k1', codec: codec.json() })
	 * ```
	 */
	send<V>(topic: string, value: V, options?: SendMessageOptions<V>): Promise<void>

	/**
	 * Send multiple messages to a topic
	 */
	sendAll<V>(topic: string, values: V[], options?: SendMessageOptions<V>): Promise<void>

	/**
	 * Get all output messages for a topic
	 *
	 * @example
	 * ```typescript
	 * // Get raw buffers
	 * const raw = ctx.output('out')
	 *
	 * // Get decoded values
	 * const orders = ctx.output('orders', { value: codec.json<Order>() })
	 * ```
	 */
	output<K = Buffer, V = Buffer>(topic: string, options?: OutputOptions<K, V>): DecodedRecord<K, V>[]

	/**
	 * Wait for a specific number of messages on a topic
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

	/**
	 * Get the underlying mock producer for advanced assertions
	 */
	readonly producer: MockProducer

	/**
	 * Get the underlying mock consumer for advanced control
	 */
	readonly consumer: MockConsumer

	/**
	 * Clear all captured output
	 */
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
	value: V
	partition?: number
}

class TestContextImpl implements TestContextApi {
	private readonly _getConsumer: () => MockConsumer
	private readonly _getProducer: () => MockProducer

	constructor(getConsumer: () => MockConsumer, getProducer: () => MockProducer) {
		this._getConsumer = getConsumer
		this._getProducer = getProducer

		// Bind all methods to preserve 'this' when destructured
		this.send = this.send.bind(this)
		this.sendAll = this.sendAll.bind(this)
		this.output = this.output.bind(this)
		this.waitForOutput = this.waitForOutput.bind(this)
		this.clear = this.clear.bind(this)
	}

	get consumer(): MockConsumer {
		return this._getConsumer()
	}

	get producer(): MockProducer {
		return this._getProducer()
	}

	async send<V>(topic: string, value: V, options: SendMessageOptions<V> = {}): Promise<void> {
		let encodedValue: Buffer
		if (Buffer.isBuffer(value)) {
			encodedValue = value
		} else if (options.codec) {
			encodedValue = options.codec.encode(value)
		} else {
			// Default to JSON encoding for non-buffer values
			encodedValue = Buffer.from(JSON.stringify(value))
		}

		let key: Buffer | null = options.key ?? null
		if (options.keyValue !== undefined && options.keyCodec) {
			key = options.keyCodec.encode(options.keyValue)
		}

		await this.consumer.sendMessage(topic, encodedValue, {
			key,
			partition: options.partition,
			timestamp: options.timestamp,
			headers: options.headers,
		})
	}

	async sendAll<V>(topic: string, values: V[], options: SendMessageOptions<V> = {}): Promise<void> {
		for (const value of values) {
			await this.send(topic, value, options)
		}
	}

	output<K = Buffer, V = Buffer>(topic: string, options: OutputOptions<K, V> = {}): DecodedRecord<K, V>[] {
		const messages = this.producer.messagesFor(topic)
		return messages.map(msg => this.decodeRecord(msg, options))
	}

	async waitForOutput<K = Buffer, V = Buffer>(
		topic: string,
		count: number,
		options: WaitOptions<K, V> = {}
	): Promise<DecodedRecord<K, V>[]> {
		const { timeout = 5000, interval = 10 } = options
		const start = Date.now()

		while (Date.now() - start < timeout) {
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

	clear(): void {
		this.producer.clear()
	}

	private decodeRecord<K, V>(record: TestRecord<Buffer, Buffer>, options: OutputOptions<K, V>): DecodedRecord<K, V> {
		let key: K | null = null
		if (record.key !== null) {
			key = options.key ? options.key.decode(record.key) : (record.key as unknown as K)
		}

		let value: V
		if (options.value) {
			value = options.value.decode(record.value)
		} else {
			value = record.value as unknown as V
		}

		return {
			topic: record.topic,
			key,
			value,
			partition: record.partition,
		}
	}
}

// ============================================================================
// Result Collector
// ============================================================================

/**
 * ResultCollector provides a declarative way to capture stream output
 * without sending to a topic.
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
 *   await send('orders', { id: '1', amount: 150 }, { keyValue: 'k1', keyCodec: codec.string() })
 *   await send('orders', { id: '2', amount: 50 }, { keyValue: 'k2', keyCodec: codec.string() })
 *
 *   expect(results.values).toHaveLength(1)
 *   expect(results.get(0).value.amount).toBe(150)
 * })
 * ```
 */
export class ResultCollector<K, V> {
	private readonly items: Array<{ key: K; value: V }> = []

	/**
	 * Get a collector function to use with .peek()
	 */
	collector(): (key: K, value: V) => void {
		return (key, value) => {
			this.items.push({ key, value })
		}
	}

	/**
	 * Get all collected records
	 */
	get records(): ReadonlyArray<{ key: K; value: V }> {
		return this.items
	}

	/**
	 * Get all collected values
	 */
	get values(): V[] {
		return this.items.map(item => item.value)
	}

	/**
	 * Get all collected keys
	 */
	get keys(): K[] {
		return this.items.map(item => item.key)
	}

	/**
	 * Get a specific record by index
	 */
	get(index: number): { key: K; value: V } {
		const item = this.items[index]
		if (!item) {
			throw new Error(`No record at index ${index}. Collection has ${this.items.length} items.`)
		}
		return item
	}

	/**
	 * Get the last record
	 */
	get last(): { key: K; value: V } | undefined {
		return this.items[this.items.length - 1]
	}

	/**
	 * Get the first record
	 */
	get first(): { key: K; value: V } | undefined {
		return this.items[0]
	}

	/**
	 * Number of collected records
	 */
	get length(): number {
		return this.items.length
	}

	/**
	 * Check if collection is empty
	 */
	get isEmpty(): boolean {
		return this.items.length === 0
	}

	/**
	 * Clear all collected records
	 */
	clear(): void {
		this.items.length = 0
	}

	/**
	 * Find records matching a predicate
	 */
	filter(predicate: (key: K, value: V) => boolean): Array<{ key: K; value: V }> {
		return this.items.filter(item => predicate(item.key, item.value))
	}

	/**
	 * Check if any record matches a predicate
	 */
	some(predicate: (key: K, value: V) => boolean): boolean {
		return this.items.some(item => predicate(item.key, item.value))
	}

	/**
	 * Check if all records match a predicate
	 */
	every(predicate: (key: K, value: V) => boolean): boolean {
		return this.items.every(item => predicate(item.key, item.value))
	}
}

// ============================================================================
// Processor Testing Utilities
// ============================================================================

/**
 * Create a test record for processor testing
 *
 * @example
 * ```typescript
 * const record = testRecord('orders', 'key1', { id: '1', amount: 100 })
 * await myProcessor.process(record)
 * ```
 */
export function testRecord<K, V>(
	topic: string,
	key: K | null,
	value: V,
	options: {
		partition?: number
		offset?: bigint
		timestamp?: bigint
		headers?: Record<string, Buffer>
	} = {}
): {
	topic: string
	key: K | null
	value: V
	partition: number
	offset: bigint
	timestamp: bigint
	headers: Record<string, Buffer>
} {
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
 * Create a sequence of test records with incrementing timestamps
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
	options: {
		startTimestamp?: bigint
		timestampIncrement?: bigint
		partition?: number
	} = {}
): Array<ReturnType<typeof testRecord<K, V>>> {
	const { startTimestamp = 0n, timestampIncrement = 1n, partition = 0 } = options

	return items.map((item, index) =>
		testRecord(topic, item.key, item.value, {
			partition,
			offset: BigInt(index),
			timestamp: startTimestamp + timestampIncrement * BigInt(index),
		})
	)
}

// ============================================================================
// Fixtures and Factories
// ============================================================================

/**
 * Create a factory for generating test data
 *
 * @example
 * ```typescript
 * const orderFactory = createFactory<Order>((index) => ({
 *   id: `order-${index}`,
 *   amount: Math.random() * 1000,
 *   customer: `customer-${index % 10}`,
 * }))
 *
 * const orders = orderFactory.createMany(100)
 * const bigOrder = orderFactory.create({ amount: 10000 })
 * ```
 */
export function createFactory<T>(generator: (index: number) => T) {
	let counter = 0

	return {
		/**
		 * Create a single instance with optional overrides
		 */
		create(overrides: Partial<T> = {}): T {
			const base = generator(counter++)
			return { ...base, ...overrides }
		},

		/**
		 * Create multiple instances
		 */
		createMany(count: number, overrides: Partial<T> = {}): T[] {
			return Array.from({ length: count }, () => this.create(overrides))
		},

		/**
		 * Reset the counter
		 */
		reset(): void {
			counter = 0
		},
	}
}

// ============================================================================
// Timing Utilities
// ============================================================================

/**
 * Create timestamps for time-based testing
 *
 * @example
 * ```typescript
 * const ts = timestamps()
 * await ctx.send('events', data, { timestamp: ts.at(0) })      // Base time
 * await ctx.send('events', data, { timestamp: ts.after('5s') }) // 5 seconds later
 * await ctx.send('events', data, { timestamp: ts.after('1m') }) // 1 minute after base
 * ```
 */
export function timestamps(baseTime: bigint = 0n) {
	const parseMs = (duration: string | number): bigint => {
		if (typeof duration === 'number') return BigInt(duration)

		const match = duration.match(/^(\d+)(ms|s|m|h|d)$/)
		if (!match) throw new Error(`Invalid duration: ${duration}`)

		const value = BigInt(match[1]!)
		const unit = match[2]!

		switch (unit) {
			case 'ms':
				return value
			case 's':
				return value * 1000n
			case 'm':
				return value * 60n * 1000n
			case 'h':
				return value * 60n * 60n * 1000n
			case 'd':
				return value * 24n * 60n * 60n * 1000n
			default:
				throw new Error(`Unknown unit: ${unit}`)
		}
	}

	return {
		/**
		 * Get timestamp at a specific offset from base
		 */
		at(offset: string | number): bigint {
			return baseTime + parseMs(offset)
		},

		/**
		 * Alias for at() - get timestamp after duration
		 */
		after(duration: string | number): bigint {
			return this.at(duration)
		},

		/**
		 * Get the base timestamp
		 */
		get base(): bigint {
			return baseTime
		},

		/**
		 * Create a new timestamps helper with updated base
		 */
		advance(duration: string | number) {
			return timestamps(baseTime + parseMs(duration))
		},
	}
}

// ============================================================================
// Quick Test Helpers
// ============================================================================

/**
 * Create and run a simple test with minimal setup
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
	let ctx: TestContextApi | null = null
	let started = false

	// Helper to ensure the driver is started before sending
	const ensureStarted = async (): Promise<TestContextApi> => {
		if (!started) {
			started = true
			ctx = await driver.start()
		}
		return ctx!
	}

	try {
		await fn({
			driver,
			input: driver.input.bind(driver),
			table: driver.table.bind(driver),
			get flow() {
				return driver.flow
			},
			async send<V>(topic: string, value: V, options?: SendMessageOptions<V>) {
				const c = await ensureStarted()
				return c.send(topic, value, options)
			},
			async sendAll<V>(topic: string, values: V[], options?: SendMessageOptions<V>) {
				const c = await ensureStarted()
				return c.sendAll(topic, values, options)
			},
			output<K = Buffer, V = Buffer>(topic: string, options?: OutputOptions<K, V>) {
				const producer = driver.producers[0]
				if (!producer) return []
				const messages = producer.messagesFor(topic)
				return messages.map(m => ({
					topic: m.topic,
					key: (options?.key ? options.key.decode(m.key!) : m.key) as K | null,
					value: (options?.value ? options.value.decode(m.value) : m.value) as V,
					partition: m.partition,
				}))
			},
			async waitForOutput<K = Buffer, V = Buffer>(topic: string, count: number, options?: WaitOptions<K, V>) {
				const c = await ensureStarted()
				return c.waitForOutput(topic, count, options)
			},
			clear() {
				driver.producers[0]?.clear()
			},
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

// ============================================================================
// Re-exports for convenience
// ============================================================================

export { vi } from 'vitest'
