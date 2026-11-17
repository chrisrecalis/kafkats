/**
 * TestDriver (3.16, 4.14): runs a topology without Kafka, deterministically, on the production task runtime (KS
 * `TopologyTestDriver`). Each `send()` resolves once the record and everything it led to were processed, records
 * written to topics the topology reads included; commits follow every record by default. The wall clock is a mock
 * that moves only through `advanceWallClockTime()`.
 */
import type { FlowApp, FlowConfig } from '@/api/app.js'
import type { KStream } from '@/api/kstream.js'
import type { GlobalKTable, KTable } from '@/api/ktable.js'
import type { Consumed, Materialized } from '@/api/options.js'
import type { StoreDefinition } from '@/api/stores.js'
import { flow } from '@/app/flow.js'
import { internals } from '@/app/internal.js'
import type { Codec } from '@/codec.js'
import type { RecordHeaders } from '@/processor/record.js'
import type { StateStore } from '@/state.js'
import { TestLoop } from '@/testing/loop.js'
import type { MockConsumer, MockProducer, TestRecord } from '@/testing/mocks.js'

export type TestDriverConfig = Partial<Omit<FlowConfig, 'client' | 'applicationId'>> & {
	/** Default 'test-app'. */
	applicationId?: string
	/** Start of the mock wall clock. Default: `Date.now()` when the driver is created. */
	initialWallClockTimeMs?: number
	/**
	 * Partition counts by topic, as partitioners see them. Topics the topology reads that are not listed have as many
	 * partitions as the records sent to them need; other topics have 1.
	 */
	partitions?: Record<string, number>
	/**
	 * When offsets (under exactly_once: transactions) are committed: after every record (default, like Kafka Streams'
	 * TopologyTestDriver), only when the driver stops, or whenever the mock wall clock moved this many ms since the
	 * last commit.
	 */
	commitEvery?: 'record' | 'stop' | number
	/** Throttle of emit-on-window-close and outer-join emission, on the mock wall clock. Default 0. */
	emitIntervalMs?: number
}

export interface SendMessageOptions<V> {
	/** Encodes the value; without one, values that are not Buffers are sent as JSON. */
	codec?: Codec<V>
	/** The key as bytes. */
	key?: Buffer | null
	/** The key, encoded with `keyCodec`. */
	keyValue?: unknown
	keyCodec?: Codec<unknown>
	/** Default 0. */
	partition?: number
	/** Default: the mock wall clock. */
	timestamp?: bigint | number
	headers?: RecordHeaders
}

export interface OutputOptions<K, V> {
	key?: Codec<K>
	value?: Codec<V>
}

export interface WaitOptions<K, V> extends OutputOptions<K, V> {
	/** Default 5000 ms. */
	timeout?: number
	/** Default 10 ms. */
	interval?: number
}

export interface DecodedRecord<K, V> {
	topic: string
	key: K | null
	value: V | null
	partition?: number
}

/** Sends records to a started driver and reads what it produced. The methods may be destructured. */
export interface TestContextApi {
	/** Sends a record: Buffers as they are, null as a tombstone, other values with `codec` or as JSON. */
	send<V>(topic: string, value: V | Buffer | null, options?: SendMessageOptions<V>): Promise<void>
	/** Sends several records, in order. */
	sendAll<V>(topic: string, values: readonly V[], options?: SendMessageOptions<V>): Promise<void>
	/** Records produced to a topic, decoded with the given codecs (raw Buffers otherwise). */
	output<K = Buffer, V = Buffer>(topic: string, options?: OutputOptions<K, V>): DecodedRecord<K, V>[]
	/** Polls until at least `count` records were produced to a topic. */
	waitForOutput<K = Buffer, V = Buffer>(
		topic: string,
		count: number,
		options?: WaitOptions<K, V>
	): Promise<DecodedRecord<K, V>[]>
	/** Moves the mock wall clock: wall-clock punctuations run, then whatever became processable. */
	advanceWallClockTime(ms: number): Promise<void>
	/**
	 * A state store of the task of `partition` (default 0), or a global store. A store definition (`Stores`) gives it
	 * its type.
	 */
	getStateStore<S extends StateStore>(store: StoreDefinition<S>, partition?: number): S
	getStateStore<S extends StateStore>(name: string, partition?: number): S
	readonly producer: MockProducer
	readonly consumer: MockConsumer
	/** Forgets the produced records. */
	clear(): void
}

export class TestDriver {
	private app: FlowApp | undefined
	private running: Promise<TestLoop> | undefined
	/** The loop of the last start: its producer and consumer stay readable after stop(). */
	private loop: TestLoop | undefined
	private readonly initialWallClockTimeMs: number

	constructor(private readonly config: TestDriverConfig = {}) {
		this.initialWallClockTimeMs = config.initialWallClockTimeMs ?? Date.now()
	}

	/** The app under test, created on first use. It never connects: the driver runs its topology. */
	get flow(): FlowApp {
		if (this.app === undefined) {
			const {
				initialWallClockTimeMs: _clock,
				partitions: _partitions,
				commitEvery: _commitEvery,
				emitIntervalMs: _emitIntervalMs,
				...config
			} = this.config
			const applicationId = config.applicationId ?? 'test-app'
			this.app = flow({
				...config,
				applicationId,
				client: { clientId: applicationId, brokers: ['localhost:9092'] },
			})
		}
		return this.app
	}

	input<K = Buffer, V = Buffer>(topic: string, consumed?: Consumed<K, V>): KStream<K, V> {
		return this.flow.stream(topic, consumed)
	}

	table<K = Buffer, V = Buffer>(
		topic: string,
		consumed?: Consumed<K, V> & { materialized?: Materialized<K, V> }
	): KTable<K, V> {
		return this.flow.table(topic, consumed)
	}

	globalTable<K = Buffer, V = Buffer>(
		topic: string,
		consumed?: Consumed<K, V> & { materialized?: Materialized<K, V> }
	): GlobalKTable<K, V> {
		return this.flow.globalTable(topic, consumed)
	}

	/** Builds the topology (later DSL calls throw) and creates the tasks of partition 0. */
	async start(): Promise<TestContextApi> {
		const running = (this.running ??= TestLoop.start(internals(this.flow), {
			commitEvery: this.config.commitEvery ?? 'record',
			partitions: this.config.partitions ?? {},
			initialWallClockTimeMs: this.initialWallClockTimeMs,
			emitIntervalMs: this.config.emitIntervalMs ?? 0,
		}))
		try {
			this.loop = await running
		} catch (error) {
			if (this.running === running) this.running = undefined
			throw error
		}
		return new TestContextImpl(this.loop)
	}

	/** Commits and closes every task (after a failure: closes them dirty). */
	async stop(): Promise<void> {
		const running = this.running
		this.running = undefined
		const loop = await running?.catch(() => undefined)
		await loop?.close()
	}

	/** start(), `fn`, then stop(). */
	async run(fn: (ctx: TestContextApi) => Promise<void>): Promise<void> {
		const ctx = await this.start()
		try {
			await fn(ctx)
		} finally {
			await this.stop()
		}
	}

	/** The consumer: one once started. */
	get consumers(): readonly MockConsumer[] {
		return this.loop ? [this.loop.consumer] : []
	}

	/** The producer: one once started. */
	get producers(): readonly MockProducer[] {
		return this.loop ? [this.loop.producer] : []
	}
}

export function decodeRecord<K, V>(
	record: TestRecord<Buffer, Buffer>,
	options: OutputOptions<K, V> = {}
): DecodedRecord<K, V> {
	const decode = <T>(bytes: Buffer | null, codec?: Codec<T>): T | null =>
		bytes === null ? null : codec ? codec.decode(bytes) : (bytes as T)
	return {
		topic: record.topic,
		key: decode(record.key, options.key),
		value: decode(record.value, options.value),
		partition: record.partition,
	}
}

class TestContextImpl implements TestContextApi {
	constructor(private readonly loop: TestLoop) {}

	get producer(): MockProducer {
		return this.loop.producer
	}

	get consumer(): MockConsumer {
		return this.loop.consumer
	}

	// Arrow functions, so that the methods can be destructured.

	send = async <V>(topic: string, value: V | Buffer | null, options: SendMessageOptions<V> = {}): Promise<void> => {
		const encoded =
			value === null
				? null
				: Buffer.isBuffer(value)
					? value
					: options.codec
						? options.codec.encode(value)
						: Buffer.from(JSON.stringify(value))
		const key =
			options.keyValue !== undefined && options.keyCodec
				? options.keyCodec.encode(options.keyValue)
				: (options.key ?? null)
		const { partition, timestamp, headers } = options
		await this.consumer.sendMessage(topic, encoded, { key, partition, timestamp, headers })
	}

	sendAll = async <V>(topic: string, values: readonly V[], options: SendMessageOptions<V> = {}): Promise<void> => {
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
		for (;;) {
			const output = this.output(topic, options)
			if (output.length >= count) {
				return output
			}
			if (Date.now() >= deadline) {
				throw new Error(
					`Timeout waiting for ${count} messages on topic "${topic}". ` +
						`Only received ${output.length} messages after ${timeout}ms.`
				)
			}
			await new Promise(resolve => setTimeout(resolve, interval))
		}
	}

	advanceWallClockTime = (ms: number): Promise<void> => this.loop.advanceWallClockTime(ms)

	getStateStore: TestContextApi['getStateStore'] = <S extends StateStore>(
		store: string | StoreDefinition<S>,
		partition?: number
	): S => this.loop.store<S>(typeof store === 'string' ? store : store.name, partition)

	clear = (): void => {
		this.producer.clear()
	}
}
