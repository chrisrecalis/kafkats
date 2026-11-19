/**
 * Test helpers of `@kafkats/flow/testing`: ResultCollector (peek callbacks), records and record sequences for processor
 * tests, data factories, timestamp arithmetic and quickTest. Free of any test framework.
 */
import type { RecordHeaders } from '@/processor/record.js'
import { decodeRecord, TestDriver, type TestContextApi, type TestDriverConfig } from '@/testing/driver.js'
import type { MockConsumer, MockProducer } from '@/testing/mocks.js'

/** Captures stream output without writing it to a topic: `stream.peek(results.collector())`. */
export class ResultCollector<K, V> {
	private readonly items: { key: K; value: V }[] = []

	/** A callback for `peek()`. Throws on a null key or value. */
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

	get records(): readonly { key: K; value: V }[] {
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
		return this.items.at(-1)
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

	filter(predicate: (key: K, value: V) => boolean): { key: K; value: V }[] {
		return this.items.filter(item => predicate(item.key, item.value))
	}

	some(predicate: (key: K, value: V) => boolean): boolean {
		return this.items.some(item => predicate(item.key, item.value))
	}

	every(predicate: (key: K, value: V) => boolean): boolean {
		return this.items.every(item => predicate(item.key, item.value))
	}
}

/** A consumed record as a processor test sees it. */
export interface ProcessorTestRecord<K, V> {
	topic: string
	key: K | null
	value: V
	partition: number
	offset: bigint
	timestamp: bigint
	headers: RecordHeaders
}

/** A record for testing a processor directly. */
export function testRecord<K, V>(
	topic: string,
	key: K | null,
	value: V,
	options: { partition?: number; offset?: bigint; timestamp?: bigint; headers?: RecordHeaders } = {}
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

/** Records with consecutive offsets and evenly spaced timestamps. */
export function testRecordSequence<K, V>(
	topic: string,
	items: readonly { key: K | null; value: V }[],
	options: { startTimestamp?: bigint; timestampIncrement?: bigint; partition?: number } = {}
): ProcessorTestRecord<K, V>[] {
	const { startTimestamp = 0n, timestampIncrement = 1n, partition = 0 } = options
	return items.map((item, index) =>
		testRecord(topic, item.key, item.value, {
			partition,
			offset: BigInt(index),
			timestamp: startTimestamp + timestampIncrement * BigInt(index),
		})
	)
}

/** A test data factory: `generator` receives a counter that increments on every `create()`. */
export function createFactory<T>(generator: (index: number) => T) {
	let counter = 0
	return {
		/** One instance, with optional overrides. */
		create(overrides: Partial<T> = {}): T {
			return { ...generator(counter++), ...overrides }
		},
		/** `count` instances, with optional overrides. */
		createMany(count: number, overrides: Partial<T> = {}): T[] {
			return Array.from({ length: count }, () => this.create(overrides))
		},
		/** Restarts the counter at 0. */
		reset(): void {
			counter = 0
		},
	}
}

const DURATION_UNIT_MS: Readonly<Record<string, bigint>> = {
	ms: 1n,
	s: 1000n,
	m: 60_000n,
	h: 3_600_000n,
	d: 86_400_000n,
}

function durationMs(duration: string | number): bigint {
	if (typeof duration === 'number') {
		return BigInt(duration)
	}
	const match = /^(\d+)(ms|s|m|h|d)$/.exec(duration)
	if (!match) {
		throw new Error(`Invalid duration: ${duration}`)
	}
	return BigInt(match[1]!) * DURATION_UNIT_MS[match[2]!]!
}

/** Timestamps relative to a base time; durations are milliseconds or `'500ms'`, `'5s'`, `'1m'`, `'2h'`, `'1d'`. */
export function timestamps(baseTime: bigint = 0n) {
	return {
		/** Base time plus `offset`. */
		at(offset: string | number): bigint {
			return baseTime + durationMs(offset)
		},
		/** Alias of `at()`. */
		after(duration: string | number): bigint {
			return this.at(duration)
		},
		get base(): bigint {
			return baseTime
		},
		/** A helper whose base is `duration` later. */
		advance(duration: string | number) {
			return timestamps(baseTime + durationMs(duration))
		},
	}
}

export interface QuickTestHelpers {
	driver: TestDriver
	input: TestDriver['input']
	table: TestDriver['table']
	flow: TestDriver['flow']
	send: TestContextApi['send']
	sendAll: TestContextApi['sendAll']
	output: TestContextApi['output']
	waitForOutput: TestContextApi['waitForOutput']
	clear: TestContextApi['clear']
	producer: MockProducer
	consumer: MockConsumer
}

/**
 * Builds and exercises a topology in one function. The driver starts on the first `send`, `sendAll` or
 * `waitForOutput` and is stopped afterwards; `output()` is readable before that (no records yet).
 */
export async function quickTest(
	fn: (helpers: QuickTestHelpers) => Promise<void>,
	config: TestDriverConfig = {}
): Promise<void> {
	const driver = new TestDriver(config)
	let started: Promise<TestContextApi> | undefined
	const context = () => (started ??= driver.start())
	try {
		await fn({
			driver,
			input: (topic, consumed) => driver.input(topic, consumed),
			table: (topic, consumed) => driver.table(topic, consumed),
			get flow() {
				return driver.flow
			},
			send: async (topic, value, options) => (await context()).send(topic, value, options),
			sendAll: async (topic, values, options) => (await context()).sendAll(topic, values, options),
			waitForOutput: async (topic, count, options) => (await context()).waitForOutput(topic, count, options),
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
	}
}
