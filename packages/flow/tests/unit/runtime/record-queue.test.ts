import { describe, expect, it } from 'vitest'
import type { Message } from '@kafkats/client'
import { FailedProcessingError, InvalidTimestampError, StreamsError } from '@/api/errors.js'
import type { DeserializationExceptionHandler, ErrorHandlerContext } from '@/api/handlers.js'
import { json, string, type Codec } from '@/codec.js'
import type { DroppedReason } from '@/processor/api.js'
import {
	builtInRule,
	TimestampExtractors,
	type ConsumedRecord,
	type TimestampExtractor,
} from '@/processor/timestamps.js'
import { isSkipMarker, RecordQueue, type StampedRecord } from '@/runtime/record-queue.js'
import { TaskIdRegistry } from '@/runtime/task-id.js'
import type { SourceDef } from '@/topology/nodes.js'
import { message } from '../support/task-harness.js'

const TP = { topic: 't', partition: 0 }
const TASK = new TaskIdRegistry().get(0, 0)

function source(name: string, extra: Partial<SourceDef> = {}): SourceDef {
	return { kind: 'source', name, topics: ['t'], keyCodec: string(), valueCodec: string(), internal: false, ...extra }
}

/** Counts its decode calls. */
function counting<T>(codec: Codec<T>): Codec<T> & { decoded: number } {
	const counted = {
		decoded: 0,
		encode: (value: T) => codec.encode(value),
		decode: (bytes: Buffer) => {
			counted.decoded++
			return codec.decode(bytes)
		},
	}
	return counted
}

interface QueueOptions {
	sources?: readonly SourceDef[]
	extractor?: TimestampExtractor
	handler?: DeserializationExceptionHandler
}

function queue(options: QueueOptions = {}) {
	const drops: { reason: DroppedReason; source: string; detail: Readonly<Record<string, unknown>> }[] = []
	const q = new RecordQueue(
		TP,
		options.sources ?? [source('src')],
		options.extractor ?? TimestampExtractors.failOnInvalid,
		options.handler ?? (() => 'fail'),
		(reason, src, detail) => drops.push({ reason, source: src, detail }),
		TASK
	)
	return { q, drops }
}

function batch(from: number, timestamps: readonly number[]): Message<Buffer, Buffer>[] {
	return timestamps.map((timestamp, i) =>
		message('t', 0, BigInt(from + i), { key: `k${from + i}`, value: `v${from + i}`, timestamp })
	)
}

function stamped(record: ReturnType<RecordQueue['poll']>): StampedRecord {
	if (isSkipMarker(record)) throw new Error('expected a record')
	return record
}

describe('RecordQueue', () => {
	it('keeps delivered batches in order across many batches and reports its size', () => {
		const { q } = queue()
		// The batches the queue still references: read ones are released (no growth under a constant backlog).
		const retained = () => (q as unknown as { batches: unknown[] }).batches.length
		let polled = 0
		let maxRetained = 0
		// A backlog of 10 single-record batches while thousands pass through.
		for (let b = 0; b < 3000; b++) {
			expect(q.addRawRecords(batch(b, [b]))).toBe(b + 1 - polled)
			if (b >= 10) expect(stamped(q.poll()).offset).toBe(BigInt(polled++))
			maxRetained = Math.max(maxRetained, retained())
		}
		expect(maxRetained).toBeLessThanOrEqual(1024 + 11)
		while (q.size > 0) expect(stamped(q.poll()).offset).toBe(BigInt(polled++))
		expect(polled).toBe(3000)
		expect(retained()).toBe(0)
		expect(q.addRawRecords([])).toBe(0)
		expect(q.head()).toBeNull()
		expect(q.headOffset).toBeUndefined()
		expect(q.headTimestamp).toBeUndefined()
	})

	it('decodes lazily: only the head', () => {
		const key = counting(string())
		const value = counting(string())
		const { q } = queue({ sources: [source('src', { keyCodec: key, valueCodec: value })] })
		q.addRawRecords(batch(0, [1, 2, 3]))
		expect(key.decoded + value.decoded).toBe(0)
		expect(q.headOffset).toBe(0n)
		expect(key.decoded).toBe(0)
		const head = stamped(q.head()!)
		expect([head.key, head.value, head.timestamp]).toEqual(['k0', 'v0', 1])
		expect(q.head()).toBe(head)
		expect(key.decoded).toBe(1)
		q.poll()
		expect(key.decoded).toBe(1)
		expect(q.headTimestamp).toBe(2)
		expect(key.decoded).toBe(2)
		expect(q.size).toBe(2)
	})

	it('passes raw Buffers without codecs and keeps null keys and values', () => {
		const { q } = queue({ sources: [source('src', { keyCodec: undefined, valueCodec: undefined })] })
		q.addRawRecords([message('t', 0, 0n, { key: 'k', value: 'v' }), message('t', 0, 1n, {})])
		const first = stamped(q.poll())
		expect(first.key).toEqual(Buffer.from('k'))
		expect(first.value).toEqual(Buffer.from('v'))
		const tombstone = stamped(q.poll())
		expect(tombstone.key).toBeNull()
		expect(tombstone.value).toBeNull()
	})

	it('decodes for every source node of the topic, in registration order', () => {
		const { q } = queue({
			sources: [source('a'), source('b', { valueCodec: json<number>() }), source('c')],
		})
		q.addRawRecords([message('t', 0, 0n, { key: 'k', value: '42' })])
		const head = stamped(q.poll())
		expect(head.value).toBe('42')
		expect(head.more).toEqual([
			{ key: 'k', value: 42 },
			{ key: 'k', value: '42' },
		])
	})

	it('each source node gets its own decoded objects, also with the same codecs', () => {
		const codec = json<{ n: number }>()
		const { q } = queue({ sources: [source('a', { valueCodec: codec }), source('b', { valueCodec: codec })] })
		q.addRawRecords([message('t', 0, 0n, { key: 'k', value: '{"n":1}' })])
		const head = stamped(q.poll())
		expect(head.more).toEqual([{ key: 'k', value: { n: 1 } }])
		expect(head.more![0]!.value).not.toBe(head.value)
	})

	it('one source node: no other decodings', () => {
		const { q } = queue()
		q.addRawRecords(batch(0, [1]))
		expect(stamped(q.poll()).more).toBeUndefined()
	})

	describe('deserialization handler', () => {
		const broken = source('src', {
			valueCodec: {
				encode: (v: unknown) => Buffer.from(String(v)),
				decode: (b: Buffer) => {
					if (b.toString() === 'bad') throw new Error('cannot decode')
					return b.toString()
				},
			},
		})

		it("'continue' skips the record, reports the drop and sees where it came from", () => {
			const seen: { ctx: ErrorHandlerContext; record: Message<Buffer, Buffer>; error: unknown }[] = []
			const { q, drops } = queue({
				sources: [broken],
				handler: (ctx, record, error) => {
					seen.push({ ctx, record, error })
					return 'continue'
				},
			})
			q.addRawRecords([
				message('t', 0, 0n, { key: 'k', value: 'bad', timestamp: 7, headers: { h: Buffer.from('x') } }),
				message('t', 0, 1n, { value: 'good', timestamp: 8 }),
			])
			expect(stamped(q.poll()).value).toBe('good')
			expect(seen).toHaveLength(1)
			expect(seen[0]!.ctx).toEqual({
				topic: 't',
				partition: 0,
				offset: 0n,
				headers: { h: Buffer.from('x') },
				timestamp: 7,
				processorNodeId: 'src',
				taskId: TASK,
				sourceRawKey: Buffer.from('k'),
				sourceRawValue: Buffer.from('bad'),
			})
			expect(seen[0]!.record.offset).toBe(0n)
			expect((seen[0]!.error as Error).message).toBe('cannot decode')
			expect(drops.map(d => [d.reason, d.source, d.detail.offset])).toEqual([['deserialization', 'src', 0n]])
		})

		for (const [name, handler] of [
			["'fail'", () => 'fail'],
			[
				'a throwing handler',
				() => {
					throw new Error('handler broke')
				},
			],
			['an invalid answer', () => 'retry'],
		] as const) {
			it(`${name} is a FailedProcessingError`, () => {
				const { q } = queue({ sources: [broken], handler: handler as DeserializationExceptionHandler })
				q.addRawRecords([message('t', 0, 0n, { value: 'bad' })])
				expect(() => q.head()).toThrow(FailedProcessingError)
			})
		}
	})

	describe('skipped records (KAFKA-6502)', () => {
		it('a queue whose remaining records were all skipped has a SkipMarker at the last of them', () => {
			const { q, drops } = queue({ extractor: TimestampExtractors.logAndSkipOnInvalid })
			q.addRawRecords(batch(0, [5, -1, -1]))
			expect(stamped(q.poll()).timestamp).toBe(5)
			expect(q.headTimestamp).toBe(-1)
			expect(q.headOffset).toBe(2n)
			expect(q.size).toBe(1)
			const marker = q.poll()
			expect(isSkipMarker(marker)).toBe(true)
			expect(marker.offset).toBe(2n)
			expect(q.partitionTime).toBe(5)
			expect(q.size).toBe(0)
			expect(drops.map(d => d.reason)).toEqual(['negative-timestamp', 'negative-timestamp'])
			expect(drops[0]!.detail).toMatchObject({ offset: 1n, timestamp: -1 })
		})

		it('skipped records followed by a valid one are passed over', () => {
			const { q } = queue({ extractor: TimestampExtractors.logAndSkipOnInvalid })
			q.addRawRecords(batch(0, [-1, -1, 3]))
			expect(stamped(q.poll()).offset).toBe(2n)
		})
	})

	describe('timestamps', () => {
		it('partition time is the max polled timestamp and is what the extractor sees', () => {
			const seen: number[] = []
			const { q } = queue({
				extractor: (record, partitionTime) => {
					seen.push(partitionTime)
					return record.timestamp
				},
			})
			q.addRawRecords(batch(0, [10, 4, 12]))
			for (let i = 0; i < 3; i++) q.poll()
			expect(seen).toEqual([-1, 10, 10])
			expect(q.partitionTime).toBe(12)
		})

		it('a user extractor sees the decoded record', () => {
			let seen: ConsumedRecord<unknown, unknown> | undefined
			const { q } = queue({
				extractor: record => {
					seen = record
					return 99
				},
			})
			q.addRawRecords([
				message('t', 0, 3n, { key: 'k', value: 'v', timestamp: 1, headers: { a: Buffer.from('b') } }),
			])
			expect(stamped(q.poll()).timestamp).toBe(99)
			expect(seen).toEqual({
				topic: 't',
				partition: 0,
				offset: 3n,
				key: 'k',
				value: 'v',
				timestamp: 1,
				headers: { a: Buffer.from('b') },
			})
		})

		it('a throwing user extractor is a StreamsError; a StreamsError passes as is', () => {
			const cause = new Error('bad extractor')
			const failing = queue({
				extractor: () => {
					throw cause
				},
			}).q
			failing.addRawRecords(batch(0, [1]))
			let error: unknown
			try {
				failing.head()
			} catch (e) {
				error = e
			}
			expect(error).toBeInstanceOf(StreamsError)
			expect((error as StreamsError).cause).toBe(cause)

			const own = new InvalidTimestampError('own')
			const passing = queue({
				extractor: () => {
					throw own
				},
			}).q
			passing.addRawRecords(batch(0, [1]))
			expect(() => passing.head()).toThrow(own)
		})

		it('a user extractor returning a non-integer is an InvalidTimestampError', () => {
			for (const result of [1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
				const { q } = queue({ extractor: () => result })
				q.addRawRecords(batch(0, [1]))
				expect(() => q.head()).toThrow(InvalidTimestampError)
			}
		})

		it('failOnInvalid (the default) stops on a negative timestamp', () => {
			const { q } = queue()
			q.addRawRecords(batch(0, [-5]))
			expect(() => q.head()).toThrow(InvalidTimestampError)
		})

		it('usePartitionTimeOnInvalid takes the partition time, and fails while it is unknown', () => {
			const { q } = queue({ extractor: TimestampExtractors.usePartitionTimeOnInvalid })
			q.addRawRecords(batch(0, [-1]))
			expect(() => q.head()).toThrow(InvalidTimestampError)

			const known = queue({ extractor: TimestampExtractors.usePartitionTimeOnInvalid }).q
			known.addRawRecords(batch(0, [8, -1]))
			known.poll()
			expect(stamped(known.poll()).timestamp).toBe(8)
		})

		it('built-in extractors run on the raw message; user extractors do not have a rule', () => {
			for (const extractor of Object.values(TimestampExtractors)) {
				expect(builtInRule(extractor)).toBeDefined()
			}
			expect(builtInRule(record => record.timestamp)).toBeUndefined()
			const position = { topic: 't', partition: 0, offset: 0n }
			expect(
				TimestampExtractors.logAndSkipOnInvalid(
					{ ...position, key: null, value: null, timestamp: -3, headers: {} },
					1
				)
			).toBe(-3)
			const now = Date.now()
			expect(builtInRule(TimestampExtractors.wallClock)!(0, -1, position)).toBeGreaterThanOrEqual(now)
		})
	})

	it('clear() drops records, head and partition time', () => {
		const { q } = queue()
		q.addRawRecords(batch(0, [3, 4]))
		q.poll()
		q.head()
		q.clear()
		expect(q.size).toBe(0)
		expect(q.head()).toBeNull()
		expect(q.partitionTime).toBe(-1)
		q.addRawRecords(batch(10, [1]))
		expect(stamped(q.poll()).offset).toBe(10n)
	})
})
