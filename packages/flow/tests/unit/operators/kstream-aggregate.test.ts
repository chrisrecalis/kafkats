import { describe, expect, it } from 'vitest'
import { codec } from '@/codec.js'
import { COUNT_CODEC, counting } from '@/dsl/ops/aggregate.js'
import { TimeWindows } from '@/dsl/windows.js'
import type { Change } from '@/operators/change.js'
import { aggregateWith, KStreamAggregate } from '@/operators/kstream-aggregate.js'
import type { TimestampedKeyValueStore, ValueAndTimestamp } from '@/state.js'
import { InMemoryKeyValueStore } from '@/state/memory.js'
import { valueAndTimestampCodec } from '@/state/timestamped.js'
import { TestDriver, type TestDriverConfig } from '@/testing/driver.js'
import { capture, processorDef, ProcessorHarness, sourceDef } from '../support/processor-harness.js'

const str = codec.string()
const S = { key: str, value: str }
const hex = (...parts: string[]) => Buffer.from(parts.join(''), 'hex')
const header = (n: number) => ({ n: Buffer.from(String(n)) })
/** A string value of key `k` (none: a null key) at `timestamp`, with header `n` = the timestamp. */
const at = (k: string | null, timestamp: number) => ({
	key: k === null ? null : Buffer.from(k),
	timestamp,
	headers: header(timestamp),
	codec: str,
})

const GUARANTEES: ReadonlyArray<readonly [string, TestDriverConfig]> = [
	['at-least-once', {}],
	['exactly-once, committed per record', { processingGuarantee: 'exactly_once' }],
	['exactly-once, committed on stop', { processingGuarantee: 'exactly_once', commitEvery: 'stop' }],
]

/** count(), reduce() and aggregate() of one grouping, each sent to the topic of its name. */
function aggregations(driver: TestDriver): void {
	const grouped = driver.input('in', S).groupByKey()
	grouped.count({ storeName: 'counts' }).toStream().to('count')
	grouped
		.reduce((agg, v) => agg + v, { storeName: 'reduced' })
		.toStream()
		.to('reduce')
	grouped
		.aggregate(
			() => '',
			(_k, v, agg) => agg + v,
			{ storeName: 'aggregated' }
		)
		.toStream()
		.to('aggregate')
}

/** [key, value, timestamp, header n] of every record sent to `topic`. */
function sent(driver: TestDriver, topic: string) {
	const decode = topic === 'count' ? COUNT_CODEC.decode : (bytes: Buffer) => bytes.toString()
	return driver.producers[0]!.messagesFor(topic).map(m => [
		m.key?.toString(),
		m.value && decode(m.value),
		Number(m.timestamp),
		m.headers?.n?.toString(),
	])
}

describe.each(GUARANTEES)('KStream aggregations (%s)', (_guarantee, config) => {
	it('a result carries the max timestamp of its records (Kafka Streams) and the headers of the record that produced it', async () => {
		const driver = new TestDriver(config)
		aggregations(driver)
		await driver.run(async ({ send, getStateStore }) => {
			await send('in', 'a', at('k', 7))
			await send('in', 'b', at('k', 3))
			await send('in', 'c', at('j', 2))
			await send('in', 'd', at('k', 9))
			const counts = getStateStore<TimestampedKeyValueStore<string, number>>('counts')
			expect([await counts.get('k'), await counts.get('j')]).toEqual([
				{ value: 3, timestamp: 9 },
				{ value: 1, timestamp: 2 },
			])
		})
		expect(sent(driver, 'count')).toEqual([
			['k', 1, 7, '7'],
			['k', 2, 7, '3'],
			['j', 1, 2, '2'],
			['k', 3, 9, '9'],
		])
		const concatenated = [
			['k', 'a', 7, '7'],
			['k', 'ab', 7, '3'],
			['j', 'c', 2, '2'],
			['k', 'abd', 9, '9'],
		]
		expect(sent(driver, 'reduce')).toEqual(concatenated)
		expect(sent(driver, 'aggregate')).toEqual(concatenated)
	})

	it('a null result is a tombstone at the max timestamp; the next record starts over at its own timestamp', async () => {
		const driver = new TestDriver(config)
		driver
			.input('in', S)
			.groupByKey()
			.reduce((agg, v) => (v === 'reset' ? (null as unknown as string) : agg + v), { storeName: 'reduced' })
			.toStream()
			.to('reduce')
		await driver.run(async ({ send, getStateStore }) => {
			const reduced = getStateStore<TimestampedKeyValueStore<string, string>>('reduced')
			await send('in', 'a', at('k', 5))
			await send('in', 'reset', at('k', 2))
			expect(await reduced.get('k')).toBeUndefined()
			await send('in', 'b', at('k', 1))
			expect(await reduced.get('k')).toEqual({ value: 'b', timestamp: 1 })
		})
		expect(sent(driver, 'reduce')).toEqual([
			['k', 'a', 5, '5'],
			['k', null, 5, '2'],
			['k', 'b', 1, '1'],
		])
	})

	it('records with a null key or a null value are dropped (Kafka Streams)', async () => {
		const driver = new TestDriver(config)
		aggregations(driver)
		await driver.run(async ({ send }) => {
			await send('in', 'a', at(null, 1))
			await send('in', null, at('k', 9))
			await send('in', 'b', at('k', 3))
		})
		expect(sent(driver, 'count')).toEqual([['k', 1, 3, '3']])
		expect(sent(driver, 'reduce')).toEqual([['k', 'b', 3, '3']])
		expect(sent(driver, 'aggregate')).toEqual([['k', 'b', 3, '3']])
	})
})

describe('KStream aggregations', () => {
	it('dropped records are counted by reason', async () => {
		const store = new InMemoryKeyValueStore<string, ValueAndTimestamp<number>>('counts', {
			keyCodec: str,
			valueCodec: valueAndTimestampCodec(COUNT_CODEC),
		})
		const node = new KStreamAggregate('counts', aggregateWith(counting.initializer, counting.aggregator))
		const out = capture<string, Change<number>>('out', ['count'])
		const harness = await ProcessorHarness.start(
			[sourceDef('source', ['in']), processorDef('count', ['source'], node.supplier, ['counts']), out.def],
			{ stores: [store] }
		)
		await harness.process('in', { key: null, value: 'a', timestamp: 1 })
		await harness.process('in', { key: 'k', value: null, timestamp: 2 })
		await harness.process('in', { key: 'k', value: 'b', timestamp: 3 })
		expect([harness.task.droppedCount('null-key'), harness.task.droppedCount('null-value')]).toEqual([1, 1])
		expect(out.records.map(r => [r.key, r.value.newValue, r.timestamp])).toEqual([['k', 1, 3]])
	})

	it('the count codec is an int64 big-endian (Kafka Streams Serdes.Long)', () => {
		expect(COUNT_CODEC.encode(1)).toEqual(hex('0000000000000001'))
		expect(COUNT_CODEC.encode(258)).toEqual(hex('0000000000000102'))
		expect(COUNT_CODEC.encode(-1)).toEqual(hex('ffffffffffffffff'))
		expect(COUNT_CODEC.decode(hex('0000000000000102'))).toBe(258)
		expect(COUNT_CODEC.decode(COUNT_CODEC.encode(Number.MAX_SAFE_INTEGER))).toBe(Number.MAX_SAFE_INTEGER)
	})

	it('BC21: every count() is sent and logged as int64 big-endian unless a value codec is given', async () => {
		const driver = new TestDriver()
		const grouped = driver.input('in', S).groupByKey()
		grouped.count({ storeName: 'counts' }).toStream().to('counts')
		grouped.count({ value: codec.json<number>() }).toStream().to('json')
		grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(10)).count().toStream().to('windows')
		driver
			.table('t', S)
			.groupBy((k, v) => [v, k], S)
			.count()
			.toStream()
			.to('rows')
		await driver.run(async ({ send, producer }) => {
			await send('in', 'x', at('k', 5))
			await send('in', 'x', at('k', 6))
			await send('t', 'g', at('k', 7))
			const values = (topic: string) => producer.messagesFor(topic).map(m => m.value)
			expect(values('counts')).toEqual([hex('0000000000000001'), hex('0000000000000002')])
			expect(values('test-app-counts-changelog')).toEqual([
				hex('0000000000000005', '0000000000000001'),
				hex('0000000000000006', '0000000000000002'),
			])
			expect(values('json').map(String)).toEqual(['1', '2'])
			expect(values('windows')).toEqual([hex('0000000000000001'), hex('0000000000000002')])
			expect(values('rows')).toEqual([hex('0000000000000001')])
		})
	})

	it('aggregate() keeps aggregates of the value type with the value codec', async () => {
		const driver = new TestDriver()
		driver
			.input('in', S)
			.groupByKey()
			.aggregate(
				() => '',
				(_k, v, agg) => agg + v,
				{ storeName: 'joined' }
			)
			.toStream()
			.to('out')
		await driver.run(async ({ send, producer, output }) => {
			await send('in', 'a', at('k', 1))
			await send('in', 'b', at('k', 2))
			expect(output('out', S).map(o => o.value)).toEqual(['a', 'ab'])
			const [, logged] = producer.messagesFor('test-app-joined-changelog').map(m => m.value)
			expect(logged).toEqual(Buffer.concat([hex('0000000000000002'), Buffer.from('ab')]))
		})
	})
})
