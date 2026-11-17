import { describe, expect, it } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import type { Windowed } from '@/api/grouped.js'
import { codec, type Codec } from '@/codec.js'
import { WindowedCodecs } from '@/operators/windowed.js'
import type { RecordMetadata } from '@/processor/api.js'
import { TestDriver } from '@/testing/driver.js'

const str = codec.string()
const num = codec.json<number>()
const key = (k: string) => ({ key: Buffer.from(k) })
/** A topic name extractor that names the partition a record was read from: `out-<partition>`. */
const byPartition = (_key: unknown, _value: unknown, metadata: RecordMetadata | null) => `out-${metadata?.partition}`

/** The repartition topics written to, in order of first use. */
function repartitionTopics(messages: readonly { topic: string }[]): string[] {
	return [...new Set(messages.map(m => m.topic).filter(topic => topic.endsWith('-repartition')))]
}

describe('KStream.repartition (6.18, KS KStreamImpl.doRepartition)', () => {
	it('always writes and reads back <app>-repartition-<n>-repartition; records keep their timestamp and headers', async () => {
		const driver = new TestDriver()
		const seen: unknown[] = []
		driver
			.input('in', { key: str, value: str })
			.repartition()
			.peek((k, v) => seen.push([k, v]))
			.to('out')
		await driver.run(async ({ send, output, producer }) => {
			await send('in', 'v', { ...key('k'), codec: str, timestamp: 42n, headers: { h: Buffer.from('x') } })
			expect(output('test-app-repartition-0-repartition', { key: str, value: str })).toEqual([
				{ topic: 'test-app-repartition-0-repartition', key: 'k', value: 'v', partition: undefined },
			])
			expect(producer.messagesFor('out').map(m => [m.key, m.value, m.timestamp, m.headers])).toEqual([
				[Buffer.from('k'), Buffer.from('v'), 42n, { h: Buffer.from('x') }],
			])
		})
		expect(seen).toEqual([['k', 'v']])
	})

	it('a name names the topic; a name another operation uses is a TopologyError', async () => {
		const driver = new TestDriver()
		const stream = driver.input('in', { key: str, value: str })
		stream.repartition({ name: 'by-user' }).to('out')
		const used = "The repartition topic name 'by-user' is already used by another operation"
		expect(() => stream.repartition({ name: 'by-user' })).toThrow(TopologyError)
		expect(() => stream.repartition({ name: 'by-user' })).toThrow(used)
		expect(() => stream.groupBy((_k, v) => v, { name: 'by-user', key: str }).count()).toThrow(used)
		await driver.run(async ({ send, producer }) => {
			await send('in', 'v', { ...key('k'), codec: str })
			expect(repartitionTopics(producer.messages)).toEqual(['test-app-by-user-repartition'])
		})
	})

	it('clears a key change: key-based operations downstream read its topic and do not repartition again', async () => {
		const driver = new TestDriver()
		const counts: unknown[] = []
		driver
			.input('in', { key: str, value: str })
			.selectKey((_k, v) => v)
			.repartition({ key: str })
			.groupByKey()
			.count()
			.toStream()
			.peek((k, count) => counts.push([k, count]))
		await driver.run(async ({ sendAll, producer }) => {
			await sendAll('in', ['a', 'b', 'a'], { codec: str })
			expect(repartitionTopics(producer.messages)).toEqual(['test-app-repartition-0-repartition'])
		})
		expect(counts).toEqual([
			['a', 1],
			['b', 1],
			['a', 2],
		])
	})

	it('writes and continues with the Repartitioned codecs; a missing codec is a TopologyError naming repartition()', async () => {
		const driver = new TestDriver()
		const stream = driver.input('in', { key: str, value: str })
		const lengths = stream.mapValues(v => v!.length)
		expect(() => lengths.repartition()).toThrow('repartition() needs a value codec')
		expect(() => stream.map((_k, v) => [v, v] as const).repartition({ value: str })).toThrow(
			'repartition() needs a key codec'
		)
		lengths.repartition({ value: num }).to('out')
		await driver.run(async ({ send, output }) => {
			await send('in', 'abc', { ...key('k'), codec: str })
			expect(output('test-app-repartition-0-repartition', { value: num }).map(r => r.value)).toEqual([3])
			expect(output('out', { key: str, value: num }).map(r => [r.key, r.value])).toEqual([['k', 3]])
		})
	})

	it('numberOfPartitions is the count its partitioner sees; records are read back in the partition it picks', async () => {
		const driver = new TestDriver({ partitions: { in: 1 } })
		const seenCounts: number[] = []
		driver
			.input('in', { key: str, value: str })
			.repartition({
				numberOfPartitions: 3,
				partitioner: (k, _v, count) => {
					seenCounts.push(count)
					return Number(k) % count
				},
			})
			.to(byPartition)
		await driver.run(async ({ sendAll, output }) => {
			for (const k of ['1', '2', '4']) await sendAll('in', ['v'], { ...key(k), codec: str })
			expect(output('test-app-repartition-0-repartition').map(r => r.partition)).toEqual([1, 2, 1])
			expect(output('out-1', { key: str }).map(r => r.key)).toEqual(['1', '4'])
			expect(output('out-2', { key: str }).map(r => r.key)).toEqual(['2'])
		})
		expect(seenCounts).toEqual([3, 3, 3])
	})

	it('without numberOfPartitions, the topic has as many partitions as its upstream or co-partitioned topics', async () => {
		const seenCounts: number[] = []
		const partitioner = (_k: string | null, _v: string | null, count: number) => {
			seenCounts.push(count)
			return 0
		}
		const upstream = new TestDriver({ partitions: { in: 4 } })
		upstream.input('in', { key: str, value: str }).repartition({ partitioner }).to('out')
		await upstream.run(({ send }) => send('in', 'v', { ...key('k'), codec: str }))

		const copartitioned = new TestDriver({ partitions: { in: 1, table: 2 } })
		const table = copartitioned.table('table', { key: str, value: str })
		copartitioned
			.input('in', { key: str, value: str })
			.repartition({ partitioner })
			.join(table, (v, label) => `${v}:${label}`)
			.to('out')
		await copartitioned.run(({ send }) => send('in', 'v', { ...key('k'), codec: str }))
		expect(seenCounts).toEqual([4, 2])
	})

	it('an explicit count must be a positive integer and equal the count of the topics it is co-partitioned with', async () => {
		const driver = new TestDriver({ partitions: { in: 1, table: 2 } })
		const stream = driver.input('in', { key: str, value: str })
		expect(() => stream.repartition({ numberOfPartitions: 0 })).toThrow(TopologyError)
		expect(() => stream.repartition({ numberOfPartitions: 1.5 })).toThrow(TopologyError)
		const table = driver.table('table', { key: str, value: str })
		stream.repartition({ numberOfPartitions: 3 }).join(table, (v, label) => `${v}:${label}`)
		const start = driver.start()
		await expect(start).rejects.toBeInstanceOf(TopologyError)
		await expect(start).rejects.toThrow(/has 3 partitions, but the topics it is co-partitioned with have 2/)
	})

	it('a windowed key codec keeps base-key partitioning; a plain codec of the same keys does not (6.9, BC10)', async () => {
		const timeKeys = WindowedCodecs.timeWindowed(str, 10)
		const plainKeys: Codec<Windowed<string>> = { encode: k => timeKeys.encode(k), decode: b => timeKeys.decode(b) }
		const windowed = (k: string): Windowed<string> => ({ key: k, window: { start: 0, end: 10 } })
		const driver = new TestDriver({ partitions: { in: 4 } })
		const stream = driver.input('in', { key: timeKeys, value: str })
		stream.repartition({ name: 'base' }).to('out')
		stream.repartition({ name: 'plain', key: plainKeys }).to('out')
		await driver.run(async ({ send, output }) => {
			for (const k of ['a', 'c', 'f']) {
				await send('in', 'v', { keyValue: windowed(k), keyCodec: timeKeys, codec: str })
			}
			const partitions = (topic: string) => output(topic).map(r => r.partition)
			expect(partitions('test-app-base-repartition')).toEqual([0, 2, 3])
			expect(partitions('test-app-plain-repartition')).toEqual([undefined, undefined, undefined])
		})
	})
})

describe('null keys on repartition() topics (4.1.6, KS ksdsl§15.3)', () => {
	it('records with a null key are dropped', async () => {
		const driver = new TestDriver()
		const seen: unknown[] = []
		driver
			.input('in', { key: str, value: str })
			.repartition()
			.peek(k => seen.push(k))
		await driver.run(async ({ send, output }) => {
			await send('in', 'v', { key: null, codec: str })
			await send('in', 'v', { ...key('k'), codec: str })
			expect(output('test-app-repartition-0-repartition', { key: str }).map(r => r.key)).toEqual(['k'])
		})
		expect(seen).toEqual(['k'])
	})

	it('records with a null key are kept when a left join is downstream (KF12), which joins them with null (KIP-962)', async () => {
		const driver = new TestDriver()
		const joined: unknown[] = []
		const labels = driver.table('labels', { key: str, value: str })
		driver
			.input('in', { key: str, value: str })
			.repartition()
			.leftJoin(labels, (v, label) => `${v}+${label}`)
			.peek((k, v) => joined.push([k, v]))
		await driver.run(async ({ send, output }) => {
			await send('labels', 'L', { ...key('k'), codec: str })
			await send('in', 'v', { key: null, codec: str })
			await send('in', 'w', { ...key('k'), codec: str })
			expect(output('test-app-repartition-0-repartition', { key: str }).map(r => r.key)).toEqual([null, 'k'])
		})
		expect(joined).toEqual([
			[null, 'v+null'],
			['k', 'w+L'],
		])
	})
})

describe('KStream.through (deprecated)', () => {
	it('writes a user topic, keeps null keys and clears a key change', async () => {
		const driver = new TestDriver()
		const counts: unknown[] = []
		driver
			.input('in', { key: str, value: str })
			.selectKey((_k, v) => (v === 'none' ? null : v))
			.through('mid', { key: str })
			.groupByKey()
			.count()
			.toStream()
			.peek((k, count) => counts.push([k, count]))
		await driver.run(async ({ sendAll, producer, output }) => {
			await sendAll('in', ['none', 'a', 'a'], { codec: str })
			expect(output('mid', { key: str, value: str }).map(r => [r.key, r.value])).toEqual([
				[null, 'none'],
				['a', 'a'],
				['a', 'a'],
			])
			expect(repartitionTopics(producer.messages)).toEqual([])
		})
		expect(counts).toEqual([
			['a', 1],
			['a', 2],
		])
	})
})
