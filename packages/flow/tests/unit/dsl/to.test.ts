import { murmur2, toPositive } from '@kafkats/client'
import { describe, expect, it } from 'vitest'
import type { Windowed } from '@/api/grouped.js'
import { codec, type Codec } from '@/codec.js'
import { TimeWindows } from '@/dsl/windows.js'
import { WindowedCodecs } from '@/operators/windowed.js'
import { TestDriver } from '@/testing/driver.js'

const str = codec.string()
const timeKeys = WindowedCodecs.timeWindowed(str, 10)
const sessionKeys = WindowedCodecs.sessionWindowed(str)
const windowed = (key: string, start: number, end = start + 10): Windowed<string> => ({ key, window: { start, end } })
const basePartition = (key: string, count = 4) => toPositive(murmur2(Buffer.from(key))) % count

/** Windows of three base keys, in partitions 0, 2 and 3 of 4; the whole windowed keys of 'a' hash elsewhere. */
const KEYS = [windowed('a', 0), windowed('a', 10), windowed('a', 20), windowed('c', 0), windowed('f', 0)]
const BASE_PARTITIONS = [0, 0, 0, 2, 3]

describe('windowed keys at sinks (6.9, KS WindowedStreamPartitioner)', () => {
	it('test keys: the base key decides the partition, not the whole windowed key', () => {
		expect(KEYS.map(k => basePartition(k.key))).toEqual(BASE_PARTITIONS)
		expect(KEYS.slice(0, 3).map(k => toPositive(murmur2(timeKeys.encode(k))) % 4)).not.toEqual([0, 0, 0])
	})

	it('to() writes windowed keys of the stream format to the partition of their base key', async () => {
		const driver = new TestDriver({ partitions: { out: 4 } })
		driver.input('in', { key: timeKeys, value: str }).to('out')
		await driver.run(async ({ send, output }) => {
			for (const key of KEYS) await send('in', 'v', { keyValue: key, keyCodec: timeKeys, codec: str })
			const written = output('out', { key: timeKeys })
			expect(written.map(r => r.key)).toEqual(KEYS)
			expect(written.map(r => r.partition)).toEqual(BASE_PARTITIONS)
		})
	})

	it('to() with a windowed Produced.key partitions by base key; a given partitioner or a plain codec wins', async () => {
		const driver = new TestDriver({ partitions: { sessions: 4, pinned: 4, plain: 4 } })
		const stream = driver
			.input('in', { key: str, value: str })
			.map((key, value) => [windowed(key!, 0, 5), value] as const)
		stream.to('sessions', { key: sessionKeys, value: str })
		stream.to('pinned', { key: sessionKeys, value: str, partitioner: () => 1 })
		const plainKeys: Codec<Windowed<string>> = {
			encode: k => sessionKeys.encode(k),
			decode: b => sessionKeys.decode(b),
		}
		stream.to('plain', { key: plainKeys, value: str })
		await driver.run(async ({ sendAll, output }) => {
			await sendAll('in', ['x', 'y', 'z'], { key: Buffer.from('f'), codec: str })
			await sendAll('in', ['x'], { key: Buffer.from('c'), codec: str })
			expect(output('sessions', { key: sessionKeys }).map(r => [r.key, r.partition])).toEqual([
				[windowed('f', 0, 5), 3],
				[windowed('f', 0, 5), 3],
				[windowed('f', 0, 5), 3],
				[windowed('c', 0, 5), 2],
			])
			expect(output('pinned').map(r => r.partition)).toEqual([1, 1, 1, 1])
			expect(output('plain').map(r => r.partition)).toEqual([undefined, undefined, undefined, undefined])
		})
	})

	it('a null windowed key is left to the producer', async () => {
		const driver = new TestDriver({ partitions: { out: 4 } })
		driver.input('in', { key: timeKeys, value: str }).to('out')
		await driver.run(async ({ send, output }) => {
			await send('in', 'v', { key: null, codec: str })
			expect(output('out')).toEqual([{ topic: 'out', key: null, value: Buffer.from('v'), partition: undefined }])
		})
	})

	it('through() writes by base key and continues with the windowed key codec', async () => {
		const driver = new TestDriver({ partitions: { mid: 4, out: 4 } })
		driver.input('in', { key: timeKeys, value: str }).through('mid').to('out')
		await driver.run(async ({ send, output }) => {
			for (const key of KEYS) await send('in', 'v', { keyValue: key, keyCodec: timeKeys, codec: str })
			expect(output('mid').map(r => r.partition)).toEqual(BASE_PARTITIONS)
			expect(output('out', { key: timeKeys }).map(r => [r.key, r.partition])).toEqual(
				KEYS.map((key, i) => [key, BASE_PARTITIONS[i]])
			)
		})
	})

	it('repartition sinks write windowed keys by base key too (KF15)', async () => {
		const driver = new TestDriver({ partitions: { in: 4 } })
		const counts: unknown[] = []
		driver
			.input('in', { key: str, value: str })
			.map((_key, value) => [windowed(value!, 0), value] as const)
			.groupByKey({ key: timeKeys, value: str })
			.count()
			.toStream()
			.peek((key, count) => counts.push([key?.key, count]))
		await driver.run(async ({ sendAll, output }) => {
			await sendAll('in', ['a', 'c', 'f', 'a'], { codec: str })
			const repartitioned = output('test-app-repartition-0-repartition', { key: timeKeys })
			expect(repartitioned.map(r => [r.key, r.partition])).toEqual([
				[windowed('a', 0), 0],
				[windowed('c', 0), 2],
				[windowed('f', 0), 3],
				[windowed('a', 0), 0],
			])
		})
		expect(counts).toEqual([
			['a', 1],
			['c', 1],
			['f', 1],
			['a', 2],
		])
	})

	it('windowed aggregation results written with a windowed codec land in the partition of their base key', async () => {
		const driver = new TestDriver({ partitions: { out: 4 } })
		driver
			.input('in', { key: str, value: str })
			.groupByKey()
			.windowedBy(TimeWindows.ofSizeWithNoGrace(10))
			.count()
			.toStream()
			.to('out', { key: timeKeys })
		await driver.run(async ({ send, output }) => {
			for (const [key, timestamp] of [
				['a', 1],
				['c', 3],
				['a', 12],
				['f', 25],
			] as const) {
				await send('in', 'v', { key: Buffer.from(key), timestamp, codec: str })
			}
			expect(output('out', { key: timeKeys }).map(r => [r.key, r.partition])).toEqual([
				[windowed('a', 0), 0],
				[windowed('c', 0), 2],
				[windowed('a', 10), 0],
				[windowed('f', 20), 3],
			])
		})
	})
})
