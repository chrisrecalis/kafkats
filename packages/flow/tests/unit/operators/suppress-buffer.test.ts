import { describe, expect, it } from 'vitest'
import { codec } from '@/codec.js'
import type { Change } from '@/operators/change.js'
import { SuppressBuffer, type BufferStore } from '@/operators/suppress-buffer.js'
import { bufferedChangeCodec, priorValueOf, type BufferedChange } from '@/operators/suppress-entry.js'
import { StreamRecord, type RecordHeaders } from '@/processor/record.js'
import { InMemoryKeyValueStore } from '@/state/memory.js'

const str = codec.string()
const entries = bufferedChangeCodec(str)

function newStore(): BufferStore {
	return new InMemoryKeyValueStore('buffer', { keyCodec: codec.buffer(), valueCodec: codec.buffer() })
}

const update = (key: string, newValue: string | null, oldValue: string | null, timestamp: number, headers = {}) =>
	new StreamRecord<string, Change<string>>(key, { newValue, oldValue }, timestamp, headers)

type Evicted = [key: string, bufferTime: number, prior: string | null, value: string | null, timestamp: number]

/** Evicts every buffered key, in order. */
async function drain(buffer: SuppressBuffer<string, string>): Promise<Evicted[]> {
	const evicted: Evicted[] = []
	await buffer.evictWhile(
		() => true,
		(key, change) =>
			void evicted.push([key, change.bufferTime, change.priorValue, change.newValue, change.timestamp])
	)
	return evicted
}

/** What the store holds, decoded, by key. */
async function stored(store: BufferStore): Promise<Record<string, BufferedChange<string>>> {
	const result: Record<string, BufferedChange<string>> = {}
	for await (const [key, bytes] of store.all()) {
		result[key.toString()] = entries.decode(bytes)
	}
	return result
}

describe('the stored form of a buffered change (6.16)', () => {
	it('is [bufferTime][timestamp][prior][new][headers], with -1 as the length of a null value', () => {
		const change: BufferedChange<string> = {
			bufferTime: 10,
			timestamp: 7,
			headers: { trace: Buffer.from('t1') },
			priorValue: null,
			newValue: 'v',
		}
		const bytes = entries.encode(change)
		expect(bytes.toString('hex')).toBe(
			[
				'000000000000000a', // buffer time
				'0000000000000007', // timestamp
				'ffffffff', // no prior value
				'00000001' + '76', // 'v'
				'0001', // one header
				'0005' + Buffer.from('trace').toString('hex'),
				'00000002' + Buffer.from('t1').toString('hex'),
			].join('')
		)
		expect(entries.decode(bytes)).toEqual(change)
		expect(priorValueOf(str, bytes)).toBeNull()
	})

	it('round-trips empty values, deletions, several headers and UTF-8 header keys', () => {
		const headers: RecordHeaders = { clé: Buffer.from([0, 255]), empty: Buffer.alloc(0) }
		const change = { bufferTime: 2 ** 40, timestamp: 0, headers, priorValue: '', newValue: null }
		const bytes = entries.encode(change)
		expect(entries.decode(bytes)).toEqual(change)
		expect(priorValueOf(str, bytes)).toBe('')
	})
})

describe('SuppressBuffer', () => {
	it('evicts by buffer time, then by key bytes', async () => {
		const buffer = new SuppressBuffer(newStore(), { key: str, value: str })
		await buffer.put(20, update('a', '1', null, 1))
		await buffer.put(10, update('c', '2', null, 2))
		await buffer.put(10, update('b', '3', null, 3))
		await buffer.put(5, update('d', '4', null, 4))
		expect(buffer.minTime).toBe(5)
		expect((await drain(buffer)).map(([key, bufferTime]) => [key, bufferTime])).toEqual([
			['d', 5],
			['b', 10],
			['c', 10],
			['a', 20],
		])
		expect(buffer.minTime).toBe(Number.POSITIVE_INFINITY)
	})

	it('keeps the buffer time, place and prior value of a buffered key; takes the latest value, timestamp and headers', async () => {
		const buffer = new SuppressBuffer(newStore(), { key: str, value: str })
		await buffer.put(10, update('k', 'b', 'a', 1))
		await buffer.put(5, update('j', 'x', null, 2))
		await buffer.put(30, update('k', 'c', 'b', 3, { h: Buffer.from('3') }))
		const evicted: [string, BufferedChange<string>][] = []
		await buffer.evictWhile(
			() => true,
			(key, change) => void evicted.push([key, change])
		)
		expect(evicted).toEqual([
			['j', { bufferTime: 5, timestamp: 2, headers: {}, priorValue: null, newValue: 'x' }],
			['k', { bufferTime: 10, timestamp: 3, headers: { h: Buffer.from('3') }, priorValue: 'a', newValue: 'c' }],
		])
	})

	it('counts the records and the key and entry bytes it holds', async () => {
		const buffer = new SuppressBuffer(newStore(), { key: str, value: str })
		const size = (key: string, change: BufferedChange<string>) => key.length + entries.encode(change).length
		await buffer.put(10, update('k', 'v', null, 1))
		const first = size('k', { bufferTime: 10, timestamp: 1, headers: {}, priorValue: null, newValue: 'v' })
		expect([buffer.numRecords, buffer.bytes]).toEqual([1, first])
		await buffer.put(10, update('k', 'longer', 'v', 2))
		const updated = size('k', { bufferTime: 10, timestamp: 2, headers: {}, priorValue: null, newValue: 'longer' })
		expect([buffer.numRecords, buffer.bytes]).toEqual([1, updated])
		await buffer.put(10, update('kk', 'v', null, 3))
		const second = size('kk', { bufferTime: 10, timestamp: 3, headers: {}, priorValue: null, newValue: 'v' })
		expect([buffer.numRecords, buffer.bytes]).toEqual([2, updated + second])
		await buffer.evictWhile(
			() => buffer.numRecords > 1,
			() => undefined
		)
		expect([buffer.numRecords, buffer.bytes]).toEqual([1, second])
	})

	it('writes every change through to its store, and a new buffer loads the same contents in the same order', async () => {
		const store = newStore()
		const buffer = new SuppressBuffer(store, { key: str, value: str })
		await buffer.put(10, update('b', '1', null, 1))
		await buffer.put(10, update('a', '2', 'z', 2))
		await buffer.put(30, update('b', '3', '1', 3))
		await buffer.put(20, update('c', '4', null, 4))
		expect(await stored(store)).toEqual({
			a: { bufferTime: 10, timestamp: 2, headers: {}, priorValue: 'z', newValue: '2' },
			b: { bufferTime: 10, timestamp: 3, headers: {}, priorValue: null, newValue: '3' },
			c: { bufferTime: 20, timestamp: 4, headers: {}, priorValue: null, newValue: '4' },
		})
		await buffer.evictWhile(
			() => buffer.minTime <= 10 && buffer.numRecords > 2,
			() => undefined
		)
		expect(Object.keys(await stored(store))).toEqual(['b', 'c'])

		const loaded = new SuppressBuffer(store, { key: str, value: str })
		await loaded.load()
		expect([loaded.numRecords, loaded.bytes]).toEqual([buffer.numRecords, buffer.bytes])
		expect(await drain(loaded)).toEqual([
			['b', 10, null, '3', 3],
			['c', 20, null, '4', 4],
		])
		expect(await stored(store)).toEqual({})
	})

	it('a failed emission leaves the key buffered and stored (KS removes after the callback)', async () => {
		const store = newStore()
		const buffer = new SuppressBuffer(store, { key: str, value: str })
		await buffer.put(10, update('k', 'v', null, 1))
		const failure = buffer.evictWhile(
			() => true,
			() => Promise.reject(new Error('downstream failed'))
		)
		await expect(failure).rejects.toThrow('downstream failed')
		expect(buffer.numRecords).toBe(1)
		expect(Object.keys(await stored(store))).toEqual(['k'])
		expect(await drain(buffer)).toEqual([['k', 10, null, 'v', 1]])
	})

	it('keeps many keys in order (the heap)', async () => {
		const buffer = new SuppressBuffer(newStore(), { key: str, value: str })
		const times = Array.from({ length: 200 }, (_, i) => (i * 7919) % 211)
		for (const [i, time] of times.entries()) {
			await buffer.put(time, update(`k${i}`, 'v', null, i))
		}
		const evicted = (await drain(buffer)).map(([, bufferTime]) => bufferTime)
		expect(evicted).toEqual([...times].sort((a, b) => a - b))
	})
})
