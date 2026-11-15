import { describe, expect, it } from 'vitest'
import { OrderedWindowStore, codec, readSignedTime, type OrderedByteStore } from '../../src/index.js'

/** A byte-ordered store in memory that records where range scans started. */
function memoryBytes(): OrderedByteStore & { scans: Array<Buffer | undefined> } {
	const entries = new Map<string, { key: Buffer; value: Buffer }>()
	const scans: Array<Buffer | undefined> = []
	return {
		scans,
		get: key => entries.get(key.toString('hex'))?.value,
		put: (key, value) => {
			entries.set(key.toString('hex'), { key, value })
			return Promise.resolve()
		},
		remove: key => {
			entries.delete(key.toString('hex'))
			return Promise.resolve()
		},
		range: ({ start, end }) => {
			scans.push(start)
			return [...entries.values()]
				.filter(e => (!start || e.key.compare(start) >= 0) && (!end || e.key.compare(end) < 0))
				.sort((a, b) => a.key.compare(b.key))
		},
		approximateCount: () => entries.size,
		flush: () => Promise.resolve(),
	}
}

const windowStore = (bytes: OrderedByteStore) =>
	new OrderedWindowStore<string, number>('windows', bytes, {
		keyCodec: codec.string(),
		valueCodec: codec.json<number>(),
		windowSizeMs: 10,
		retentionMs: 0,
	})

const starts = async (store: OrderedWindowStore<string, number>): Promise<number[]> => {
	const result: number[] = []
	for await (const [key] of store.all()) result.push(key.windowStart)
	return result
}

describe('OrderedWindowStore.expireOldWindows', () => {
	it('expires windows that ended, keeps overlapping ones, and expires those later', async () => {
		const store = windowStore(memoryBytes())
		await store.put({ key: 'a', windowStart: 0, windowEnd: 10 }, 1)
		await store.put({ key: 'a', windowStart: 5, windowEnd: 40 }, 2) // long window, still open at 20
		await store.put({ key: 'a', windowStart: 30, windowEnd: 40 }, 3)

		expect(await store.expireOldWindows(20)).toBe(1)
		expect(await starts(store)).toEqual([5, 30])

		// The kept window started before the previous cutoff; a later pass must still find it.
		expect(await store.expireOldWindows(50)).toBe(2)
		expect(await starts(store)).toEqual([])
	})

	it('does not rescan the range earlier passes already emptied', async () => {
		const bytes = memoryBytes()
		const store = windowStore(bytes)
		for (let start = 0; start < 100; start += 10) {
			await store.put({ key: 'a', windowStart: start, windowEnd: start + 10 }, start)
		}
		await store.expireOldWindows(50)
		bytes.scans.length = 0
		await store.expireOldWindows(80)

		// The first pass removed the windows that ended before 50 and kept the one starting at 40,
		// so the second pass starts there instead of at the beginning of the store.
		expect(bytes.scans).toHaveLength(1)
		expect(readSignedTime(bytes.scans[0]!, 0)).toBe(40)
		expect(await starts(store)).toEqual([70, 80, 90])
	})

	it('finds a window written late, before the point earlier passes reached', async () => {
		const store = windowStore(memoryBytes())
		await store.put({ key: 'a', windowStart: 100, windowEnd: 110 }, 1)
		await store.expireOldWindows(100)

		await store.put({ key: 'late', windowStart: 10, windowEnd: 20 }, 2)
		expect(await store.expireOldWindows(105)).toBe(1)
		expect(await starts(store)).toEqual([100])
	})
})
