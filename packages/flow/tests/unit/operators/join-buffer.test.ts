import { describe, expect, it } from 'vitest'
import { codec } from '@/codec.js'
import { JoinBuffer } from '@/operators/join-buffer.js'
import { listCodec } from '@/operators/list-value.js'
import { ProcessorMetadataImpl } from '@/processor/metadata.js'
import { OrderedSessionStore, type OrderedByteStore } from '@/state/ordered.js'
import { ClockedSessionStore, TaskStoreClock } from '@/state/time-clipping.js'

const str = codec.string()

/** A byte-ordered store in memory that counts the entries its range scans visit. */
function countingBytes(): OrderedByteStore & { visited: number } {
	const entries = new Map<string, { key: Buffer; value: Buffer }>()
	const bytes = {
		visited: 0,
		get: (key: Buffer) => entries.get(key.toString('hex'))?.value,
		put: (key: Buffer, value: Buffer) => {
			entries.set(key.toString('hex'), { key, value })
			return Promise.resolve()
		},
		remove: (key: Buffer) => {
			entries.delete(key.toString('hex'))
			return Promise.resolve()
		},
		*range({ start, end }: { start?: Buffer; end?: Buffer }) {
			const inRange = [...entries.values()]
				.filter(e => (!start || e.key.compare(start) >= 0) && (!end || e.key.compare(end) < 0))
				.sort((a, b) => a.key.compare(b.key))
			for (const entry of inRange) {
				bytes.visited++
				yield entry
			}
		},
		approximateCount: () => entries.size,
		flush: () => Promise.resolve(),
	}
	return bytes
}

/** A buffer over a provider's session store, under the clipping layer of its store clock. */
function joinBuffer(retentionMs = 100) {
	const bytes = countingBytes()
	const raw = new OrderedSessionStore<string, string[]>('buffer', bytes, {
		keyCodec: str,
		valueCodec: listCodec(str),
		retentionMs,
	})
	const clock = new TaskStoreClock(new ProcessorMetadataImpl(), 'buffer', retentionMs)
	return { bytes, raw, buffer: new JoinBuffer(new ClockedSessionStore(raw, clock, retentionMs)) }
}

const fetched = async (entries: AsyncIterable<[number, string[]]>) => {
	const result: [number, string[]][] = []
	for await (const entry of entries) result.push(entry)
	return result
}

describe('JoinBuffer', () => {
	it('B8: keeps every record of one key and timestamp, in arrival order, in one [ts, ts] entry', async () => {
		const { raw, buffer } = joinBuffer()
		await buffer.put('a', 100, 'x')
		await buffer.put('a', 100, 'y')
		await buffer.put('a', 100, 'x')
		expect(await raw.get({ key: 'a', windowStart: 100, windowEnd: 100 })).toEqual(['x', 'y', 'x'])
		expect(await raw.approximateNumEntries()).toBe(1)
		expect(await fetched(buffer.fetch('a', 100, 100))).toEqual([[100, ['x', 'y', 'x']]])
	})

	it('fetches [from, to], both inclusive, in timestamp order, within the retention of its own clock', async () => {
		const { buffer } = joinBuffer()
		for (const timestamp of [201, 50, 150, 100]) {
			await buffer.put('a', timestamp, String(timestamp))
		}
		await buffer.put('b', 150, 'other key')
		// The clock is at 201: with a retention of 100, entries below 102 are gone for reads.
		expect(await fetched(buffer.fetch('a', 0, 300))).toEqual([
			[150, ['150']],
			[201, ['201']],
		])
		expect(await fetched(buffer.fetch('a', 150, 200))).toEqual([[150, ['150']]])
	})

	it('B15: a probe reads the entries of its key only', async () => {
		const { bytes, buffer } = joinBuffer(10_000)
		for (const key of ['a', 'b', 'c']) {
			for (let timestamp = 0; timestamp < 50; timestamp++) {
				await buffer.put(key, timestamp, key)
			}
		}
		bytes.visited = 0
		expect(await fetched(buffer.fetch('b', 10, 19))).toHaveLength(10)
		expect(bytes.visited).toBe(50)
	})

	it('expire() deletes the entries past the retention by its own clock', async () => {
		const { raw, buffer } = joinBuffer()
		await buffer.put('a', 100, 'old')
		await buffer.put('a', 300, 'new')
		await buffer.expire()
		expect(await raw.approximateNumEntries()).toBe(1)
		expect(await raw.get({ key: 'a', windowStart: 300, windowEnd: 300 })).toEqual(['new'])
	})
})
