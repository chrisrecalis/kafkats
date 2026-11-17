import { describe, expect, it } from 'vitest'
import { codec } from '../../src/index.js'
import { InMemoryKeyValueStore } from '../../src/state/memory.js'
import { clearStore, latestPerKey, writeChanges } from '../../src/state/bulk.js'

const str = codec.string()
const record = (key: string | null, value: string | null) => ({
	key: key === null ? null : Buffer.from(key),
	value: value === null ? null : Buffer.from(value),
})

/** A store that counts how many writes are in flight at once. */
function trackedStore() {
	const store = new InMemoryKeyValueStore<string, string>('s', { keyCodec: str, valueCodec: str })
	const stats = { inFlight: 0, mostInFlight: 0 }
	const tracked = <A extends unknown[]>(write: (...args: A) => Promise<void>) => {
		return async (...args: A): Promise<void> => {
			stats.mostInFlight = Math.max(stats.mostInFlight, ++stats.inFlight)
			await new Promise(resolve => setImmediate(resolve))
			await write(...args)
			stats.inFlight--
		}
	}
	store.put = tracked(store.put.bind(store))
	store.delete = tracked(store.delete.bind(store))
	return { store, stats }
}

describe('bulk writes to a store', () => {
	it('keeps the last change of each key, in the order of their last occurrence', () => {
		const changes = latestPerKey([record('a', '1'), record('b', '1'), record(null, 'x'), record('a', null)])

		expect(changes.map(c => [c.key.toString(), c.value?.toString() ?? null])).toEqual([
			['b', '1'],
			['a', null],
		])
	})

	it('issues the changes of a batch together', async () => {
		const { store, stats } = trackedStore()
		await store.put('gone', 'x')
		stats.mostInFlight = 0

		await writeChanges(store, str, str, latestPerKey([record('a', '1'), record('b', '2'), record('gone', null)]))

		expect(stats.mostInFlight).toBe(3)
		expect(await store.get('a')).toBe('1')
		expect(await store.get('b')).toBe('2')
		expect(await store.get('gone')).toBeUndefined()
	})

	it('wipes a store without waiting for each removal before issuing the next', async () => {
		const { store, stats } = trackedStore()
		await writeChanges(
			store,
			str,
			str,
			Array.from({ length: 500 }, (_, i) => ({ key: Buffer.from(`k${i}`), value: Buffer.from('v') }))
		)
		stats.mostInFlight = 0

		await clearStore(store)

		expect(stats.mostInFlight).toBe(500)
		expect(await store.approximateNumEntries()).toBe(0)
	})

	it('stops a wipe between two rounds of removals once its signal aborts (a restore stopped meanwhile)', async () => {
		const { store } = trackedStore()
		const entries = Array.from({ length: 10_001 }, (_, i) => ({
			key: Buffer.from(`k${i}`),
			value: Buffer.from('v'),
		}))
		await writeChanges(store, str, str, entries)
		const stopped = new AbortController()
		const remove = store.delete.bind(store)
		store.delete = key => (stopped.abort(), remove(key))

		await expect(clearStore(store, stopped.signal)).rejects.toThrow(expect.objectContaining({ name: 'AbortError' }))

		expect(await store.approximateNumEntries()).toBe(1)
	})
})
