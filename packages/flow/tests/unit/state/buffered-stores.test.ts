import { describe, expect, it, vi } from 'vitest'
import { codec } from '@/codec.js'
import { YIELD_AFTER_MS } from '@/runtime/signals.js'
import type { KeyValueStore } from '@/state.js'
import { InMemoryKeyValueStore, InMemorySessionStore, InMemoryWindowStore } from '@/state/memory.js'
import { TaskStateManager } from '@/state/state-manager.js'
import { keyValueStoreBuilder } from '@/state/store-builders.js'
import {
	TransactionalKeyValueStore,
	TransactionalSessionStore,
	TransactionalWindowStore,
} from '@/state/transactional.js'
import { DEFAULTS, RecordingOutput, collect, directory, metadata, str, taskId } from './support.js'

function kvStore() {
	const raw = new InMemoryKeyValueStore<string, string>('kv', { keyCodec: str, valueCodec: str })
	return { raw, buf: new TransactionalKeyValueStore<string, string>(raw, str, str) }
}
function windowStore() {
	const raw = new InMemoryWindowStore<string, string>('w', {
		keyCodec: str,
		valueCodec: str,
		retentionMs: 1_000,
		windowSizeMs: 100,
	})
	return { raw, buf: new TransactionalWindowStore<string, string>(raw, str, str, 1_000) }
}
function sessionStore() {
	const raw = new InMemorySessionStore<string, string>('s', { keyCodec: str, valueCodec: str, retentionMs: 1_000 })
	return { raw, buf: new TransactionalSessionStore<string, string>(raw, str, str, 1_000) }
}

const w = (key: string, windowStart: number, windowEnd: number) => ({ key, windowStart, windowEnd })

describe('Transactional stores - writes are buffered until the transaction commits', () => {
	it('holds writes in the buffer, serves them to reads, and applies them on commitBuffer()', async () => {
		const { raw, buf } = kvStore()
		await raw.put('gone', 'old')
		await buf.put('a', '1')
		await buf.delete('gone')
		expect(buf.size).toBe(2)
		expect(await raw.get('a')).toBeUndefined()
		expect(await buf.get('a')).toBe('1')
		expect(await buf.get('gone')).toBeUndefined()

		await buf.commitBuffer()
		expect(buf.size).toBe(0)
		expect(await raw.get('a')).toBe('1')
		expect(await raw.get('gone')).toBeUndefined()
	})

	it('drops the buffer on discardBuffer()', async () => {
		const { raw, buf } = kvStore()
		await raw.put('a', 'committed')
		await buf.put('a', 'aborted')
		buf.discardBuffer()
		expect(buf.size).toBe(0)
		expect(await buf.get('a')).toBe('committed')
		await buf.commitBuffer()
		expect(await raw.get('a')).toBe('committed')
	})
})

describe('Transactional stores - expiry is transactional', () => {
	it('window expiry in an aborted transaction leaves committed windows in place', async () => {
		const { raw, buf } = windowStore()
		await raw.put(w('a', 0, 100), 'committed')
		await buf.put(w('a', 50_000, 50_100), 'new')
		await buf.expireOldWindows(50_000)
		buf.discardBuffer()
		expect(await raw.get(w('a', 0, 100))).toBe('committed')
	})

	it('session expiry in an aborted transaction leaves committed sessions in place', async () => {
		const { raw, buf } = sessionStore()
		await raw.put(w('a', 0, 100), 'committed')
		await buf.put(w('a', 50_000, 50_100), 'new')
		await buf.expireOldSessions(50_000)
		buf.discardBuffer()
		expect(await raw.get(w('a', 0, 100))).toBe('committed')
	})

	it('expired windows disappear from reads in the transaction and from the store after commit', async () => {
		const { raw, buf } = windowStore()
		await raw.put(w('a', 0, 100), 'old')
		await buf.expireOldWindows(50_000)
		expect(await buf.get(w('a', 0, 100))).toBeUndefined()
		await buf.commitBuffer()
		expect(await raw.get(w('a', 0, 100))).toBeUndefined()
	})
})

describe('Transactional stores - expiry reads only what can have expired', () => {
	it('window expiry does not read the windows still in use', async () => {
		const { raw, buf } = windowStore()
		await raw.put(w('old', 0, 100), 'expired')
		for (let i = 0; i < 50; i++) {
			await raw.put(w(`k${i}`, 49_500, 49_600), 'live')
		}
		const all = vi.spyOn(raw, 'all')
		const fetchAll = vi.spyOn(raw, 'fetchAll')
		// Written and expired within the same transaction.
		await buf.put(w('late', 10, 110), 'expired too')
		expect(await buf.expireOldWindows(50_000)).toBe(2)
		await buf.commitBuffer()

		expect(all).not.toHaveBeenCalled()
		// Retention is 1s: only windows that started before 49_000 can have ended before it.
		expect(fetchAll).toHaveBeenCalledWith(Number.MIN_SAFE_INTEGER, 48_999)
		expect(await raw.approximateNumEntries()).toBe(50)
	})

	it('removing the sessions of a key reads only that key', async () => {
		const { raw, buf } = sessionStore()
		await raw.put(w('a', 0, 100), 'a1')
		await raw.put(w('b', 0, 100), 'b1')
		const all = vi.spyOn(raw, 'all')
		await buf.put(w('a', 200, 300), 'a2')
		await buf.remove('a')
		await buf.commitBuffer()

		expect(all).not.toHaveBeenCalled()
		expect(await raw.approximateNumEntries()).toBe(1)
		expect(await raw.get(w('b', 0, 100))).toBe('b1')
	})

	it('B11: removing the sessions of a key removes those with negative times', async () => {
		const { raw, buf } = sessionStore()
		await raw.put(w('a', -500, -400), 'negative')
		await buf.remove('a')
		await buf.commitBuffer()
		expect(await raw.approximateNumEntries()).toBe(0)
	})
})

describe('Transactional stores - a key is the bytes it had when it was written', () => {
	type Id = { id: string }
	const idCodec = codec.json<Id>()

	it('commits the key that was written, even if the caller mutates the key object afterwards', async () => {
		const raw = new InMemoryKeyValueStore<Id, string>('kv', { keyCodec: idCodec, valueCodec: str })
		const buf = new TransactionalKeyValueStore<Id, string>(raw, idCodec, str)
		const key = { id: 'a' }
		await buf.put(key, 'value')
		// e.g. a downstream peek() that reuses the key object
		key.id = 'mutated'
		expect(await collect(buf.all())).toEqual([[{ id: 'a' }, 'value']])
		await buf.commitBuffer()

		// The changelog record of this write carries the original key; the store must agree with it.
		expect(await raw.get({ id: 'a' })).toBe('value')
		expect(await raw.get({ id: 'mutated' })).toBeUndefined()
	})
})

describe('Transactional stores - two task instances are independent', () => {
	async function twoTasks() {
		const { directory: stateDirectory, checkpoints } = directory()
		const builder = keyValueStoreBuilder(DEFAULTS, {
			name: 'counts',
			keyCodec: str,
			valueCodec: str,
			logging: false,
		})
		const managers = [0, 1].map(
			partition =>
				new TaskStateManager(taskId(0, partition), partition, [builder], {
					stateDirectory,
					output: new RecordingOutput(),
					eos: true,
					checkpoints,
					metadata: metadata(),
				})
		)
		for (const manager of managers) await manager.register()
		const [a, b] = managers.map(manager => ({
			manager,
			store: manager.store('counts') as KeyValueStore<string, string>,
			raw: manager.stores[0]!.raw as KeyValueStore<string, string>,
		}))
		return { a: a!, b: b! }
	}

	it("one task's commit does not apply another task's uncommitted writes", async () => {
		const { a, b } = await twoTasks()
		await a.store.put('from-a', 'uncommitted')
		await b.store.put('from-b', 'committed')
		await b.manager.commitBuffers()
		expect(await b.raw.get('from-b')).toBe('committed')
		expect(await a.raw.get('from-a')).toBeUndefined()
		expect(a.manager.dirty).toBe(true)
	})

	it("one task's abort does not drop another task's writes", async () => {
		const { a, b } = await twoTasks()
		await a.store.put('from-a', 'v')
		await b.store.put('from-b', 'v')
		b.manager.discardBuffers()
		await a.manager.commitBuffers()
		expect(await a.raw.get('from-a')).toBe('v')
		expect(b.manager.dirty).toBe(false)
	})

	it("a task never reads another task's uncommitted writes", async () => {
		const { a, b } = await twoTasks()
		await a.store.put('x', 'dirty')
		expect(await b.store.get('x')).toBeUndefined()
	})
})

describe('Transactional stores - overlay iterators behave as if writes were applied', () => {
	it('kv: delete then re-insert, all() and range()', async () => {
		const { raw, buf } = kvStore()
		for (const k of ['a', 'b', 'c', 'd']) await raw.put(k, `raw-${k}`)
		await buf.delete('b')
		await buf.put('b', 'new-b')
		await buf.delete('c')
		await buf.put('e', 'new-e')
		await buf.put('a', 'new-a')
		const expected = { a: 'new-a', b: 'new-b', d: 'raw-d', e: 'new-e' }
		expect(Object.fromEntries(await collect(buf.all()))).toEqual(expected)
		expect((await collect(buf.range('b', 'd'))).map(([k]) => k)).toEqual(['b', 'd'])
		expect((await collect(buf.range('c', 'e'))).map(([k]) => k)).toEqual(['d', 'e'])
		await buf.commitBuffer()
		expect(Object.fromEntries(await collect(raw.all()))).toEqual(expected)
	})

	it('window: fetch/fetchAll/fetchRange see buffered puts and deletes', async () => {
		const { raw, buf } = windowStore()
		await raw.put(w('a', 0, 100), 'a0')
		await raw.put(w('a', 100, 200), 'a1')
		await raw.put(w('b', 0, 100), 'b0')
		await buf.delete(w('a', 0, 100))
		await buf.put(w('a', 200, 300), 'a2')
		await buf.put(w('a', 100, 200), 'a1-new')
		expect((await collect(buf.fetch('a', 0, 1_000))).map(([, v]) => v)).toEqual(['a1-new', 'a2'])
		expect((await collect(buf.fetch('a', 150, 1_000))).map(([, v]) => v)).toEqual(['a2'])
		expect((await collect(buf.fetchAll(0, 0))).map(([, v]) => v)).toEqual(['b0'])
		expect((await collect(buf.fetchRange('a', 'a', 0, 150))).map(([, v]) => v)).toEqual(['a1-new'])
	})

	it('session: merge of several sessions inside one transaction', async () => {
		const { raw, buf } = sessionStore()
		await raw.put(w('k', 0, 10), 's1')
		// three records of the same key merging into one session
		for (const [ts, tag] of [
			[15, 'r1'],
			[30, 'r2'],
			[12, 'r3'],
		] as const) {
			const found = await collect(buf.findSessions('k', ts - 20, ts + 20))
			let start: number = ts
			let end: number = ts
			let value: string = tag
			for (const [session, v] of found) {
				await buf.delete(session)
				start = Math.min(start, session.windowStart)
				end = Math.max(end, session.windowEnd)
				value = `${v}+${value}`
			}
			await buf.put(w('k', start, end), value)
		}
		const sessions = await collect(buf.findSessions('k', 0, 100))
		expect(sessions).toHaveLength(1)
		expect(sessions[0]![0]).toEqual(w('k', 0, 30))
		await buf.remove('k')
		expect(await collect(buf.findSessions('k', 0, 100))).toHaveLength(0)
		await buf.commitBuffer()
		expect(await raw.approximateNumEntries()).toBe(0)
	})

	it('writes made while iterating do not disturb the scan', async () => {
		const { raw, buf } = kvStore()
		await raw.put('a', 'raw-a')
		await buf.put('b', 'new-b')
		const seen: string[] = []
		for await (const [key] of buf.range('a', 'z')) {
			seen.push(key)
			await buf.put('c', 'written while iterating')
			await buf.delete('b')
		}
		expect(seen).toEqual(['a', 'b'])
	})
})

describe('B12: ordered scans merge the buffer in the order of the store', () => {
	it('kv range() returns buffered and stored keys in key order', async () => {
		const { raw, buf } = kvStore()
		for (const k of ['a', 'c', 'e']) await raw.put(k, `raw-${k}`)
		await buf.put('d', 'new-d')
		await buf.put('b', 'new-b')
		await buf.delete('c')
		await buf.put('e', 'new-e')
		await buf.put('f', 'new-f')
		expect(await collect(buf.range('a', 'z'))).toEqual([
			['a', 'raw-a'],
			['b', 'new-b'],
			['d', 'new-d'],
			['e', 'new-e'],
			['f', 'new-f'],
		])
	})

	it('window fetch() returns buffered and stored windows in (start, end) order', async () => {
		const { raw, buf } = windowStore()
		await raw.put(w('k', 0, 100), 'raw-0')
		await raw.put(w('k', 200, 300), 'raw-200')
		await raw.put(w('k', 400, 500), 'raw-400')
		await buf.put(w('k', 300, 400), 'new-300')
		await buf.put(w('k', 100, 200), 'new-100')
		await buf.delete(w('k', 200, 300))
		await buf.put(w('other', 150, 250), 'other key')
		expect((await collect(buf.fetch('k', 0, 1_000))).map(([, v]) => v)).toEqual([
			'raw-0',
			'new-100',
			'new-300',
			'raw-400',
		])
	})

	it('session findSessions() returns buffered and stored sessions in (start, end) order, negative times included', async () => {
		const { raw, buf } = sessionStore()
		await raw.put(w('k', -50, -40), 'raw-neg')
		await raw.put(w('k', 40, 50), 'raw-40')
		await buf.put(w('k', -100, -90), 'new-neg')
		await buf.put(w('k', 20, 30), 'new-20')
		await buf.put(w('k', 40, 60), 'new-40-60')
		await buf.put(w('ka', 0, 5), 'longer key with the same prefix')
		expect((await collect(buf.findSessions('k', -1_000, 1_000))).map(([, v]) => v)).toEqual([
			'new-neg',
			'raw-neg',
			'new-20',
			'raw-40',
			'new-40-60',
		])
	})

	it('streams the store below instead of reading it all first', async () => {
		const { raw, buf } = kvStore()
		for (let i = 0; i < 100; i++) await raw.put(`k${String(i).padStart(3, '0')}`, 'v')
		let pulled = 0
		const inner = raw.range.bind(raw)
		vi.spyOn(raw, 'range').mockImplementation(async function* (from, to) {
			for await (const pair of inner(from, to)) {
				pulled += 1
				yield pair
			}
		})
		await buf.put('k000a', 'new')
		const first: string[] = []
		for await (const [key] of buf.range('k000', 'k999')) {
			first.push(key)
			if (first.length === 3) break
		}
		expect(first).toEqual(['k000', 'k000a', 'k001'])
		expect(pulled).toBeLessThanOrEqual(3)
	})
})

describe('Transactional stores - applying a committed buffer', () => {
	/** A key-value store whose writes complete only when the test settles them. */
	function slowStore() {
		const raw = new InMemoryKeyValueStore<string, string>('kv', { keyCodec: str, valueCodec: str })
		const pending: Array<(error?: Error) => void> = []
		const write = (apply: () => Promise<void>) =>
			new Promise<void>((resolve, reject) => {
				pending.push(error => (error ? reject(error) : apply().then(resolve, reject)))
			})
		const store = Object.assign(Object.create(raw) as InMemoryKeyValueStore<string, string>, {
			put: (key: string, value: string) => write(() => raw.put(key, value)),
			delete: (key: string) => write(() => raw.delete(key)),
		})
		return { raw, store, pending }
	}

	it('issues every buffered write before waiting for any of them', async () => {
		const { raw, store, pending } = slowStore()
		await raw.put('gone', 'old')
		const buf = new TransactionalKeyValueStore<string, string>(store, str, str)
		await buf.put('a', '1')
		await buf.put('b', '2')
		await buf.put('a', '3')
		await buf.delete('gone')
		const committed = buf.commitBuffer()
		await new Promise(resolve => setImmediate(resolve))
		// One write per buffered key, all in flight at once (a batching provider commits them together).
		expect(pending).toHaveLength(3)
		for (const settle of pending) settle()
		await committed
		expect(await raw.get('a')).toBe('3')
		expect(await raw.get('b')).toBe('2')
		expect(await raw.get('gone')).toBeUndefined()
	})

	it('fails the commit when a write fails, once no write is in flight any more', async () => {
		const { raw, store, pending } = slowStore()
		const buf = new TransactionalKeyValueStore<string, string>(store, str, str)
		await buf.put('a', '1')
		await buf.put('b', '2')
		let outcome: unknown = 'pending'
		const committed = buf.commitBuffer().then(
			() => (outcome = 'committed'),
			(error: Error) => (outcome = error.message)
		)
		await new Promise(resolve => setImmediate(resolve))
		pending[0]!(new Error('disk full'))
		await new Promise(resolve => setImmediate(resolve))
		expect(outcome).toBe('pending')
		pending[1]!()
		await committed
		expect(outcome).toBe('disk full')
		expect(await raw.get('b')).toBe('2')
	})

	it('a write that fails while the issue loop yields is not an unhandled rejection; the commit fails with it', async () => {
		const raw = new InMemoryKeyValueStore<string, string>('kv', { keyCodec: str, valueCodec: str })
		let writes = 0
		const store = Object.assign(Object.create(raw) as InMemoryKeyValueStore<string, string>, {
			put: (key: string, value: string) =>
				writes++ === 0 ? Promise.reject(new Error('disk full')) : raw.put(key, value),
		})
		const buf = new TransactionalKeyValueStore<string, string>(store, str, str)
		for (let i = 0; i < 2_500; i++) await buf.put(`k${i}`, `v${i}`)
		const unhandled: unknown[] = []
		const listener = (reason: unknown) => unhandled.push(reason)
		process.on('unhandledRejection', listener)
		try {
			// Past the yield interval: the issue loop yields to the event loop after its first 1000 writes.
			await new Promise(resolve => setTimeout(resolve, YIELD_AFTER_MS + 5))
			await expect(buf.commitBuffer()).rejects.toThrow('disk full')
			await new Promise(resolve => setImmediate(resolve))
		} finally {
			process.off('unhandledRejection', listener)
		}
		expect(unhandled).toEqual([])
		expect(writes).toBe(2_500)
	})

	it('keeps writes made while the buffer is applied for the next commit', async () => {
		const { raw, store, pending } = slowStore()
		const buf = new TransactionalKeyValueStore<string, string>(store, str, str)
		await buf.put('a', '1')
		const committed = buf.commitBuffer()
		await buf.put('b', 'next transaction')
		await new Promise(resolve => setImmediate(resolve))
		for (const settle of pending.splice(0)) settle()
		await committed
		expect(buf.size).toBe(1)
		expect(await raw.get('b')).toBeUndefined()
		const next = buf.commitBuffer()
		await new Promise(resolve => setImmediate(resolve))
		for (const settle of pending.splice(0)) settle()
		await next
		expect(await raw.get('b')).toBe('next transaction')
	})

	it('applies a large buffer completely', async () => {
		const { raw, buf } = kvStore()
		for (let i = 0; i < 5_000; i++) await buf.put(`k${i}`, `v${i}`)
		await buf.commitBuffer()
		expect(await raw.approximateNumEntries()).toBe(5_000)
		expect(await raw.get('k4999')).toBe('v4999')
	})
})
