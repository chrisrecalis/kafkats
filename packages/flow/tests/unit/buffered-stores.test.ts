import { describe, expect, it } from 'vitest'
import { codec } from '../../src/index.js'
import { windowedKeyCodec } from '../../src/changelog.js'
import { InMemoryKeyValueStore, InMemorySessionStore, InMemoryWindowStore } from '../../src/state/memory.js'
import { BufferedKeyValueStore, BufferedSessionStore, BufferedWindowStore } from '../../src/state/buffered.js'
import { recordContextStorage, type RecordContext } from '../../src/runtime/context.js'

const str = codec.string()

function ctx(partition = 0): RecordContext {
	return {
		worker: { activeTransaction: {}, dirtyStores: new Set() } as never,
		topic: 't',
		partition,
	}
}
const inTx = <T>(c: RecordContext, fn: () => Promise<T>) => recordContextStorage.run(c, fn)
async function collect<T>(it: AsyncIterable<T>): Promise<T[]> {
	const out: T[] = []
	for await (const x of it) out.push(x)
	return out
}
async function commit(c: RecordContext) {
	for (const s of c.worker.dirtyStores) await s.commitBuffer(c.worker.dirtyStores)
	c.worker.dirtyStores.clear()
}
function abort(c: RecordContext) {
	for (const s of c.worker.dirtyStores) s.discardBuffer(c.worker.dirtyStores)
	c.worker.dirtyStores.clear()
}

function windowStore() {
	const raw = new InMemoryWindowStore<string, string>('w', {
		keyCodec: str,
		valueCodec: str,
		retentionMs: 1_000,
		windowSizeMs: 100,
	})
	return { raw, buf: new BufferedWindowStore<string, string>(raw, windowedKeyCodec(str), str, str, 1_000) }
}
function sessionStore() {
	const raw = new InMemorySessionStore<string, string>('s', { keyCodec: str, valueCodec: str, retentionMs: 1_000 })
	return { raw, buf: new BufferedSessionStore<string, string>(raw, windowedKeyCodec(str), str, str, 1_000) }
}

describe('Buffered stores - expiry must be transactional', () => {
	// BUG (F3): expireOldWindows bypasses the buffer, so an aborted transaction permanently deletes committed windows.
	it('window expiry performed inside an aborted transaction leaves committed windows in place', async () => {
		const { raw, buf } = windowStore()
		await raw.put({ key: 'a', windowStart: 0, windowEnd: 100 }, 'committed')
		const c = ctx()
		await inTx(c, async () => {
			await buf.put({ key: 'a', windowStart: 50_000, windowEnd: 50_100 }, 'new')
			await buf.expireOldWindows(50_000)
		})
		abort(c)
		expect(await raw.get({ key: 'a', windowStart: 0, windowEnd: 100 })).toBe('committed')
	})

	it('session expiry performed inside an aborted transaction leaves committed sessions in place', async () => {
		const { raw, buf } = sessionStore()
		await raw.put({ key: 'a', windowStart: 0, windowEnd: 100 }, 'committed')
		const c = ctx()
		await inTx(c, async () => {
			await buf.put({ key: 'a', windowStart: 50_000, windowEnd: 50_100 }, 'new')
			await buf.expireOldSessions(50_000)
		})
		abort(c)
		expect(await raw.get({ key: 'a', windowStart: 0, windowEnd: 100 })).toBe('committed')
	})

	// Same root cause, different symptom: an expiry in the same transaction must be visible to reads
	// only if the transaction commits; reads inside the tx should already see it (read-your-writes).
	it('expired windows disappear from reads inside the transaction and from the store after commit', async () => {
		const { raw, buf } = windowStore()
		await raw.put({ key: 'a', windowStart: 0, windowEnd: 100 }, 'old')
		const c = ctx()
		await inTx(c, async () => {
			await buf.expireOldWindows(50_000)
			expect(await buf.get({ key: 'a', windowStart: 0, windowEnd: 100 })).toBeUndefined()
		})
		await commit(c)
		expect(await raw.get({ key: 'a', windowStart: 0, windowEnd: 100 })).toBeUndefined()
	})
})

describe('Buffered stores - one buffer shared by two workers', () => {
	// BUG (F4): the buffer is per store, not per worker/transaction. App-scoped stores (KTable.groupBy mapping and
	// aggregate stores) are one StoreStack shared by every worker (numStreamThreads > 1), so worker B's commit applies
	// worker A's uncommitted writes and worker B's abort throws away worker A's writes.
	it("one worker's commit does not apply another worker's uncommitted writes", async () => {
		const raw = new InMemoryKeyValueStore<string, string>('kv', { keyCodec: str, valueCodec: str })
		const buf = new BufferedKeyValueStore<string, string>(raw, str, str)
		const a = ctx()
		const b = ctx()
		await inTx(a, () => buf.put('from-a', 'uncommitted'))
		await inTx(b, () => buf.put('from-b', 'committed'))
		await commit(b)
		expect(await raw.get('from-b')).toBe('committed')
		expect(await raw.get('from-a')).toBeUndefined()
	})

	it("one worker's abort does not drop another worker's writes", async () => {
		const raw = new InMemoryKeyValueStore<string, string>('kv', { keyCodec: str, valueCodec: str })
		const buf = new BufferedKeyValueStore<string, string>(raw, str, str)
		const a = ctx()
		const b = ctx()
		await inTx(a, () => buf.put('from-a', 'v'))
		await inTx(b, () => buf.put('from-b', 'v'))
		abort(b)
		await commit(a)
		expect(await raw.get('from-a')).toBe('v')
	})

	it("a worker never reads another worker's uncommitted writes", async () => {
		const raw = new InMemoryKeyValueStore<string, string>('kv', { keyCodec: str, valueCodec: str })
		const buf = new BufferedKeyValueStore<string, string>(raw, str, str)
		const a = ctx()
		const b = ctx()
		await inTx(a, () => buf.put('x', 'dirty'))
		expect(await inTx(b, () => buf.get('x'))).toBeUndefined()
	})
})

describe('Buffered stores - overlay iterators behave as if writes were applied (guards)', () => {
	it('kv: delete then re-insert, all() and range()', async () => {
		const raw = new InMemoryKeyValueStore<string, string>('kv', { keyCodec: str, valueCodec: str })
		for (const k of ['a', 'b', 'c', 'd']) await raw.put(k, `raw-${k}`)
		const buf = new BufferedKeyValueStore<string, string>(raw, str, str)
		const c = ctx()
		await inTx(c, async () => {
			await buf.delete('b')
			await buf.put('b', 'new-b')
			await buf.delete('c')
			await buf.put('e', 'new-e')
			await buf.put('a', 'new-a')
			const all = Object.fromEntries(await collect(buf.all()))
			expect(all).toEqual({ a: 'new-a', b: 'new-b', d: 'raw-d', e: 'new-e' })
			expect((await collect(buf.range('b', 'd'))).map(([k]) => k)).toEqual(['b', 'd'])
			expect((await collect(buf.range('c', 'e'))).map(([k]) => k)).toEqual(['d', 'e'])
		})
		await commit(c)
		expect(Object.fromEntries(await collect(raw.all()))).toEqual({ a: 'new-a', b: 'new-b', d: 'raw-d', e: 'new-e' })
	})

	it('window: fetch/fetchAll/fetchRange see buffered puts and deletes', async () => {
		const { raw, buf } = windowStore()
		await raw.put({ key: 'a', windowStart: 0, windowEnd: 100 }, 'a0')
		await raw.put({ key: 'a', windowStart: 100, windowEnd: 200 }, 'a1')
		await raw.put({ key: 'b', windowStart: 0, windowEnd: 100 }, 'b0')
		const c = ctx()
		await inTx(c, async () => {
			await buf.delete({ key: 'a', windowStart: 0, windowEnd: 100 })
			await buf.put({ key: 'a', windowStart: 200, windowEnd: 300 }, 'a2')
			await buf.put({ key: 'a', windowStart: 100, windowEnd: 200 }, 'a1-new')
			expect((await collect(buf.fetch('a', 0, 1_000))).map(([, v]) => v).sort()).toEqual(['a1-new', 'a2'])
			expect((await collect(buf.fetch('a', 150, 1_000))).map(([, v]) => v)).toEqual(['a2'])
			expect((await collect(buf.fetchAll(0, 0))).map(([, v]) => v)).toEqual(['b0'])
			expect((await collect(buf.fetchRange('a', 'a', 0, 150))).map(([, v]) => v)).toEqual(['a1-new'])
		})
	})

	it('session: merge of several sessions inside one transaction', async () => {
		const { raw, buf } = sessionStore()
		await raw.put({ key: 'k', windowStart: 0, windowEnd: 10 }, 's1')
		const c = ctx()
		await inTx(c, async () => {
			// emulate three records of the same key merging into one session
			for (const [ts, tag] of [
				[15, 'r1'],
				[30, 'r2'],
				[12, 'r3'],
			] as const) {
				const found = await collect(buf.findSessions('k', ts - 20, ts + 20))
				let start: number = ts
				let end: number = ts
				let value: string = tag
				for (const [wk, v] of found) {
					await buf.delete(wk)
					start = Math.min(start, wk.windowStart)
					end = Math.max(end, wk.windowEnd)
					value = `${v}+${value}`
				}
				await buf.put({ key: 'k', windowStart: start, windowEnd: end }, value)
			}
			const sessions = await collect(buf.findSessions('k', 0, 100))
			expect(sessions).toHaveLength(1)
			expect(sessions[0]![0]).toEqual({ key: 'k', windowStart: 0, windowEnd: 30 })
			await buf.remove('k')
			expect(await collect(buf.findSessions('k', 0, 100))).toHaveLength(0)
		})
		await commit(c)
		expect(await raw.approximateNumEntries()).toBe(0)
	})
})

describe('Buffered stores - applying a committed buffer', () => {
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
		const buf = new BufferedKeyValueStore<string, string>(store, str, str)
		const c = ctx()
		await inTx(c, async () => {
			await buf.put('a', '1')
			await buf.put('b', '2')
			await buf.put('a', '3')
			await buf.delete('gone')
		})
		const committed = commit(c)
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
		const buf = new BufferedKeyValueStore<string, string>(store, str, str)
		const c = ctx()
		await inTx(c, async () => {
			await buf.put('a', '1')
			await buf.put('b', '2')
		})
		let outcome: unknown = 'pending'
		const committed = commit(c).then(
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
})
