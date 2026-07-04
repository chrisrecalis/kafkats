import { describe, expect, it, vi } from 'vitest'
import { codec } from '../../src/index.js'
import { TestDriver } from '../../src/testing.js'
import type { KeyValueStore, WindowedKey } from '../../src/state.js'
import { windowedKeyCodec, type ChangelogWriter } from '../../src/changelog.js'
import { ChangelogBackedSessionStore, ChangelogBackedWindowStore } from '../../src/state/changelog.js'
import { InMemorySessionStore, InMemoryWindowStore } from '../../src/state/memory.js'
import type { WorkerContext } from '../../src/processors/base.js'

/**
 * G3 regression: under exactly_once, local state must not be mutated before the transaction
 * commits. Pre-fix, ChangelogBackedKeyValueStore.put wrote local-first and abortTransactionBatch
 * only cleared bookkeeping — an aborted transaction left uncommitted mutations in the local store,
 * so redelivery re-applied the aggregation on the polluted value (double count).
 */
describe('exactly_once store overlay', () => {
	function buildDriver() {
		const driver = new TestDriver({
			applicationId: 'eos-app',
			processingGuarantee: 'exactly_once',
			// Commit inline after every processed message so the test observes commit-time flushes.
			commitIntervalMs: 0,
		})
		let boom = false
		driver
			.input('events', { key: codec.string(), value: codec.json<{ n: number }>() })
			.groupByKey()
			.count({ storeName: 'counts' })
			.toStream()
			.peek(() => {
				if (boom) throw new Error('boom')
			})
		const stores = (driver.flow as unknown as { stateStores: Map<string, KeyValueStore<unknown, unknown>> })
			.stateStores
		return {
			driver,
			store: () => stores.get('counts') as KeyValueStore<string, number>,
			setBoom: (b: boolean) => (boom = b),
		}
	}

	it('an aborted transaction leaves the local store untouched', async () => {
		const { driver, store, setBoom } = buildDriver()
		await driver.run(async ({ send }) => {
			setBoom(true)
			await expect(send('events', { n: 1 }, { key: Buffer.from('a') })).rejects.toThrow('boom')

			// The transaction aborted: the local store must NOT contain the uncommitted count.
			expect(await store().get('a')).toBeUndefined()
		})
	})

	it('redelivery after an abort does not double-count', async () => {
		const { driver, store, setBoom } = buildDriver()
		await driver.run(async ({ send }) => {
			setBoom(true)
			await expect(send('events', { n: 1 }, { key: Buffer.from('a') })).rejects.toThrow('boom')

			// Redeliver the same record; this transaction commits (commitIntervalMs=0 commits inline).
			setBoom(false)
			await send('events', { n: 1 }, { key: Buffer.from('a') })

			// Pre-fix the polluted value (1) was re-aggregated to 2.
			expect(await store().get('a')).toBe(1)
		})
	})

	it('reads within a transaction see the uncommitted writes (read-your-writes)', async () => {
		const driver = new TestDriver({
			applicationId: 'eos-app',
			processingGuarantee: 'exactly_once',
			// Large interval: both records below are processed inside ONE open transaction.
			commitIntervalMs: 60_000,
		})
		const seen: number[] = []
		driver
			.input('events', { key: codec.string(), value: codec.json<{ n: number }>() })
			.groupByKey()
			.count({ storeName: 'counts' })
			.toStream()
			.peek((_key, count) => {
				if (count !== null) seen.push(count)
			})
		const stores = (driver.flow as unknown as { stateStores: Map<string, KeyValueStore<unknown, unknown>> })
			.stateStores
		const store = stores.get('counts') as KeyValueStore<string, number>

		await driver.run(async ({ send }) => {
			await send('events', { n: 1 }, { key: Buffer.from('a') })
			await send('events', { n: 1 }, { key: Buffer.from('a') })
			// Second record must see the first record's in-transaction write.
			expect(seen).toEqual([1, 2])
			// ...but outside the transaction, the local store only shows committed state.
			expect(await store.get('a')).toBeUndefined()
		})
	})
})

/**
 * Scan-path coverage for the overlay: session aggregation reads via findSessions and stream-stream
 * joins read via fetch — both must see in-transaction writes and must not see tombstoned entries.
 */
describe('exactly_once overlay merges into range scans', () => {
	const wkCodec = windowedKeyCodec(codec.string())

	function sessionWriter() {
		return {
			write: vi.fn().mockResolvedValue(undefined),
			writeTombstone: vi.fn().mockResolvedValue(undefined),
			encodeKey: (key: WindowedKey<string>) => wkCodec.encode(key),
		} as unknown as ChangelogWriter<WindowedKey<string>, number>
	}

	function eosFor(worker: WorkerContext, isActive: () => boolean) {
		return { getTransactionalWorker: () => (isActive() ? worker : null) }
	}

	async function collect<K, V>(iter: AsyncIterable<[K, V]>): Promise<Array<[K, V]>> {
		const out: Array<[K, V]> = []
		for await (const entry of iter) {
			out.push(entry)
		}
		return out
	}

	it('findSessions sees overlay puts and hides overlay tombstones', async () => {
		const inner = new InMemorySessionStore<string, number>('sessions', {
			keyCodec: codec.string(),
			valueCodec: codec.json<number>(),
			retentionMs: 60_000,
		})
		const worker = {} as WorkerContext
		let active = false
		const store = new ChangelogBackedSessionStore(
			inner,
			sessionWriter(),
			eosFor(worker, () => active)
		)

		// Committed session already in the local store.
		await inner.put({ key: 'u', windowStart: 1_000, windowEnd: 1_000 }, 1)

		active = true
		// In-transaction: merge deletes the committed session and puts the merged one.
		await store.delete({ key: 'u', windowStart: 1_000, windowEnd: 1_000 })
		await store.put({ key: 'u', windowStart: 1_000, windowEnd: 2_000 }, 2)

		const sessions = await collect(store.findSessions('u', 0, 5_000))
		expect(sessions).toEqual([[{ key: 'u', windowStart: 1_000, windowEnd: 2_000 }, 2]])
		// Other keys never see overlay entries.
		expect(await collect(store.findSessions('other', 0, 5_000))).toEqual([])
		// Inner store still untouched.
		expect(await inner.get({ key: 'u', windowStart: 1_000, windowEnd: 1_000 })).toBe(1)
		expect(await inner.get({ key: 'u', windowStart: 1_000, windowEnd: 2_000 })).toBeUndefined()

		// Commit: overlay applied to the inner store.
		await store.flushTransactionBuffer(worker)
		active = false
		expect(await inner.get({ key: 'u', windowStart: 1_000, windowEnd: 1_000 })).toBeUndefined()
		expect(await inner.get({ key: 'u', windowStart: 1_000, windowEnd: 2_000 })).toBe(2)
	})

	it('discardTransactionBuffer drops in-transaction session writes', async () => {
		const inner = new InMemorySessionStore<string, number>('sessions', {
			keyCodec: codec.string(),
			valueCodec: codec.json<number>(),
			retentionMs: 60_000,
		})
		const worker = {} as WorkerContext
		let active = true
		const store = new ChangelogBackedSessionStore(
			inner,
			sessionWriter(),
			eosFor(worker, () => active)
		)

		await store.put({ key: 'u', windowStart: 1_000, windowEnd: 1_000 }, 1)
		store.discardTransactionBuffer(worker)
		active = false

		expect(await inner.approximateNumEntries()).toBe(0)
		expect(await collect(store.findSessions('u', 0, 5_000))).toEqual([])
	})

	it('window fetch sees overlay puts and hides overlay tombstones', async () => {
		const inner = new InMemoryWindowStore<string, number>('windows', {
			keyCodec: codec.string(),
			valueCodec: codec.json<number>(),
			windowSizeMs: 1_000,
			retentionMs: 60_000,
		})
		const worker = {} as WorkerContext
		let active = false
		const store = new ChangelogBackedWindowStore(
			inner,
			sessionWriter(),
			eosFor(worker, () => active)
		)

		await inner.put({ key: 'u', windowStart: 0, windowEnd: 1_000 }, 1)

		active = true
		await store.put({ key: 'u', windowStart: 2_000, windowEnd: 3_000 }, 2)
		await store.delete({ key: 'u', windowStart: 0, windowEnd: 1_000 })

		const windows = await collect(store.fetch('u', 0, 5_000))
		expect(windows).toEqual([[{ key: 'u', windowStart: 2_000, windowEnd: 3_000 }, 2]])
		// Time filter applies to overlay entries too.
		expect(await collect(store.fetch('u', 0, 1_500))).toEqual([])
		// Inner store untouched until commit.
		expect(await inner.get({ key: 'u', windowStart: 0, windowEnd: 1_000 })).toBe(1)
	})
})
