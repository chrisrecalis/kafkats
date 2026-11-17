import { describe, expect, it, vi } from 'vitest'
import { IllegalStateError } from '@/api/errors.js'
import { windowedKeyCodec } from '@/changelog/writer.js'
import type { ProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import { decodeCommitMetadata } from '@/processor/metadata.js'
import type { SessionStore, WindowedKey, WindowStore } from '@/state.js'
import { storeHandle } from '@/state/handles.js'
import { TaskStateManager } from '@/state/state-manager.js'
import { sessionStoreBuilder, windowStoreBuilder } from '@/state/store-builders.js'
import { ClockedSessionStore, ClockedWindowStore, TaskStoreClock, type ClockMetadata } from '@/state/time-clipping.js'
import type { EmitFinalWindows, StoreBuilder, StoreClockSpec } from '@/topology/nodes.js'
import { pipe, TaskHarness } from '../support/task-harness.js'
import { DEFAULTS, RecordingOutput, collect, directory, metadata, str, taskId } from './support.js'

const w = (key: string, windowStart: number, windowEnd: number): WindowedKey<string> => ({
	key,
	windowStart,
	windowEnd,
})
const ends = (entries: [WindowedKey<string>, string][]) => entries.map(([key]) => key.windowEnd)

/** Metadata that records its puts; `markCommitted()` stands for a commit of the task. */
function recordingMetadata(): ClockMetadata & { readonly puts: [string, number][]; markCommitted(): void } {
	const inner = metadata()
	const puts: [string, number][] = []
	return {
		puts,
		get: key => inner.get(key),
		put: (key, value) => {
			puts.push([key, value])
			inner.put(key, value)
		},
		committed: key => inner.committed(key),
		markCommitted: () => inner.markCommitted(),
	}
}

/** The registered stack of one store, logged to a changelog. */
async function stack(builder: StoreBuilder, eos = false) {
	const output = new RecordingOutput()
	const meta = recordingMetadata()
	const { directory: stateDirectory, checkpoints } = directory()
	const state = new TaskStateManager(taskId(0, 0), 0, [builder], {
		stateDirectory,
		output,
		eos,
		checkpoints,
		metadata: meta,
	})
	await state.register()
	const [registered] = state.stores
	return { state, output, metadata: meta, registered: registered!, clock: () => registered!.clock!.time }
}

/** Ten-wide windows kept 100 ms by window end. */
const windows = (clock: StoreClockSpec = {}, retentionMs = 100) =>
	windowStoreBuilder(DEFAULTS, { name: 'w', keyCodec: str, valueCodec: str, retentionMs, windowSizeMs: 10, clock })

const sessions = (retentionMs: number, clock: StoreClockSpec = {}) =>
	sessionStoreBuilder(DEFAULTS, { name: 's', keyCodec: str, valueCodec: str, retentionMs, clock })

describe('TaskStoreClock (6.2)', () => {
	it('is the max key time advanced to; each increase is persisted as <store>.observed in the task metadata', () => {
		const meta = recordingMetadata()
		const clock = new TaskStoreClock(meta, 'w', 100)
		expect(clock.time).toBe(-1)
		clock.advance(100)
		clock.advance(50)
		clock.advance(100)
		clock.advance(150)
		expect(clock.time).toBe(150)
		expect(meta.puts).toEqual([
			['w.observed', 100],
			['w.observed', 150],
		])
	})

	it('seeding raises the time without writing the metadata (not a change to commit)', () => {
		const meta = recordingMetadata()
		const clock = new TaskStoreClock(meta, 'w', 100)
		clock.seed(80)
		clock.seed(40)
		expect(clock.time).toBe(80)
		clock.advance(60)
		expect(clock.time).toBe(80)
		expect(meta.puts).toEqual([])
	})

	it('reads stop at time - retention + 1; expiry at the time of the last commit - retention + 1', () => {
		const meta = metadata()
		const clock = new TaskStoreClock(meta, 'w', 10)
		clock.advance(100)
		expect(clock.readFloor).toBe(91)
		expect(clock.expiryCutoff).toBe(-10)
		meta.markCommitted()
		clock.advance(200)
		expect(clock.readFloor).toBe(191)
		// A task revived from this commit replays the records since, which read from 91 on.
		expect(clock.expiryCutoff).toBe(91)
	})

	it('a retention of 0 keeps the clock time itself readable', () => {
		const clock = new TaskStoreClock(metadata(), 's', 0)
		clock.advance(100)
		expect(clock.readFloor).toBe(100)
	})

	it.each<[string, EmitFinalWindows, number, number]>([
		// Time windows of 10: the next scan starts at window start max(0, last - size) + 1, i.e. window end + 10.
		['time windows, nothing emitted yet', { kind: 'time', sizeMs: 10 }, -1, 10],
		['time windows', { kind: 'time', sizeMs: 10 }, 500, 501],
		// Sliding windows of 10: start max(0, last - d), end + 10.
		['sliding windows, nothing emitted yet', { kind: 'sliding', sizeMs: 10 }, -1, 10],
		['sliding windows', { kind: 'sliding', sizeMs: 10 }, 500, 500],
		// Sessions: by session end, from the last close time.
		['sessions, nothing emitted yet', { kind: 'session' }, -1, 0],
		['sessions', { kind: 'session' }, 500, 500],
	])(
		'KF1: emit on window close, %s: expiry waits for the next scan from the committed <store>.emit',
		(_, emitFinal, last, bound) => {
			const meta = metadata()
			if (last !== -1) meta.put('w.emit', last)
			const clock = new TaskStoreClock(meta, 'w', 10, emitFinal)
			clock.advance(10_000)
			meta.markCommitted()
			expect(clock.readFloor).toBe(9_991)
			expect(clock.expiryCutoff).toBe(bound)
			meta.put('w.emit', 20_000)
			expect(clock.expiryCutoff).toBe(bound)
			meta.markCommitted()
			expect(clock.expiryCutoff).toBe(9_991)
		}
	)
})

describe('clocked window store (6.2)', () => {
	it('is the top of the stack of a store with a clock; the clock advances on put and delete, by window end', async () => {
		const { state, registered, clock, metadata: meta } = await stack(windows())
		const store = state.store('w') as ClockedWindowStore<string, string>
		expect(store).toBeInstanceOf(ClockedWindowStore)
		expect(registered.top).toBe(store)

		await store.put(w('k', 100, 110), 'a')
		expect(clock()).toBe(110)
		await store.get(w('k', 200, 210))
		await collect(store.fetch('k', 0, 1_000))
		await store.put(w('k', 50, 60), 'older')
		await store.expire()
		expect(clock()).toBe(110)
		// Kafka Streams' segmented stores advance their clock on remove and on putting null too.
		await store.delete(w('k', 300, 310))
		expect(clock()).toBe(310)
		expect(meta.puts).toEqual([
			['w.observed', 110],
			['w.observed', 310],
		])
	})

	it('a delete above the clock clips the reads of what it left below the new floor', async () => {
		const { state } = await stack(windows())
		const store = state.store('w') as ClockedWindowStore<string, string>
		await store.put(w('k', 50, 60), 'a')
		await store.delete(w('j', 200, 210))
		// clock 210, retention 100: the floor is 111
		expect(await store.get(w('k', 50, 60))).toBeUndefined()
		expect(await collect(store.fetch('k', 0, 1_000))).toEqual([])
	})

	it('reads return nothing below time - retention + 1, whichever read it is', async () => {
		const { state } = await stack(windows())
		const store = state.store('w') as ClockedWindowStore<string, string>
		await store.put(w('k', 90, 100), 'below')
		await store.put(w('k', 91, 101), 'at the floor')
		await store.put(w('k', 190, 200), 'newest')
		// clock 200, retention 100: the floor is 101

		expect(await store.get(w('k', 90, 100))).toBeUndefined()
		expect(await store.get(w('k', 91, 101))).toBe('at the floor')
		expect(ends(await collect(store.fetch('k', 0, 1_000)))).toEqual([101, 200])
		expect(ends(await collect(store.fetchAll(0, 1_000)))).toEqual([101, 200])
		expect(ends(await collect(store.fetchRange('a', 'z', 0, 1_000)))).toEqual([101, 200])
		expect(ends(await collect(store.all())).sort()).toEqual([101, 200])
		expect(ends(await collect(store.range(w('k', 0, 0), w('k', 1_000, 1_000)))).sort()).toEqual([101, 200])
		// Present until expired.
		expect(await store.approximateNumEntries()).toBe(3)
	})

	it('emit-final scans read below the floor through unclipped (6.8)', async () => {
		const { state } = await stack(windows({ emitFinal: { kind: 'time', sizeMs: 10 } }))
		const store = state.store('w') as ClockedWindowStore<string, string>
		await store.put(w('k', 0, 10), 'unemitted')
		await store.put(w('k', 1_000, 1_010), 'newest')
		expect(await collect(store.fetchAll(0, 1_000))).toEqual([[w('k', 1_000, 1_010), 'newest']])
		expect(await collect(store.unclipped.fetchAll(0, 1_000))).toEqual([
			[w('k', 0, 10), 'unemitted'],
			[w('k', 1_000, 1_010), 'newest'],
		])
	})

	it('expire() deletes what lies below the cutoff, locally only (KF17)', async () => {
		const { state, registered, output, metadata: meta } = await stack(windows())
		const store = state.store('w') as ClockedWindowStore<string, string>
		await store.put(w('k', 90, 100), 'below')
		await store.put(w('k', 91, 101), 'at the floor')
		await store.put(w('k', 190, 200), 'newest')
		meta.markCommitted()
		await store.expire()

		const raw = registered.raw as WindowStore<string, string>
		expect(ends(await collect(raw.all())).sort()).toEqual([101, 200])
		expect(output.changelog().map(record => record.value)).toEqual(['below', 'at the floor', 'newest'])
	})

	it('KF1: emit on window close, expiry keeps the windows the next scan has not emitted', async () => {
		const {
			state,
			registered,
			metadata: meta,
		} = await stack(windows({ emitFinal: { kind: 'time', sizeMs: 10 } }, 20))
		const store = state.store('w') as ClockedWindowStore<string, string>
		const raw = registered.raw as WindowStore<string, string>
		await store.put(w('k', 0, 10), 'a')
		await store.put(w('k', 100, 110), 'b')
		await store.put(w('k', 500, 510), 'c')
		meta.markCommitted()
		// clock 510, retention 20: reads stop at 491, but nothing was emitted yet
		expect(await store.get(w('k', 100, 110))).toBeUndefined()
		await store.expire()
		expect(ends(await collect(raw.all())).sort()).toEqual([10, 110, 510])

		// A scan closed everything up to 120: once committed, the next one starts at window start 111 (end 121).
		meta.put('w.emit', 120)
		await store.expire()
		expect(ends(await collect(raw.all())).sort()).toEqual([10, 110, 510])
		meta.markCommitted()
		await store.expire()
		expect(ends(await collect(raw.all()))).toEqual([510])
	})

	it('exactly-once: expiry deletes through the write buffer, applied with the transaction', async () => {
		const { state, registered, output, metadata: meta } = await stack(windows(), true)
		const store = state.store('w') as ClockedWindowStore<string, string>
		const raw = registered.raw as WindowStore<string, string>
		await store.put(w('k', 0, 10), 'old')
		await state.commitBuffers()
		await store.put(w('k', 500, 510), 'new')
		meta.markCommitted()
		await store.expire()
		expect(ends(await collect(raw.all()))).toEqual([10])
		expect(ends(await collect(store.unclipped.all())).sort()).toEqual([510])

		await state.commitBuffers()
		expect(ends(await collect(raw.all()))).toEqual([510])
		expect(output.changelog().map(record => record.value)).toEqual(['old', 'new'])
	})

	it('a write the closed stack rejects does not advance the clock', async () => {
		const { state, clock } = await stack(windows())
		const store = state.store('w') as ClockedWindowStore<string, string>
		await store.put(w('k', 0, 10), 'a')
		state.closeStack()
		await expect(store.put(w('k', 100, 110), 'late')).rejects.toBeInstanceOf(IllegalStateError)
		await expect(store.expire()).rejects.toBeInstanceOf(IllegalStateError)
		expect(clock()).toBe(10)
	})
})

describe('clocked session store (6.2)', () => {
	it('KF2: sessions of gap 10 at 100 and 110 merge (store clock 100, floor 91); one that ended at 90 is gone', async () => {
		const { state, clock } = await stack(sessions(10))
		const store = state.store('s') as ClockedSessionStore<string, string>
		expect(store).toBeInstanceOf(ClockedSessionStore)
		await store.put(w('k', 80, 90), 'old')
		await store.put(w('k', 100, 100), 'live')
		expect(clock()).toBe(100)

		// The record at 110 looks for sessions ending at 100 or later and starting at 120 or earlier.
		expect(await collect(store.findSessions('k', 100, 120))).toEqual([[w('k', 100, 100), 'live']])
		// A record at 95 would merge the old session, but it ended below the floor.
		expect(await collect(store.findSessions('k', 85, 105))).toEqual([[w('k', 100, 100), 'live']])
		expect(await store.get(w('k', 80, 90))).toBeUndefined()
		expect(await collect(store.all())).toEqual([[w('k', 100, 100), 'live']])
		expect(await collect(store.unclipped.findSessions('k', 85, 105))).toEqual([
			[w('k', 80, 90), 'old'],
			[w('k', 100, 100), 'live'],
		])
	})

	it('a retention of 0: a second record of the same timestamp finds the first one', async () => {
		const { state } = await stack(sessions(0))
		const store = state.store('s') as ClockedSessionStore<string, string>
		await store.put(w('k', 100, 100), 'first')
		expect(await collect(store.findSessions('k', 100, 100))).toEqual([[w('k', 100, 100), 'first']])
	})

	it('remove() logs a tombstone for every session of the key, clipped ones included; expire() logs none', async () => {
		const { state, registered, output, metadata: meta } = await stack(sessions(10))
		const store = state.store('s') as ClockedSessionStore<string, string>
		const raw = registered.raw as SessionStore<string, string>
		const tombstones = () =>
			output.sent
				.filter(({ record }) => record.value === null)
				.map(({ record }) => windowedKeyCodec(str).decode(record.key!))
		await store.put(w('k', 70, 80), 'expired')
		await store.put(w('j', 85, 90), 'clipped')
		await store.put(w('j', 100, 100), 'live')

		await store.remove('j')
		expect(tombstones()).toEqual([w('j', 85, 90), w('j', 100, 100)])
		meta.markCommitted()
		await store.expire()
		expect(await collect(raw.all())).toEqual([])
		expect(tombstones()).toHaveLength(2)
	})
})

describe('stacks without a clock', () => {
	it('window and session stores without a clock and key-value stores get no clipping layer', async () => {
		const unclocked = [
			windowStoreBuilder(DEFAULTS, {
				name: 'w',
				keyCodec: str,
				valueCodec: str,
				retentionMs: 1,
				windowSizeMs: 1,
			}),
			sessionStoreBuilder(DEFAULTS, { name: 's', keyCodec: str, valueCodec: str, retentionMs: 1 }),
		]
		for (const builder of unclocked) {
			const { registered } = await stack(builder)
			expect(registered.clock).toBeUndefined()
			expect(registered.top).not.toBeInstanceOf(ClockedWindowStore)
			expect(registered.top).not.toBeInstanceOf(ClockedSessionStore)
		}
	})
})

describe('the handle processors get of a store with a clock', () => {
	it('keeps expire() and the unclipped reads; expire() rejects once the handle closed', async () => {
		for (const builder of [windows(), sessions(10)]) {
			const { state } = await stack(builder)
			const top = state.store(builder.name) as
				| ClockedWindowStore<string, string>
				| ClockedSessionStore<string, string>
			const expire = vi.spyOn(top, 'expire')
			let closed = false
			const handle = storeHandle(top, builder.kind, () => closed, 'processors of task 0_0') as typeof top
			expect(handle.unclipped).toBe(top.unclipped)
			await handle.expire()
			closed = true
			await expect(handle.expire()).rejects.toBeInstanceOf(IllegalStateError)
			expect(expire).toHaveBeenCalledTimes(1)
		}
	})
})

/** Puts the window `[ts, ts + 10)` of each record into store 'w'. */
const windowWriter: ProcessorSupplier<string, string, string, string> = () => {
	let store: ClockedWindowStore<string, string>
	return {
		init: (context: ProcessorContext<string, string>) => {
			store = context.getStateStore<ClockedWindowStore<string, string>>('w')
		},
		process: record => store.put(w(record.key!, record.timestamp, record.timestamp + 10), record.value!),
	}
}

describe('store clocks across task incarnations (6.2, 4.4)', () => {
	for (const eos of [false, true]) {
		it(`${eos ? 'exactly-once' : 'at-least-once'}: committed as <store>.observed, seeding the next incarnation`, async () => {
			const h = new TaskHarness({ eos, build: b => pipe(b, { processor: windowWriter, stores: [windows()] }) })
			const first = await h.start(h.task())
			h.enqueue(first, 'input', [
				{ key: 'k', value: 'old', timestamp: 100 },
				{ key: 'k', value: 'new', timestamp: 500 },
			])
			await h.process(first)
			expect(await h.commit()).toBe('committed')
			const [offset] = h.committedOffsets()
			expect(decodeCommitMetadata(offset!.metadata).metadata.get('w.observed')).toBe(510)
			await first.suspend()
			await first.close('dirty')

			const second = await h.restart()
			const [registered] = second.stores
			expect(registered!.clock!.time).toBe(510)
			// Still in the store, but past the retention of the seeded clock (floor 411).
			expect(await (registered!.raw as WindowStore<string, string>).get(w('k', 100, 110))).toBe('old')
			const store = second.store('w') as ClockedWindowStore<string, string>
			expect(await store.get(w('k', 100, 110))).toBeUndefined()
			expect(await store.get(w('k', 500, 510))).toBe('new')
		})
	}

	it('the max key time a restore applied seeds the clock when it is above the committed one', async () => {
		const h = new TaskHarness({ build: b => pipe(b, { processor: windowWriter, stores: [windows()] }) })
		const task = h.task()
		await task.initialize()
		task.setStarts(new Map())
		await task.completeRestoration(new Map([['w', 900]]))
		expect(task.stores[0]!.clock!.time).toBe(900)
	})
})
