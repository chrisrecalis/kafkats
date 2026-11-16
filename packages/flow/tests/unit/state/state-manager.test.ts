import { describe, expect, it, vi } from 'vitest'
import { IllegalStateError } from '@/api/errors.js'
import { windowedKeyCodec } from '@/changelog/writer.js'
import type { KeyValueStore, SessionStore, WindowStore } from '@/state.js'
import { Checkpoints } from '@/state/checkpoints.js'
import type { StateDirectory } from '@/state/state-directory.js'
import { TaskStateManager } from '@/state/state-manager.js'
import { keyValueStoreBuilder, sessionStoreBuilder, windowStoreBuilder } from '@/state/store-builders.js'
import type { StoreBuilder } from '@/topology/nodes.js'
import { DEFAULTS, RecordingOutput, checkpointStore, collect, directory, metadata, str, taskId } from './support.js'

const counts = keyValueStoreBuilder(DEFAULTS, { name: 'counts', keyCodec: str, valueCodec: str })
const scratch = keyValueStoreBuilder(DEFAULTS, { name: 'scratch', keyCodec: str, valueCodec: str, logging: false })
const windows = windowStoreBuilder(DEFAULTS, {
	name: 'windows',
	keyCodec: str,
	valueCodec: str,
	retentionMs: 1_000,
	windowSizeMs: 100,
})
const sessions = sessionStoreBuilder(DEFAULTS, { name: 'sessions', keyCodec: str, valueCodec: str, retentionMs: 1_000 })

const w = (key: string, windowStart: number, windowEnd: number) => ({ key, windowStart, windowEnd })

function manager(
	builders: readonly StoreBuilder[],
	options: { eos?: boolean; partition?: number; stateDirectory?: StateDirectory; checkpoints?: Checkpoints } = {}
) {
	const partition = options.partition ?? 2
	const env = directory({ checkpoints: options.checkpoints })
	const output = new RecordingOutput()
	const checkpoints = options.checkpoints ?? env.checkpoints
	const stateDirectory = options.stateDirectory ?? env.directory
	const state = new TaskStateManager(taskId(0, partition), partition, builders, {
		stateDirectory,
		output,
		eos: options.eos ?? false,
		checkpoints,
		metadata: metadata(),
	})
	return { state, output, checkpoints, stateDirectory }
}

const kv = (state: TaskStateManager, name: string) => state.store(name) as KeyValueStore<string, string>
const rawKv = (state: TaskStateManager, index = 0) => state.stores[index]!.raw as KeyValueStore<string, string>

describe('TaskStateManager - store stacks', () => {
	it('at-least-once without changelog: processors write the provider store', async () => {
		const { state, output } = manager([scratch])
		await state.register()
		const [store] = state.stores
		expect(store!.raw.name).toBe('app.scratch.p2')
		expect(store!.buffer).toBeUndefined()
		expect(store!.changelog).toBeUndefined()

		await kv(state, 'scratch').put('k', 'v')
		expect(await rawKv(state).get('k')).toBe('v')
		expect(output.sent).toEqual([])
	})

	it("at-least-once with changelog: local first, then the changelog partition of the task's partition", async () => {
		const { state, output } = manager([counts])
		await state.register()
		expect(state.stores[0]!.changelog).toEqual({
			topic: 'app-counts-changelog',
			partition: 2,
			skipRestoration: false,
		})

		await kv(state, 'counts').put('k', 'v')
		expect(await rawKv(state).get('k')).toBe('v')
		await kv(state, 'counts').delete('k')
		expect(output.changelog()).toEqual([
			{ topic: 'app-counts-changelog', partition: 2, key: 'k', value: 'v' },
			{ topic: 'app-counts-changelog', partition: 2, key: 'k', value: null },
		])
	})

	it('exactly-once: writes are buffered and logged, and reach the provider store when the buffers are applied', async () => {
		const { state, output } = manager([counts], { eos: true })
		await state.register()
		expect(state.stores[0]!.buffer).toBeDefined()
		expect(state.dirty).toBe(false)

		await kv(state, 'counts').put('k', 'v')
		expect(state.dirty).toBe(true)
		expect(await kv(state, 'counts').get('k')).toBe('v')
		expect(await rawKv(state).get('k')).toBeUndefined()
		expect(output.changelog()).toEqual([{ topic: 'app-counts-changelog', partition: 2, key: 'k', value: 'v' }])

		await state.commitBuffers()
		expect(state.dirty).toBe(false)
		expect(await rawKv(state).get('k')).toBe('v')
	})

	it('exactly-once: discarded buffers never reach the provider store', async () => {
		const { state } = manager([counts, scratch], { eos: true })
		await state.register()
		await kv(state, 'counts').put('k', 'v')
		await kv(state, 'scratch').put('k', 'v')
		state.discardBuffers()
		expect(state.dirty).toBe(false)
		await state.commitBuffers()
		expect(await rawKv(state, 0).get('k')).toBeUndefined()
		expect(await rawKv(state, 1).get('k')).toBeUndefined()
	})

	it('commitBuffers() applies every store, then throws the first failure', async () => {
		const { state } = manager([counts, scratch], { eos: true })
		await state.register()
		await kv(state, 'counts').put('k', 'v')
		await kv(state, 'scratch').put('k', 'v')
		vi.spyOn(rawKv(state, 0), 'put').mockRejectedValue(new Error('disk full'))

		await expect(state.commitBuffers()).rejects.toThrow('disk full')
		expect(await rawKv(state, 1).get('k')).toBe('v')
	})

	it('restoration writes the provider store: below the buffer and the changelog', async () => {
		const { state, output } = manager([counts], { eos: true })
		await state.register()
		await rawKv(state).put('restored', 'v')
		expect(await kv(state, 'counts').get('restored')).toBe('v')
		expect(output.sent).toEqual([])
		expect(state.dirty).toBe(false)
	})

	it('window and session changelogs are keyed [key][start][end]', async () => {
		const { state, output } = manager([windows, sessions], { eos: true })
		await state.register()
		await (state.store('windows') as WindowStore<string, string>).put(w('k', 0, 100), 'win')
		await (state.store('sessions') as SessionStore<string, string>).put(w('k', -10, 5), 'session')

		const keys = output.sent.map(({ record }) => windowedKeyCodec(str).decode(record.key!))
		expect(keys).toEqual([w('k', 0, 100), w('k', -10, 5)])
		expect(output.sent.map(({ record }) => [record.topic, record.partition])).toEqual([
			['app-windows-changelog', 2],
			['app-sessions-changelog', 2],
		])
		expect(await collect((state.store('windows') as WindowStore<string, string>).fetch('k', 0, 0))).toEqual([
			[w('k', 0, 100), 'win'],
		])
		await state.commitBuffers()
		expect(await (state.stores[1]!.raw as SessionStore<string, string>).get(w('k', -10, 5))).toBe('session')
	})

	it('store() throws for a store the task does not have', async () => {
		const { state } = manager([counts])
		await state.register()
		expect(() => state.store('other')).toThrow(IllegalStateError)
	})
})

describe('TaskStateManager - checkpoints', () => {
	it('at-least-once: the offset after the last acknowledged changelog write', async () => {
		const durable = checkpointStore()
		const { state } = manager([counts, scratch], { checkpoints: new Checkpoints(durable, () => {}) })
		await state.register()
		await state.checkpoint(
			new Map([
				['app-counts-changelog:2', 41n],
				['other-changelog:2', 7n],
			]),
			false
		)
		expect([...durable.values]).toEqual([['app-counts-changelog:2', 42n]])
		// One flush per commit, by the caller.
		expect(durable.flushes).toBe(0)
	})

	it('exactly-once: one past the commit marker that follows the last write', async () => {
		const durable = checkpointStore()
		const { state } = manager([counts], { eos: true, checkpoints: new Checkpoints(durable, () => {}) })
		await state.register()
		await state.checkpoint(new Map([['app-counts-changelog:2', 41n]]), true)
		expect(durable.values.get('app-counts-changelog:2')).toBe(43n)
	})

	it('changelog partitions without writes keep their checkpoint', async () => {
		const durable = checkpointStore()
		durable.values.set('app-counts-changelog:2', 5n)
		const { state } = manager([counts], { checkpoints: new Checkpoints(durable, () => {}) })
		await state.register()
		await state.checkpoint(new Map(), false)
		expect(durable.values.get('app-counts-changelog:2')).toBe(5n)
	})
})

describe('TaskStateManager - closing and releasing', () => {
	it('closeStack(): writes reject with IllegalStateError, reads still work', async () => {
		const { state } = manager([counts, windows, sessions], { eos: true })
		await state.register()
		const store = kv(state, 'counts')
		await store.put('k', 'v')
		state.closeStack()

		await expect(store.put('k', 'late')).rejects.toThrow(IllegalStateError)
		await expect(store.delete('k')).rejects.toThrow(/of task 0_2 was written after it closed/)
		await expect((state.store('windows') as WindowStore<string, string>).expireOldWindows(1)).rejects.toThrow(
			IllegalStateError
		)
		await expect((state.store('sessions') as SessionStore<string, string>).remove('k')).rejects.toThrow(
			IllegalStateError
		)
		expect(await store.get('k')).toBe('v')
	})

	it('processors cannot init() or close() a store: its lifecycle belongs to flow', async () => {
		const { state } = manager([counts])
		await state.register()
		await expect(state.store('counts').init()).rejects.toThrow(IllegalStateError)
		await expect(state.store('counts').close()).rejects.toThrow(IllegalStateError)
	})

	it('release() returns every store to the state directory', async () => {
		const { state, stateDirectory } = manager([counts, scratch])
		await state.register()
		await kv(state, 'counts').put('k', 'kept')
		await state.release()

		expect(state.stores).toEqual([])
		const next = manager([counts, scratch], { stateDirectory }).state
		await next.register()
		expect(await kv(next, 'counts').get('k')).toBe('kept')
	})

	it('release({ wipe }) invalidates the checkpoints and clears the stores', async () => {
		const durable = checkpointStore()
		const checkpoints = new Checkpoints(durable, () => {})
		const { state, stateDirectory } = manager([counts], { checkpoints })
		await state.register()
		await kv(state, 'counts').put('k', 'v')
		await checkpoints.set('app-counts-changelog', 2, 10n)

		await state.release({ wipe: true })

		expect(durable.values.get('app-counts-changelog:2')).toBe(-1n)
		const next = manager([counts], { stateDirectory, checkpoints }).state
		await next.register()
		expect(await rawKv(next).approximateNumEntries()).toBe(0)
	})

	it('register() releases what it leased when a lease fails', async () => {
		const env = directory()
		const holder = manager([scratch], { stateDirectory: env.directory }).state
		await holder.register()
		const { state } = manager([counts, scratch], { stateDirectory: env.directory })

		await expect(state.register()).rejects.toThrow(/already leased/)
		expect(state.stores).toEqual([])
		const other = manager([counts], { stateDirectory: env.directory }).state
		await expect(other.register()).resolves.toBeUndefined()
	})

	it('register() releases the store it could not stack', async () => {
		const env = directory()
		const invalid: StoreBuilder = { ...windows, retentionMs: undefined }
		const { state } = manager([invalid], { stateDirectory: env.directory, eos: true })

		await expect(state.register()).rejects.toThrow(IllegalStateError)
		const other = manager([windows], { stateDirectory: env.directory }).state
		await expect(other.register()).resolves.toBeUndefined()
	})
})
