import { noopLogger, type Logger } from '@kafkats/client'
import { describe, expect, it, vi } from 'vitest'
import { IllegalStateError } from '@/api/errors.js'
import type { KeyValueStore } from '@/state.js'
import { Checkpoints } from '@/state/checkpoints.js'
import { InMemoryStateStoreProvider } from '@/state/memory.js'
import { StateDirectory } from '@/state/state-directory.js'
import { keyValueStoreBuilder, timestampedKeyValueStoreBuilder } from '@/state/store-builders.js'
import { APP, DEFAULTS, checkpointStore, directory, str, taskId } from './support.js'

const counts = keyValueStoreBuilder(DEFAULTS, { name: 'counts', keyCodec: str, valueCodec: str })
const unlogged = keyValueStoreBuilder(DEFAULTS, { name: 'scratch', keyCodec: str, valueCodec: str, logging: false })
const T0 = taskId(0, 0)
const T1 = taskId(0, 1)

describe('StateDirectory', () => {
	it('names provider stores <applicationId>.<store>.p<partition>', async () => {
		const provider = new InMemoryStateStoreProvider()
		const create = vi.spyOn(provider, 'createKeyValueStore')
		const { directory: dir } = directory({ provider })
		expect(dir.providerName('counts', 3)).toBe('app.counts.p3')

		const store = await dir.lease(T1, counts, 1)
		expect(store.name).toBe('app.counts.p1')
		expect(create).toHaveBeenCalledWith('app.counts.p1', { keyCodec: str, valueCodec: str })
	})

	it('creates and initializes a store once, and lends the same instance again after a release', async () => {
		const provider = new InMemoryStateStoreProvider()
		const create = vi.spyOn(provider, 'createKeyValueStore')
		const { directory: dir } = directory({ provider })

		const first = (await dir.lease(T0, counts, 0)) as KeyValueStore<string, string>
		const init = vi.spyOn(first, 'init')
		await first.put('k', 'kept')
		await dir.release(T0, 'counts', 0)
		const second = (await dir.lease(T0, counts, 0)) as KeyValueStore<string, string>

		expect(second).toBe(first)
		expect(await second.get('k')).toBe('kept')
		expect(create).toHaveBeenCalledTimes(1)
		expect(init).not.toHaveBeenCalled()
	})

	it('lends a store to one task at a time', async () => {
		const { directory: dir } = directory()
		await dir.lease(T0, counts, 0)
		await expect(dir.lease(taskId(0, 9), counts, 0)).rejects.toThrow(IllegalStateError)
		await expect(dir.lease(taskId(0, 9), counts, 0)).rejects.toThrow(/already leased by task 0_0/)
		// Another partition of the same store is another instance.
		await expect(dir.lease(T1, counts, 1)).resolves.toBeDefined()

		await dir.release(T0, 'counts', 0)
		await expect(dir.lease(taskId(0, 9), counts, 0)).resolves.toBeDefined()
	})

	it('rejects a release by a task that holds no lease', async () => {
		const { directory: dir } = directory()
		await dir.lease(T0, counts, 0)
		await expect(dir.release(T1, 'counts', 0)).rejects.toThrow(IllegalStateError)
		await expect(dir.release(T1, 'counts', 1)).rejects.toThrow(IllegalStateError)
	})

	it('W19: a failed init() is not cached; the next lease retries it', async () => {
		const provider = new InMemoryStateStoreProvider()
		const { directory: dir } = directory({ provider })
		let fail = true
		const original = provider.createKeyValueStore.bind(provider)
		vi.spyOn(provider, 'createKeyValueStore').mockImplementation((name, options) => {
			const store = original(name, options)
			const init = store.init.bind(store)
			store.init = () => (fail ? Promise.reject(new Error('cannot open')) : init())
			return store
		})

		await expect(dir.lease(T0, counts, 0)).rejects.toThrow('cannot open')
		fail = false
		// The failed lease was not kept either.
		await expect(dir.lease(T0, counts, 0)).resolves.toBeDefined()
	})

	it('release with wipe invalidates the checkpoint, then clears the store', async () => {
		const durable = checkpointStore()
		const checkpoints = new Checkpoints(durable, () => {})
		const { directory: dir } = directory({ checkpoints })
		const store = (await dir.lease(T0, counts, 0)) as KeyValueStore<string, string>
		await store.put('k', 'v')
		await checkpoints.set('app-counts-changelog', 0, 10n)
		let entriesWhenInvalidated: number | undefined
		const set = durable.set
		durable.set = async (topic, partition, offset) => {
			if (offset === -1n) entriesWhenInvalidated = await store.approximateNumEntries()
			return set(topic, partition, offset)
		}

		await dir.release(T0, 'counts', 0, { wipe: true })

		expect(entriesWhenInvalidated).toBe(1)
		expect(durable.values.get('app-counts-changelog:0')).toBe(-1n)
		expect(await store.approximateNumEntries()).toBe(0)
		await expect(dir.lease(T1, counts, 0)).resolves.toBe(store)
	})

	it('release with wipe of a store without changelog only clears it', async () => {
		const { directory: dir, checkpoints } = directory()
		const invalidate = vi.spyOn(checkpoints, 'invalidate')
		const store = (await dir.lease(T0, unlogged, 0)) as KeyValueStore<string, string>
		await store.put('k', 'v')
		await dir.release(T0, 'scratch', 0, { wipe: true })
		expect(invalidate).not.toHaveBeenCalled()
		expect(await store.approximateNumEntries()).toBe(0)
	})

	it('a release whose wipe fails still ends the lease, and rejects', async () => {
		const { directory: dir, checkpoints } = directory()
		vi.spyOn(checkpoints, 'invalidate').mockRejectedValue(new Error('disk full'))
		const store = (await dir.lease(T0, counts, 0)) as KeyValueStore<string, string>
		await store.put('k', 'v')
		await expect(dir.release(T0, 'counts', 0, { wipe: true })).rejects.toThrow('disk full')
		// Never cleared without an invalidated checkpoint.
		expect(await store.get('k')).toBe('v')
		await expect(dir.lease(T1, counts, 0)).resolves.toBe(store)
	})

	describe('unowned in-memory state', () => {
		it('unleased() lists the instances no task holds, only for in-process checkpoints', async () => {
			const { directory: dir } = directory()
			await dir.lease(T0, counts, 0)
			await dir.lease(T1, counts, 1)
			await dir.release(T1, 'counts', 1)
			expect(dir.unleased()).toEqual([{ store: 'counts', partition: 1 }])

			const durable = directory({ checkpoints: new Checkpoints(checkpointStore(), () => {}) }).directory
			await durable.lease(T0, counts, 0)
			await durable.release(T0, 'counts', 0)
			expect(durable.unleased()).toEqual([])
		})

		it('prune() invalidates the checkpoint first, clears and forgets the instance', async () => {
			const { directory: dir, checkpoints } = directory()
			const store = (await dir.lease(T0, counts, 0)) as KeyValueStore<string, string>
			await store.put('k', 'v')
			await checkpoints.set('app-counts-changelog', 0, 10n)
			await dir.release(T0, 'counts', 0)

			await dir.prune('counts', 0, new AbortController().signal)

			expect(await checkpoints.get('app-counts-changelog', 0)).toBe(-1n)
			expect(await store.approximateNumEntries()).toBe(0)
			expect(dir.unleased()).toEqual([])
			const next = (await dir.lease(T0, counts, 0)) as KeyValueStore<string, string>
			expect(next).not.toBe(store)
			expect(await next.get('k')).toBeUndefined()
		})

		it('prune() leaves an instance that a task leased meanwhile untouched', async () => {
			const { directory: dir, checkpoints } = directory()
			const store = (await dir.lease(T0, counts, 0)) as KeyValueStore<string, string>
			await store.put('k', 'v')
			await checkpoints.set('app-counts-changelog', 0, 10n)
			await dir.release(T0, 'counts', 0)
			await dir.lease(T1, counts, 0)

			await dir.prune('counts', 0, new AbortController().signal)

			expect(await store.get('k')).toBe('v')
			expect(await checkpoints.get('app-counts-changelog', 0)).toBe(10n)
		})

		it('CR6: a lease during a slow prune gets a cleared store with an invalidated checkpoint', async () => {
			const provider = new InMemoryStateStoreProvider()
			const { directory: dir, checkpoints } = directory({ provider })
			const store = (await dir.lease(T0, counts, 0)) as KeyValueStore<string, string>
			for (let i = 0; i < 10; i++) await store.put(`k${i}`, 'v')
			await checkpoints.set('app-counts-changelog', 0, 10n)
			await dir.release(T0, 'counts', 0)
			let unblock!: () => void
			const blocked = new Promise<void>(resolve => (unblock = resolve))
			const remove = store.delete.bind(store)
			vi.spyOn(store, 'delete').mockImplementation(async key => {
				await blocked
				await remove(key)
			})

			const pruned = dir.prune('counts', 0, new AbortController().signal)
			await new Promise(resolve => setImmediate(resolve))
			let leased: KeyValueStore<string, string> | undefined
			const lease = dir.lease(T0, counts, 0).then(s => (leased = s as KeyValueStore<string, string>))
			await new Promise(resolve => setImmediate(resolve))
			expect(leased).toBeUndefined()

			unblock()
			await pruned
			await lease
			expect(await checkpoints.get('app-counts-changelog', 0)).toBe(-1n)
			expect(await leased!.approximateNumEntries()).toBe(0)
			expect(leased).not.toBe(store)
		})

		it('prune() with an aborted signal touches nothing', async () => {
			const { directory: dir, checkpoints } = directory()
			const store = (await dir.lease(T0, counts, 0)) as KeyValueStore<string, string>
			await store.put('k', 'v')
			await dir.release(T0, 'counts', 0)
			const controller = new AbortController()
			controller.abort()
			await expect(dir.prune('counts', 0, controller.signal)).rejects.toThrow()
			expect(await store.get('k')).toBe('v')
			expect(await checkpoints.get('app-counts-changelog', 0)).toBeUndefined()
		})
	})

	it('globalStore() creates <applicationId>.<store> once and initializes it once', async () => {
		const provider = new InMemoryStateStoreProvider()
		const create = vi.spyOn(provider, 'createKeyValueStore')
		const { directory: dir } = directory({ provider })
		const builder = timestampedKeyValueStoreBuilder(DEFAULTS, {
			name: 'rates',
			keyCodec: str,
			valueCodec: str,
			logging: false,
		})

		const store = await dir.globalStore(builder)
		expect(store.name).toBe(`${APP}.rates`)
		expect(await dir.globalStore(builder)).toBe(store)
		expect(create).toHaveBeenCalledTimes(1)
	})

	it('close() forgets every instance and lease and closes the provider', async () => {
		const provider = new InMemoryStateStoreProvider()
		const close = vi.spyOn(provider, 'close')
		const { directory: dir } = directory({ provider })
		const store = (await dir.lease(T0, counts, 0)) as KeyValueStore<string, string>

		await dir.close()

		expect(close).toHaveBeenCalledTimes(1)
		const next = await dir.lease(T1, counts, 0)
		expect(next).not.toBe(store)
	})

	it('close() logs a provider that fails to close instead of throwing', async () => {
		const provider = new InMemoryStateStoreProvider()
		vi.spyOn(provider, 'close').mockRejectedValue(new Error('busy'))
		const logger = Object.assign(Object.create(noopLogger) as Logger, { error: vi.fn() })
		const dir = new StateDirectory(APP, provider, new Checkpoints(undefined, () => {}), logger)
		await expect(dir.close()).resolves.toBeUndefined()
		expect(logger.error).toHaveBeenCalledWith('closing the state store provider failed', { error: 'busy' })
	})
})
