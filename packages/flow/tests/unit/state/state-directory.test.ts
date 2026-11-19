import { noopLogger, type Logger } from '@kafkats/client'
import { describe, expect, it, vi } from 'vitest'
import { IllegalStateError } from '@/api/errors.js'
import type { KeyValueStore } from '@/state.js'
import { Checkpoints } from '@/state/checkpoints.js'
import { InMemoryStateStoreProvider } from '@/state/memory.js'
import { StateDirectory, type StateCleanup } from '@/state/state-directory.js'
import { keyValueStoreBuilder, timestampedKeyValueStoreBuilder } from '@/state/store-builders.js'
import { APP, DEFAULTS, checkpointStore, directory, str, taskId } from './support.js'

const counts = keyValueStoreBuilder(DEFAULTS, { name: 'counts', keyCodec: str, valueCodec: str })
const unlogged = keyValueStoreBuilder(DEFAULTS, { name: 'scratch', keyCodec: str, valueCodec: str, logging: false })
const T0 = taskId(0, 0)
const T1 = taskId(0, 1)

const tick = () => new Promise(resolve => setImmediate(resolve))

async function filled(store: KeyValueStore<string, string>, entries: number): Promise<void> {
	await Promise.all(Array.from({ length: entries }, (_, i) => store.put(`k${i}`, 'v')))
}

/** Holds the first delete (the first round of a clear) until `release()`; `onDelete` sees every delete. */
function blockFirstDelete(store: KeyValueStore<string, string>, onDelete: () => void = () => {}) {
	let started!: () => void
	let release!: () => void
	const startedP = new Promise<void>(resolve => (started = resolve))
	const blocked = new Promise<void>(resolve => (release = resolve))
	const remove = store.delete.bind(store)
	let first = true
	vi.spyOn(store, 'delete').mockImplementation(async key => {
		onDelete()
		if (first) {
			first = false
			started()
			await blocked
		}
		await remove(key)
	})
	return { started: startedP, release }
}

/** A provider whose stores of the given names are the ones an earlier start left (a durable provider reopens them). */
function reopening(provider: InMemoryStateStoreProvider, stores: readonly KeyValueStore<string, string>[]) {
	const byName = new Map(stores.map(store => [store.name, store]))
	const create = provider.createKeyValueStore.bind(provider)
	provider.createKeyValueStore = ((name, options) =>
		byName.get(name) ?? create(name, options)) as typeof provider.createKeyValueStore
	return provider
}

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

	it("release with invalidate only invalidates the checkpoint: the next owner's restore clears the store", async () => {
		const durable = checkpointStore()
		const checkpoints = new Checkpoints(durable, () => {})
		const { directory: dir } = directory({ checkpoints })
		const store = (await dir.lease(T0, counts, 0)) as KeyValueStore<string, string>
		await store.put('k', 'v')
		await checkpoints.set('app-counts-changelog', 0, 10n)
		const remove = vi.spyOn(store, 'delete')
		const scan = vi.spyOn(store, 'all')

		await dir.release(T0, 'counts', 0, { invalidate: true })

		expect(durable.values.get('app-counts-changelog:0')).toBe(-1n)
		expect([remove.mock.calls, scan.mock.calls]).toEqual([[], []])
		await expect(dir.lease(T1, counts, 0)).resolves.toBe(store)
	})

	it('release with invalidate of a store without changelog keeps it as is', async () => {
		const { directory: dir, checkpoints } = directory()
		const invalidate = vi.spyOn(checkpoints, 'invalidate')
		const store = (await dir.lease(T0, unlogged, 0)) as KeyValueStore<string, string>
		await store.put('k', 'v')
		await dir.release(T0, 'scratch', 0, { invalidate: true })
		expect(invalidate).not.toHaveBeenCalled()
		expect(await store.get('k')).toBe('v')
	})

	it('a release whose invalidation fails still ends the lease, and rejects', async () => {
		const { directory: dir, checkpoints } = directory()
		vi.spyOn(checkpoints, 'invalidate').mockRejectedValue(new Error('disk full'))
		const store = (await dir.lease(T0, counts, 0)) as KeyValueStore<string, string>
		await store.put('k', 'v')
		await expect(dir.release(T0, 'counts', 0, { invalidate: true })).rejects.toThrow('disk full')
		expect(await store.get('k')).toBe('v')
		await expect(dir.lease(T1, counts, 0)).resolves.toBe(store)
	})

	it('released() resolves once no store is leased any more: released, failed to lease, or closed (4.10.2)', async () => {
		const { directory: dir } = directory()
		await expect(dir.released()).resolves.toBeUndefined()
		await dir.lease(T0, counts, 0)
		await dir.lease(T1, counts, 1)
		let released = false
		const all = dir.released().then(() => (released = true))
		await dir.release(T0, 'counts', 0)
		await Promise.resolve()
		expect(released).toBe(false)
		await dir.release(T1, 'counts', 1)
		await all

		await dir.lease(T0, counts, 0)
		const closing = dir.released()
		await dir.close()
		await expect(closing).resolves.toBeUndefined()
	})

	describe('unowned in-memory state', () => {
		it('unleased() lists the instances no task holds, prunable from their release', async () => {
			let now = 100
			const { directory: dir } = directory({ cleanup: { delayMs: 600_000, now: () => now } })
			await dir.lease(T0, counts, 0)
			await dir.lease(T1, counts, 1)
			now = 250
			await dir.release(T1, 'counts', 1)
			expect(dir.unleased()).toEqual([{ store: 'counts', partition: 1, prunableAt: 250 }])
			// Nothing of an earlier start is in memory.
			dir.register([{ builder: counts, partitions: 4 }])
			expect(dir.unleased()).toHaveLength(1)
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

		it('CR6: a lease during a prune stops its clear at the next round; finishClears() completes it', async () => {
			const { directory: dir, checkpoints } = directory()
			const store = (await dir.lease(T0, counts, 0)) as KeyValueStore<string, string>
			await filled(store, 25_000)
			await checkpoints.set('app-counts-changelog', 0, 10n)
			await dir.release(T0, 'counts', 0)
			const round = blockFirstDelete(store)

			const pruned = dir.prune('counts', 0, new AbortController().signal)
			await round.started
			let leased: KeyValueStore<string, string> | undefined
			const lease = dir.lease(T1, counts, 0).then(s => (leased = s as KeyValueStore<string, string>))
			await tick()
			// It waits for the round of deletes in flight only.
			expect(leased).toBeUndefined()
			round.release()
			await lease
			await pruned

			// The same, partly cleared instance: the checkpoint is invalid, a lease made it its owner's to finish.
			expect(leased).toBe(store)
			expect(await checkpoints.get('app-counts-changelog', 0)).toBe(-1n)
			expect(await store.approximateNumEntries()).toBe(15_000)
			await dir.finishClears(T0, new AbortController().signal)
			expect(await store.approximateNumEntries()).toBe(15_000)
			await dir.finishClears(T1, new AbortController().signal)
			expect(await store.approximateNumEntries()).toBe(0)
			// Finished: nothing more to clear.
			await store.put('k', 'v')
			await dir.finishClears(T1, new AbortController().signal)
			expect(await store.get('k')).toBe('v')
		})

		it('finishClears() rejects with an AbortError once its signal aborts; the store stays partly cleared', async () => {
			const { directory: dir } = directory()
			const store = (await dir.lease(T0, counts, 0)) as KeyValueStore<string, string>
			await filled(store, 25_000)
			await dir.release(T0, 'counts', 0)
			const round = blockFirstDelete(store)
			const pruned = dir.prune('counts', 0, new AbortController().signal)
			await round.started
			const lease = dir.lease(T1, counts, 0)
			round.release()
			await lease
			await pruned
			const signal = AbortSignal.abort()
			await expect(dir.finishClears(T1, signal)).rejects.toThrow(expect.objectContaining({ name: 'AbortError' }))
			await dir.release(T1, 'counts', 0)
			await dir.lease(T0, counts, 0)
			await dir.finishClears(T0, new AbortController().signal)
			expect(await store.approximateNumEntries()).toBe(0)
		})

		it('unleased() leaves out an instance being pruned: no timer waits for it again', async () => {
			const { directory: dir } = directory()
			const store = (await dir.lease(T0, counts, 0)) as KeyValueStore<string, string>
			await filled(store, 15_000)
			await dir.release(T0, 'counts', 0)
			const round = blockFirstDelete(store)
			const pruned = dir.prune('counts', 0, new AbortController().signal)
			await round.started
			expect(dir.unleased()).toEqual([])
			round.release()
			await pruned
			expect(dir.unleased()).toEqual([])
		})

		it('a failed prune leaves the instance partly cleared and unleased: pruned again, or cleared by its next owner', async () => {
			const { directory: dir } = directory()
			const store = (await dir.lease(T0, counts, 0)) as KeyValueStore<string, string>
			await filled(store, 15_000)
			await dir.release(T0, 'counts', 0)
			const remove = store.delete.bind(store)
			let failed = false
			vi.spyOn(store, 'delete').mockImplementation(async key => {
				if (!failed) {
					failed = true
					throw new Error('disk full')
				}
				await remove(key)
			})
			await expect(dir.prune('counts', 0, new AbortController().signal)).rejects.toThrow('disk full')
			expect(dir.unleased().map(u => `${u.store}:${u.partition}`)).toEqual(['counts:0'])
			await dir.lease(T1, counts, 0)
			await dir.finishClears(T1, new AbortController().signal)
			expect(await store.approximateNumEntries()).toBe(0)
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

	describe('unowned durable state (Kafka Streams: state.cleanup.delay.ms)', () => {
		function durable() {
			const clock = { now: 1000 }
			const cleanup: StateCleanup = { delayMs: 600_000, now: () => clock.now }
			const store = checkpointStore()
			const checkpoints = new Checkpoints(store, () => {})
			return { clock, store, ...directory({ checkpoints, cleanup }) }
		}

		it('an instance released at t is prunable at t + the cleanup delay; prune() invalidates, clears and forgets it', async () => {
			const d = durable()
			const store = (await d.directory.lease(T0, counts, 0)) as KeyValueStore<string, string>
			await store.put('k', 'v')
			await d.checkpoints.set('app-counts-changelog', 0, 10n)
			d.clock.now = 5000
			await d.directory.release(T0, 'counts', 0)
			expect(d.directory.unleased()).toEqual([{ store: 'counts', partition: 0, prunableAt: 605_000 }])

			await d.directory.prune('counts', 0, new AbortController().signal)

			expect(d.store.values.get('app-counts-changelog:0')).toBe(-1n)
			expect(await store.approximateNumEntries()).toBe(0)
			expect(d.directory.unleased()).toEqual([])
			// The next owner rebuilds it from the changelog: an old checkpoint is no longer trusted over a compacted log.
			const next = (await d.directory.lease(T0, counts, 0)) as KeyValueStore<string, string>
			expect(await next.get('k')).toBeUndefined()
		})

		it('register(): the partitions an earlier start left on disk count from now; their prune marks the clear and invalidates a trusted checkpoint, without opening the store', async () => {
			const d = durable()
			d.store.values.set('app-counts-changelog:1', 42n)
			d.store.values.set('app-counts-changelog:2', -1n)
			d.store.values.set('clearing app.counts:3', 1n)
			const create = vi.spyOn(d.provider, 'createKeyValueStore')
			await d.directory.lease(T0, counts, 0)
			d.clock.now = 2000
			d.directory.register([{ builder: counts, partitions: 4 }])
			// Partition 0 is leased: it counts from its release, like any other instance.
			expect(d.directory.unleased()).toEqual([
				{ store: 'counts', partition: 1, prunableAt: 602_000 },
				{ store: 'counts', partition: 2, prunableAt: 602_000 },
				{ store: 'counts', partition: 3, prunableAt: 602_000 },
			])

			const flushes = d.store.flushes
			for (const partition of [1, 2, 3])
				await d.directory.prune('counts', partition, new AbortController().signal)

			expect(
				[1, 2, 3].map(p => [
					d.store.flushed.get(`clearing app.counts:${p}`),
					d.store.flushed.get(`app-counts-changelog:${p}`),
				])
			).toEqual([
				[1n, -1n],
				[1n, -1n],
				[1n, undefined],
			])
			// Partition 1: the marker and the invalidation; 2: the marker only (already invalid); 3: neither (marked already).
			expect(d.store.flushes).toBe(flushes + 3)
			expect(create).toHaveBeenCalledTimes(1)
			expect(d.directory.unleased()).toEqual([])
		})

		it('register(): the state an earlier start left of a store nothing restores (unlogged, skipRestoration) is opened and cleared', async () => {
			const provider = new InMemoryStateStoreProvider()
			// What an earlier start left in the provider (an in-memory provider forgets nothing until it is closed).
			const left = async (name: string) => {
				const store = provider.createKeyValueStore<string, string>(name, { keyCodec: str, valueCodec: str })
				await store.put('old', 'v')
				return store
			}
			const scratch = await left('app.scratch.p1')
			const skipped = await left('app.skipped.p1')
			const skipping = keyValueStoreBuilder(DEFAULTS, {
				name: 'skipped',
				keyCodec: str,
				valueCodec: str,
				logging: { skipRestoration: true },
			})
			const clock = { now: 1000 }
			const store = checkpointStore()
			store.values.set('app-skipped-changelog:1', 42n)
			const { directory: dir } = directory({
				provider: reopening(provider, [scratch, skipped]),
				checkpoints: new Checkpoints(store, () => {}),
				cleanup: { delayMs: 600_000, now: () => clock.now },
			})
			dir.register([
				{ builder: unlogged, partitions: 2 },
				{ builder: skipping, partitions: 2 },
			])
			expect(dir.unleased()).toHaveLength(4)

			for (const name of ['scratch', 'skipped']) await dir.prune(name, 1, new AbortController().signal)

			expect(await scratch.get('old')).toBeUndefined()
			expect(await skipped.get('old')).toBeUndefined()
			expect(store.values.get('app-skipped-changelog:1')).toBe(-1n)
			expect(dir.unleased().map(u => `${u.store}:${u.partition}`)).toEqual(['scratch:0', 'skipped:0'])
		})

		it('a partition of an earlier start that a task leases is no longer pruned as such', async () => {
			const d = durable()
			d.store.values.set('app-counts-changelog:1', 42n)
			d.directory.register([{ builder: counts, partitions: 2 }])
			await d.directory.lease(T1, counts, 1)
			await d.directory.prune('counts', 1, new AbortController().signal)
			expect(d.store.values.get('app-counts-changelog:1')).toBe(42n)
			d.clock.now = 7000
			await d.directory.release(T1, 'counts', 1)
			expect(d.directory.unleased()).toEqual([
				{ store: 'counts', partition: 1, prunableAt: 607_000 },
				{ store: 'counts', partition: 0, prunableAt: 601_000 },
			])
		})

		describe('a clear or drop that the next start finishes', () => {
			/** One store partition, `app.scratch.p0`, under each changelog config a start may give it. */
			const configs = {
				unlogged,
				skipRestoration: keyValueStoreBuilder(DEFAULTS, {
					name: 'scratch',
					keyCodec: str,
					valueCodec: str,
					logging: { skipRestoration: true },
				}),
				restored: keyValueStoreBuilder(DEFAULTS, { name: 'scratch', keyCodec: str, valueCodec: str }),
			}
			type Config = keyof typeof configs
			const MARKER = 'clearing app.scratch:0'
			const CHECKPOINT = 'app-scratch-changelog:0'

			/** A durable provider over memory: close() keeps its stores, and the next start reopens them by name. */
			function durableProvider() {
				const provider = new InMemoryStateStoreProvider()
				vi.spyOn(provider, 'close').mockResolvedValue()
				const left = provider.createKeyValueStore<string, string>('app.scratch.p0', {
					keyCodec: str,
					valueCodec: str,
				})
				return { provider: reopening(provider, [left]), left }
			}
			const start = (
				provider: InMemoryStateStoreProvider,
				store: ReturnType<typeof checkpointStore>,
				config: Config
			) => {
				const clock = { now: 1000 }
				const checkpoints = new Checkpoints(store, () => {})
				const cleanup: StateCleanup = { delayMs: 600_000, now: () => clock.now }
				const { directory: dir } = directory({ provider, checkpoints, cleanup })
				dir.register([{ builder: configs[config], partitions: 1 }])
				return dir
			}
			/** A start with `config` whose prune of the store, released by its task, a close stops after 10k deletes. */
			async function stoppedPrune(config: Config) {
				const { provider, left } = durableProvider()
				const durable = checkpointStore()
				await filled(left, 25_000)
				const first = start(provider, durable, config)
				await first.lease(T0, configs[config], 0)
				await first.release(T0, 'scratch', 0)
				const markedAtDelete: (bigint | undefined)[] = []
				const round = blockFirstDelete(left, () => markedAtDelete.push(durable.flushed.get(MARKER)))
				const pruned = first.prune('scratch', 0, new AbortController().signal)
				await round.started
				const closed = first.close()
				round.release()
				await closed
				await pruned
				vi.mocked(left.delete).mockRestore()
				expect(await left.approximateNumEntries()).toBe(15_000)
				// Marked durably before the first delete: a crash at any delete leaves the marker.
				expect(markedAtDelete).toHaveLength(10_000)
				expect(new Set(markedAtDelete)).toEqual(new Set([1n]))
				return { provider, left, durable }
			}

			it.each(Object.keys(configs) as Config[])(
				"%s: is finished by the next start's owner before it uses the store",
				async config => {
					const { provider, left, durable } = await stoppedPrune(config)
					expect(durable.values.get(MARKER)).toBe(1n)
					expect(durable.values.get(CHECKPOINT)).toBe(config === 'unlogged' ? undefined : -1n)

					// The next start leases it at once, long before its cleanup delay.
					const second = start(provider, durable, config)
					expect(await second.lease(T0, configs[config], 0)).toBe(left)
					await second.finishClears(T0, new AbortController().signal)

					expect(await left.approximateNumEntries()).toBe(0)
					expect(durable.values.get(MARKER)).toBe(0n)
				}
			)

			it.each(Object.keys(configs) as Config[])(
				'%s: a finished clear is marked done: the next start keeps what was written since',
				async config => {
					const { provider, left } = durableProvider()
					const durable = checkpointStore()
					await filled(left, 10)
					const first = start(provider, durable, config)
					await first.prune('scratch', 0, new AbortController().signal)
					expect(await left.approximateNumEntries()).toBe(config === 'restored' ? 10 : 0)
					await first.lease(T0, configs[config], 0)
					await first.release(T0, 'scratch', 0)
					await first.prune('scratch', 0, new AbortController().signal)
					expect(await left.approximateNumEntries()).toBe(0)
					expect(durable.values.get(MARKER)).toBe(0n)
					await first.lease(T0, configs[config], 0)
					await left.put('k', 'new')
					await first.close()

					const second = start(provider, durable, config)
					await second.lease(T0, configs[config], 0)
					await second.finishClears(T0, new AbortController().signal)
					expect(await left.get('k')).toBe('new')
				}
			)

			// The marker, not the logging config of the start that reads it, says the store is partly cleared: a deploy
			// that stops a prune often changes the store's changelog settings too.
			it.each<[Config, Config]>([
				['restored', 'skipRestoration'],
				['restored', 'unlogged'],
				['skipRestoration', 'restored'],
				['unlogged', 'restored'],
			])('%s, then %s at the next start: the clear is finished', async (before, after) => {
				const { provider, left, durable } = await stoppedPrune(before)

				const second = start(provider, durable, after)
				await second.lease(T0, configs[after], 0)
				await second.finishClears(T0, new AbortController().signal)
				expect(await left.approximateNumEntries()).toBe(0)
				expect(durable.values.get(MARKER)).toBe(0n)
				// What the store holds afterwards is valid: a start that changes the config back keeps it.
				await left.put('valid', 'state')
				await second.close()

				const third = start(provider, durable, before)
				await third.lease(T0, configs[before], 0)
				await third.finishClears(T0, new AbortController().signal)
				expect(await left.get('valid')).toBe('state')
			})

			// The prune of a partition an earlier start left, which drops a restored store without opening it, records the
			// drop the same way: the next start clears it before its task uses it, also when it no longer restores it.
			it.each(Object.keys(configs) as Config[])(
				'restored, dropped unopened, then %s at the next start: the dropped state is cleared',
				async after => {
					const { provider, left } = durableProvider()
					const durable = checkpointStore()
					await left.put('k', 'stale')
					durable.values.set(CHECKPOINT, 5n)
					const first = start(provider, durable, 'restored')
					await first.prune('scratch', 0, new AbortController().signal)
					expect(await left.get('k')).toBe('stale')
					expect([durable.flushed.get(MARKER), durable.flushed.get(CHECKPOINT)]).toEqual([1n, -1n])
					await first.close()

					// Its task gets the partition at once, long before this start's cleanup delay.
					const second = start(provider, durable, after)
					await second.lease(T0, configs[after], 0)
					await second.finishClears(T0, new AbortController().signal)
					expect(await left.get('k')).toBeUndefined()
					expect(durable.values.get(MARKER)).toBe(0n)
				}
			)

			it("clear(): a restore's rebuild cut short is finished by the next start, whatever its config", async () => {
				const { provider, left } = durableProvider()
				const durable = checkpointStore()
				await filled(left, 25_000)
				const first = start(provider, durable, 'restored')
				await first.lease(T0, configs.restored, 0)
				// The restore invalidated the checkpoint before it clears the store for its rebuild.
				durable.values.set(CHECKPOINT, -1n)
				const markedAtDelete: (bigint | undefined)[] = []
				const round = blockFirstDelete(left, () => markedAtDelete.push(durable.flushed.get(MARKER)))
				const cleared = first.clear(T0, 'scratch', 0, new AbortController().signal)
				await round.started
				const closed = first.close()
				round.release()
				await closed
				await expect(cleared).rejects.toMatchObject({ name: 'AbortError' })
				vi.mocked(left.delete).mockRestore()
				expect(await left.approximateNumEntries()).toBe(15_000)
				expect(new Set(markedAtDelete)).toEqual(new Set([1n]))

				const second = start(provider, durable, 'skipRestoration')
				await second.lease(T0, configs.skipRestoration, 0)
				await second.finishClears(T0, new AbortController().signal)
				expect(await left.approximateNumEntries()).toBe(0)
				expect(durable.values.get(MARKER)).toBe(0n)
			})

			it('a clear with nothing to delete writes no marker (the rebuild of a new store)', async () => {
				const { provider } = durableProvider()
				const durable = checkpointStore()
				const dir = start(provider, durable, 'restored')
				await dir.lease(T0, configs.restored, 0)
				const flushes = durable.flushes
				await dir.clear(T0, 'scratch', 0, new AbortController().signal)
				expect(durable.values.has(MARKER)).toBe(false)
				expect(durable.flushes).toBe(flushes)
			})

			it('a marker left in progress over a store already empty (a crash after the last delete) is ended', async () => {
				const { provider, left } = durableProvider()
				const durable = checkpointStore()
				durable.values.set(MARKER, 1n)
				const dir = start(provider, durable, 'unlogged')
				await dir.lease(T0, configs.unlogged, 0)
				await dir.finishClears(T0, new AbortController().signal)
				expect(durable.values.get(MARKER)).toBe(0n)
				await left.put('valid', 'state')
				await dir.close()

				const next = start(provider, durable, 'unlogged')
				await next.lease(T0, configs.unlogged, 0)
				await next.finishClears(T0, new AbortController().signal)
				expect(await left.get('valid')).toBe('state')
			})

			it('clear() of a store its task does not lease is an IllegalStateError', async () => {
				const { provider } = durableProvider()
				const dir = start(provider, checkpointStore(), 'restored')
				await expect(dir.clear(T0, 'scratch', 0, new AbortController().signal)).rejects.toThrow(
					IllegalStateError
				)
			})
		})

		it('a lease during the prune of a partition of an earlier start waits until the drop is recorded', async () => {
			const d = durable()
			d.store.values.set('app-counts-changelog:0', 42n)
			d.directory.register([{ builder: counts, partitions: 1 }])
			let unblock!: () => void
			const blocked = new Promise<void>(resolve => (unblock = resolve))
			const set = d.store.set
			d.store.set = async (topic, partition, offset) => {
				await blocked
				await set(topic, partition, offset)
			}
			const pruned = d.directory.prune('counts', 0, new AbortController().signal)
			let leased = false
			const lease = d.directory.lease(T0, counts, 0).then(() => (leased = true))
			await new Promise(resolve => setImmediate(resolve))
			expect(leased).toBe(false)

			unblock()
			await pruned
			await lease
			expect(d.store.values.get('app-counts-changelog:0')).toBe(-1n)
			expect(d.store.values.get('clearing app.counts:0')).toBe(1n)
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

	it('close() stops a prune in progress and waits for it before it closes the provider', async () => {
		const provider = new InMemoryStateStoreProvider()
		const { directory: dir, checkpoints } = directory({ provider })
		const store = (await dir.lease(T0, counts, 0)) as KeyValueStore<string, string>
		await filled(store, 25_000)
		await checkpoints.set('app-counts-changelog', 0, 10n)
		await dir.release(T0, 'counts', 0)
		const events: string[] = []
		const round = blockFirstDelete(store, () => events.push('delete'))
		vi.spyOn(provider, 'close').mockImplementation(() => {
			events.push('provider.close')
			return Promise.resolve()
		})
		const pruned = dir.prune('counts', 0, new AbortController().signal)
		await round.started

		const closed = dir.close()
		await tick()
		expect(events).not.toContain('provider.close')
		round.release()
		await closed
		await pruned

		// One round of deletes, then nothing after the provider closed.
		expect(events.filter(e => e === 'delete')).toHaveLength(10_000)
		expect(events.at(-1)).toBe('provider.close')
		expect(await checkpoints.get('app-counts-changelog', 0)).toBe(-1n)
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
