import { describe, expect, it, vi } from 'vitest'
import { OffsetOutOfRangeError } from '@kafkats/client'
import { ChangelogDecodeError } from '@/changelog/errors.js'
import { restoreChangelogs, restoreTarget, type RestoreTarget } from '@/changelog/restorer.js'
import { windowedKeyCodec } from '@/changelog/writer.js'
import { codec } from '@/codec.js'
import { classify } from '@/runtime/errors.js'
import type { RestoreConsumerPort } from '@/runtime/io.js'
import type { ChangelogCheckpointStore, KeyValueStore, WindowedKey } from '@/state.js'
import { Checkpoints } from '@/state/checkpoints.js'
import { InMemoryKeyValueStore, inMemory } from '@/state/memory.js'
import { deferred, FakeAdmin, FakeCluster, FakeProducer, FakeRestoreConsumer } from '../support/fake-io.js'
import { RecordingLogger } from '../support/task-harness.js'

const str = codec.string()
const windowed = windowedKeyCodec(str)

/** A durable checkpoint store that records every write and flush ('set c:0=5', 'flush'). */
class RecordingCheckpointStore implements ChangelogCheckpointStore {
	readonly values = new Map<string, bigint>()
	readonly events: string[] = []
	failSet: Error | undefined

	get(topic: string, partition: number): Promise<bigint | undefined> {
		return Promise.resolve(this.values.get(`${topic}:${partition}`))
	}

	set(topic: string, partition: number, offset: bigint): Promise<void> {
		this.events.push(`set ${topic}:${partition}=${offset}`)
		if (this.failSet) return Promise.reject(this.failSet)
		this.values.set(`${topic}:${partition}`, offset)
		return Promise.resolve()
	}

	flush(): Promise<void> {
		this.events.push('flush')
		return Promise.resolve()
	}
}

/** The fake cluster, an admin over it and restore consumers that read it; real Checkpoints over a recording store. */
class Harness {
	readonly cluster = new FakeCluster()
	readonly admin = new FakeAdmin(this.cluster)
	readonly consumers: FakeRestoreConsumer[] = []
	readonly logger = new RecordingLogger()
	readonly checkpointStore = new RecordingCheckpointStore()
	readonly checkpoints = new Checkpoints(this.checkpointStore, error => this.logger.error(error.message))
	readonly abort = new AbortController()
	batchSize = 100
	/** Replaces the restore consumers when set. */
	consumer: (() => RestoreConsumerPort) | undefined

	/** Appends records to a changelog partition (created with one partition on first use). */
	write(topic: string, partition: number, ...records: [key: string | Buffer, value: string | null][]): void {
		const created = this.cluster.createTopic(topic, partition + 1)
		created.grow(partition + 1)
		const log = created.partitions[partition]!
		for (const [key, value] of records) {
			const keyBytes = typeof key === 'string' ? Buffer.from(key) : key
			log.append({
				key: keyBytes,
				value: (value === null ? null : Buffer.from(value)) as Buffer,
				timestamp: 0n,
				headers: {},
			})
		}
	}

	restore(
		targets: readonly RestoreTarget[],
		options: { commitIntervalMs?: number; idleTimeoutMs?: number; signal?: AbortSignal } = {}
	) {
		const idleTimeoutMs = options.idleTimeoutMs ?? 2000
		return restoreChangelogs(
			{
				consumer: () => {
					if (this.consumer) return this.consumer()
					const consumer = new FakeRestoreConsumer(this.cluster, 'restore')
					consumer.batchSize = this.batchSize
					this.consumers.push(consumer)
					return consumer
				},
				admin: this.admin,
			},
			targets,
			this.checkpoints,
			{
				idleTimeoutMs,
				initialIdleTimeoutMs: idleTimeoutMs,
				checkIntervalMs: 10,
				commitIntervalMs: options.commitIntervalMs ?? Number.POSITIVE_INFINITY,
				signal: options.signal ?? this.abort.signal,
				logger: this.logger,
			}
		)
	}
}

async function kvStore(name = 's'): Promise<InMemoryKeyValueStore<string, string>> {
	const store = new InMemoryKeyValueStore<string, string>(name, { keyCodec: str, valueCodec: str })
	await store.init()
	return store
}

function kv(store: KeyValueStore<string, string>, topic = 'c', partition = 0): RestoreTarget {
	return restoreTarget({ topic, partition }, store, { key: str, value: str })
}

async function entries<K, V>(store: KeyValueStore<K, V>): Promise<string[]> {
	const all: string[] = []
	for await (const [key, value] of store.all()) all.push(`${JSON.stringify(key)}=${JSON.stringify(value)}`)
	return all.sort()
}

/** `store` whose put number `at` (from 1) waits until released. */
function blockPut<K, V>(store: KeyValueStore<K, V>, at: number) {
	const blocked = deferred()
	const release = deferred()
	let puts = 0
	const wrapped = new Proxy(store, {
		get(target, property) {
			if (property === 'put') {
				return async (key: K, value: V) => {
					if (++puts === at) {
						blocked.resolve()
						await release.promise
					}
					await target.put(key, value)
				}
			}
			const value: unknown = Reflect.get(target, property, target)
			return typeof value === 'function' ? (value as (...args: unknown[]) => unknown).bind(target) : value
		},
	})
	return { store: wrapped, blocked: blocked.promise, release: () => release.resolve() }
}

const records = (n: number, prefix = 'k'): [string, string | null][] =>
	Array.from({ length: n }, (_, i) => [`${prefix}${i % 7}`, i % 5 === 4 ? null : `v${i}`])

describe('restoreChangelogs: replay (4.7.2)', () => {
	it('replays every target with one consumer up to the end, applies the last change per key, checkpoints the end', async () => {
		const h = new Harness()
		h.write('a', 0, ['x', '1'], ['y', '1'], ['x', null], ['y', '2'], ['z', '3'])
		h.write('b', 1, ['q', '1'])
		const sa = await kvStore('a')
		const sb = await kvStore('b')

		const result = await h.restore([kv(sa, 'a', 0), kv(sb, 'b', 1)])

		expect(await entries(sa)).toEqual(['"y"="2"', '"z"="3"'])
		expect(await entries(sb)).toEqual(['"q"="1"'])
		expect(result).toEqual({
			restored: 5,
			rebuilt: ['a:0', 'b:1'],
			replayed: ['a:0', 'b:1'],
			maxKeyTime: new Map(),
		})
		expect(h.consumers).toHaveLength(1)
		expect(h.consumers[0]!.calls.slice(0, 2)).toEqual(['assign a:0@0 b:1@0', 'run'])
		expect(h.checkpointStore.values).toEqual(
			new Map([
				['a:0', 5n],
				['b:1', 1n],
			])
		)
	})

	it('reports the max key time of window (start) and session (end) targets', async () => {
		const h = new Harness()
		const provider = inMemory()
		const window = provider.createWindowStore<string, string>('w', {
			keyCodec: str,
			valueCodec: str,
			retentionMs: 1000,
			windowSizeMs: 10,
		})
		const session = provider.createSessionStore<string, string>('s', {
			keyCodec: str,
			valueCodec: str,
			retentionMs: 1000,
		})
		const key = (k: string, windowStart: number, windowEnd: number) =>
			windowed.encode({ key: k, windowStart, windowEnd })
		h.write('w', 0, [key('a', 20, 30), 'x'], [key('a', 10, 20), 'y'], [key('b', 30, 40), null])
		h.write('s', 0, [key('a', 5, 50), 'x'], [key('b', 7, 9), 'y'])
		const codecs = { key: windowed, value: str }

		const result = await h.restore([
			restoreTarget({ topic: 'w', partition: 0 }, window, codecs, (k: WindowedKey<string>) => k.windowStart),
			restoreTarget({ topic: 's', partition: 0 }, session, codecs, (k: WindowedKey<string>) => k.windowEnd),
		])

		expect(result.maxKeyTime).toEqual(
			new Map([
				['w:0', 30],
				['s:0', 50],
			])
		)
		expect(await entries(window)).toEqual([
			'{"key":"a","windowStart":10,"windowEnd":20}="y"',
			'{"key":"a","windowStart":20,"windowEnd":30}="x"',
		])
		expect(await entries(session)).toHaveLength(2)
	})

	it('does not apply records written after the restore started (end = high watermark at the start)', async () => {
		const h = new Harness()
		h.batchSize = 1
		h.write('c', 0, ['a', '1'], ['b', '1'])
		const blocked = blockPut(await kvStore(), 1)

		const restoring = h.restore([kv(blocked.store)])
		await blocked.blocked
		h.write('c', 0, ['a', '2'], ['c', '2'])
		blocked.release()
		await restoring

		expect(await entries(blocked.store)).toEqual(['"a"="1"', '"b"="1"'])
		expect(h.checkpointStore.values.get('c:0')).toBe(2n)
	})

	it('a missing changelog topic has nothing to restore: no consumer, the store untouched (runtime§6.39)', async () => {
		const h = new Harness()
		const store = await kvStore()
		await store.put('local', '1')

		const result = await h.restore([kv(store, 'missing')])

		expect(result).toEqual({ restored: 0, rebuilt: [], replayed: [], maxKeyTime: new Map() })
		expect(await entries(store)).toEqual(['"local"="1"'])
		expect(h.consumers).toEqual([])
		expect(h.checkpointStore.events).toEqual([])
	})

	it('creates no consumer when every target is current (a resumed task, KF3)', async () => {
		const h = new Harness()
		h.write('c', 0, ['a', '1'], ['b', '2'])
		const store = await kvStore()
		await store.put('a', '1')
		await h.checkpoints.set('c', 0, 2n)
		h.checkpointStore.events.length = 0

		const result = await h.restore([kv(store)])

		expect(result).toEqual({ restored: 0, rebuilt: [], replayed: [], maxKeyTime: new Map() })
		expect(h.consumers).toEqual([])
		expect(h.checkpointStore.events).toEqual([])
	})

	it('completes a transactional changelog whose end is past a commit marker without waiting for the idle timeout', async () => {
		const h = new Harness()
		h.write('c', 0, ['a', '1'])
		const producer = new FakeProducer(h.cluster)
		const signal = new AbortController().signal
		const aborted = await producer.beginTransaction({ signal })
		await aborted.send('c', { key: Buffer.from('a'), value: Buffer.from('aborted'), partition: 0 })
		await aborted.abort({ signal })
		const committed = await producer.beginTransaction({ signal })
		await committed.send('c', { key: Buffer.from('b'), value: Buffer.from('2'), partition: 0 })
		await committed.commit({ signal })
		const store = await kvStore()
		// The second fetch reads only the commit marker: the end is reached without a batch.
		h.batchSize = 2

		const started = Date.now()
		const result = await h.restore([kv(store)], { idleTimeoutMs: 60_000 })

		expect(Date.now() - started).toBeLessThan(1000)
		expect(await entries(store)).toEqual(['"a"="1"', '"b"="2"'])
		expect(result.restored).toBe(2)
		expect(h.checkpointStore.values.get('c:0')).toBe(5n)
	})

	it('waits out an open transaction holding the last stable offset (warns once), then restores what committed after it', async () => {
		const h = new Harness()
		h.write('c', 0, ['a', '1'])
		const producer = new FakeProducer(h.cluster)
		const signal = new AbortController().signal
		const dangling = await producer.beginTransaction({ signal })
		await dangling.send('c', { key: Buffer.from('a'), value: Buffer.from('uncommitted'), partition: 0 })
		h.write('c', 0, ['a', '2'], ['b', '1'])
		const store = await kvStore()

		const restoring = h.restore([kv(store)], { idleTimeoutMs: 30 })
		await new Promise(resolve => setTimeout(resolve, 200))
		await dangling.abort({ signal })
		await restoring

		expect(await entries(store)).toEqual(['"a"="2"', '"b"="1"'])
		expect(h.logger.warnings).toEqual(['a changelog restore is waiting for an open transaction to complete'])
	})

	it('a stall check right after an open transaction ended gives the fetch of the released records an idle timeout', async () => {
		const h = new Harness()
		h.write('c', 0, ['a', '1'])
		const producer = new FakeProducer(h.cluster)
		const signal = new AbortController().signal
		const dangling = await producer.beginTransaction({ signal })
		await dangling.send('c', { key: Buffer.from('a'), value: Buffer.from('uncommitted'), partition: 0 })
		h.write('c', 0, ['b', '1'])
		const log = h.cluster.log('c', 0)
		// A consumer whose fetch of what the abort released is still in flight at the next stall check (the real one
		// long-polls at the last stable offset): it delivers only when the test says so.
		let position = 0n
		let deliver!: Parameters<RestoreConsumerPort['run']>[0]
		const running = deferred()
		h.consumer = () => ({
			assign: () => {},
			run: onBatch => {
				deliver = onBatch
				running.resolve()
				return new Promise<void>(() => {})
			},
			pause: () => {},
			position: () => position,
			stop: () => Promise.resolve(),
		})
		const releasedCheck = deferred()
		h.admin.steps.listOffsets.always(() => {
			const stallCheck = h.admin.calls.at(-1)?.startsWith('listOffsets latest read_committed')
			if (stallCheck && log.lastStable === log.end) releasedCheck.resolve()
			return 'ok'
		})
		const store = await kvStore()

		const restoring = h.restore([kv(store)], { idleTimeoutMs: 30 })
		await running.promise
		await deliver({ topic: 'c', partition: 0 }, log.read(0n, 'read_committed').messages)
		position = 1n
		await vi.waitFor(() => expect(h.logger.warnings).toHaveLength(1), { interval: 1 })
		await dangling.abort({ signal })
		// The first stall check that sees the transaction ended, then the fetch completes.
		await releasedCheck.promise
		await new Promise(resolve => setTimeout(resolve, 0))
		await deliver({ topic: 'c', partition: 0 }, log.read(1n, 'read_committed').messages)
		position = log.end

		await restoring
		expect(await entries(store)).toEqual(['"a"="1"', '"b"="1"'])
	})

	it('a store write blocked longer than the idle timeout is not a stall', async () => {
		const h = new Harness()
		h.write('c', 0, ['a', '1'], ['b', '2'])
		const blocked = blockPut(await kvStore(), 1)

		const restoring = h.restore([kv(blocked.store)], { idleTimeoutMs: 30 })
		await blocked.blocked
		await new Promise(resolve => setTimeout(resolve, 150))
		blocked.release()
		await restoring

		expect(await entries(blocked.store)).toEqual(['"a"="1"', '"b"="2"'])
	})

	it('fails after the idle timeout without progress, naming the pending partitions', async () => {
		const h = new Harness()
		h.write('c', 0, ['a', '1'])
		const stopped: string[] = []
		h.consumer = () => ({
			assign: () => {},
			run: () => new Promise<void>(() => {}),
			pause: () => {},
			position: () => 0n,
			stop: () => {
				stopped.push('stop')
				return Promise.resolve()
			},
		})

		const error: unknown = await h.restore([kv(await kvStore())], { idleTimeoutMs: 30 }).catch((e: unknown) => e)

		expect(error).toEqual(new Error('Timed out restoring changelog partitions c:0: no progress for 30ms'))
		expect(classify(error, 'restore')).toBe('migrate')
		expect(stopped).toEqual(['stop'])
	})

	it('rejects with the failure of the consumer run', async () => {
		const h = new Harness()
		h.write('c', 0, ['a', '1'])
		const failure = new Error('fetch failed')
		h.consumer = () => {
			const consumer = new FakeRestoreConsumer(h.cluster, 'restore')
			consumer.fail(failure)
			return consumer
		}
		await expect(h.restore([kv(await kvStore())])).rejects.toBe(failure)
	})

	it('a record that does not decode is a ChangelogDecodeError, which is fatal', async () => {
		const h = new Harness()
		h.write('c', 0, ['a', '1'])
		const failing = { encode: str.encode, decode: () => JSON.parse('{') as string }
		const target = restoreTarget({ topic: 'c', partition: 0 }, await kvStore(), { key: str, value: failing })

		const error: unknown = await h.restore([target]).catch((e: unknown) => e)

		expect(error).toBeInstanceOf(ChangelogDecodeError)
		expect((error as Error).message).toContain('"c" partition 0')
		expect(classify(error, 'restore')).toBe('fatal')
	})

	it('an out-of-range position rejects with OffsetOutOfRangeError (C7)', async () => {
		const h = new Harness()
		h.write('c', 0, ...records(10))
		const store = await kvStore()
		await store.put('k0', 'local')
		await h.checkpoints.set('c', 0, 3n)
		h.consumer = () => {
			// Retention deletes what the replay still needs once the restore planned it.
			h.cluster.log('c', 0).deleteBefore(6n)
			return new FakeRestoreConsumer(h.cluster, 'restore')
		}

		const error: unknown = await h.restore([kv(store)]).catch((e: unknown) => e)

		expect(error).toBeInstanceOf(OffsetOutOfRangeError)
		expect(error).toMatchObject({ topic: 'c', partition: 0, offset: 3n })
	})
})

describe('restoreChangelogs: checkpoint trust and invalidation (4.7.2 step 3, CR3)', () => {
	async function trusted(
		checkpoint: bigint | undefined,
		options: { storeEntries?: number; deleteBefore?: bigint } = {}
	): Promise<{ rebuilt: boolean; events: string[]; entries: string[] }> {
		const h = new Harness()
		h.write('c', 0, ['a', '1'], ['b', '2'], ['c', '3'])
		if (options.deleteBefore !== undefined) h.cluster.log('c', 0).deleteBefore(options.deleteBefore)
		const store = await kvStore()
		for (let i = 0; i < (options.storeEntries ?? 1); i++) await store.put(`local${i}`, 'x')
		if (checkpoint !== undefined) h.checkpointStore.values.set('c:0', checkpoint)
		const result = await h.restore([kv(store)])
		return {
			rebuilt: result.rebuilt.includes('c:0'),
			events: h.checkpointStore.events,
			entries: await entries(store),
		}
	}

	it.each([
		['no checkpoint', undefined, {}, true],
		['the invalidation sentinel', -1n, {}, true],
		['a checkpoint beyond the end (a recreated, shorter changelog)', 100n, {}, true],
		['a checkpoint before the log start', 1n, { deleteBefore: 2n }, true],
		['a checkpoint past the start on an empty store', 2n, { storeEntries: 0 }, true],
		['a checkpoint at the start on an empty store', 0n, { storeEntries: 0 }, false],
		['a checkpoint within the log on a non-empty store', 2n, {}, false],
		['a checkpoint at the end on a non-empty store', 3n, {}, false],
	] as const)('%s', async (_, checkpoint, options, rebuilt) => {
		const result = await trusted(checkpoint, options)
		expect(result.rebuilt).toBe(rebuilt)
		if (rebuilt) {
			// Invalidated (and flushed) before the store is wiped, then the end once caught up.
			expect(result.events).toEqual(['set c:0=-1', 'flush', 'set c:0=3', 'flush'])
			expect(result.entries).not.toContain('"local0"="x"')
		} else {
			expect(result.events.filter(e => e.endsWith('=-1'))).toEqual([])
		}
	})

	it('resumes a trusted checkpoint: records before it are not replayed', async () => {
		const h = new Harness()
		h.write('c', 0, ['a', '1'], ['b', '2'], ['c', '3'])
		const store = await kvStore()
		await store.put('a', 'local')
		h.checkpointStore.values.set('c:0', 2n)

		const result = await h.restore([kv(store)])

		expect(await entries(store)).toEqual(['"a"="local"', '"c"="3"'])
		expect(result).toMatchObject({ restored: 1, rebuilt: [], replayed: ['c:0'] })
		expect(h.consumers[0]!.calls[0]).toBe('assign c:0@2')
		expect(h.checkpointStore.values.get('c:0')).toBe(3n)
	})

	it('targets sharing a store are rebuilt together when one of them is not trusted (wiped once)', async () => {
		const h = new Harness()
		h.write('c', 0, ['a', '1'])
		h.write('c', 1, ['b', '1'])
		const store = await kvStore()
		await store.put('stale', 'x')
		h.checkpointStore.values.set('c:0', 1n)

		const result = await h.restore([kv(store, 'c', 0), kv(store, 'c', 1)])

		expect(result.rebuilt).toEqual(['c:0', 'c:1'])
		expect(await entries(store)).toEqual(['"a"="1"', '"b"="1"'])
	})

	it('an invalidation that fails fails the restore before anything is wiped', async () => {
		const h = new Harness()
		h.write('c', 0, ['a', '1'])
		const store = await kvStore()
		await store.put('local', 'x')
		h.checkpointStore.failSet = new Error('disk full')

		await expect(h.restore([kv(store)])).rejects.toThrow('disk full')
		expect(await entries(store)).toEqual(['"local"="x"'])
		expect(h.consumers).toEqual([])
	})

	it('CR3: a truncated changelog (checkpoint beyond the end) is rebuilt, and its later growth past the old checkpoint is restored', async () => {
		const h = new Harness()
		// The previous incarnation of the changelog went up to 100: the checkpoints' monotonic floor is there.
		await h.checkpoints.set('c', 0, 100n)
		h.write('c', 0, ['a', '1'], ['b', '1'], ['c', '1'])
		const store = await kvStore()
		await store.put('stale', 'x')

		await h.restore([kv(store)])
		expect(h.checkpointStore.values.get('c:0')).toBe(3n)

		h.write('c', 0, ...records(150, 'later'))
		const grown = await h.restore([kv(store)])
		expect(grown).toMatchObject({ rebuilt: [], replayed: ['c:0'] })

		const full = await kvStore('full')
		const reference = new Harness()
		reference.write('c', 0, ['a', '1'], ['b', '1'], ['c', '1'], ...records(150, 'later'))
		await reference.restore([kv(full)])
		expect(await entries(store)).toEqual(await entries(full))
		expect(h.checkpointStore.values.get('c:0')).toBe(153n)
	})
})

describe('restoreChangelogs: progress checkpoints and abort (KF4, CR3)', () => {
	it('checkpoints the applied progress after batches, at most every commit interval, then the end', async () => {
		const h = new Harness()
		h.batchSize = 3
		h.write('c', 0, ...records(10))

		await h.restore([kv(await kvStore())], { commitIntervalMs: 0 })

		expect(h.checkpointStore.events).toEqual([
			'set c:0=-1',
			'flush',
			'set c:0=3',
			'flush',
			'set c:0=6',
			'flush',
			'set c:0=9',
			'flush',
			'set c:0=10',
			'flush',
			'set c:0=10',
			'flush',
		])
	})

	it('writes no progress checkpoint before the commit interval passed', async () => {
		const h = new Harness()
		h.batchSize = 3
		h.write('c', 0, ...records(10))

		await h.restore([kv(await kvStore())], { commitIntervalMs: 60_000 })

		expect(h.checkpointStore.events).toEqual(['set c:0=-1', 'flush', 'set c:0=10', 'flush'])
	})

	type Kind = 'key-value with deletes' | 'window' | 'session'
	const kinds: Kind[] = ['key-value with deletes', 'window', 'session']

	function storeOf(kind: Kind, name: string): KeyValueStore<unknown, unknown> {
		const provider = inMemory()
		const options = { keyCodec: str, valueCodec: str, retentionMs: 10_000 }
		const store =
			kind === 'key-value with deletes'
				? provider.createKeyValueStore<string, string>(name, options)
				: kind === 'window'
					? provider.createWindowStore<string, string>(name, { ...options, windowSizeMs: 10 })
					: provider.createSessionStore<string, string>(name, options)
		return store as KeyValueStore<unknown, unknown>
	}

	/** Window and session stores restore windowed keys. */
	function targetOf(kind: Kind, store: KeyValueStore<unknown, unknown>): RestoreTarget {
		const key = kind === 'key-value with deletes' ? str : windowed
		return restoreTarget({ topic: 'c', partition: 0 }, store, { key: key as typeof str, value: str })
	}

	function changelogOf(kind: Kind, h: Harness): void {
		for (let i = 0; i < 40; i++) {
			const value = i % 6 === 5 ? null : `v${i}`
			const key =
				kind === 'key-value with deletes'
					? `k${i % 9}`
					: windowed.encode({ key: `k${i % 4}`, windowStart: (i % 5) * 10, windowEnd: (i % 5) * 10 + 10 })
			h.write('c', 0, [key, value])
		}
	}

	it.each(kinds)(
		'%s: a rebuild aborted halfway rejects after its in-flight write, checkpoints its progress, and the next restore resumes to the state of a full restore',
		async kind => {
			const h = new Harness()
			h.batchSize = 4
			changelogOf(kind, h)
			const store = storeOf(kind, 'aborted')
			await store.init()
			await store.put(
				kind === 'key-value with deletes' ? 'stale' : { key: 'stale', windowStart: 0, windowEnd: 1 },
				'x'
			)
			const blocked = blockPut(store, 18)

			const restoring = h.restore([targetOf(kind, blocked.store)])
			await blocked.blocked
			h.abort.abort()
			let settled = false
			void restoring.catch(() => (settled = true))
			await new Promise(resolve => setTimeout(resolve, 20))
			expect(settled).toBe(false)
			blocked.release()
			await expect(restoring).rejects.toMatchObject({ name: 'AbortError' })

			const progress = h.checkpointStore.values.get('c:0')!
			expect(progress).toBeGreaterThan(0n)
			expect(progress).toBeLessThan(40n)
			expect(h.checkpointStore.events.at(-1)).toBe('flush')

			const resumed = await h.restore([targetOf(kind, store)], { signal: new AbortController().signal })
			expect(resumed.rebuilt).toEqual([])
			expect(h.consumers[1]!.calls[0]).toBe(`assign c:0@${progress}`)

			const full = new Harness()
			changelogOf(kind, full)
			const reference = storeOf(kind, 'full')
			await reference.init()
			await full.restore([targetOf(kind, reference)])
			expect(await entries(store)).toEqual(await entries(reference))
		}
	)

	it('an abort while waiting for records rejects at once with an AbortError and checkpoints the progress', async () => {
		const h = new Harness()
		h.write('c', 0, ['a', '1'])
		const producer = new FakeProducer(h.cluster)
		const dangling = await producer.beginTransaction({ signal: new AbortController().signal })
		await dangling.send('c', { key: Buffer.from('b'), value: Buffer.from('1'), partition: 0 })
		const store = await kvStore()

		const restoring = h.restore([kv(store)], { idleTimeoutMs: 60_000 })
		await waitFor(() => h.consumers[0]?.position({ topic: 'c', partition: 0 }) === 1n)
		await new Promise(resolve => setTimeout(resolve, 20))
		h.abort.abort()

		await expect(restoring).rejects.toMatchObject({ name: 'AbortError' })
		expect(h.checkpointStore.events).toEqual(['set c:0=-1', 'flush', 'set c:0=1', 'flush'])
		expect(h.consumers[0]!.calls).toContain('stop')
	})

	it('an abort before the restore started rejects without a consumer', async () => {
		const h = new Harness()
		h.write('c', 0, ['a', '1'])
		h.abort.abort()
		await expect(h.restore([kv(await kvStore())])).rejects.toMatchObject({ name: 'AbortError' })
		expect(h.consumers).toEqual([])
	})
})

async function waitFor(condition: () => boolean): Promise<void> {
	for (let i = 0; i < 200 && !condition(); i++) await new Promise(resolve => setTimeout(resolve, 5))
	expect(condition()).toBe(true)
}
