/**
 * Changelog restoration, checkpoint handling, at-least-once failure behaviour and app lifecycle
 * (start/close/restart) against a real broker.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { KafkaClient, type Producer } from '@kafkats/client'
import {
	ChangelogRestorer,
	InMemoryKeyValueStore,
	InMemoryStateStoreProvider,
	buildChangelogTopicName,
	codec,
	flow,
	type ChangelogCheckpointStore,
	type KeyValueStore,
	type KeyValueStoreOptions,
} from '../../src/index.js'
import { MultiTopicCollector, createTopics, requireKafkaBrokers, uniqueTopicName } from './test-helpers.js'

const stringCodec = codec.string()
const numberCodec = {
	encode: (v: number) => {
		const buf = Buffer.alloc(8)
		buf.writeDoubleLE(v, 0)
		return buf
	},
	decode: (b: Buffer) => b.readDoubleLE(0),
}

let client: KafkaClient
let producer: Producer

beforeAll(async () => {
	client = new KafkaClient({ clientId: `restore-${Date.now()}`, brokers: requireKafkaBrokers() })
	await client.connect()
	producer = client.producer()
})

afterAll(async () => {
	await producer.disconnect()
	await client.disconnect()
})

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

async function withTimeout<T>(label: string, promise: Promise<T>, timeoutMs: number): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined
	try {
		return await Promise.race([
			promise,
			new Promise<T>((_r, reject) => {
				timer = setTimeout(() => reject(new Error(`Timed out after ${timeoutMs}ms: ${label}`)), timeoutMs)
			}),
		])
	} finally {
		if (timer) clearTimeout(timer)
	}
}

async function waitUntil(label: string, cond: () => boolean, timeoutMs = 10_000): Promise<void> {
	const start = Date.now()
	while (Date.now() - start < timeoutMs) {
		if (cond()) return
		await sleep(50)
	}
	throw new Error(`Timed out waiting for: ${label}`)
}

class MemoryCheckpointStore implements ChangelogCheckpointStore {
	readonly offsets = new Map<string, bigint>()
	async get(topic: string, partition: number) {
		return this.offsets.get(`${topic}:${partition}`)
	}
	async set(topic: string, partition: number, offset: bigint) {
		this.offsets.set(`${topic}:${partition}`, offset)
	}
}

/** Wraps a KV store so that put() can be made to block once (simulates a slow/blocked store). */
function slowStore<K, V>(inner: KeyValueStore<K, V>, hooks: { armed: boolean; delayMs: number }): KeyValueStore<K, V> {
	return new Proxy(inner, {
		get(target, prop) {
			if (prop === 'put') {
				return async (k: K, v: V) => {
					if (hooks.armed) {
						hooks.armed = false
						await sleep(hooks.delayMs)
					}
					return target.put(k, v)
				}
			}
			const value = Reflect.get(target, prop) as unknown
			return typeof value === 'function' ? (value as (...a: unknown[]) => unknown).bind(target) : value
		},
	})
}

/** In-memory provider whose first store put blocks once when armed; exposes the per-partition inner stores. */
class SlowProvider extends InMemoryStateStoreProvider {
	readonly hooks = { armed: false, delayMs: 3_000 }
	readonly inners = new Map<string, KeyValueStore<unknown, unknown>>()
	constructor(private readonly checkpoints?: ChangelogCheckpointStore) {
		super()
	}
	getChangelogCheckpointStore() {
		return this.checkpoints
	}
	override createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>): KeyValueStore<K, V> {
		const inner = super.createKeyValueStore(name, options)
		this.inners.set(name, inner as KeyValueStore<unknown, unknown>)
		return slowStore(inner, this.hooks)
	}
}

/** Provider that behaves like a persistent one: stores (`<name>.p<N>`) + checkpoints survive close() and re-creation. */
class PersistentProvider extends InMemoryStateStoreProvider {
	readonly checkpoints = new MemoryCheckpointStore()
	readonly kvStores = new Map<string, KeyValueStore<unknown, unknown>>()
	getChangelogCheckpointStore() {
		return this.checkpoints
	}
	override createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>): KeyValueStore<K, V> {
		let s = this.kvStores.get(name)
		if (!s) {
			s = new InMemoryKeyValueStore<unknown, unknown>(name, options as KeyValueStoreOptions<unknown, unknown>)
			this.kvStores.set(name, s)
		}
		return s as KeyValueStore<K, V>
	}
	override close(): Promise<void> {
		return Promise.resolve()
	}
}

const RESTORE_FAST = {
	idleTimeoutMs: 1_000,
	initialIdleTimeoutMs: 10_000,
	checkIntervalMs: 100,
	consumerMaxWaitMs: 100,
}

async function newKvStore(name: string) {
	const store = new InMemoryKeyValueStore<string, number>(name, { keyCodec: stringCodec, valueCodec: numberCodec })
	await store.init()
	return store
}

describe('changelog restorer', () => {
	it('a store put blocked longer than idleTimeoutMs is not a stall: restore completes with full state and end checkpoint', async () => {
		const topic = uniqueTopicName('restore-slow-put')
		await createTopics(client, [{ name: topic }])
		for (let i = 1; i <= 5; i++) {
			await producer.send(topic, { key: `k${i}`, value: numberCodec.encode(i) })
		}

		const inner = await newKvStore('slow-put')
		const slow = slowStore(inner, { armed: true, delayMs: 3_000 })
		const checkpoints = new MemoryCheckpointStore()

		const restorer = new ChangelogRestorer(topic, stringCodec, numberCodec, slow)
		await restorer.restore(client, RESTORE_FAST, [0], checkpoints)

		for (let i = 1; i <= 5; i++) expect(await inner.get(`k${i}`)).toBe(i)
		expect(await checkpoints.get(topic, 0)).toBe(5n)
	}, 30_000)

	it('flow start() with a slow store put during restore comes up with the full restored state', async () => {
		const input = uniqueTopicName('restore-slow-start-in')
		const appId = `restore-slow-start-${Date.now()}`
		const changelog = buildChangelogTopicName(appId, 'counts')
		await createTopics(client, [{ name: input }, { name: changelog }])
		for (let i = 1; i <= 5; i++) {
			await producer.send(changelog, { key: `k${i}`, value: numberCodec.encode(i) })
		}

		const provider = new SlowProvider(new MemoryCheckpointStore())
		provider.hooks.armed = true
		provider.hooks.delayMs = 3_000
		const app = flow({
			applicationId: appId,
			client,
			numStreamThreads: 1,
			consumer: { maxWaitMs: 100 },
			stateStoreProvider: provider,
			changelog: { restoration: RESTORE_FAST },
		})
		app.stream(input, { key: stringCodec, value: numberCodec })
			.groupByKey()
			.count({ storeName: 'counts', key: stringCodec, value: numberCodec })

		try {
			await withTimeout('start', app.start(), 25_000)
			const inner = [...provider.inners].find(([name]) => name.endsWith('.counts.p0'))![1]
			for (let i = 1; i <= 5; i++) expect(await inner.get(`k${i}`)).toBe(i)
		} finally {
			await app.close().catch(() => {})
		}
	}, 40_000)

	it('restoring a transactional (EOS-written) changelog does not wait for the idle timeout', async () => {
		const topic = uniqueTopicName('restore-txn')
		await createTopics(client, [{ name: topic }])
		const tx = client.producer({ transactionalId: uniqueTopicName('restore-txn-tx'), idempotent: true })
		try {
			for (let i = 1; i <= 3; i++) {
				await tx.transaction(async t => {
					await t.send(topic, { key: `k${i}`, value: numberCodec.encode(i) })
				})
			}
		} finally {
			await tx.disconnect()
		}
		const store = await newKvStore('txn')
		const restorer = new ChangelogRestorer(topic, stringCodec, numberCodec, store)
		const t0 = Date.now()
		await restorer.restore(client, { consumerMaxWaitMs: 100 }, [0])
		const elapsed = Date.now() - t0
		expect(await store.get('k3')).toBe(3)
		expect(elapsed).toBeLessThan(3_000)
	}, 30_000)

	it('startup time does not scale with idle timeouts per store when changelogs are transactional', async () => {
		const storeCount = 4
		const input = uniqueTopicName('restore-txn-start-in')
		const appId = `restore-txn-start-${Date.now()}`
		await createTopics(client, [
			{ name: input },
			...Array.from({ length: storeCount }, (_, i) => ({ name: buildChangelogTopicName(appId, `s${i}`) })),
		])
		const tx = client.producer({ transactionalId: uniqueTopicName('restore-txn-start-tx'), idempotent: true })
		try {
			for (let s = 0; s < storeCount; s++) {
				await tx.transaction(async t => {
					await t.send(buildChangelogTopicName(appId, `s${s}`), { key: 'a', value: numberCodec.encode(1) })
				})
			}
		} finally {
			await tx.disconnect()
		}
		const app = flow({
			applicationId: appId,
			client,
			numStreamThreads: 1,
			consumer: { maxWaitMs: 100 },
			changelog: { restoration: { consumerMaxWaitMs: 100 } },
		})
		const base = app.stream(input, { key: stringCodec, value: numberCodec })
		for (let s = 0; s < storeCount; s++) {
			base.groupByKey().count({ storeName: `s${s}`, key: stringCodec, value: numberCodec })
		}
		const t0 = Date.now()
		try {
			await withTimeout('start', app.start(), 55_000)
			// Restoring ~1 record per store should not take seconds per store.
			expect(Date.now() - t0).toBeLessThan(storeCount * 1_000)
		} finally {
			await app.close().catch(() => {})
		}
	}, 60_000)

	it('a checkpoint beyond the changelog end offset (recreated topic) is not trusted: store is rebuilt', async () => {
		const topic = uniqueTopicName('restore-cp-beyond')
		await createTopics(client, [{ name: topic }])
		await producer.send(topic, { key: 'a', value: numberCodec.encode(1) })
		await producer.send(topic, { key: 'b', value: numberCodec.encode(2) })
		await producer.send(topic, { key: 'c', value: numberCodec.encode(3) })

		const store = await newKvStore('cp-beyond')
		await store.put('a', 1) // non-empty so only the range check can reject the checkpoint
		const checkpoints = new MemoryCheckpointStore()
		await checkpoints.set(topic, 0, 100n) // stale checkpoint from the previous incarnation of the topic

		const restorer = new ChangelogRestorer(topic, stringCodec, numberCodec, store)
		await restorer.restore(client, RESTORE_FAST, [0], checkpoints)
		expect([await store.get('a'), await store.get('b'), await store.get('c')]).toEqual([1, 2, 3])
		expect(await checkpoints.get(topic, 0)).toBe(3n)
	}, 30_000)

	it('a valid checkpoint with an empty store is not trusted: store is rebuilt from the beginning', async () => {
		const topic = uniqueTopicName('restore-cp-empty')
		await createTopics(client, [{ name: topic }])
		await producer.send(topic, { key: 'a', value: numberCodec.encode(1) })
		await producer.send(topic, { key: 'b', value: numberCodec.encode(2) })
		await producer.send(topic, { key: 'c', value: numberCodec.encode(3) })

		const store = await newKvStore('cp-empty')
		const checkpoints = new MemoryCheckpointStore()
		await checkpoints.set(topic, 0, 2n)

		const restorer = new ChangelogRestorer(topic, stringCodec, numberCodec, store)
		await restorer.restore(client, RESTORE_FAST, [0], checkpoints)
		expect([await store.get('a'), await store.get('b'), await store.get('c')]).toEqual([1, 2, 3])
	}, 30_000)

	it('a valid checkpoint with a non-empty store resumes from the checkpoint instead of replaying', async () => {
		const topic = uniqueTopicName('restore-cp-resume')
		await createTopics(client, [{ name: topic }])
		await producer.send(topic, { key: 'a', value: numberCodec.encode(1) })
		await producer.send(topic, { key: 'b', value: numberCodec.encode(2) })
		await producer.send(topic, { key: 'c', value: numberCodec.encode(3) })

		const store = await newKvStore('cp-resume')
		await store.put('a', 100) // differs from the changelog: proves offset 0 is not replayed
		const checkpoints = new MemoryCheckpointStore()
		await checkpoints.set(topic, 0, 2n)

		const restorer = new ChangelogRestorer(topic, stringCodec, numberCodec, store)
		await restorer.restore(client, RESTORE_FAST, [0], checkpoints)
		expect(await store.get('a')).toBe(100)
		expect(await store.get('b')).toBeUndefined()
		expect(await store.get('c')).toBe(3)
		expect(await checkpoints.get(topic, 0)).toBe(3n)
	}, 30_000)

	it('with no checkpoint, stale local keys that are absent from the changelog are removed', async () => {
		const topic = uniqueTopicName('restore-ghost')
		await createTopics(client, [{ name: topic }])
		await producer.send(topic, { key: 'a', value: numberCodec.encode(1) })

		const store = await newKvStore('ghost')
		await store.put('ghost', 99) // local persistent state whose tombstone has been compacted away
		const restorer = new ChangelogRestorer(topic, stringCodec, numberCodec, store)
		await restorer.restore(client, RESTORE_FAST, [0], new MemoryCheckpointStore())
		expect(await store.get('a')).toBe(1)
		expect(await store.get('ghost')).toBeUndefined()
	}, 30_000)
})

describe('restart cycles with a persistent store and checkpoint store', () => {
	it.each(['at_least_once', 'exactly_once'] as const)(
		'counts continue exactly across restarts (%s)',
		async guarantee => {
			const input = uniqueTopicName(`restore-cycles-${guarantee}-in`)
			const out = uniqueTopicName(`restore-cycles-${guarantee}-out`)
			const appId = `restore-cycles-${guarantee}-${Date.now()}`
			await createTopics(client, [{ name: input }, { name: out }])
			const provider = new PersistentProvider()

			const collector = new MultiTopicCollector({
				client,
				groupId: `${appId}-collector`,
				topics: [{ topic: out, keyCodec: stringCodec, valueCodec: numberCodec }],
			})
			await collector.start()

			try {
				for (let cycle = 1; cycle <= 3; cycle++) {
					const app = flow({
						applicationId: appId,
						client,
						numStreamThreads: 1,
						processingGuarantee: guarantee,
						commitIntervalMs: 100,
						consumer: { maxWaitMs: 100 },
						stateStoreProvider: provider,
						changelog: {
							restoration: {
								idleTimeoutMs: 2_000,
								initialIdleTimeoutMs: 2_000,
								checkIntervalMs: 200,
								consumerMaxWaitMs: 100,
							},
						},
					})
					app.stream(input, { key: stringCodec, value: numberCodec })
						.groupByKey()
						.count({ storeName: 'counts', key: stringCodec, value: numberCodec })
						.toStream()
						.to(out, { key: stringCodec, value: numberCodec })
					await withTimeout(`start cycle ${cycle}`, app.start(), 30_000)
					for (let i = 0; i < 3; i++) await producer.send(input, { key: 'a', value: numberCodec.encode(1) })
					await collector.waitFor<string, number>(out, m => m.key === 'a' && m.value === cycle * 3, 15_000)
					await withTimeout(`close cycle ${cycle}`, app.close(), 20_000)
				}
			} finally {
				await collector.stop()
			}
			const seen = collector.getTopicMessages<string, number>(out).map(m => m.value)
			const uniq = [...new Set(seen)]
			expect(uniq).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9])
			if (guarantee === 'exactly_once') expect(seen).toEqual(uniq)
		},
		120_000
	)
})

describe('processing failures', () => {
	it('a throwing user function puts the app in ERROR and does not lose the failed record', async () => {
		const input = uniqueTopicName('restore-throw-in')
		const out = uniqueTopicName('restore-throw-out')
		const appId = `restore-throw-${Date.now()}`
		await createTopics(client, [{ name: input }, { name: out }])
		const collector = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector`,
			topics: [{ topic: out, keyCodec: stringCodec, valueCodec: numberCodec }],
		})
		await collector.start()
		const build = (throwOn: number | null) => {
			const app = flow({ applicationId: appId, client, numStreamThreads: 1, consumer: { maxWaitMs: 100 } })
			app.stream(input, { key: stringCodec, value: numberCodec })
				.mapValues(v => {
					if (v === throwOn) throw new Error('boom')
					return v
				})
				.to(out, { key: stringCodec, value: numberCodec })
			return app
		}
		const app1 = build(2)
		try {
			await app1.start()
			for (const v of [1, 2, 3]) await producer.send(input, { key: 'a', value: numberCodec.encode(v) })
			await collector.waitFor<string, number>(out, m => m.value === 1, 10_000)
			await waitUntil('app1 ERROR', () => app1.state() === 'ERROR', 10_000)
			expect(app1.getError()?.message).toContain('boom')
			await app1.close()
			const app2 = build(null)
			await app2.start()
			await collector.waitFor<string, number>(out, m => m.value === 2, 15_000)
			await collector.waitFor<string, number>(out, m => m.value === 3, 15_000)
			await app2.close()
		} finally {
			await app1.close().catch(() => {})
			await collector.stop()
		}
	}, 60_000)

	it('a sink send that fails (record too large) does not commit the input record', async () => {
		const input = uniqueTopicName('restore-sinkfail-in')
		const out = uniqueTopicName('restore-sinkfail-out')
		const appId = `restore-sinkfail-${Date.now()}`
		await createTopics(client, [{ name: input }, { name: out }])
		const collector = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector`,
			topics: [{ topic: out, keyCodec: stringCodec, valueCodec: numberCodec }],
		})
		await collector.start()
		const build = (huge: boolean) => {
			const app = flow({
				applicationId: appId,
				client,
				numStreamThreads: 1,
				consumer: { maxWaitMs: 100 },
				runEach: { autoCommitIntervalMs: 200 },
			})
			app.stream(input, { key: stringCodec, value: numberCodec })
				.mapValues(v => v)
				.to(out, {
					key: stringCodec,
					value: {
						encode: (v: number) => (huge && v === 2 ? Buffer.alloc(3_000_000) : numberCodec.encode(v)),
						decode: numberCodec.decode,
					},
				})
			return app
		}
		const app1 = build(true)
		try {
			await app1.start()
			for (const v of [1, 2, 3]) await producer.send(input, { key: 'a', value: numberCodec.encode(v) })
			await collector.waitFor<string, number>(out, m => m.value === 1, 10_000)
			await sleep(3_000) // app1 keeps failing on record 2 (ERROR or restart loop); it must never commit past it
			await app1.close()
			const app2 = build(false)
			await app2.start()
			await collector.waitFor<string, number>(out, m => m.value === 2, 15_000)
			await collector.waitFor<string, number>(out, m => m.value === 3, 15_000)
			await app2.close()
		} finally {
			await app1.close().catch(() => {})
			await collector.stop()
		}
	}, 90_000)

	it('a failing changelog write is not silent: the app does not keep running as if it succeeded', async () => {
		const input = uniqueTopicName('restore-clfail-in')
		const out = uniqueTopicName('restore-clfail-out')
		const appId = `restore-clfail-${Date.now()}`
		await createTopics(client, [{ name: input }, { name: out }])
		const collector = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector`,
			topics: [{ topic: out, keyCodec: stringCodec, valueCodec: numberCodec }],
		})
		await collector.start()
		const provider = new PersistentProvider()
		const app = flow({
			applicationId: appId,
			client,
			numStreamThreads: 1,
			consumer: { maxWaitMs: 100 },
			stateStoreProvider: provider,
		})
		app.stream(input, { key: stringCodec, value: numberCodec })
			.groupByKey()
			.count({
				storeName: 'counts',
				key: stringCodec,
				value: {
					encode: (v: number) => (v === 3 ? Buffer.alloc(3_000_000) : numberCodec.encode(v)),
					decode: numberCodec.decode,
				},
			})
			.toStream()
			.to(out, { key: stringCodec, value: numberCodec })
		try {
			await app.start()
			for (let i = 0; i < 4; i++) await producer.send(input, { key: 'a', value: numberCodec.encode(1) })
			await collector.waitFor<string, number>(out, m => m.value === 2, 10_000)
			await sleep(3_000)
			// the record whose changelog write fails (count 3) must never be emitted downstream
			const seen = collector.getTopicMessages<string, number>(out).map(m => m.value)
			expect(seen).not.toContain(3)
			expect(seen).not.toContain(4)
		} finally {
			await app.close().catch(() => {})
			await collector.stop()
		}
	}, 60_000)
})

function socketCount(): number {
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const handles = (process as any)._getActiveHandles() as Array<{ constructor?: { name?: string } }>
	return handles.filter(h => h?.constructor?.name === 'Socket' && (h as { destroyed?: boolean }).destroyed !== true)
		.length
}

describe('app lifecycle', () => {
	it('concurrent start() calls: the second does not resolve before the first finished starting', async () => {
		const input = uniqueTopicName('restore-start2-in')
		await createTopics(client, [{ name: input }])
		const app = flow({ applicationId: `restore-start2-${Date.now()}`, client, consumer: { maxWaitMs: 100 } })
		app.stream(input, { key: stringCodec, value: numberCodec }).mapValues(v => v)
		try {
			let firstDone = false
			const p1 = app.start().then(() => {
				firstDone = true
			})
			await app.start()
			const firstDoneWhenSecondResolved = firstDone
			await p1
			expect(firstDoneWhenSecondResolved).toBe(true)
		} finally {
			await app.close().catch(() => {})
		}
	}, 30_000)

	it('a failed start() (missing source topic) is not RUNNING, is retryable and does not leak sockets', async () => {
		const missing = uniqueTopicName('restore-missing')
		const app = flow({
			applicationId: `restore-missing-${Date.now()}`,
			client: { brokers: requireKafkaBrokers(), clientId: 'restore-missing' }, // app owns its client
			consumer: { maxWaitMs: 100 },
		})
		app.stream(missing, { key: stringCodec, value: numberCodec })
			.groupByKey()
			.count({ storeName: 'counts', key: stringCodec, value: numberCodec })
		const socketsBefore = socketCount()
		try {
			await expect(app.start()).rejects.toThrow(/does not exist/)
			await sleep(500)
			expect(socketCount()).toBeLessThanOrEqual(socketsBefore)
			expect(app.state()).not.toBe('RUNNING')
			// A retry must run the startup again (and fail again), not be a silent no-op.
			await expect(app.start()).rejects.toThrow(/does not exist/)
		} finally {
			await app.close().catch(() => {})
		}
	}, 30_000)

	it('close() before start(), then close() twice concurrently, resolve cleanly', async () => {
		const input = uniqueTopicName('restore-close-in')
		await createTopics(client, [{ name: input }])
		const mk = () => {
			const app = flow({
				applicationId: `restore-close-${Date.now()}`,
				client: { brokers: requireKafkaBrokers(), clientId: 'restore-close' },
				consumer: { maxWaitMs: 100 },
			})
			app.stream(input, { key: stringCodec, value: numberCodec }).mapValues(v => v)
			return app
		}
		const a = mk()
		await withTimeout('close before start', a.close(), 10_000)
		expect(a.state()).toBe('STOPPED')

		const b = mk()
		await b.start()
		const results = await withTimeout('double close', Promise.allSettled([b.close(), b.close()]), 20_000)
		expect(results.map(r => r.status)).toEqual(['fulfilled', 'fulfilled'])
		expect(b.state()).toBe('STOPPED')
	}, 60_000)

	it('start() after close() on the same app instance processes data again with restored state', async () => {
		const input = uniqueTopicName('restore-reopen-in')
		const out = uniqueTopicName('restore-reopen-out')
		const appId = `restore-reopen-${Date.now()}`
		await createTopics(client, [{ name: input }, { name: out }])
		const collector = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector`,
			topics: [{ topic: out, keyCodec: stringCodec, valueCodec: numberCodec }],
		})
		await collector.start()
		const app = flow({ applicationId: appId, client, numStreamThreads: 1, consumer: { maxWaitMs: 100 } })
		app.stream(input, { key: stringCodec, value: numberCodec })
			.groupByKey()
			.count({ storeName: 'counts', key: stringCodec, value: numberCodec })
			.toStream()
			.to(out, { key: stringCodec, value: numberCodec })
		try {
			await app.start()
			await producer.send(input, { key: 'a', value: numberCodec.encode(1) })
			await collector.waitFor<string, number>(out, m => m.value === 1, 10_000)
			await app.close()
			await app.start()
			await producer.send(input, { key: 'a', value: numberCodec.encode(1) })
			await collector.waitFor<string, number>(out, m => m.value === 2, 15_000)
		} finally {
			await app.close().catch(() => {})
			await collector.stop()
		}
	}, 60_000)

	it('close() during changelog restoration settles, leaves STOPPED, and start() does not report success', async () => {
		const input = uniqueTopicName('restore-close-restoring-in')
		const appId = `restore-close-restoring-${Date.now()}`
		const changelog = buildChangelogTopicName(appId, 'counts')
		await createTopics(client, [{ name: input }, { name: changelog }])
		for (let i = 1; i <= 3; i++) await producer.send(changelog, { key: `k${i}`, value: numberCodec.encode(i) })
		const provider = new SlowProvider()
		provider.hooks.armed = true
		provider.hooks.delayMs = 4_000
		const app = flow({
			applicationId: appId,
			client: { brokers: requireKafkaBrokers(), clientId: 'restore-close-restoring' },
			numStreamThreads: 1,
			consumer: { maxWaitMs: 100 },
			stateStoreProvider: provider,
			changelog: { restoration: { idleTimeoutMs: 30_000, initialIdleTimeoutMs: 30_000, consumerMaxWaitMs: 100 } },
		})
		app.stream(input, { key: stringCodec, value: numberCodec })
			.groupByKey()
			.count({ storeName: 'counts', key: stringCodec, value: numberCodec })
		const socketsBefore = socketCount()
		const startP = app.start().then(
			() => 'resolved',
			(e: Error) => `rejected: ${e.message}`
		)
		await sleep(2_000)
		await withTimeout('close during restore', app.close(), 25_000)
		const startResult = await withTimeout('start settles', startP, 25_000)
		await sleep(500)
		expect(app.state()).toBe('STOPPED')
		expect(socketCount()).toBeLessThanOrEqual(socketsBefore)
		expect(startResult).not.toBe('resolved')
	}, 60_000)

	it('a fatal consumer error (fenced static member) puts the app in ERROR and close() still works', async () => {
		const input = uniqueTopicName('restore-fenced-in')
		const appId = `restore-fenced-${Date.now()}`
		await createTopics(client, [{ name: input }])
		const mk = () => {
			const app = flow({
				applicationId: appId,
				client,
				numStreamThreads: 1,
				consumer: { maxWaitMs: 100, groupInstanceId: 'restore-fenced-static' },
			})
			app.stream(input, { key: stringCodec, value: numberCodec }).mapValues(v => v)
			return app
		}
		const app1 = mk()
		const app2 = mk()
		try {
			await app1.start()
			await app2.start()
			await waitUntil('app1 fenced -> ERROR', () => app1.state() === 'ERROR', 20_000)
			await withTimeout('app1.close', app1.close(), 20_000)
			expect(app1.state()).toBe('STOPPED')
		} finally {
			await app1.close().catch(() => {})
			await app2.close().catch(() => {})
		}
	}, 60_000)

	it('an app with zero sources starts and closes cleanly', async () => {
		const app = flow({
			applicationId: `restore-nosources-${Date.now()}`,
			client: { brokers: requireKafkaBrokers(), clientId: 'restore-nosources' },
		})
		await withTimeout('start', app.start(), 10_000)
		await withTimeout('close', app.close(), 10_000)
		expect(app.state()).toBe('STOPPED')
	}, 30_000)

	it('a transient (non-fatal) consumer error event does not leave state() ERROR while processing continues', async () => {
		const input = uniqueTopicName('restore-transient-in')
		const out = uniqueTopicName('restore-transient-out')
		const appId = `restore-transient-${Date.now()}`
		await createTopics(client, [{ name: input }, { name: out }])
		const collector = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector`,
			topics: [{ topic: out, keyCodec: stringCodec, valueCodec: numberCodec }],
		})
		await collector.start()
		const app = flow({ applicationId: appId, client, numStreamThreads: 1, consumer: { maxWaitMs: 100 } })
		app.stream(input, { key: stringCodec, value: numberCodec })
			.mapValues(v => v)
			.to(out, { key: stringCodec, value: numberCodec })
		try {
			await app.start()
			// The client emits 'error' for non-fatal conditions too (e.g. auto-commit failures).
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			;(app as any).workers.active[0].context.consumer.emit('error', new Error('transient commit failure'))
			await producer.send(input, { key: 'a', value: numberCodec.encode(7) })
			await collector.waitFor<string, number>(out, m => m.value === 7, 10_000) // still processing
			await sleep(300)
			expect(app.state()).not.toBe('ERROR')
		} finally {
			await app.close().catch(() => {})
			await collector.stop()
		}
	}, 30_000)

	it('start() failing while workers are starting (consumer cannot join) cleans up its sockets', async () => {
		const input = uniqueTopicName('restore-joinfail-in')
		await createTopics(client, [{ name: input }])
		const app = flow({
			applicationId: `restore-joinfail-${Date.now()}`,
			client: { brokers: requireKafkaBrokers(), clientId: 'restore-joinfail' },
			numStreamThreads: 1,
			// Invalid session timeout (below broker group.min.session.timeout.ms) => join fails
			consumer: { maxWaitMs: 100, sessionTimeoutMs: 1 },
		})
		app.stream(input, { key: stringCodec, value: numberCodec }).mapValues(v => v)
		const socketsBefore = socketCount()
		try {
			await expect(withTimeout('start', app.start(), 30_000)).rejects.toThrow()
			await sleep(1_000)
			expect(app.state()).not.toBe('RUNNING')
			expect(socketCount()).toBeLessThanOrEqual(socketsBefore)
		} finally {
			await app.close().catch(() => {})
		}
	}, 60_000)
})

describe('changelog topic creation', () => {
	it('two instances starting concurrently both start even though both try to create the changelog topic', async () => {
		const failures: string[] = []
		for (let attempt = 0; attempt < 4; attempt++) {
			const input = uniqueTopicName('restore-race-in')
			const appId = `restore-race-${Date.now()}-${attempt}`
			await createTopics(client, [{ name: input, partitions: 2 }])
			const mk = () => {
				const app = flow({
					applicationId: appId,
					client: { brokers: requireKafkaBrokers(), clientId: 'restore-race' },
					numStreamThreads: 1,
					consumer: { maxWaitMs: 100 },
				})
				app.stream(input, { key: stringCodec, value: numberCodec })
					.groupByKey()
					.count({
						storeName: 'counts',
						key: stringCodec,
						value: numberCodec,
						changelog: { skipRestoration: true },
					})
				return app
			}
			const a = mk()
			const b = mk()
			const res = await withTimeout(
				`concurrent start attempt ${attempt}`,
				Promise.allSettled([a.start(), b.start()]),
				20_000
			).catch(e => [{ status: 'rejected', reason: e as Error } as PromiseRejectedResult])
			for (const r of res) if (r.status === 'rejected') failures.push(String((r.reason as Error).message))
			await withTimeout('close both', Promise.allSettled([a.close(), b.close()]), 15_000).catch(e =>
				failures.push(String((e as Error).message))
			)
		}
		expect(failures).toEqual([])
	}, 90_000)
})
