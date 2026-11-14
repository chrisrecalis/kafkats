/**
 * Bug-hunt tests: changelog restoration, at-least-once delivery, app lifecycle.
 * Tests here assert the CORRECT behaviour; tests that FAIL on current code are confirmed bugs.
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
	client = new KafkaClient({ clientId: `hunt-restore-${Date.now()}`, brokers: requireKafkaBrokers() })
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

class HuntRestoreCheckpointStore implements ChangelogCheckpointStore {
	readonly offsets = new Map<string, bigint>()
	async get(topic: string, partition: number) {
		return this.offsets.get(`${topic}:${partition}`)
	}
	async set(topic: string, partition: number, offset: bigint) {
		this.offsets.set(`${topic}:${partition}`, offset)
	}
}

/** Wraps a KV store so that put() can be made to block once (simulates a slow/blocked store). */
function huntRestoreSlowStore<K, V>(
	inner: KeyValueStore<K, V>,
	hooks: { armed: boolean; delayMs: number }
): KeyValueStore<K, V> {
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

/** In-memory provider whose first store put blocks once when armed; exposes the inner stores. */
class HuntRestoreSlowProvider extends InMemoryStateStoreProvider {
	readonly hooks = { armed: false, delayMs: 3_000 }
	readonly inners = new Map<string, KeyValueStore<unknown, unknown>>()
	constructor(private readonly checkpoints?: ChangelogCheckpointStore) {
		super()
	}
	override getChangelogCheckpointStore() {
		return this.checkpoints
	}
	override createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>): KeyValueStore<K, V> {
		const inner = super.createKeyValueStore(name, options)
		this.inners.set(name, inner as KeyValueStore<unknown, unknown>)
		return huntRestoreSlowStore(inner, this.hooks)
	}
}

/** Provider that behaves like a persistent one: stores + checkpoints survive close() and re-creation. */
class HuntRestorePersistentProvider extends InMemoryStateStoreProvider {
	readonly checkpoints = new HuntRestoreCheckpointStore()
	readonly stores = new Map<string, KeyValueStore<unknown, unknown>>()
	override getChangelogCheckpointStore() {
		return this.checkpoints
	}
	override createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>): KeyValueStore<K, V> {
		let s = this.stores.get(name)
		if (!s) {
			s = new InMemoryKeyValueStore<unknown, unknown>(name, options as KeyValueStoreOptions<unknown, unknown>)
			this.stores.set(name, s)
		}
		return s as KeyValueStore<K, V>
	}
	override close(): Promise<void> {
		return Promise.resolve()
	}
}

const RESTORE_FAST = { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 10_000, checkIntervalMs: 100, consumerMaxWaitMs: 100 }

async function newKvStore(name: string) {
	const store = new InMemoryKeyValueStore<string, number>(name, { keyCodec: stringCodec, valueCodec: numberCodec })
	await store.init()
	return store
}

// ---------------------------------------------------------------------------------------------
// Theory 1: premature completion of restore on idle timeout
// ---------------------------------------------------------------------------------------------
describe('T1 restorer premature completion', () => {
	it('T1a: a blocked store put longer than idleTimeoutMs must not yield partial state + full checkpoint', async () => {
		const topic = uniqueTopicName('hunt-restore-t1a')
		await createTopics(client, [{ name: topic }])
		for (let i = 1; i <= 5; i++) {
			await producer.send(topic, { key: `k${i}`, value: numberCodec.encode(i) })
		}

		const inner = await newKvStore('t1a')
		const hooks = { armed: true, delayMs: 3_000 }
		const slow = huntRestoreSlowStore(inner, hooks)
		const checkpoints = new HuntRestoreCheckpointStore()

		const restorer = new ChangelogRestorer(topic, stringCodec, numberCodec, slow)
		let threw: unknown = null
		try {
			await restorer.restore(client, RESTORE_FAST, [0], checkpoints)
		} catch (e) {
			threw = e
		}

		const present: string[] = []
		for (let i = 1; i <= 5; i++) if ((await inner.get(`k${i}`)) === i) present.push(`k${i}`)
		const cp = await checkpoints.get(topic, 0)
		console.log(`[T1a] threw=${String(threw)} restoredKeys=${present.join(',')} checkpoint=${cp}`)

		// Correct behaviour: either everything is restored, or restore fails/does not checkpoint the end.
		if (threw === null) {
			expect.soft(present).toEqual(['k1', 'k2', 'k3', 'k4', 'k5'])
		} else {
			expect.soft(cp).toBeUndefined()
		}
	}, 30_000)

	it('T1b: flow start() with a slow store put during restore must not silently come up with partial state', async () => {
		const input = uniqueTopicName('hunt-restore-t1b-in')
		const appId = `hunt-restore-t1b-${Date.now()}`
		const changelog = buildChangelogTopicName(appId, 'counts')
		await createTopics(client, [{ name: input }, { name: changelog }])
		for (let i = 1; i <= 5; i++) {
			await producer.send(changelog, { key: `k${i}`, value: numberCodec.encode(i) })
		}

		const provider = new HuntRestoreSlowProvider(new HuntRestoreCheckpointStore())
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

		let startErr: unknown = null
		try {
			await withTimeout('start', app.start(), 25_000)
		} catch (e) {
			startErr = e
		}
		const inner = provider.inners.get('counts')!
		const present: string[] = []
		for (let i = 1; i <= 5; i++) if ((await inner.get(`k${i}`)) === i) present.push(`k${i}`)
		console.log(`[T1b] startErr=${String(startErr)} state=${app.state()} restoredKeys=${present.join(',')}`)
		await app.close().catch(() => {})

		if (startErr === null) {
			expect(present).toEqual(['k1', 'k2', 'k3', 'k4', 'k5'])
		}
	}, 40_000)

	it('T1c: restoring a transactional (EOS-written) changelog should not wait for the idle timeout', async () => {
		const topic = uniqueTopicName('hunt-restore-t1c')
		await createTopics(client, [{ name: topic }])
		const tx = client.producer({ transactionalId: uniqueTopicName('hunt-restore-t1c-tx'), idempotent: true })
		try {
			for (let i = 1; i <= 3; i++) {
				await tx.transaction(async t => {
					await t.send(topic, { key: `k${i}`, value: numberCodec.encode(i) })
				})
			}
		} finally {
			await tx.disconnect()
		}
		const store = await newKvStore('t1c')
		const restorer = new ChangelogRestorer(topic, stringCodec, numberCodec, store)
		const t0 = Date.now()
		// defaults: idleTimeoutMs 5000, checkIntervalMs 1000
		await restorer.restore(client, { consumerMaxWaitMs: 100 }, [0])
		const elapsed = Date.now() - t0
		console.log(`[T1c] elapsed=${elapsed}ms (default idleTimeoutMs=5000)`)
		expect(await store.get('k3')).toBe(3)
		expect(elapsed).toBeLessThan(3_000)
	}, 30_000)

	it('T1d: startup time scales with #stores when changelogs are transactional', async () => {
		const storeCount = 4
		const input = uniqueTopicName('hunt-restore-t1d-in')
		const appId = `hunt-restore-t1d-${Date.now()}`
		await createTopics(client, [
			{ name: input },
			...Array.from({ length: storeCount }, (_, i) => ({ name: buildChangelogTopicName(appId, `s${i}`) })),
		])
		const tx = client.producer({ transactionalId: uniqueTopicName('hunt-restore-t1d-tx'), idempotent: true })
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
		await withTimeout('start', app.start(), 55_000)
		const elapsed = Date.now() - t0
		console.log(`[T1d] start() with ${storeCount} transactional-changelog stores took ${elapsed}ms`)
		await app.close()
		// Restoring ~1 record per store should not take seconds per store.
		expect(elapsed).toBeLessThan(storeCount * 1_000)
	}, 60_000)
})

// ---------------------------------------------------------------------------------------------
// Theory 3: restore into non-empty store / invalid checkpoint
// ---------------------------------------------------------------------------------------------
describe('T3 restore into non-empty store / invalid checkpoints', () => {
	it('T3a: checkpoint beyond changelog end offset (topic deleted+recreated) must not skip restoration', async () => {
		const topic = uniqueTopicName('hunt-restore-t3a')
		await createTopics(client, [{ name: topic }])
		await producer.send(topic, { key: 'a', value: numberCodec.encode(1) })
		await producer.send(topic, { key: 'b', value: numberCodec.encode(2) })
		await producer.send(topic, { key: 'c', value: numberCodec.encode(3) })

		const store = await newKvStore('t3a')
		const checkpoints = new HuntRestoreCheckpointStore()
		await checkpoints.set(topic, 0, 100n) // stale checkpoint from the previous incarnation of the topic

		const restorer = new ChangelogRestorer(topic, stringCodec, numberCodec, store)
		await restorer.restore(client, RESTORE_FAST, [0], checkpoints)
		const got = [await store.get('a'), await store.get('b'), await store.get('c')]
		console.log(`[T3a] restored=${JSON.stringify(got)} checkpointAfter=${await checkpoints.get(topic, 0)}`)
		expect.soft(got).toEqual([1, 2, 3])
		expect.soft(await checkpoints.get(topic, 0)).toBe(3n)
	}, 30_000)

	it('T3b: with no checkpoint, stale local keys that are absent from the changelog are removed', async () => {
		const topic = uniqueTopicName('hunt-restore-t3b')
		await createTopics(client, [{ name: topic }])
		await producer.send(topic, { key: 'a', value: numberCodec.encode(1) })

		const store = await newKvStore('t3b')
		await store.put('ghost', 99) // local persistent state whose tombstone has been compacted away
		const restorer = new ChangelogRestorer(topic, stringCodec, numberCodec, store)
		await restorer.restore(client, RESTORE_FAST, [0], new HuntRestoreCheckpointStore())
		expect(await store.get('a')).toBe(1)
		expect(await store.get('ghost')).toBeUndefined()
	}, 30_000)
})

// ---------------------------------------------------------------------------------------------
// Theory 2: restart cycles with a persistent-like provider + checkpoint store
// ---------------------------------------------------------------------------------------------
describe('T2 restart cycles with checkpoint store', () => {
	// REFUTED on current code (passes): skipped.
	it.skip.each(['at_least_once', 'exactly_once'] as const)(
		'T2: counts continue exactly across restarts (%s)',
		async guarantee => {
			const input = uniqueTopicName(`hunt-restore-t2-${guarantee}-in`)
			const out = uniqueTopicName(`hunt-restore-t2-${guarantee}-out`)
			const appId = `hunt-restore-t2-${guarantee}-${Date.now()}`
			await createTopics(client, [{ name: input }, { name: out }])
			const provider = new HuntRestorePersistentProvider()

			const collector = new MultiTopicCollector({
				client,
				groupId: `${appId}-collector`,
				topics: [{ topic: out, keyCodec: stringCodec, valueCodec: numberCodec }],
			})
			await collector.start()

			const timings: number[] = []
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
						// Short initial timeout so a stuck-restore error surfaces quickly.
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
					const t0 = Date.now()
					await withTimeout(`start cycle ${cycle}`, app.start(), 30_000)
					timings.push(Date.now() - t0)
					for (let i = 0; i < 3; i++) await producer.send(input, { key: 'a', value: numberCodec.encode(1) })
					await collector.waitFor<string, number>(out, m => m.key === 'a' && m.value === cycle * 3, 15_000)
					await withTimeout(`close cycle ${cycle}`, app.close(), 20_000)
				}
			} finally {
				const seen = collector.getTopicMessages<string, number>(out).map(m => m.value)
				console.log(`[T2 ${guarantee}] start timings=${timings.join(',')}ms outputs=${seen.join(',')}`)
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

// ---------------------------------------------------------------------------------------------
// Theory 4/5: at-least-once failure behaviour
// ---------------------------------------------------------------------------------------------
describe('T4/T5 at-least-once failures', () => {
	// REFUTED on current code (passes): skipped to keep the suite signal-only.
	it.skip('T4a: a throwing user function surfaces ERROR and does not lose the failed record', async () => {
		const input = uniqueTopicName('hunt-restore-t4a-in')
		const out = uniqueTopicName('hunt-restore-t4a-out')
		const appId = `hunt-restore-t4a-${Date.now()}`
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
			console.log(`[T4a] state=${app1.state()} err=${app1.getError()?.message}`)
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

	// REFUTED on current code (passes): skipped to keep the suite signal-only.
	it.skip('T4b: a sink send that fails (record too large) surfaces ERROR and the input record is not committed', async () => {
		const input = uniqueTopicName('hunt-restore-t4b-in')
		const out = uniqueTopicName('hunt-restore-t4b-out')
		const appId = `hunt-restore-t4b-${Date.now()}`
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
					value: { encode: (v: number) => (huge && v === 2 ? Buffer.alloc(3_000_000) : numberCodec.encode(v)), decode: numberCodec.decode },
				})
			return app
		}
		const app1 = build(true)
		try {
			await app1.start()
			for (const v of [1, 2, 3]) await producer.send(input, { key: 'a', value: numberCodec.encode(v) })
			await collector.waitFor<string, number>(out, m => m.value === 1, 10_000)
			await waitUntil('app1 ERROR', () => app1.state() === 'ERROR', 20_000).catch(() => {})
			console.log(`[T4b] state=${app1.state()} err=${app1.getError()?.message}`)
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

	// REFUTED on current code (passes): skipped to keep the suite signal-only.
	it.skip('T5: a failing changelog write surfaces ERROR (not silent)', async () => {
		const input = uniqueTopicName('hunt-restore-t5-in')
		const appId = `hunt-restore-t5-${Date.now()}`
		await createTopics(client, [{ name: input }])
		const provider = new HuntRestorePersistentProvider()
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
				value: { encode: (v: number) => (v === 3 ? Buffer.alloc(3_000_000) : numberCodec.encode(v)), decode: numberCodec.decode },
			})
		try {
			await app.start()
			for (let i = 0; i < 4; i++) await producer.send(input, { key: 'a', value: numberCodec.encode(1) })
			await waitUntil('ERROR', () => app.state() === 'ERROR', 20_000)
			const local = await provider.stores.get('counts')!.get('a')
			console.log(`[T5] state=${app.state()} err=${app.getError()?.message} localValue=${String(local)}`)
		} finally {
			await app.close().catch(() => {})
		}
	}, 60_000)
})

// ---------------------------------------------------------------------------------------------
// Theory 6: lifecycle
// ---------------------------------------------------------------------------------------------
function huntRestoreSocketCount(): number {
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const handles = (process as any)._getActiveHandles() as Array<{ constructor?: { name?: string } }>
	return handles.filter(h => h?.constructor?.name === 'Socket' && (h as { destroyed?: boolean }).destroyed !== true).length
}

describe('T6 lifecycle', () => {
	it('T6a: concurrent start() calls - the second must not resolve before the first finished starting', async () => {
		const input = uniqueTopicName('hunt-restore-t6a-in')
		await createTopics(client, [{ name: input }])
		const app = flow({ applicationId: `hunt-restore-t6a-${Date.now()}`, client, consumer: { maxWaitMs: 100 } })
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

	it('T6b: failed start() (SourceTopicNotFoundError) must not report RUNNING, must be retryable, and must not leak sockets', async () => {
		const missing = uniqueTopicName('hunt-restore-t6b-missing')
		const app = flow({
			applicationId: `hunt-restore-t6b-${Date.now()}`,
			client: { brokers: requireKafkaBrokers(), clientId: 'hunt-restore-t6b' }, // app owns its client
			consumer: { maxWaitMs: 100 },
		})
		app.stream(missing, { key: stringCodec, value: numberCodec })
			.groupByKey()
			.count({ storeName: 'counts', key: stringCodec, value: numberCodec })
		const socketsBefore = huntRestoreSocketCount()
		await expect(app.start()).rejects.toThrow(/does not exist/)
		await sleep(500)
		const socketsAfter = huntRestoreSocketCount()
		const stateAfterFailure = app.state()
		console.log(`[T6b] state after failed start=${stateAfterFailure} sockets before=${socketsBefore} after=${socketsAfter}`)
		// A retry must run the startup again (and fail again), not be a silent no-op.
		const retry = await app.start().then(
			() => 'resolved',
			() => 'rejected'
		)
		console.log(`[T6b] retry start() ${retry}; state=${app.state()}`)
		try {
			expect.soft(stateAfterFailure).not.toBe('RUNNING')
			expect.soft(retry).toBe('rejected')
			expect.soft(socketsAfter).toBeLessThanOrEqual(socketsBefore)
		} finally {
			await app.close().catch(() => {})
		}
	}, 30_000)

	// REFUTED on current code (passes): skipped to keep the suite signal-only.
	it.skip('T6c: close() before start(), then close() twice concurrently, resolve cleanly', async () => {
		const input = uniqueTopicName('hunt-restore-t6c-in')
		await createTopics(client, [{ name: input }])
		const mk = () => {
			const app = flow({
				applicationId: `hunt-restore-t6c-${Date.now()}`,
				client: { brokers: requireKafkaBrokers(), clientId: 'hunt-restore-t6c' },
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
		const results = await withTimeout(
			'double close',
			Promise.allSettled([b.close(), b.close()]),
			20_000
		)
		console.log(`[T6c] concurrent close results=${results.map(r => r.status).join(',')} state=${b.state()}`)
		expect(results.map(r => r.status)).toEqual(['fulfilled', 'fulfilled'])
		expect(b.state()).toBe('STOPPED')
	}, 60_000)

	// REFUTED on current code (passes): skipped to keep the suite signal-only.
	it.skip('T6d: start() after close() on the same app instance works (or fails loudly) and processes data', async () => {
		const input = uniqueTopicName('hunt-restore-t6d-in')
		const out = uniqueTopicName('hunt-restore-t6d-out')
		const appId = `hunt-restore-t6d-${Date.now()}`
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
			console.log(`[T6d] state after restart=${app.state()}`)
			await producer.send(input, { key: 'a', value: numberCodec.encode(1) })
			await collector.waitFor<string, number>(out, m => m.value === 2, 15_000)
		} finally {
			await app.close().catch(() => {})
			await collector.stop()
		}
	}, 60_000)

	it('T6e: close() during changelog restoration settles, leaves STOPPED and start() does not report success on a closed app', async () => {
		const input = uniqueTopicName('hunt-restore-t6e-in')
		const appId = `hunt-restore-t6e-${Date.now()}`
		const changelog = buildChangelogTopicName(appId, 'counts')
		await createTopics(client, [{ name: input }, { name: changelog }])
		for (let i = 1; i <= 3; i++) await producer.send(changelog, { key: `k${i}`, value: numberCodec.encode(i) })
		const provider = new HuntRestoreSlowProvider()
		provider.hooks.armed = true
		provider.hooks.delayMs = 4_000
		const app = flow({
			applicationId: appId,
			client: { brokers: requireKafkaBrokers(), clientId: 'hunt-restore-t6e' },
			numStreamThreads: 1,
			consumer: { maxWaitMs: 100 },
			stateStoreProvider: provider,
			changelog: { restoration: { idleTimeoutMs: 30_000, initialIdleTimeoutMs: 30_000, consumerMaxWaitMs: 100 } },
		})
		app.stream(input, { key: stringCodec, value: numberCodec })
			.groupByKey()
			.count({ storeName: 'counts', key: stringCodec, value: numberCodec })
		const socketsBefore = huntRestoreSocketCount()
		const startP = app.start().then(
			() => 'resolved',
			(e: Error) => `rejected: ${e.message}`
		)
		await sleep(2_000)
		const closeResult = await withTimeout('close during restore', app.close().then(() => 'closed'), 25_000)
		const startResult = await withTimeout('start settles', startP, 25_000)
		await sleep(500)
		console.log(
			`[T6e] close=${closeResult} start=${startResult} state=${app.state()} sockets before=${socketsBefore} after=${huntRestoreSocketCount()}`
		)
		expect.soft(app.state()).toBe('STOPPED')
		expect.soft(huntRestoreSocketCount()).toBeLessThanOrEqual(socketsBefore)
		// start() must not report success for an app that was closed while still starting.
		expect.soft(startResult).not.toBe('resolved')
	}, 60_000)

	// REFUTED on current code (passes): skipped to keep the suite signal-only.
	it.skip('T6f: fatal consumer error (fenced static member) is reported as ERROR, and close() still works', async () => {
		const input = uniqueTopicName('hunt-restore-t6f-in')
		const appId = `hunt-restore-t6f-${Date.now()}`
		await createTopics(client, [{ name: input }])
		const mk = () => {
			const app = flow({
				applicationId: appId,
				client,
				numStreamThreads: 1,
				consumer: { maxWaitMs: 100, groupInstanceId: 'hunt-restore-t6f-static' },
			})
			app.stream(input, { key: stringCodec, value: numberCodec }).mapValues(v => v)
			return app
		}
		const app1 = mk()
		const app2 = mk()
		try {
			await app1.start()
			await app2.start()
			await waitUntil('app1 fenced -> ERROR', () => app1.state() === 'ERROR', 20_000).catch(() => {})
			console.log(`[T6f] app1=${app1.state()} err=${app1.getError()?.message} app2=${app2.state()}`)
			expect(app1.state()).toBe('ERROR')
			await withTimeout('app1.close', app1.close(), 20_000)
			expect(app1.state()).toBe('STOPPED')
		} finally {
			await app1.close().catch(() => {})
			await app2.close().catch(() => {})
		}
	}, 60_000)

	// REFUTED on current code (passes): skipped to keep the suite signal-only.
	it.skip('T6g: app with zero sources starts and closes cleanly', async () => {
		const app = flow({
			applicationId: `hunt-restore-t6g-${Date.now()}`,
			client: { brokers: requireKafkaBrokers(), clientId: 'hunt-restore-t6g' },
		})
		await withTimeout('start', app.start(), 10_000)
		console.log(`[T6g] state=${app.state()}`)
		await withTimeout('close', app.close(), 10_000)
		expect(app.state()).toBe('STOPPED')
	}, 30_000)

	it('T6h: a transient (non-fatal) consumer error event must not leave state() permanently ERROR while processing continues', async () => {
		const input = uniqueTopicName('hunt-restore-t6h-in')
		const out = uniqueTopicName('hunt-restore-t6h-out')
		const appId = `hunt-restore-t6h-${Date.now()}`
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
			// The client emits 'error' for non-fatal conditions too (e.g. auto-commit failures, see
			// consumer.ts handleAutoCommitError / cleanup). Emit one the same way.
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			;(app as any).workers[0].consumer.emit('error', new Error('transient commit failure'))
			await producer.send(input, { key: 'a', value: numberCodec.encode(7) })
			await collector.waitFor<string, number>(out, m => m.value === 7, 10_000) // still processing
			await sleep(300)
			console.log(`[T6h] state after transient error while still processing=${app.state()}`)
			expect(app.state()).not.toBe('ERROR')
		} finally {
			await app.close().catch(() => {})
			await collector.stop()
		}
	}, 30_000)

	it('T6i: start() failing while workers are starting (consumer cannot join) must clean up consumers/producers', async () => {
		const input = uniqueTopicName('hunt-restore-t6i-in')
		await createTopics(client, [{ name: input }])
		const app = flow({
			applicationId: `hunt-restore-t6i-${Date.now()}`,
			client: { brokers: requireKafkaBrokers(), clientId: 'hunt-restore-t6i' },
			numStreamThreads: 1,
			// Invalid session timeout (below broker group.min.session.timeout.ms) => join fails
			consumer: { maxWaitMs: 100, sessionTimeoutMs: 1 },
		})
		app.stream(input, { key: stringCodec, value: numberCodec }).mapValues(v => v)
		const socketsBefore = huntRestoreSocketCount()
		const t0 = Date.now()
		const res = await withTimeout(
			'start',
			app.start().then(
				() => 'resolved',
				(e: Error) => `rejected: ${e.message}`
			),
			30_000
		)
		await sleep(1_000)
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const consumerRunning = (app as any).workers?.[0]?.consumer?.isRunning
		console.log(
			`[T6i] start ${res} after ${Date.now() - t0}ms state=${app.state()} consumerStillRunning=${String(consumerRunning)} sockets before=${socketsBefore} after=${huntRestoreSocketCount()}`
		)
		try {
			expect.soft(consumerRunning).not.toBe(true)
			expect.soft(huntRestoreSocketCount()).toBeLessThanOrEqual(socketsBefore)
		} finally {
			await app.close().catch(() => {})
		}
	}, 60_000)
})

// ---------------------------------------------------------------------------------------------
// Theory 7: changelog topic creation races
// ---------------------------------------------------------------------------------------------
describe('T7 changelog topic validation', () => {
	// REFUTED on current code (passes): skipped to keep the suite signal-only.
	it.skip('T7a: two instances starting concurrently must both start even though both try to create the changelog topic', async () => {
		const failures: string[] = []
		for (let attempt = 0; attempt < 4; attempt++) {
			const input = uniqueTopicName('hunt-restore-t7a-in')
			const appId = `hunt-restore-t7a-${Date.now()}-${attempt}`
			await createTopics(client, [{ name: input, partitions: 2 }])
			const mk = () => {
				// NOTE: separate KafkaClient per instance. Two consumers of the same group sharing ONE KafkaClient
				// stall in JoinGroup ("Consumer did not start within 10000ms") - separate client-level issue.
				const app = flow({
					applicationId: appId,
					client: { brokers: requireKafkaBrokers(), clientId: 'hunt-restore-t7a' },
					numStreamThreads: 1,
					consumer: { maxWaitMs: 100 },
				})
				app.stream(input, { key: stringCodec, value: numberCodec })
					.groupByKey()
					.count({ storeName: 'counts', key: stringCodec, value: numberCodec, changelog: { skipRestoration: true } })
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
			await withTimeout('close both', Promise.allSettled([a.close(), b.close()]), 15_000).catch(e => failures.push(String((e as Error).message)))
		}
		console.log(`[T7a] failures=${JSON.stringify(failures)}`)
		expect(failures).toEqual([])
	}, 90_000)
})
