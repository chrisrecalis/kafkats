/**
 * State / changelog partitioning and multi-instance correctness: per-partition state stores must follow
 * their source partition across restarts, rebalances and instance changes.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import {
	TimeWindows,
	codec,
	flow,
	InMemoryStateStoreProvider,
	type ChangelogCheckpointStore,
	type FlowApp,
	type KeyValueStore,
	type StateStoreProvider,
} from '../../src/index.js'
import {
	createTopics,
	float64Codec as numberCodec,
	MultiTopicCollector,
	newClient,
	requireKafkaBrokers,
	sleep,
	uniqueTopicName,
	waitUntil,
	withTimeout,
} from './test-helpers.js'

const stringCodec = codec.string()

let client: KafkaClient
let producer: Producer

beforeAll(async () => {
	client = newClient('part-part')
	await client.connect()
	producer = client.producer()
})

afterAll(async () => {
	await producer.disconnect()
	await client.disconnect()
})

/** Java-compatible murmur2 (copy of the client's default partitioner; it is not exported from the package index). */
function murmur2(data: Buffer): number {
	const m = 0x5bd1e995
	let h = 0x9747b28c ^ data.length
	let length = data.length
	let offset = 0
	while (length >= 4) {
		let k = data[offset]! | (data[offset + 1]! << 8) | (data[offset + 2]! << 16) | (data[offset + 3]! << 24)
		k = Math.imul(k, m)
		k ^= k >>> 24
		k = Math.imul(k, m)
		h = Math.imul(h, m)
		h ^= k
		offset += 4
		length -= 4
	}
	switch (length) {
		case 3:
			h ^= data[offset + 2]! << 16
		// falls through
		case 2:
			h ^= data[offset + 1]! << 8
		// falls through
		case 1:
			h ^= data[offset]!
			h = Math.imul(h, m)
	}
	h ^= h >>> 13
	h = Math.imul(h, m)
	h ^= h >>> 15
	return h
}

/** Partition the default (murmur2) partitioner would pick for a string key. */
function hashPartition(key: string, partitions: number): number {
	return (murmur2(Buffer.from(key)) & 0x7fffffff) % partitions
}

/** Find `count` keys (with prefix) whose default partition equals `partition`. */
function keysForPartition(prefix: string, partition: number, partitions: number, count = 1): string[] {
	const out: string[] = []
	for (let i = 0; out.length < count && i < 100_000; i++) {
		const k = `${prefix}${i}`
		if (hashPartition(k, partitions) === partition) out.push(k)
	}
	return out
}

const RESTORATION = {
	idleTimeoutMs: 1_500,
	initialIdleTimeoutMs: 10_000,
	checkIntervalMs: 200,
	consumerMaxWaitMs: 200,
}

function makeApp(
	appId: string,
	clientId: string,
	build: (app: FlowApp) => void,
	stateStoreProvider?: StateStoreProvider
): FlowApp {
	const app = flow({
		applicationId: appId,
		client: { clientId, brokers: requireKafkaBrokers() },
		consumer: { autoOffsetReset: 'earliest', maxWaitMs: 100 },
		changelog: { restoration: RESTORATION },
		...(stateStoreProvider ? { stateStoreProvider } : {}),
	})
	build(app)
	return app
}

async function closeAll(...apps: Array<FlowApp | undefined>): Promise<void> {
	for (const app of apps) {
		if (!app) continue
		await withTimeout('app.close', app.close(), 20_000).catch(() => {})
	}
}

/**
 * Wait until the group is Stable, contains exactly `clientIds`, every member owns >=1 partition of
 * `topic` (when several members) and all `partitions` are covered.
 */
async function waitAssign(
	groupId: string,
	clientIds: string[],
	topic: string,
	partitions: number,
	timeoutMs = 30_000
): Promise<Map<string, number[]>> {
	const admin = client.admin()
	const start = Date.now()
	while (Date.now() - start < timeoutMs) {
		const [desc] = await admin.describeGroups([groupId])
		if (desc?.state === 'Stable' && desc.members.length === clientIds.length) {
			const map = new Map<string, number[]>()
			for (const id of clientIds) {
				const m = desc.members.find(x => x.clientId === id)
				if (m) {
					map.set(
						id,
						m.assignment.filter(tp => tp.topic === topic).map(tp => tp.partition)
					)
				}
			}
			const covered = new Set([...map.values()].flat())
			if (
				map.size === clientIds.length &&
				covered.size === partitions &&
				[...map.values()].every(v => v.length > 0)
			) {
				return map
			}
		}
		await sleep(200)
	}
	const [last] = await admin.describeGroups([groupId])
	throw new Error(
		`Timed out waiting for group assignment of ${clientIds.join(',')} on ${topic}; last state=${last?.state}: ` +
			JSON.stringify(
				last?.members.map(m => ({
					id: m.clientId,
					a: m.assignment.map(t => `${t.topic.slice(0, 18)}:${t.partition}`),
				}))
			)
	)
}

async function expectValues<V>(
	out: MultiTopicCollector,
	topic: string,
	expectations: Array<{ key: string; value: V }>,
	timeoutMs: number
): Promise<void> {
	const results = await Promise.allSettled(
		expectations.map(e => out.waitFor<string, V>(topic, m => m.key === e.key && m.value === e.value, timeoutMs))
	)
	const failed: string[] = []
	results.forEach((r, i) => {
		if (r.status === 'rejected') {
			const e = expectations[i]!
			const seen = out
				.getTopicMessages<string, V>(topic)
				.filter(m => m.key === e.key)
				.map(m => m.value)
			failed.push(`key=${e.key}: expected ${String(e.value)}, saw [${seen.join(',')}]`)
		}
	})
	expect(failed, `\n${failed.join('\n')}\n`).toEqual([])
}

describe('state/changelog partitioning', () => {
	// Control: plain count with default partitioning keeps counting after a restart onto 2 instances.
	it('plain count, default partitioning, restart onto 2 instances keeps counting', async () => {
		const input = uniqueTopicName('part-ctl-src')
		const out = uniqueTopicName('part-ctl-out')
		const appId = `part-ctl-${Date.now()}`
		await createTopics(client, [{ name: input, partitions: 2 }, { name: out }])
		const keys = [keysForPartition('c', 0, 2)[0]!, keysForPartition('c', 1, 2)[0]!]

		const build = (app: FlowApp) =>
			app
				.stream(input, { key: stringCodec, value: numberCodec })
				.groupByKey()
				.count({ storeName: 'counts', key: stringCodec, value: numberCodec })
				.toStream()
				.to(out, { key: stringCodec, value: numberCodec })

		const collector = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector`,
			topics: [{ topic: out, keyCodec: stringCodec, valueCodec: numberCodec }],
		})
		const a1 = makeApp(appId, `${appId}-a1`, build)
		const a2 = makeApp(appId, `${appId}-a2`, build)
		const a3 = makeApp(appId, `${appId}-a3`, build)
		try {
			await collector.start()
			await a1.start()
			for (const k of keys)
				for (let i = 0; i < 2; i++) await producer.send(input, { key: k, value: numberCodec.encode(1) })
			await expectValues(
				collector,
				out,
				keys.map(k => ({ key: k, value: 2 })),
				15_000
			)
			await a1.close()

			await a2.start()
			await a3.start()
			await waitAssign(appId, [`${appId}-a2`, `${appId}-a3`], input, 2)
			for (const k of keys) await producer.send(input, { key: k, value: numberCodec.encode(1) })
			await expectValues(
				collector,
				out,
				keys.map(k => ({ key: k, value: 3 })),
				15_000
			)
		} finally {
			await closeAll(a1, a2, a3)
			await collector.stop().catch(() => {})
		}
	}, 90_000)

	// Windowed state is per source partition, so it is restored on whichever instance owns the partition.
	it('windowed count survives restart onto 2 instances (4-partition source)', async () => {
		const P = 4
		const input = uniqueTopicName('part-win-src')
		const out = uniqueTopicName('part-win-out')
		const appId = `part-win-${Date.now()}`
		await createTopics(client, [{ name: input, partitions: P }, { name: out }])
		const keys = Array.from({ length: 24 }, (_, i) => `w${i}`)
		const ts = Math.floor(Date.now() / 3_600_000) * 3_600_000 + 1_000

		const build = (app: FlowApp) =>
			app
				.stream(input, { key: stringCodec, value: numberCodec })
				.groupByKey()
				.windowedBy(TimeWindows.of(3_600_000))
				.count({ storeName: 'wcounts', value: numberCodec })
				.toStream()
				.selectKey(k => k!.key)
				.to(out, { key: stringCodec, value: numberCodec })

		const collector = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector`,
			topics: [{ topic: out, keyCodec: stringCodec, valueCodec: numberCodec }],
		})
		const a1 = makeApp(appId, `${appId}-a1`, build)
		const a2 = makeApp(appId, `${appId}-a2`, build)
		const a3 = makeApp(appId, `${appId}-a3`, build)
		try {
			await collector.start()
			await a1.start()
			for (const k of keys)
				for (let i = 0; i < 2; i++)
					await producer.send(input, { key: k, value: numberCodec.encode(1), timestamp: new Date(ts) })
			await expectValues(
				collector,
				out,
				keys.map(k => ({ key: k, value: 2 })),
				20_000
			)
			await a1.close()

			await a2.start()
			await a3.start()
			await waitAssign(appId, [`${appId}-a2`, `${appId}-a3`], input, P)
			for (const k of keys)
				await producer.send(input, { key: k, value: numberCodec.encode(1), timestamp: new Date(ts) })
			await expectValues(
				collector,
				out,
				keys.map(k => ({ key: k, value: 3 })),
				20_000
			)
		} finally {
			await closeAll(a1, a2, a3)
			await collector.stop().catch(() => {})
		}
	}, 90_000)

	// State follows the SOURCE partition even when input was produced with a non-murmur2 partitioning.
	it('count over input with non-default partitioning survives restart onto 2 instances', async () => {
		const P = 2
		const input = uniqueTopicName('part-np-src')
		const out = uniqueTopicName('part-np-out')
		const appId = `part-np-${Date.now()}`
		await createTopics(client, [{ name: input, partitions: P }, { name: out }])
		// kA hashes to 1 but is produced to partition 0; kB hashes to 0 but is produced to partition 1.
		const kA = keysForPartition('np', 1, P)[0]!
		const kB = keysForPartition('np', 0, P)[0]!
		const sendTo = (k: string, partition: number) =>
			producer.send(input, { key: k, value: numberCodec.encode(1), partition })

		const build = (app: FlowApp) =>
			app
				.stream(input, { key: stringCodec, value: numberCodec })
				.groupByKey()
				.count({ storeName: 'counts', key: stringCodec, value: numberCodec })
				.toStream()
				.to(out, { key: stringCodec, value: numberCodec })

		const collector = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector`,
			topics: [{ topic: out, keyCodec: stringCodec, valueCodec: numberCodec }],
		})
		const a1 = makeApp(appId, `${appId}-a1`, build)
		const a2 = makeApp(appId, `${appId}-a2`, build)
		const a3 = makeApp(appId, `${appId}-a3`, build)
		try {
			await collector.start()
			await a1.start()
			for (let i = 0; i < 2; i++) {
				await sendTo(kA, 0)
				await sendTo(kB, 1)
			}
			await expectValues(
				collector,
				out,
				[kA, kB].map(k => ({ key: k, value: 2 })),
				15_000
			)
			await a1.close()

			await a2.start()
			await a3.start()
			await waitAssign(appId, [`${appId}-a2`, `${appId}-a3`], input, P)
			await sendTo(kA, 0)
			await sendTo(kB, 1)
			await expectValues(
				collector,
				out,
				[kA, kB].map(k => ({ key: k, value: 3 })),
				15_000
			)
		} finally {
			await closeAll(a1, a2, a3)
			await collector.stop().catch(() => {})
		}
	}, 90_000)

	// groupBy(newKey) repartitions through an internal topic, so counts are global across instances.
	it('groupBy(newKey).count() across 2 instances yields the true total', async () => {
		const P = 2
		const input = uniqueTopicName('part-gb-src')
		const out = uniqueTopicName('part-gb-out')
		const appId = `part-gb-${Date.now()}`
		await createTopics(client, [{ name: input, partitions: P }, { name: out }])

		const build = (app: FlowApp) =>
			app
				.stream(input, { key: stringCodec, value: stringCodec })
				.groupBy((_k, v) => v as string, { key: stringCodec, value: stringCodec })
				.count({ storeName: 'gcounts', key: stringCodec, value: numberCodec })
				.toStream()
				.to(out, { key: stringCodec, value: numberCodec })

		const collector = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector`,
			topics: [{ topic: out, keyCodec: stringCodec, valueCodec: numberCodec }],
		})
		const a1 = makeApp(appId, `${appId}-a1`, build)
		const a2 = makeApp(appId, `${appId}-a2`, build)
		try {
			await collector.start()
			await a1.start()
			await a2.start()
			await waitAssign(appId, [`${appId}-a1`, `${appId}-a2`], input, P)
			await sleep(1_500)
			for (let i = 0; i < 10; i++) {
				for (let p = 0; p < P; p++) {
					await producer.send(input, {
						key: `src-${p}-${i}`,
						value: Buffer.from('G'),
						partition: p,
					})
				}
			}
			await sleep(5_000)
			const values = collector
				.getTopicMessages<string, number>(out)
				.filter(m => m.key === 'G')
				.map(m => m.value)
			const max = Math.max(0, ...values)
			expect(max, `20 records with new key G; observed count sequence: [${values.join(',')}]`).toBe(20)
			// values must never regress
			const regress = values.some((v, i) => i > 0 && v < values[i - 1]!)
			expect(regress, `count values regressed: [${values.join(',')}]`).toBe(false)
		} finally {
			await closeAll(a1, a2)
			await collector.stop().catch(() => {})
		}
	}, 90_000)

	// A partition lost and regained (incl. tombstoned keys) has correct table state.
	class CheckpointingProvider extends InMemoryStateStoreProvider {
		readonly offsets = new Map<string, bigint>()
		getChangelogCheckpointStore(): ChangelogCheckpointStore {
			return {
				get: async (t, p) => this.offsets.get(`${t}:${p}`),
				set: async (t, p, o) => {
					this.offsets.set(`${t}:${p}`, o)
				},
			}
		}
	}

	for (const withCheckpoints of [false, true]) {
		it(`regained partition has correct table state incl. tombstones (checkpoints=${withCheckpoints})`, async () => {
			const P = 2
			const tbl = uniqueTopicName('part-t4-tbl')
			const probe = uniqueTopicName('part-t4-probe')
			const out = uniqueTopicName('part-t4-out')
			const appId = `part-t4-${Date.now()}`
			await createTopics(client, [{ name: tbl, partitions: P }, { name: probe, partitions: P }, { name: out }])
			// per partition p: key a_p (will be tombstoned by whoever owns p) and b_p (updated v1 -> v2)
			const a = [0, 1].map(p => keysForPartition('a', p, P)[0]!)
			const b = [0, 1].map(p => keysForPartition('b', p, P)[0]!)
			const allKeys = [...a, ...b]

			const build = (app: FlowApp) => {
				const table = app.table(tbl, {
					key: stringCodec,
					value: stringCodec,
					materialized: { storeName: 't4' },
				})
				app.stream(probe, { key: stringCodec, value: stringCodec })
					.leftJoin(table, (v, t) => `${v}=${t ?? 'null'}`)
					.to(out, { key: stringCodec, value: stringCodec })
			}
			const mk = (id: string) =>
				makeApp(appId, id, build, withCheckpoints ? new CheckpointingProvider() : undefined)
			const A = `${appId}-A`
			const B = `${appId}-B`
			const appA = mk(A)
			const appB = mk(B)
			const collector = new MultiTopicCollector({
				client,
				groupId: `${appId}-collector`,
				topics: [{ topic: out, keyCodec: stringCodec, valueCodec: stringCodec }],
			})
			let probeN = 0
			const probeKey = async (k: string, expected: string, timeoutMs: number) => {
				const id = `r${probeN++}`
				await producer.send(probe, { key: k, value: Buffer.from(id) })
				try {
					await collector.waitFor<string, string>(
						out,
						m => m.key === k && String(m.value).startsWith(`${id}=`),
						timeoutMs
					)
				} catch {
					return `${k}: no answer`
				}
				const m = collector
					.getTopicMessages<string, string>(out)
					.find(x => x.key === k && String(x.value).startsWith(`${id}=`))!
				return m.value === `${id}=${expected}`
					? null
					: `${k}: expected ${expected} got ${String(m.value).split('=')[1]}`
			}
			try {
				await collector.start()
				await appA.start()
				for (const k of allKeys) await producer.send(tbl, { key: k, value: Buffer.from('v1') })
				// wait until A has applied the table records (poll probes)
				for (const k of allKeys) {
					for (let i = 0; i < 20; i++) {
						if ((await probeKey(k, 'v1', 3_000)) === null) break
					}
				}

				await appB.start()
				await waitAssign(appId, [A, B], tbl, P)

				// owners (A or B) apply tombstones for a_p and v2 for b_p
				for (const k of a) await producer.send(tbl, { key: k, value: null })
				for (const k of b) await producer.send(tbl, { key: k, value: Buffer.from('v2') })
				// sync: wait until each owner applied it
				const expectedNow = new Map<string, string>([
					...a.map(k => [k, 'null'] as const),
					...b.map(k => [k, 'v2'] as const),
				])
				for (const [k, v] of expectedNow) {
					let ok = false
					for (let i = 0; i < 20 && !ok; i++) ok = (await probeKey(k, v, 3_000)) === null
					expect(ok, `owner did not apply update for ${k}`).toBe(true)
				}

				// B leaves; A regains every partition and must restore the ones B owned
				await appB.close()
				await waitAssign(appId, [A], tbl, P)
				await sleep(1_500)

				const failures: string[] = []
				for (const [k, v] of expectedNow) {
					const f = await probeKey(k, v, 15_000)
					if (f) failures.push(f)
				}
				expect(failures, `\n${failures.join('\n')}\n`).toEqual([])
			} finally {
				await closeAll(appA, appB)
				await collector.stop().catch(() => {})
			}
		}, 120_000)
	}

	// Partitions (re)assigned while start() is still restoring are restored too, not left at a stale snapshot.
	// A starts with slow store puts; B joins mid-restore, advances some partitions, then leaves.
	it('partitions regained while start() is still restoring are re-restored', async () => {
		const P = 4
		const FILLER = 120
		const input = uniqueTopicName('part-race-src')
		const out = uniqueTopicName('part-race-out')
		const appId = `part-race-${Date.now()}`
		await createTopics(client, [{ name: input, partitions: P }, { name: out }])
		const probeKeys = Array.from({ length: P }, (_, p) => keysForPartition('pk', p, P)[0]!)

		const build = (app: FlowApp) =>
			app
				.stream(input, { key: stringCodec, value: numberCodec })
				.groupByKey()
				.count({ storeName: 'counts', key: stringCodec, value: numberCodec })
				.toStream()
				.to(out, { key: stringCodec, value: numberCodec })

		class SlowProvider extends InMemoryStateStoreProvider {
			puts = 0
			override createKeyValueStore<K, V>(
				name: string,
				options: Parameters<InMemoryStateStoreProvider['createKeyValueStore']>[1]
			) {
				const s = super.createKeyValueStore(name, options as never) as unknown as KeyValueStore<K, V>
				const put = s.put.bind(s)
				s.put = async (k, v) => {
					this.puts++
					await sleep(40)
					return put(k, v)
				}
				return s as never
			}
		}

		const collector = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector`,
			topics: [{ topic: out, keyCodec: stringCodec, valueCodec: numberCodec }],
		})
		const seed = makeApp(appId, `${appId}-seed`, build)
		const A = `${appId}-A`
		const B = `${appId}-B`
		const slow = new SlowProvider()
		const appA = makeApp(appId, A, build, slow)
		const appB = makeApp(appId, B, build)
		try {
			await collector.start()
			await seed.start()
			const batch: Array<Promise<unknown>> = []
			for (let p = 0; p < P; p++) {
				for (const k of keysForPartition(`fill${p}-`, p, P, FILLER))
					batch.push(producer.send(input, { key: k, value: numberCodec.encode(1) }))
			}
			for (const k of probeKeys)
				for (let i = 0; i < 2; i++) batch.push(producer.send(input, { key: k, value: numberCodec.encode(1) }))
			await Promise.all(batch)
			await expectValues(
				collector,
				out,
				probeKeys.map(k => ({ key: k, value: 2 })),
				20_000
			)
			await seed.close()

			const aStart = appA.start()
			aStart.catch(() => {})
			const t0 = Date.now()
			while (slow.puts < 5 && Date.now() - t0 < 30_000) await sleep(25)

			await withTimeout('B.start', appB.start(), 40_000)
			const admin = client.admin()
			let bParts: number[] = []
			const t1 = Date.now()
			while (Date.now() - t1 < 30_000) {
				const [d] = await admin.describeGroups([appId])
				const mb = d?.members.find(x => x.clientId === B)
				if (d?.state === 'Stable' && d.members.length === 2 && mb && mb.assignment.length > 0) {
					bParts = mb.assignment.map(x => x.partition)
					break
				}
				await sleep(200)
			}
			expect(bParts.length, 'B should own some partitions').toBeGreaterThan(0)

			const bKeys = probeKeys.filter((_, p) => bParts.includes(p))
			const otherKeys = probeKeys.filter(k => !bKeys.includes(k))
			// B advances its keys to 3 (restored from changelog during B.start())
			for (const k of bKeys) await producer.send(input, { key: k, value: numberCodec.encode(1) })
			await expectValues(
				collector,
				out,
				bKeys.map(k => ({ key: k, value: 3 })),
				20_000
			)
			await appB.close()
			await waitAssign(appId, [A], input, P)

			await withTimeout('A.start', aStart, 120_000)
			await sleep(1_000)
			for (const k of probeKeys) await producer.send(input, { key: k, value: numberCodec.encode(1) })
			await expectValues(
				collector,
				out,
				[...bKeys.map(k => ({ key: k, value: 4 })), ...otherKeys.map(k => ({ key: k, value: 3 }))],
				20_000
			)
		} finally {
			await closeAll(seed, appA, appB)
			await collector.stop().catch(() => {})
		}
	}, 170_000)

	// A global table is fully replicated to every instance: stream lookups hit regardless of which instance
	// owns the stream partition or which table partition holds the key.
	it('globalTable is fully replicated to every instance', async () => {
		const T = 2
		const cfg = uniqueTopicName('part-gt-cfg')
		const events = uniqueTopicName('part-gt-events')
		const out = uniqueTopicName('part-gt-out')
		const appId = `part-gt-${Date.now()}`
		await createTopics(client, [{ name: cfg, partitions: T }, { name: events, partitions: 2 }, { name: out }])
		const cfgKeys = [keysForPartition('cfg', 0, T)[0]!, keysForPartition('cfg', 1, T)[0]!]

		const build = (app: FlowApp) => {
			const gt = app.globalTable(cfg, { key: stringCodec, value: stringCodec, materialized: { storeName: 'gt' } })
			app.stream(events, { key: stringCodec, value: stringCodec })
				.leftJoin(gt, (v, c) => `${v}:${c ?? 'MISSING'}`)
				.to(out, { key: stringCodec, value: stringCodec })
		}
		const a1 = makeApp(appId, `${appId}-a1`, build)
		const a2 = makeApp(appId, `${appId}-a2`, build)
		const collector = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector`,
			topics: [{ topic: out, keyCodec: stringCodec, valueCodec: stringCodec }],
		})
		try {
			await collector.start()
			for (const k of cfgKeys) await producer.send(cfg, { key: k, value: Buffer.from(`val-${k}`) })
			// start() loads the whole global table before processing any record
			await a1.start()
			await a2.start()
			await waitAssign(appId, [`${appId}-a1`, `${appId}-a2`], events, 2)
			// every event partition (one per instance) looks up keys of both table partitions
			for (const p of [0, 1])
				for (const k of cfgKeys) await producer.send(events, { key: k, value: Buffer.from(k), partition: p })
			const expected = [...cfgKeys, ...cfgKeys].map(k => `${k}:val-${k}`)
			await waitUntil(
				'global table lookups',
				() => collector.getTopicMessages<string, string>(out).length >= expected.length,
				20_000
			).catch(() => {}) // the assertion below reports what is missing
			const results = collector.getTopicMessages<string, string>(out).map(m => String(m.value))
			expect(results.sort(), 'lookups against a global table').toEqual(expected.sort())
		} finally {
			await closeAll(a1, a2)
			await collector.stop().catch(() => {})
		}
	}, 90_000)

	// Stream-stream join buffered state is restored on the new owners after a restart onto 2 instances.
	it('stream-stream join buffered state survives restart onto 2 instances', async () => {
		const P = 2
		const left = uniqueTopicName('part-ss-left')
		const right = uniqueTopicName('part-ss-right')
		const out = uniqueTopicName('part-ss-out')
		const appId = `part-ss-${Date.now()}`
		await createTopics(client, [{ name: left, partitions: P }, { name: right, partitions: P }, { name: out }])
		const keys = Array.from({ length: 16 }, (_, i) => `j${i}`)
		const ts = Date.now() - 5_000

		const build = (app: FlowApp) => {
			const l = app.stream(left, { key: stringCodec, value: numberCodec })
			const r = app.stream(right, { key: stringCodec, value: numberCodec })
			l.leftJoin(r, (lv, rv) => lv + (rv ?? 0), { within: TimeWindows.of(3_600_000) }).to(out, {
				key: stringCodec,
				value: numberCodec,
			})
		}
		const collector = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector`,
			topics: [{ topic: out, keyCodec: stringCodec, valueCodec: numberCodec }],
		})
		const a1 = makeApp(appId, `${appId}-a1`, build)
		const a2 = makeApp(appId, `${appId}-a2`, build)
		const a3 = makeApp(appId, `${appId}-a3`, build)
		try {
			await collector.start()
			await a1.start()
			for (const k of keys)
				await producer.send(left, { key: k, value: numberCodec.encode(1), timestamp: new Date(ts) })
			await expectValues(
				collector,
				out,
				keys.map(k => ({ key: k, value: 1 })),
				20_000
			) // left-only emissions
			await a1.close()

			await a2.start()
			await a3.start()
			await waitAssign(appId, [`${appId}-a2`, `${appId}-a3`], left, P)
			for (const k of keys)
				await producer.send(right, { key: k, value: numberCodec.encode(100), timestamp: new Date(ts + 100) })
			await expectValues(
				collector,
				out,
				keys.map(k => ({ key: k, value: 101 })),
				20_000
			)
		} finally {
			await closeAll(a1, a2, a3)
			await collector.stop().catch(() => {})
		}
	}, 90_000)

	// Stream-table join: stream partition p and table partition p are assigned to the same instance.
	it('stream-table join gets co-partitioned assignment', async () => {
		const P = 2
		const stream = uniqueTopicName('part-cp-stream')
		const tbl = uniqueTopicName('part-cp-table')
		const out = uniqueTopicName('part-cp-out')
		const appId = `part-cp-${Date.now()}`
		await createTopics(client, [{ name: stream, partitions: P }, { name: tbl, partitions: P }, { name: out }])
		const keys = [0, 1].flatMap(p => keysForPartition(`cp${p}-`, p, P, 4))

		const build = (app: FlowApp) => {
			const t = app.table(tbl, { key: stringCodec, value: stringCodec, materialized: { storeName: 'cp-tbl' } })
			app.stream(stream, { key: stringCodec, value: stringCodec })
				.join(t, (v, tv) => `${v}+${tv}`)
				.to(out, { key: stringCodec, value: stringCodec })
		}
		const A1 = `${appId}-a1`
		const A2 = `${appId}-a2`
		const a1 = makeApp(appId, A1, build)
		const a2 = makeApp(appId, A2, build)
		const collector = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector`,
			topics: [{ topic: out, keyCodec: stringCodec, valueCodec: stringCodec }],
		})
		try {
			await collector.start()
			for (const k of keys) await producer.send(tbl, { key: k, value: Buffer.from('T') })
			await a1.start()
			await a2.start()
			const admin = client.admin()
			const t0 = Date.now()
			let owners = new Map<string, string>()
			while (Date.now() - t0 < 30_000) {
				const [d] = await admin.describeGroups([appId])
				if (d?.state === 'Stable' && d.members.length === 2) {
					owners = new Map()
					for (const m of d.members)
						for (const tp of m.assignment) owners.set(`${tp.topic}:${tp.partition}`, m.clientId)
					if (owners.size === 2 * P) break
				}
				await sleep(200)
			}
			const mismatched: string[] = []
			for (let p = 0; p < P; p++) {
				const so = owners.get(`${stream}:${p}`)
				const to = owners.get(`${tbl}:${p}`)
				if (so !== to)
					mismatched.push(`partition ${p}: stream owned by ${so?.slice(-2)}, table owned by ${to?.slice(-2)}`)
			}
			// functional check: give the table time to load, then join
			await sleep(2_000)
			for (const k of keys) await producer.send(stream, { key: k, value: Buffer.from('S') })
			await sleep(4_000)
			const joined = new Set(collector.getTopicMessages<string, string>(out).map(m => m.key))
			const missing = keys.filter(k => !joined.has(k))
			expect(mismatched, 'stream/table partitions must be assigned to the same instance').toEqual([])
			expect(missing, 'keys whose join produced no output').toEqual([])
		} finally {
			await closeAll(a1, a2)
			await collector.stop().catch(() => {})
		}
	}, 90_000)
})
