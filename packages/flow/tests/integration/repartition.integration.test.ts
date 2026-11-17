import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { murmur2, toPositive, type KafkaClient, type Producer } from '@kafkats/client'
import { codec, flow, SessionWindows, TimeWindows, WindowedCodecs, type FlowApp } from '../../src/index.js'
import {
	createTopics,
	injectCommitFailures,
	newClient,
	readAllCommitted,
	sleep,
	uniqueTopicName,
} from './test-helpers.js'

const stringCodec = codec.string()
const numberCodec = codec.json<number>()

describe('repartitioning keeps event time, totals and topic identity', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('r2-rp-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	for (const mode of ['at_least_once', 'exactly_once'] as const) {
		it(`${mode}: two groupBy() in one topology, windowed count after repartition, output timestamps preserved`, async () => {
			const P = 4
			const input = uniqueTopicName('r2-rp-in')
			const outWin = uniqueTopicName('r2-rp-win')
			const outPar = uniqueTopicName('r2-rp-par')
			const applicationId = uniqueTopicName('r2-rp-app')
			await createTopics(admin, [{ name: input, partitions: P }, { name: outWin }, { name: outPar }])
			const hour = 3_600_000
			const T0 = Math.floor(1_650_000_000_000 / hour) * hour + 5_000 // far in the past
			const N = 60

			const build = (): FlowApp => {
				const app = flow({
					applicationId,
					client: newClient('r2-rp-app'),
					processingGuarantee: mode,
					commitIntervalMs: 100,
					consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
					producer: { lingerMs: 0 },
					changelog: {
						restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 5_000, checkIntervalMs: 50 },
					},
				})
				const src = app.stream(input, { key: stringCodec, value: numberCodec })
				src.groupBy((_k, v) => `n${(v as number) % 3}`, { key: stringCodec, value: numberCodec })
					.windowedBy(TimeWindows.of(hour))
					.count({ storeName: 'win', value: numberCodec })
					.toStream()
					.selectKey(k => `${k!.key}@${k!.window.start}`)
					.to(outWin, { key: stringCodec, value: numberCodec })
				src.groupBy((_k, v) => `p${(v as number) % 2}`, { key: stringCodec, value: numberCodec })
					.count({ storeName: 'par', key: stringCodec, value: numberCodec })
					.toStream()
					.to(outPar, { key: stringCodec, value: numberCodec })
				return app
			}
			const a1 = build()
			const a2 = build()
			try {
				await Promise.all([a1.start(), a2.start()])
				const a = admin.admin()
				for (const deadline = Date.now() + 30_000; Date.now() < deadline; await sleep(200)) {
					const [d] = await a.describeGroups([applicationId])
					if (
						d?.state === 'Stable' &&
						d.members.length === 2 &&
						d.members.every(m => m.assignment.length > 0)
					)
						break
				}
				await producer.send(
					input,
					Array.from({ length: N }, (_, i) => ({
						// a null key for some records: groupBy derives the key from the value
						key: i % 10 === 0 ? null : `k${i}`,
						value: numberCodec.encode(i),
						timestamp: new Date(T0),
						headers: { h: Buffer.from('x') },
					}))
				)
				let win: Awaited<ReturnType<typeof readAllCommitted>> = []
				let par: Awaited<ReturnType<typeof readAllCommitted>> = []
				for (const deadline = Date.now() + 25_000; Date.now() < deadline;) {
					win = await readAllCommitted(admin, outWin, 800)
					par = await readAllCommitted(admin, outPar, 800)
					if (win.length >= N && par.length >= N) break
				}
				const last = (msgs: typeof win) => {
					const m = new Map<string, number>()
					for (const x of msgs) m.set(x.key!.toString(), numberCodec.decode(x.value!))
					return Object.fromEntries([...m].sort())
				}
				expect(last(win)).toEqual({
					[`n0@${T0 - 5_000}`]: N / 3,
					[`n1@${T0 - 5_000}`]: N / 3,
					[`n2@${T0 - 5_000}`]: N / 3,
				})
				expect(last(par)).toEqual({ p0: N / 2, p1: N / 2 })
				expect(win).toHaveLength(N)
				expect(par).toHaveLength(N)
				for (const m of [...win, ...par]) expect(Number(m.timestamp)).toBe(T0)
				for (const m of par) expect(m.headers.h?.toString()).toBe('x')
			} finally {
				await a1.close().catch(() => {})
				await a2.close().catch(() => {})
			}
		}, 85_000)
	}

	it('exactly_once session windows: out-of-order merges inside transactions with failing commits equal the failure-free result', async () => {
		const input = uniqueTopicName('r2-ss-in')
		const output = uniqueTopicName('r2-ss-out')
		const applicationId = uniqueTopicName('r2-ss-app')
		await createTopics(admin, [{ name: input }, { name: output }])
		const B = 1_700_000_000_000
		// session 1 = {0, 500, 900}, session 2 = {5000, 5400}, session 3 = {20000}; arrival order scrambled
		const order = [900, 0, 5400, 500, 5000, 20_000]
		const client = newClient('r2-ss-app')
		let commits = 0
		injectCommitFailures(client, () => ++commits <= 6 && commits % 2 === 1)
		const app = flow({
			applicationId,
			client,
			processingGuarantee: 'exactly_once',
			commitIntervalMs: 120,
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			producer: { lingerMs: 0 },
			changelog: { restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 5_000, checkIntervalMs: 50 } },
		})
		app.stream(input, { key: stringCodec, value: numberCodec })
			.groupByKey()
			.windowedBy(SessionWindows.withInactivityGap(1_000))
			.count({ storeName: 'sess', value: numberCodec })
			.toStream()
			.selectKey(k => `a@${k!.window.start - B}-${k!.window.end - B}`)
			.to(output, { key: stringCodec, value: numberCodec })
		try {
			await app.start()
			for (const t of order) {
				await producer.send(input, { key: 'a', value: numberCodec.encode(1), timestamp: new Date(B + t) })
				await sleep(110)
			}
			await sleep(6_000)
			expect(commits).toBeGreaterThanOrEqual(2) // at least one commit failed and was retried
			const view = new Map<string, number | null>()
			for (const m of await readAllCommitted(admin, output)) {
				view.set(m.key!.toString(), m.value === null ? null : numberCodec.decode(m.value))
			}
			const live = Object.fromEntries([...view].filter(([, v]) => v !== null).sort())
			expect(live).toEqual({ 'a@0-900': 3, 'a@5000-5400': 2, 'a@20000-20000': 1 })
		} finally {
			await app.close().catch(() => {})
		}
	}, 80_000)

	it('a stream re-keyed before a global-table join is repartitioned before it is aggregated', async () => {
		const input = uniqueTopicName('r2-rp-gt-in')
		const table = uniqueTopicName('r2-rp-gt-table')
		const output = uniqueTopicName('r2-rp-gt-out')
		await createTopics(admin, [{ name: input, partitions: 2 }, { name: table }, { name: output }])
		await producer.send(table, { key: 'eu', value: Buffer.from('Europe') })

		const app = flow({
			applicationId: uniqueTopicName('r2-rp-gt-app'),
			client: newClient('r2-rp-gt-app'),
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			producer: { lingerMs: 0 },
		})
		const regions = app.globalTable(table, { key: stringCodec, value: stringCodec })
		app.stream(input, { key: stringCodec, value: stringCodec })
			.selectKey((_k, region) => region!)
			.join(regions, (_region, name) => name)
			.groupByKey({ key: stringCodec, value: stringCodec })
			.count({ storeName: 'per-region', key: stringCodec, value: numberCodec })
			.toStream()
			.to(output, { key: stringCodec, value: numberCodec })
		try {
			await app.start()
			// The same new key arrives on two different source partitions.
			await producer.send(input, { key: 'a', value: Buffer.from('eu'), partition: 0 })
			await producer.send(input, { key: 'b', value: Buffer.from('eu'), partition: 1 })

			let counts: number[] = []
			for (const deadline = Date.now() + 20_000; counts.length < 2 && Date.now() < deadline;) {
				await sleep(300)
				counts = (await readAllCommitted(admin, output, 800)).map(m => numberCodec.decode(m.value!))
			}
			expect(counts).toEqual([1, 2])
		} finally {
			await app.close().catch(() => {})
		}
	}, 40_000)

	it('the results of a windowed aggregation are joined with a table keyed by (key, window)', async () => {
		const P = 4
		const input = uniqueTopicName('r2-rp-win-in')
		const labels = uniqueTopicName('r2-rp-win-labels')
		const output = uniqueTopicName('r2-rp-win-out')
		await createTopics(admin, [{ name: input, partitions: P }, { name: labels, partitions: P }, { name: output }])
		type Window = { key: string; window: { start: number; end: number } }
		const windowCodec = codec.json<Window>()
		const size = 10_000
		const start = Math.floor(1_650_000_000_000 / size) * size

		// One label per window of each key, wherever the windowed key hashes to.
		const keys = ['k0', 'k1', 'k2', 'k3', 'k4', 'k5']
		await producer.send(
			labels,
			keys.map(key => ({
				key: windowCodec.encode({ key, window: { start, end: start + size } }),
				value: Buffer.from(`label-${key}`),
			}))
		)

		const app = flow({
			applicationId: uniqueTopicName('r2-rp-win-app'),
			client: newClient('r2-rp-win-app'),
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			producer: { lingerMs: 0 },
		})
		const labelTable = app.table(labels, { key: windowCodec, value: stringCodec })
		app.stream(input, { key: stringCodec, value: stringCodec })
			.groupByKey()
			.windowedBy(TimeWindows.of(size))
			.count({ storeName: 'per-window', value: numberCodec })
			.toStream()
			// Windowed results stay in the partition of their base key; the labels are where the whole windowed key
			// hashes to, so the results move there through a repartition with a plain key codec (BC10).
			.repartition({ key: windowCodec, value: numberCodec })
			.join(labelTable, (count, label) => `${label}=${count}`)
			.to(output, { key: windowCodec, value: stringCodec })
		try {
			await app.start()
			// Let the table load, then send every key into partition 0.
			await sleep(2_000)
			await producer.send(
				input,
				keys.map(key => ({ key, value: Buffer.from('x'), partition: 0, timestamp: new Date(start + 1) }))
			)

			let joined: string[] = []
			for (const deadline = Date.now() + 20_000; joined.length < keys.length && Date.now() < deadline;) {
				await sleep(300)
				joined = (await readAllCommitted(admin, output, 800)).map(m => m.value!.toString())
			}
			expect(joined.sort()).toEqual(keys.map(key => `label-${key}=1`))
		} finally {
			await app.close().catch(() => {})
		}
	}, 60_000)

	// BC10, KS `WindowedStreamPartitioner`: a sink writes a windowed key to the partition its base key hashes to (with the
	// producer's default partitioner), not where the bytes of the whole windowed key hash to.
	it('windowed results are written to the partition of their base key', async () => {
		const input = uniqueTopicName('r2-rp-wsink-in')
		const timeOut = uniqueTopicName('r2-rp-wsink-time')
		const sessionOut = uniqueTopicName('r2-rp-wsink-session')
		const OUT_PARTITIONS = 3
		await createTopics(admin, [
			{ name: input, partitions: 4 },
			{ name: timeOut, partitions: OUT_PARTITIONS },
			{ name: sessionOut, partitions: OUT_PARTITIONS },
		])
		const size = 10_000
		const app = flow({
			applicationId: uniqueTopicName('r2-rp-wsink-app'),
			client: newClient('r2-rp-wsink-app'),
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			producer: { lingerMs: 0 },
		})
		const grouped = app.stream(input, { key: stringCodec, value: stringCodec }).groupByKey()
		grouped
			.windowedBy(TimeWindows.ofSizeWithNoGrace(size))
			.count({ storeName: 'per-window', value: numberCodec })
			.toStream()
			.to(timeOut)
		grouped
			.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(size))
			.count({ storeName: 'per-session', value: numberCodec })
			.toStream()
			.to(sessionOut)
		const keys = Array.from({ length: 12 }, (_, i) => `key-${i}`)
		const start = Math.floor(1_650_000_000_000 / size) * size
		try {
			await app.start()
			await producer.send(
				input,
				keys.map(key => ({ key, value: Buffer.from('x'), timestamp: new Date(start + 1) }))
			)
			const formats = [
				{ topic: timeOut, key: WindowedCodecs.timeWindowed(stringCodec, size) },
				{ topic: sessionOut, key: WindowedCodecs.sessionWindowed(stringCodec) },
			]
			for (const { topic, key } of formats) {
				let rows: Awaited<ReturnType<typeof readAllCommitted>> = []
				for (const deadline = Date.now() + 20_000; rows.length < keys.length && Date.now() < deadline;) {
					await sleep(300)
					rows = await readAllCommitted(admin, topic, 800)
				}
				const placed = rows.map(row => {
					const windowed = key.decode(row.key!)
					return { key: windowed.key, start: windowed.window.start, partition: row.partition }
				})
				const expected = keys.map(base => ({
					key: base,
					start: start + (topic === timeOut ? 0 : 1),
					partition: toPositive(murmur2(Buffer.from(base))) % OUT_PARTITIONS,
				}))
				const byKey = (a: { key: string }, b: { key: string }) => a.key.localeCompare(b.key)
				expect(placed.sort(byKey), topic).toEqual(expected.sort(byKey))
			}
		} finally {
			await app.close().catch(() => {})
		}
	}, 60_000)
})
