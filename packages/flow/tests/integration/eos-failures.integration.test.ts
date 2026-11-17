import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { InMemoryStateStoreProvider, buildChangelogTopicName, codec, flow, type FlowApp } from '../../src/index.js'
import {
	createTopics,
	injectCommitFailures,
	newClient,
	requireKafkaBrokers,
	sleep,
	uniqueTopicName,
	waitUntil,
} from './test-helpers.js'

const stringCodec = codec.string()
const numberCodec = codec.json<number>()
/** A plain (non-Kafka) error thrown from a transaction's sendOffsets. */
const sendOffsetsFailure = () => new Error('injected sendOffsets failure')

/** Read every committed record of a topic (read_committed) until `idleMs` passes without a new one. */
async function readCommitted(
	client: KafkaClient,
	topic: string,
	opts: { minCount?: number; timeoutMs?: number; idleMs?: number } = {}
): Promise<Array<{ key: string | null; value: number; partition: number; offset: bigint }>> {
	const out: Array<{ key: string | null; value: number; partition: number; offset: bigint }> = []
	const consumer = client.consumer({
		groupId: uniqueTopicName('eos-reader'),
		autoOffsetReset: 'earliest',
		isolationLevel: 'read_committed',
		maxWaitMs: 100,
	})
	const ready = new Promise<void>(r => consumer.once('running', () => r()))
	const run = consumer.runEach(
		[
			{
				topic,
				decoder: (b: Buffer) => numberCodec.decode(b),
				keyDecoder: (b: Buffer) => stringCodec.decode(b),
			},
		],
		async m => {
			out.push({
				key: m.key as unknown as string | null,
				value: m.value as unknown as number,
				partition: m.partition,
				offset: m.offset,
			})
		}
	)
	await ready
	const deadline = Date.now() + (opts.timeoutMs ?? 8_000)
	let lastLen = -1
	let lastChange = Date.now()
	while (Date.now() < deadline) {
		if (out.length !== lastLen) {
			lastLen = out.length
			lastChange = Date.now()
		}
		if (out.length >= (opts.minCount ?? 0) && Date.now() - lastChange > (opts.idleMs ?? 2_000)) break
		await sleep(50)
	}
	consumer.stop()
	await run.catch(() => {})
	return out
}

describe('EOS failure paths', () => {
	let client: KafkaClient
	let producer: Producer

	beforeAll(async () => {
		client = newClient('eos-eos')
		await client.connect()
		producer = client.producer({ lingerMs: 0 })
	})

	afterAll(async () => {
		await producer.disconnect()
		await client.disconnect()
	})

	const eosCfg = (applicationId: string, extra: Record<string, unknown> = {}) => ({
		applicationId,
		client,
		processingGuarantee: 'exactly_once' as const,
		commitIntervalMs: 300,
		consumer: {
			autoOffsetReset: 'earliest' as const,
			maxWaitMs: 50,
			heartbeatIntervalMs: 500,
			sessionTimeoutMs: 10_000,
			rebalanceTimeoutMs: 10_000,
		},
		producer: { lingerMs: 0, retries: 10, transactionTimeoutMs: 10_000 },
		...extra,
	})

	it(
		'failed timer-driven commit (sendOffsets error) does not lose or duplicate the aborted batch',
		{ timeout: 60_000 },
		async () => {
			const input = uniqueTopicName('eos-t1-in')
			const output = uniqueTopicName('eos-t1-out')
			await createTopics(client, [{ name: input }, { name: output }])

			// Private client so patching producer.transaction() does not affect other tests.
			const c2 = newClient('eos-t1')
			let failNextSendOffsets = false
			let failures = 0
			injectCommitFailures(
				c2,
				() => {
					if (!failNextSendOffsets) return false
					failNextSendOffsets = false
					failures++
					return true
				},
				sendOffsetsFailure
			)

			const app = flow(eosCfg(uniqueTopicName('eos-t1-app'), { client: c2 }))
			app.stream(input, { key: stringCodec, value: numberCodec })
				.mapValues(v => v)
				.to(output, { key: stringCodec, value: numberCodec })

			await app.start()
			try {
				// batch A is processed and committed
				await producer.send(
					input,
					[0, 1, 2].map(i => ({ key: 'k', value: numberCodec.encode(i) }))
				)
				await readCommitted(client, output, { minCount: 3, timeoutMs: 10_000, idleMs: 500 })

				// arm failure; batch B gets processed in a txn whose timer commit will fail
				failNextSendOffsets = true
				await producer.send(
					input,
					[3, 4, 5].map(i => ({ key: 'k', value: numberCodec.encode(i) }))
				)
				await waitUntil('injected failure', () => failures > 0, 10_000)
				await sleep(300)
				// batch C after the failure; app is (probably) still consuming
				await producer.send(
					input,
					[6, 7, 8].map(i => ({ key: 'k', value: numberCodec.encode(i) }))
				)
				await sleep(2_000)

				const got = await readCommitted(client, output, { minCount: 9, timeoutMs: 10_000, idleMs: 2_000 })
				const values = got.map(r => r.value).sort((a, b) => a - b)
				expect(values).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8])
			} finally {
				await app.close().catch(() => {})
				await c2.disconnect().catch(() => {})
			}
		}
	)

	it(
		'after a failed timer commit, stateful counts stay consistent with committed output',
		{ timeout: 60_000 },
		async () => {
			const input = uniqueTopicName('eos-t1b-in')
			const output = uniqueTopicName('eos-t1b-out')
			await createTopics(client, [{ name: input }, { name: output }])
			const c2 = newClient('eos-t1b')
			let arm = false
			let failures = 0
			injectCommitFailures(
				c2,
				() => {
					if (!arm) return false
					arm = false
					failures++
					return true
				},
				sendOffsetsFailure
			)
			const app = flow(eosCfg(uniqueTopicName('eos-t1b-app'), { client: c2 }))
			app.stream(input, { key: stringCodec, value: numberCodec })
				.groupByKey()
				.count({ storeName: 'counts', key: stringCodec, value: numberCodec })
				.toStream()
				.to(output, { key: stringCodec, value: numberCodec })
			await app.start()
			const send3 = (base: number) =>
				producer.send(
					input,
					[0, 1, 2].map(i => ({ key: 'x', value: numberCodec.encode(base + i) }))
				)
			try {
				await send3(0)
				await readCommitted(client, output, { minCount: 3, timeoutMs: 10_000, idleMs: 500 })
				arm = true
				await send3(3)
				await waitUntil('injected failure', () => failures > 0, 10_000)
				await sleep(300)
				await send3(6)
				await sleep(2_000)
				const got = await readCommitted(client, output, { minCount: 9, timeoutMs: 10_000, idleMs: 2_000 })
				// Correct exactly-once behaviour: every input record is reflected exactly once: 1..9
				expect(got.map(r => r.value)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9])
			} finally {
				await app.close().catch(() => {})
				await c2.disconnect().catch(() => {})
			}
		}
	)

	// A user-function throw aborts the txn and puts the app in ERROR without redelivery; after a restart the
	// aborted batch is reprocessed and committed output is each record exactly once.
	it(
		'handler throw mid-batch halts processing; restart yields exactly-once committed output',
		{ timeout: 60_000 },
		async () => {
			const input = uniqueTopicName('eos-t2-in')
			const output = uniqueTopicName('eos-t2-out')
			await createTopics(client, [{ name: input }, { name: output }])
			const appId = uniqueTopicName('eos-t2-app')
			let thrown = false
			const seen: number[] = []
			const build = () => {
				const app = flow(eosCfg(appId, { commitIntervalMs: 2_000 }))
				app.stream(input, { key: stringCodec, value: numberCodec })
					.mapValues(v => {
						seen.push(v as number)
						if (v === 5 && !thrown) {
							thrown = true
							throw new Error('boom at 5')
						}
						return v
					})
					.to(output, { key: stringCodec, value: numberCodec })
				return app
			}

			await producer.send(
				input,
				Array.from({ length: 10 }, (_, i) => ({ key: 'k', value: numberCodec.encode(i) }))
			)
			const app = build()
			await app.start()
			await waitUntil('ERROR', () => app.state() === 'ERROR', 10_000)
			await sleep(2_500)
			expect(seen).toEqual([0, 1, 2, 3, 4, 5])
			await app.close().catch(() => {})

			const app2 = build()
			await app2.start()
			try {
				const got = await readCommitted(client, output, { minCount: 10, timeoutMs: 15_000, idleMs: 2_500 })
				expect(got.map(r => r.value)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
			} finally {
				await app2.close().catch(() => {})
			}
		}
	)

	/** Persistent-store stand-in: per-partition stores (`<name>.p<N>`) keep their data across close()/restart. */
	class SurvivingProvider extends InMemoryStateStoreProvider {
		private readonly kv = new Map<string, any>()
		override createKeyValueStore<K, V>(name: string, options: any): any {
			let store = this.kv.get(name)
			if (!store) {
				store = super.createKeyValueStore<K, V>(name, options)
				store.close = async () => {} // "persistent": close() keeps the data
				this.kv.set(name, store)
			}
			return store
		}
		override async close(): Promise<void> {}
	}

	it(
		'aborted txn leaves no state mutations behind (restart with a surviving store)',
		{ timeout: 60_000 },
		async () => {
			const input = uniqueTopicName('eos-t3-in')
			const output = uniqueTopicName('eos-t3-out')
			await createTopics(client, [{ name: input }, { name: output }])
			const appId = uniqueTopicName('eos-t3-app')
			const provider = new SurvivingProvider()
			let failedOnce = false

			const build = () => {
				const app = flow(eosCfg(appId, { stateStoreProvider: provider, commitIntervalMs: 100 }))
				app.stream(input, { key: stringCodec, value: numberCodec })
					.groupByKey()
					.count({ storeName: 'counts', key: stringCodec, value: numberCodec })
					.toStream()
					.peek(() => {
						if (!failedOnce) {
							failedOnce = true
							throw new Error('transient failure after state mutation')
						}
					})
					.to(output, { key: stringCodec, value: numberCodec })
				return app
			}

			await producer.send(input, [
				{ key: 'x', value: numberCodec.encode(1) },
				{ key: 'x', value: numberCodec.encode(2) },
			])
			const app1 = build()
			await app1.start()
			await waitUntil('app1 ERROR', () => app1.state() === 'ERROR', 15_000)
			await app1.close().catch(() => {})

			const app2 = build()
			await app2.start()
			try {
				const got = await readCommitted(client, output, { minCount: 2, timeoutMs: 15_000, idleMs: 2_000 })
				expect(got.map(r => r.value)).toEqual([1, 2])
			} finally {
				await app2.close().catch(() => {})
			}
		}
	)

	class RecordingCheckpointProvider extends InMemoryStateStoreProvider {
		readonly checkpoints = new Map<string, bigint>()
		readonly setLog: Array<[string, bigint]> = []
		getChangelogCheckpointStore(): any {
			return {
				get: async (t: string, p: number) => this.checkpoints.get(`${t}:${p}`),
				set: async (t: string, p: number, o: bigint) => {
					this.checkpoints.set(`${t}:${p}`, o)
					this.setLog.push([`${t}:${p}`, o])
				},
			}
		}
	}

	async function checkpointVsLog(guarantee: 'exactly_once' | 'at_least_once') {
		const input = uniqueTopicName('eos-t4-in')
		const output = uniqueTopicName('eos-t4-out')
		await createTopics(client, [
			{ name: input, partitions: 3 },
			{ name: output, partitions: 3 },
		])
		const appId = uniqueTopicName('eos-t4-app')
		const provider = new RecordingCheckpointProvider()
		const app = flow({
			...eosCfg(appId, { stateStoreProvider: provider, commitIntervalMs: 100 }),
			processingGuarantee: guarantee,
		})
		app.stream(input, { key: stringCodec, value: numberCodec })
			.groupByKey()
			.count({ storeName: 'counts', key: stringCodec, value: numberCodec })
			.toStream()
			.to(output, { key: stringCodec, value: numberCodec })
		const msgs = Array.from({ length: 30 }, (_, i) => ({
			key: `k${i % 12}`,
			value: numberCodec.encode(i),
		}))
		await app.start()
		try {
			// several separate commits
			for (let r = 0; r < 3; r++) {
				await producer.send(input, msgs.slice(r * 10, r * 10 + 10))
				await sleep(500)
			}
			await readCommitted(client, output, { minCount: 30, timeoutMs: 15_000, idleMs: 800 })
			await sleep(500)
			const cl = buildChangelogTopicName(appId, 'counts')
			const admin = client.admin()
			const leo = await admin.fetchTopicOffsets(cl, [0, 1, 2], 'latest')
			const lso = await admin.fetchTopicOffsets(cl, [0, 1, 2], 'latest', { isolationLevel: 'read_committed' })
			const rows = await readCommitted(client, cl, { minCount: 30, timeoutMs: 10_000, idleMs: 800 }).catch(
				() => []
			)
			const lastData = new Map<number, bigint>()
			for (const r of rows) {
				const prev = lastData.get(r.partition)
				if (prev === undefined || r.offset > prev) lastData.set(r.partition, r.offset)
			}
			const problems: string[] = []
			for (const p of [0, 1, 2]) {
				const cp = provider.checkpoints.get(`${cl}:${p}`)
				const ld = lastData.get(p)
				// correct checkpoint = next offset after last applied data record, i.e. within (lastData, LEO]
				if (leo.get(p) === 0n) continue
				if (cp === undefined || ld === undefined || cp <= ld || cp > leo.get(p)!)
					problems.push(`p${p}: lastData=${ld} checkpoint=${cp} LEO=${leo.get(p)} LSO=${lso.get(p)}`)
			}
			return problems
		} finally {
			await app.close().catch(() => {})
		}
	}

	it('EOS changelog checkpoint lies in (lastData, LEO]', { timeout: 60_000 }, async () => {
		expect(await checkpointVsLog('exactly_once')).toEqual([])
	})
	it('ALO changelog checkpoint lies in (lastData, LEO]', { timeout: 60_000 }, async () => {
		expect(await checkpointVsLog('at_least_once')).toEqual([])
	})

	it(
		'counts stay exact (no skip/repeat in read_committed output) across rebalances with 2 EOS instances',
		{ timeout: 120_000 },
		async () => {
			const PARTS = 4
			const KEYS = 12
			const PER_KEY = 30
			const input = uniqueTopicName('eos-t5-in')
			const output = uniqueTopicName('eos-t5-out')
			await createTopics(client, [
				{ name: input, partitions: PARTS },
				{ name: output, partitions: PARTS },
			])
			const appId = uniqueTopicName('eos-t5-app')

			const build = () => {
				const app = flow(
					eosCfg(appId, {
						// each instance needs its own KafkaClient (a shared client would share one coordinator connection)
						client: {
							brokers: requireKafkaBrokers(),
							clientId: `eos-t5-${Math.random().toString(36).slice(2, 6)}`,
						},
						commitIntervalMs: 100,
						...(process.env.HUNT_ALO ? { processingGuarantee: 'at_least_once' } : {}),
						changelog: {
							restoration: {
								idleTimeoutMs: 300,
								initialIdleTimeoutMs: 5_000,
								checkIntervalMs: 50,
								consumerMaxWaitMs: 50,
							},
						},
					})
				)
				app.stream(input, { key: stringCodec, value: numberCodec })
					.groupByKey()
					.count({ storeName: 'counts', key: stringCodec, value: numberCodec })
					.toStream()
					.to(output, { key: stringCodec, value: numberCodec })
				return app
			}

			const sendRound = async (round: number) => {
				const msgs = Array.from({ length: KEYS }, (_, k) => ({
					key: `key-${k}`,
					value: numberCodec.encode(round),
				}))
				await producer.send(input, msgs)
			}

			const apps: FlowApp[] = []
			const a = build()
			apps.push(a)
			await a.start()
			let round = 0
			const pump = async (n: number, delay = 120) => {
				for (let i = 0; i < n; i++) {
					await sendRound(round++)
					await sleep(delay)
				}
			}
			try {
				await pump(5)
				const b = build()
				apps.push(b)
				await b.start() // join -> rebalance
				await pump(6)
				await a.close() // leave -> rebalance
				await pump(6)
				const c = build()
				apps.push(c)
				await c.start() // join again
				await pump(6)
				await b.close()
				await pump(PER_KEY - round)
				expect(round).toBe(PER_KEY)

				const total = KEYS * PER_KEY
				const got = await readCommitted(client, output, { minCount: total, timeoutMs: 40_000, idleMs: 4_000 })
				const byKey = new Map<string, number[]>()
				for (const r of got) {
					const l = byKey.get(r.key!) ?? []
					l.push(r.value)
					byKey.set(r.key!, l)
				}
				const problems: string[] = []
				for (let k = 0; k < KEYS; k++) {
					const seq = byKey.get(`key-${k}`) ?? []
					const expected = Array.from({ length: PER_KEY }, (_, i) => i + 1)
					if (JSON.stringify(seq) !== JSON.stringify(expected))
						problems.push(`key-${k}: ${JSON.stringify(seq)}`)
				}
				expect(problems).toEqual([])
			} finally {
				for (const x of apps) await x.close().catch(() => {})
			}
		}
	)

	it('failed commit inside onBeforeRebalance must not lose the aborted batch', { timeout: 90_000 }, async () => {
		const input = uniqueTopicName('eos-t7-in')
		const output = uniqueTopicName('eos-t7-out')
		await createTopics(client, [
			{ name: input, partitions: 2 },
			{ name: output, partitions: 2 },
		])
		const appId = uniqueTopicName('eos-t7-app')
		const c1 = newClient('eos-t7-1')
		const c2 = newClient('eos-t7-2')
		let arm = false
		let failures = 0
		injectCommitFailures(
			c1,
			() => {
				if (!arm) return false
				arm = false
				failures++
				return true
			},
			sendOffsetsFailure
		)
		const build = (c: KafkaClient) => {
			const app = flow(eosCfg(appId, { client: c, commitIntervalMs: 20_000 }))
			app.stream(input, { key: stringCodec, value: numberCodec })
				.mapValues(v => v)
				.to(output, { key: stringCodec, value: numberCodec })
			return app
		}
		const a = build(c1)
		const b = build(c2)
		await a.start()
		try {
			await producer.send(
				input,
				[0, 1, 2, 3, 4].map(i => ({ key: `k${i}`, value: numberCodec.encode(i) }))
			)
			await sleep(1_500) // processed, txn still open (20s commit interval)
			arm = true
			await b.start() // rebalance => A's onBeforeRebalance commit fails
			expect(failures).toBeGreaterThan(0)
			await producer.send(
				input,
				[5, 6, 7, 8, 9].map(i => ({ key: `k${i}`, value: numberCodec.encode(i) }))
			)
			await sleep(2_000)
			await a.close().catch(() => {})
			await b.close().catch(() => {})
			const got = await readCommitted(client, output, { minCount: 10, timeoutMs: 15_000, idleMs: 2_000 })
			const values = got.map(r => r.value).sort((x, y) => x - y)
			expect(values).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
		} finally {
			await a.close().catch(() => {})
			await b.close().catch(() => {})
			await c1.disconnect().catch(() => {})
			await c2.disconnect().catch(() => {})
		}
	})
})
