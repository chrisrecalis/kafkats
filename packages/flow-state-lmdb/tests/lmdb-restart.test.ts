/**
 * LMDB provider + flow restart/restoration cycles against a real broker.
 *
 * Requires: KAFKA_BROKERS (skipped otherwise) and a built @kafkats/flow (pnpm -C packages/flow build).
 * Run: KAFKA_BROKERS=127.0.0.1:19092 pnpm exec vitest run tests/lmdb-restart.test.ts
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { codec, flow } from '@kafkats/flow'
// @kafkats/client is not a dependency of this package; load the workspace build directly for topic admin + producing.
import { KafkaClient } from '../../client/dist/index.js'

import { lmdb } from '@/lmdb.js'

const brokers = process.env.KAFKA_BROKERS?.split(',').map(b => b.trim())
const stringCodec = codec.string()
const numberCodec = {
	encode: (v: number) => {
		const buf = Buffer.alloc(8)
		buf.writeDoubleLE(v, 0)
		return buf
	},
	decode: (b: Buffer) => b.readDoubleLE(0),
}

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))
const uniq = (p: string) => `${p}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let client: any
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let producer: any

async function createTopic(name: string) {
	await client.createTopics([{ name, numPartitions: 1, replicationFactor: 1 }])
	const deadline = Date.now() + 15_000
	while (Date.now() < deadline) {
		const meta = await client.getMetadata([name])
		const t = meta.topics.get(name)
		if (
			t &&
			[...t.partitions.values()].every((p: { leaderId: number }) => p.leaderId >= 0) &&
			t.partitions.size === 1
		)
			return
		await sleep(100)
	}
	throw new Error(`topic ${name} not ready`)
}

/** Reads every message on a topic from the beginning (value decoded as number). */
async function readAll(topic: string, quietMs = 1_500): Promise<Array<{ key: string; value: number }>> {
	const consumer = client.consumer({
		groupId: uniq('lmdb-restart-reader'),
		autoOffsetReset: 'earliest',
		maxWaitMs: 100,
	})
	const out: Array<{ key: string; value: number }> = []
	let last = Date.now()
	const run = consumer.runEach(topic, async (m: { key: Buffer | null; value: Buffer | null }) => {
		out.push({ key: m.key!.toString(), value: numberCodec.decode(m.value!) })
		last = Date.now()
	})
	await sleep(500)
	while (Date.now() - last < quietMs) await sleep(100)
	consumer.stop()
	await run
	return out
}

describe.skipIf(!brokers)('lmdb-restart: flow restart cycles on LMDB', () => {
	beforeAll(async () => {
		client = new KafkaClient({ clientId: uniq('lmdb-restart'), brokers: brokers! })
		await client.connect()
		producer = client.producer()
	})
	afterAll(async () => {
		await producer?.disconnect()
		await client?.disconnect()
	})

	function build(
		appId: string,
		input: string,
		out: string,
		stateDir: string,
		guarantee: 'at_least_once' | 'exactly_once'
	) {
		const app = flow({
			applicationId: appId,
			client: { brokers: brokers!, clientId: uniq('lmdb-restart-app') },
			numStreamThreads: 1,
			processingGuarantee: guarantee,
			commitIntervalMs: 100,
			consumer: { maxWaitMs: 100 },
			stateStoreProvider: lmdb({ stateDir }),
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
		return app
	}

	async function waitForValue(out: string, expected: number, timeoutMs = 15_000) {
		const deadline = Date.now() + timeoutMs
		while (Date.now() < deadline) {
			const all = await readAll(out, 600)
			if (all.some(m => m.value === expected)) return
		}
		throw new Error(`never saw ${expected} on ${out}`)
	}

	for (const guarantee of ['at_least_once', 'exactly_once'] as const) {
		for (const wipeState of [false, true]) {
			it(`counts continue exactly across restarts (${guarantee}, wipeLocalState=${wipeState})`, async () => {
				const input = uniq(`lmdb-restart-in-${guarantee}`)
				const out = uniq(`lmdb-restart-out-${guarantee}`)
				const appId = uniq(`lmdb-restart-${guarantee}`)
				await createTopic(input)
				await createTopic(out)
				let stateDir = mkdtempSync(join(tmpdir(), 'lmdb-restart-'))
				try {
					for (let cycle = 1; cycle <= 3; cycle++) {
						if (wipeState && cycle > 1) {
							rmSync(stateDir, { recursive: true, force: true })
							stateDir = mkdtempSync(join(tmpdir(), 'lmdb-restart-'))
						}
						const app = build(appId, input, out, stateDir, guarantee)
						await app.start()
						for (let i = 0; i < 3; i++)
							await producer.send(input, { key: 'a', value: numberCodec.encode(1) })
						await waitForValue(out, cycle * 3)
						await app.close()
					}
				} finally {
					rmSync(stateDir, { recursive: true, force: true })
				}
				const seen = (await readAll(out)).map(m => m.value)
				const unique = [...new Set(seen)]
				expect(unique).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9])
			}, 120_000)
		}
	}
})
