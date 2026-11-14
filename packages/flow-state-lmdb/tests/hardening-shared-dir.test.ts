/**
 * Round 3 / attack 8: two apps with DIFFERENT applicationIds sharing one LMDB stateDir and the same store name.
 * LMDB database names are `<storeName>.p<partition>` only (no applicationId), so app B's start-up wipes/overwrites
 * app A's store, and A's (still valid) checkpoint then makes A trust B's data.
 * Run: KAFKA_BROKERS=127.0.0.1:19092 pnpm exec vitest run tests/hardening-shared-dir.test.ts
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { codec, flow } from '@kafkats/flow'
import { createTopic, makeClient, readCommitted, sleep, uniq } from './hardening-util.js'
import { lmdb } from '@/lmdb.js'

const brokers = process.env.KAFKA_BROKERS?.split(',').map(b => b.trim())
const str = codec.string()
const num = codec.json<number>()

describe.skipIf(!brokers)('hardening: apps with different applicationIds sharing one LMDB stateDir', () => {
	let client: any
	let producer: any
	beforeAll(async () => {
		client = await makeClient(brokers!)
		producer = client.producer({ lingerMs: 0 })
	})
	afterAll(async () => {
		await producer?.disconnect()
		await client?.disconnect()
	})

	const build = (appId: string, input: string, out: string, stateDir: string) => {
		const app = flow({
			applicationId: appId,
			client: { brokers: brokers!, clientId: uniq('r3-shared') },
			processingGuarantee: 'exactly_once',
			commitIntervalMs: 100,
			consumer: { maxWaitMs: 100, autoOffsetReset: 'earliest' },
			stateStoreProvider: lmdb({ stateDir }),
			changelog: { restoration: { idleTimeoutMs: 2_000, initialIdleTimeoutMs: 2_000, checkIntervalMs: 100 } },
		})
		app.stream(input, { key: str, value: str })
			.groupByKey()
			.count({ storeName: 'counts', key: str, value: num })
			.toStream()
			.to(out, { key: str, value: num })
		return app
	}

	async function feed(topic: string, n: number) {
		for (let i = 0; i < n; i++) await producer.send(topic, { key: 'k', value: Buffer.from('x') })
	}
	async function waitFor(out: string, n: number) {
		const deadline = Date.now() + 20_000
		let vals: number[] = []
		while (Date.now() < deadline) {
			vals = (await readCommitted(client, out, 800)).map(r => r.value as number)
			if (vals.length >= n) return vals
			await sleep(300)
		}
		return vals
	}

	it("app B on the same stateDir must not corrupt app A's state (A keeps counting 1..4)", async () => {
		const stateDir = mkdtempSync(join(tmpdir(), 'r3-shared-'))
		const inA = uniq('r3s-inA')
		const outA = uniq('r3s-outA')
		const inB = uniq('r3s-inB')
		const outB = uniq('r3s-outB')
		await Promise.all([inA, outA, inB, outB].map(t => createTopic(client, t, 1)))
		try {
			const idA = uniq('r3s-appA')
			let a = build(idA, inA, outA, stateDir)
			await a.start()
			await feed(inA, 3)
			expect(await waitFor(outA, 3)).toEqual([1, 2, 3])
			await a.close()
			// B: other application, same store name, same directory
			const b = build(uniq('r3s-appB'), inB, outB, stateDir)
			await b.start()
			await feed(inB, 1)
			expect(await waitFor(outB, 1)).toEqual([1])
			await b.close()
			// A returns
			a = build(idA, inA, outA, stateDir)
			await a.start()
			await feed(inA, 1)
			const vals = await waitFor(outA, 4)
			await a.close()
			expect(vals).toEqual([1, 2, 3, 4])
		} finally {
			rmSync(stateDir, { recursive: true, force: true })
		}
	}, 80_000)
})
