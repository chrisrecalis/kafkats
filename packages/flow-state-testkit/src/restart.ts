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
import { numberCodec } from './codecs.js'
import { createTopic, makeClient, sleep, uniq } from './kafka-util.js'
import type { EngineUnderTest } from './engine.js'

export function restartSuite(engine: EngineUnderTest): void {
	const brokers = process.env.KAFKA_BROKERS?.split(',').map(b => b.trim())
	const stringCodec = codec.string()

	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let client: any
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let producer: any

	/** Reads every message on a topic from the beginning (value decoded as number). */
	async function readAll(topic: string, quietMs = 1_500): Promise<Array<{ key: string; value: number }>> {
		const consumer = client.consumer({
			groupId: uniq('state-restart-reader'),
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

	describe.skipIf(!brokers)(`${engine.name}: flow restart cycles on`, () => {
		beforeAll(async () => {
			client = await makeClient(brokers!)
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
				client: { brokers: brokers!, clientId: uniq('state-restart-app') },
				processingGuarantee: guarantee,
				commitIntervalMs: 100,
				consumer: { maxWaitMs: 100 },
				stateStoreProvider: engine.create(stateDir),
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
					const input = uniq(`state-restart-in-${guarantee}`)
					const out = uniq(`state-restart-out-${guarantee}`)
					const appId = uniq(`state-restart-${guarantee}`)
					await createTopic(client, input, 1)
					await createTopic(client, out, 1)
					let stateDir = mkdtempSync(join(tmpdir(), 'state-restart-'))
					try {
						for (let cycle = 1; cycle <= 3; cycle++) {
							if (wipeState && cycle > 1) {
								rmSync(stateDir, { recursive: true, force: true })
								stateDir = mkdtempSync(join(tmpdir(), 'state-restart-'))
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
		it('the same app can be started again after close()', async () => {
			const input = uniq('state-reopen-in')
			const out = uniq('state-reopen-out')
			await createTopic(client, input, 1)
			await createTopic(client, out, 1)
			const stateDir = mkdtempSync(join(tmpdir(), 'state-reopen-'))
			try {
				const app = build(uniq('state-reopen'), input, out, stateDir, 'exactly_once')
				for (let cycle = 1; cycle <= 2; cycle++) {
					await app.start()
					for (let i = 0; i < 3; i++) await producer.send(input, { key: 'a', value: numberCodec.encode(1) })
					await waitForValue(out, cycle * 3)
					await app.close()
				}
			} finally {
				rmSync(stateDir, { recursive: true, force: true })
			}
			const unique = [...new Set((await readAll(out)).map(m => m.value))]
			expect(unique).toEqual([1, 2, 3, 4, 5, 6])
		}, 120_000)
	})
}
