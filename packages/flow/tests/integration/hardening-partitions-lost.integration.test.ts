/**
 * Round 3 / attack 4: a worker loses its group session (event loop blocked longer than sessionTimeoutMs inside a user
 * function) while a second instance takes over. No loss (ALO) / exactly-once output (EOS); the stalled instance must
 * recover by itself and report RUNNING again; no unhandled rejections.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow, type FlowApp } from '../../src/index.js'
import {
	collectUnhandledRejections,
	createTopics,
	newClient,
	readAllCommitted,
	sleep,
	uniqueTopicName,
} from './test-helpers.js'

const str = codec.string()
const num = codec.json<number>()
const KEYS = 8
const PER_KEY = 50

describe('hardening: session loss (blocked event loop) with a second instance', () => {
	let admin: KafkaClient
	let producer: Producer
	const rejections = collectUnhandledRejections()
	beforeAll(async () => {
		admin = newClient('r3-pl-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0, idempotent: true })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	for (const guarantee of ['exactly_once', 'at_least_once'] as const) {
		it(`${guarantee}: stalled instance (event loop blocked > sessionTimeoutMs) recovers, output is exact / lossless`, async () => {
			const input = uniqueTopicName('r3-pl-in')
			const output = uniqueTopicName('r3-pl-out')
			const applicationId = uniqueTopicName('r3-pl-app')
			await createTopics(admin, [{ name: input, partitions: 4 }, { name: output }])
			let seen = 0
			let blocked = false
			const build = (stall: boolean): FlowApp => {
				const app = flow({
					applicationId,
					client: newClient('r3-pl-app'),
					processingGuarantee: guarantee,
					commitIntervalMs: 100,
					consumer: {
						autoOffsetReset: 'earliest',
						maxWaitMs: 50,
						sessionTimeoutMs: 6_000,
						heartbeatIntervalMs: 1_000,
						rebalanceTimeoutMs: 10_000,
					},
					producer: { lingerMs: 0 },
					changelog: {
						restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 5_000, checkIntervalMs: 50 },
					},
				})
				app.stream(input, { key: str, value: str })
					.peek(() => {
						if (stall && !blocked && ++seen === 40) {
							blocked = true
							const end = Date.now() + 9_000
							while (Date.now() < end) {
								/* synchronous busy wait: no heartbeats, no I/O */
							}
						}
					})
					.groupByKey()
					.count({ storeName: 'counts', key: str, value: num })
					.toStream()
					.to(output, { key: str, value: num })
				return app
			}
			const a = build(true)
			// Both apps share this process, so the busy wait stalls both. Either may trigger it:
			// which one sees the 40th record depends on the partition assignment.
			const b = build(true)
			try {
				await Promise.all([a.start(), b.start()])
				for (let s = 0; s < PER_KEY; s++) {
					await producer.send(
						input,
						Array.from({ length: KEYS }, (_, k) => ({ key: `k${k}`, value: Buffer.from('x') }))
					)
					await sleep(250)
				}
				expect(blocked).toBe(true)
				// the stalled instance must come back by itself
				const deadline = Date.now() + 40_000
				while (Date.now() < deadline && a.state() !== 'RUNNING') await sleep(500)
				const finals = new Map<string, number>()
				const seqs = new Map<string, number[]>()
				let rows = await readAllCommitted(admin, output, 2_000)
				const done = () => {
					finals.clear()
					seqs.clear()
					for (const r of rows) {
						const k = r.key!.toString()
						const v = num.decode(r.value!)
						finals.set(k, Math.max(finals.get(k) ?? 0, v))
						seqs.set(k, [...(seqs.get(k) ?? []), v])
					}
					return [...finals.values()].filter(v => v >= PER_KEY).length === KEYS
				}
				while (!done() && Date.now() < deadline) {
					await sleep(1_000)
					rows = await readAllCommitted(admin, output, 2_000)
				}
				expect(done(), JSON.stringify([...finals])).toBe(true)
				if (guarantee === 'exactly_once') {
					const want = Array.from({ length: PER_KEY }, (_, i) => i + 1)
					for (let k = 0; k < KEYS; k++) expect(seqs.get(`k${k}`)).toEqual(want)
				}
				expect(a.state()).toBe('RUNNING')
				expect(b.state()).toBe('RUNNING')
				expect(rejections).toEqual([])
			} finally {
				await a.close().catch(() => {})
				await b.close().catch(() => {})
			}
		}, 88_000)
	}
})
