import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow, type FlowApp } from '../../src/index.js'
import { createTopics, uniqueTopicName } from './test-helpers.js'
import { DurableMemoryProvider, injectCommitFailures, newClient, readAllCommitted, sleep } from './round2-util.js'

const stringCodec = codec.string()
const numberCodec = codec.json<number>()
const KEYS = 12
const BATCHES = 50

function sequences(msgs: Awaited<ReturnType<typeof readAllCommitted>>): Record<string, number[]> {
	const out: Record<string, number[]> = {}
	for (const m of msgs) (out[m.key!.toString()] ??= []).push(numberCodec.decode(m.value!))
	return out
}

describe('round2: exactly_once exactness across rebalances and commit failures', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('r2-rb-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	const scenario = async (durable: boolean) => {
		const input = uniqueTopicName('r2-rb-in')
		const outKey = uniqueTopicName('r2-rb-out-key')
		const outNew = uniqueTopicName('r2-rb-out-new')
		const applicationId = uniqueTopicName('r2-rb-app')
		await createTopics(admin, [{ name: input, partitions: 4 }, { name: outKey }, { name: outNew }])

		let commits = 0
		const providers = [new DurableMemoryProvider(), new DurableMemoryProvider()]
		const build = (slot: number): FlowApp => {
			const client = newClient('r2-rb-app')
			injectCommitFailures(client, () => {
				commits += 1
				return commits <= 60 && commits % 9 === 0
			})
			const app = flow({
				applicationId,
				client,
				processingGuarantee: 'exactly_once',
				...(durable ? { stateStoreProvider: providers[slot]! } : {}),
				commitIntervalMs: 80,
				consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
				producer: { lingerMs: 0 },
				changelog: { restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 5_000, checkIntervalMs: 50 } },
			})
			const src = app.stream(input, { key: stringCodec, value: numberCodec })
			src.groupByKey()
				.count({ storeName: 'by-key', key: stringCodec, value: numberCodec })
				.toStream()
				.to(outKey, {
					key: stringCodec,
					value: numberCodec,
				})
			src.groupBy((k, _v) => `n${Number((k as string).slice(1)) % 3}`, { key: stringCodec, value: numberCodec })
				.count({ storeName: 'by-new', key: stringCodec, value: numberCodec })
				.toStream()
				.to(outNew, { key: stringCodec, value: numberCodec })
			return app
		}

		let a1 = build(0)
		let a2: FlowApp | undefined
		try {
			await a1.start()
			for (let b = 0; b < BATCHES; b += 1) {
				if (b === 12) {
					a2 = build(1)
					void a2.start().catch(() => {})
				}
				if (b === 22 && durable) {
					// restart instance 1 on top of its surviving local state
					await a1.close().catch(() => {})
					a1 = build(0)
					void a1.start().catch(() => {})
				}
				if (b === 32 && a2) await a2.close().catch(() => {})
				await producer.send(
					input,
					Array.from({ length: KEYS }, (_, i) => ({ key: `k${i}`, value: numberCodec.encode(1) }))
				)
				await sleep(150)
			}
			const wantKey = BATCHES
			const wantNew = (BATCHES * KEYS) / 3
			const deadline = Date.now() + 40_000
			let byKey: Record<string, number[]> = {}
			let byNew: Record<string, number[]> = {}
			for (;;) {
				byKey = sequences(await readAllCommitted(admin, outKey, 1_000))
				byNew = sequences(await readAllCommitted(admin, outNew, 1_000))
				const done =
					Object.keys(byKey).length === KEYS &&
					Object.values(byKey).every(s => s[s.length - 1] === wantKey) &&
					Object.values(byNew).every(s => s[s.length - 1] === wantNew)
				if (done || Date.now() > deadline) break
			}
			expect(commits).toBeGreaterThan(9) // failures were actually injected
			for (const [k, seq] of Object.entries(byKey)) {
				expect(seq, `byKey ${k}`).toEqual(Array.from({ length: wantKey }, (_, i) => i + 1))
			}
			for (const [k, seq] of Object.entries(byNew)) {
				expect(seq, `byNew ${k}`).toEqual(Array.from({ length: wantNew }, (_, i) => i + 1))
			}
			expect(Object.keys(byNew).sort()).toEqual(['n0', 'n1', 'n2'])
		} finally {
			await a1.close().catch(() => {})
			await a2?.close().catch(() => {})
		}
	}

	it('in-memory state: counts are exact while instances join, leave and commits fail', () => scenario(false), 88_000)
	it(
		'durable local state + checkpoints: counts are exact while instances join, leave, restart and commits fail',
		() => scenario(true),
		88_000
	)
})
