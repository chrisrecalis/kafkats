/**
 * Round 3 / attack 5 (targeted race): instance A is still restoring a big changelog (slow store) when B joins and then
 * leaves again, so A is assigned partitions 2/3 a second time while restoration #1 is still running. Restoration #1's
 * completion handler resumes every partition that is "still assigned" (flow.ts attachConsumerEvents), including the
 * re-assigned ones whose catch-up restoration (#3, which must replay B's work) is still queued, so A processes records on
 * stale state.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow, type FlowApp, type KeyValueStore, type KeyValueStoreOptions } from '../../src/index.js'
import { createTopics, uniqueTopicName } from './test-helpers.js'
import { DurableMemoryProvider, newClient, readAllCommitted, sleep } from './eos-test-util.js'

const str = codec.string()
const num = codec.json<number>()
const USERS = 2_500

class SlowProvider extends DurableMemoryProvider {
	slow = false
	override createKeyValueStore<K, V>(name: string, o: KeyValueStoreOptions<K, V>): KeyValueStore<K, V> {
		const inner = super.createKeyValueStore(name, o)
		const isSlow = () => this.slow
		return new Proxy(inner, {
			get(target, prop) {
				const v = Reflect.get(target, prop, target) as unknown
				if (prop === 'put') {
					return async (...args: unknown[]) => {
						if (isSlow()) await sleep(5)
						return (v as (...a: unknown[]) => Promise<void>).apply(target, args)
					}
				}
				return typeof v === 'function' ? (v as (...a: unknown[]) => unknown).bind(target) : v
			},
		})
	}
}

describe('hardening: restore race on re-assignment', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('r3-rr-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0, idempotent: true })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	it('exactly_once: partitions re-assigned while the first restoration is still running are not processed before their catch-up restore', async () => {
		const input = uniqueTopicName('r3-rr-in')
		const output = uniqueTopicName('r3-rr-out')
		const applicationId = uniqueTopicName('r3-rr-app')
		await createTopics(admin, [{ name: input, partitions: 4 }, { name: output }])
		const mk = (provider?: DurableMemoryProvider): FlowApp => {
			const app = flow({
				applicationId,
				client: newClient('r3-rr-app'),
				processingGuarantee: 'exactly_once',
				commitIntervalMs: 100,
				consumer: {
					autoOffsetReset: 'earliest',
					maxWaitMs: 50,
					sessionTimeoutMs: 10_000,
					rebalanceTimeoutMs: 20_000,
				},
				producer: { lingerMs: 0 },
				...(provider ? { stateStoreProvider: provider } : {}),
				changelog: { restoration: { idleTimeoutMs: 5_000, initialIdleTimeoutMs: 10_000, checkIntervalMs: 50 } },
			})
			app.stream(input, { key: str, value: str })
				.groupByKey()
				.count({ storeName: 'counts', key: str, value: num })
				.toStream()
				.to(output, { key: str, value: num })
			return app
		}
		const sendAll = async () => {
			for (let i = 0; i < USERS; i += 500)
				await producer.send(
					input,
					Array.from({ length: Math.min(500, USERS - i) }, (_, j) => ({
						key: `u${i + j}`,
						value: Buffer.from('x'),
					}))
				)
		}
		// phase 1: build a big changelog with a fast app, then stop it
		const pre = mk()
		await pre.start()
		await sendAll()
		const deadline = Date.now() + 40_000
		while (Date.now() < deadline && (await readAllCommitted(admin, output, 1_000)).length < USERS) await sleep(500)
		await pre.close()

		const slow = new SlowProvider()
		slow.slow = true
		const a = mk(slow)
		let b: FlowApp | null = null
		try {
			const T0 = Date.now()
			const lap = (m: string) => console.log(`[restore-race] +${Date.now() - T0}ms ${m}`)
			const startingA = a.start().then(() => lap('A.start resolved (restoration #1 done)'))
			await sleep(1_500)
			b = mk()
			lap('B.start begin')
			await b.start() // rebalance: A is revoked mid-restore and keeps only half of the partitions
			lap('B started')
			await sendAll() // phase 2: B advances partitions 2/3 (count 2 for those users)
			await sleep(2_500)
			lap('B.close begin')
			await b.close() // rebalance: A is assigned everything again, restoration #1 still running
			b = null
			lap('B closed')
			await sendAll() // phase 3: queued while A is still restoring; consumed as soon as A resumes the partitions
			await startingA
			slow.slow = false
			const want = [1, 2, 3]
			const seqs = new Map<string, number[]>()
			const done = async () => {
				seqs.clear()
				for (const r of await readAllCommitted(admin, output, 2_000)) {
					const k = r.key!.toString()
					seqs.set(k, [...(seqs.get(k) ?? []), num.decode(r.value!)])
				}
				return seqs.size === USERS && [...seqs.values()].every(v => v.length >= 3)
			}
			const end = Date.now() + 40_000
			while (!(await done()) && Date.now() < end) await sleep(1_000)
			const bad = [...seqs].filter(([, v]) => JSON.stringify(v) !== JSON.stringify(want)).slice(0, 10)
			expect(bad).toEqual([])
		} finally {
			await b?.close().catch(() => {})
			await a.close().catch(() => {})
		}
	}, 89_000)
})
