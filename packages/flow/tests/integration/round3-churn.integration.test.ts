/**
 * Round 3 / attack 5: join/leave churn while a (deliberately slow) store is restoring, EOS count(): output must be
 * exactly 1..N per key, every input record must eventually be consumed (no partition stuck paused), and no unhandled
 * rejection may escape.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow, type FlowApp, type KeyValueStore, type KeyValueStoreOptions } from '../../src/index.js'
import { createTopics, uniqueTopicName } from './test-helpers.js'
import { DurableMemoryProvider, newClient, readAllCommitted, sleep } from './eos-test-util.js'

const str = codec.string()
const num = codec.json<number>()
const KEYS = 8
const PER_KEY = 120

class SlowProvider extends DurableMemoryProvider {
	constructor(private readonly putDelayMs: number) {
		super()
	}
	override createKeyValueStore<K, V>(name: string, o: KeyValueStoreOptions<K, V>): KeyValueStore<K, V> {
		const inner = super.createKeyValueStore(name, o)
		const delay = this.putDelayMs
		return new Proxy(inner, {
			get(target, prop) {
				const v = Reflect.get(target, prop, target) as unknown
				if (prop === 'put') {
					return async (...args: unknown[]) => {
						await sleep(delay)
						return (v as (...a: unknown[]) => Promise<void>).apply(target, args)
					}
				}
				return typeof v === 'function' ? (v as (...a: unknown[]) => unknown).bind(target) : v
			},
		})
	}
}

describe('round3: churn during slow restoration', () => {
	let admin: KafkaClient
	let producer: Producer
	const rejections: unknown[] = []
	const onRej = (r: unknown) => rejections.push(r)
	beforeAll(async () => {
		process.on('unhandledRejection', onRej)
		admin = newClient('r3-ch-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0, idempotent: true })
	})
	afterAll(async () => {
		process.off('unhandledRejection', onRej)
		await producer.disconnect()
		await admin.disconnect()
	})

	for (const durable of [true, false]) {
		it(`exactly_once: 6 join/leave cycles with ${durable ? 'durable' : 'in-memory'} slow store => exact counts, all input consumed`, async () => {
			const input = uniqueTopicName('r3-ch-in')
			const output = uniqueTopicName('r3-ch-out')
			const applicationId = uniqueTopicName('r3-ch-app')
			await createTopics(admin, [{ name: input, partitions: 6 }, { name: output }])
			const providers = [new SlowProvider(2), new SlowProvider(2)]
			const mk = (slot: number): FlowApp => {
				const app = flow({
					applicationId,
					client: newClient('r3-ch-app'),
					processingGuarantee: 'exactly_once',
					commitIntervalMs: 100,
					consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50, sessionTimeoutMs: 6_000, rebalanceTimeoutMs: 15_000 },
					producer: { lingerMs: 0 },
					...(durable ? { stateStoreProvider: providers[slot]! } : {}),
					changelog: { restoration: { idleTimeoutMs: 3_000, initialIdleTimeoutMs: 8_000, checkIntervalMs: 50 } },
				})
				app.stream(input, { key: str, value: str })
					.groupByKey()
					.count({ storeName: 'counts', key: str, value: num })
					.toStream()
					.to(output, { key: str, value: num })
				return app
			}
			let stop = false
			const producing = (async () => {
				for (let s = 0; s < PER_KEY && !stop; s++) {
					await producer.send(
						input,
						Array.from({ length: KEYS }, (_, k) => ({ key: `k${k}`, value: Buffer.from('x') }))
					)
					await sleep(300)
				}
			})()
			const a = mk(0)
			let b: FlowApp | null = null
			try {
				await a.start()
				for (let cycle = 0; cycle < 6; cycle++) {
					await sleep(500 + Math.random() * 2_000)
					b = mk(1)
					const starting = b.start().catch(() => {})
					await sleep(300 + Math.random() * 2_500) // leave while possibly still restoring
					await b.close().catch(() => {})
					await starting
					b = null
				}
				await producing
				const deadline = Date.now() + 40_000
				let seqs = new Map<string, number[]>()
				const complete = async () => {
					seqs = new Map()
					for (const r of await readAllCommitted(admin, output, 2_000)) {
						const k = r.key!.toString()
						seqs.set(k, [...(seqs.get(k) ?? []), num.decode(r.value!)])
					}
					return [...seqs.values()].filter(v => v.length >= PER_KEY).length === KEYS
				}
				while (!(await complete()) && Date.now() < deadline) await sleep(1_000)
				const want = Array.from({ length: PER_KEY }, (_, i) => i + 1)
				for (let k = 0; k < KEYS; k++) expect(seqs.get(`k${k}`), `k${k}`).toEqual(want)
				expect(a.state()).toBe('RUNNING')
				expect(rejections).toEqual([])
			} finally {
				stop = true
				await b?.close().catch(() => {})
				await a.close().catch(() => {})
			}
		}, 88_000)
	}
})
