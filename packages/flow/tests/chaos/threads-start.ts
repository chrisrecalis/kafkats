/**
 * Repro: flow numStreamThreads:2 fails start() ("Consumer did not start within 10000ms") on a fresh app, no chaos needed.
 * Also tests two plain client consumers of one group on ONE KafkaClient (what numStreamThreads>1 does) to separate client vs flow.
 * Run (from packages/flow): pnpm exec tsx tests/chaos/threads-start.ts   (broker 127.0.0.1:29092 or CHAOS_BROKERS)
 */
import { KafkaClient } from '@kafkats/client'
import { codec, flow } from '../../src/index.js'
import { BROKERS, sleep } from './common.js'

const str = codec.string()
const t = Date.now().toString(36)
const c = new KafkaClient({ clientId: 'thr' + t, brokers: BROKERS })
await c.connect()
await c.createTopics([{ name: 'in-' + t, numPartitions: 4, replicationFactor: 1 }, { name: 'out-' + t, numPartitions: 4, replicationFactor: 1 }])
await sleep(1500)

// 1) plain client: two consumers, same group, same client
const times: number[] = []
const t0 = Date.now()
const cs = [0, 1].map(() => c.consumer({ groupId: 'g' + t, autoOffsetReset: 'earliest', maxWaitMs: 50 }))
const runs = cs.map(x => { x.once('running', () => times.push(Date.now() - t0)); return x.runEach('in-' + t, async () => {}).catch(() => {}) })
await sleep(12000)
console.log('plain client: both consumers running after ms:', JSON.stringify(times))
cs.forEach(x => x.stop()); await Promise.allSettled(runs)

// 2) flow with numStreamThreads: 2
for (const threads of [1, 2]) {
	const app = flow({ applicationId: `app${threads}-` + t, client: c, numStreamThreads: threads, consumer: { autoOffsetReset: 'earliest' } })
	app.stream('in-' + t, { key: str, value: str }).to('out-' + t, { key: str, value: str })
	const s = Date.now()
	try { await app.start(); console.log(`flow threads=${threads}: started in ${Date.now() - s}ms`) } catch (e) { console.log(`flow threads=${threads}: start() FAILED after ${Date.now() - s}ms: ${(e as Error).message}; state=${app.state()}`) }
	await app.close()
}
await c.disconnect()
