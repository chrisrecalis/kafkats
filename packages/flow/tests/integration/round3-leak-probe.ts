/** Helper: count open sockets after N start()/close() cycles. Usage: KAFKA_BROKERS=... R3_MODE=stateless|count|nostart pnpm exec tsx tests/integration/round3-leak-probe.ts */
import { codec, flow } from '../../src/index.js'
import { KafkaClient } from '@kafkats/client'

const brokers = process.env.KAFKA_BROKERS!.split(',')
const mode = process.env.R3_MODE ?? 'count'
const str = codec.string()
const num = codec.json<number>()
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))
const socks = () =>
	(process as unknown as { _getActiveHandles(): Array<{ constructor: { name: string } }> })._getActiveHandles().filter(h => h.constructor.name === 'Socket').length

const admin = new KafkaClient({ clientId: 'probe-admin', brokers })
await admin.connect()
const suffix = Date.now().toString(36)
const input = `r3-probe-in-${suffix}`
const output = `r3-probe-out-${suffix}`
await admin.createTopics([
	{ name: input, numPartitions: Number(process.env.R3_PARTS ?? 2), replicationFactor: 1 },
	{ name: output, numPartitions: 1, replicationFactor: 1 },
])
await sleep(1500)
if (process.env.R3_DATA) {
	const p = admin.producer({ lingerMs: 0 })
	await p.send(input, Array.from({ length: 40 }, (_, i) => ({ key: `k${i % 8}`, value: Buffer.from('x') })))
	await p.disconnect()
}
if (process.env.R3_PEER) {
	const peer = flow({
		applicationId: `r3-probe-app-${suffix}`,
		client: { clientId: 'probe-peer', brokers },
		consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
		producer: { lingerMs: 0 },
	})
	peer.stream(input, { key: str, value: str }).groupByKey().count({ storeName: 'c', key: str, value: num }).toStream().to(output, { key: str, value: num })
	await peer.start()
	await sleep(1500)
}
console.log('baseline sockets', socks())
for (let i = 0; i < 4; i++) {
	const useOwned = process.env.R3_OWN !== '0'
	const client = useOwned ? { clientId: `probe-${i}`, brokers } : new KafkaClient({ clientId: `probe-${i}`, brokers })
	const app = flow({
		applicationId: `r3-probe-app-${suffix}`,
		client,
		processingGuarantee: (process.env.R3_EOS ? 'exactly_once' : 'at_least_once') as 'at_least_once' | 'exactly_once',
		consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50, ...(process.env.R3_FULL ? { sessionTimeoutMs: 6000, rebalanceTimeoutMs: 10000 } : {}) },
		producer: { lingerMs: 0 },
		...(process.env.R3_FULL ? { commitIntervalMs: 100, changelog: { restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 3_000, checkIntervalMs: 50 } } } : {}),
	})
	const s = app.stream(input, { key: str, value: str })
	if (mode === 'stateless') s.mapValues(v => v).to(output, { key: str, value: str })
	else s.groupByKey().count({ storeName: 'c', key: str, value: num }).toStream().to(output, { key: str, value: num })
	if (process.env.R3_RACE) {
		const started = app.start().then(() => 'ok', () => 'failed')
		await sleep(Number(process.env.R3_DMIN ?? 0) + Math.random() * Number(process.env.R3_DSPAN ?? 1800))
		await app.close()
		console.log('start settled as', await started, 'state', app.state())
	} else {
		if (mode !== 'nostart') await app.start()
		await sleep(1500)
		await app.close()
	}
	if (!useOwned) await (client as KafkaClient).disconnect()
	await sleep(1500)
	console.log(`after cycle ${i}: sockets`, socks())
}
await admin.disconnect()
process.exit(0)
