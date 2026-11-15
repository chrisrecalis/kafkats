// Client-level throughput: N single-message send() calls, then runEach over them.
import { mode } from './otel-mode.js'
import { KafkaClient } from '@kafkats/client'

const N = Number(process.env.N ?? 200_000)
const brokers = (process.env.KAFKA_BROKERS ?? '127.0.0.1:19092').split(',')
const unique = (p: string) => `${p}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`

const client = new KafkaClient({ brokers, clientId: 'otel-bench', logLevel: 'silent' })
await client.connect()
const topic = unique('otel-bench')
await client.createTopics([{ name: topic, numPartitions: 4, replicationFactor: 1 }])
await new Promise(r => setTimeout(r, 500))
const value = Buffer.alloc(100, 'x')

const producer = client.producer({ lingerMs: 0 })
await producer.send(topic, { key: 'warm', value })
let t = performance.now()
const pending: Promise<unknown>[] = []
for (let i = 0; i < N; i++) {
	pending.push(producer.send(topic, { key: `k${i % 1000}`, value }))
	if (pending.length === 2000) await Promise.all(pending.splice(0))
}
await Promise.all(pending)
const sendRate = (N / (performance.now() - t)) * 1000
await producer.disconnect()

const consumer = client.consumer({ groupId: unique('otel-bench-group'), autoOffsetReset: 'earliest' })
let seen = 0
let started = 0
// HANDLER: what the handler does per message. 'none' (default) measures the ceiling; 'cpu' parses
// and re-serializes a 1 KB JSON document; 'io' waits for a 1 ms timer, like a fast database call.
const work = process.env.HANDLER ?? 'none'
const document = JSON.stringify({
	id: 1,
	items: Array.from({ length: 40 }, (_, i) => ({ sku: `sku-${i}`, quantity: i })),
})
await consumer.runEach(
	topic,
	async () => {
		if (seen === 0) started = performance.now()
		if (work === 'cpu') JSON.stringify(JSON.parse(document))
		else if (work === 'io') await new Promise(resolve => setTimeout(resolve, 1))
		if (++seen === N + 1) consumer.stop()
	},
	{ partitionConcurrency: 4 }
)
const consumeRate = (N / (performance.now() - started)) * 1000
await client.disconnect()
console.log(
	`${mode.padEnd(8)} handler=${work.padEnd(4)} send ${Math.round(sendRate)} msg/s   runEach ${Math.round(consumeRate)} msg/s`
)
process.exit(0)
