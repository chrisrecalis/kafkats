import { mkdirSync } from 'node:fs'
import { join } from 'node:path'
import { writeHeapSnapshot } from 'node:v8'

import { KafkaClient, codec, topic } from '@kafkats/client'

import { createTopicWithAssignments, startKafkaCluster } from './kafka-cluster.js'
import { generateMessage, uniqueName } from './utils.js'

interface Args {
	messageCount: number
	messageSize: number
	batchSize: number
	outputDir: string
}

function parseArgs(argv: string[]): Args {
	const out: Args = {
		messageCount: 50_000,
		messageSize: 1024,
		batchSize: 500,
		outputDir: 'heaps/producer',
	}

	for (let i = 0; i < argv.length; i++) {
		const raw = argv[i]
		if (!raw) continue
		if (!raw.startsWith('--')) continue
		const [key, inlineValue] = raw.slice(2).split('=', 2)
		const value = inlineValue ?? argv[i + 1]
		if (!value || value.startsWith('--')) continue

		switch (key) {
			case 'messageCount': {
				const n = Number(value)
				if (Number.isFinite(n)) out.messageCount = n
				break
			}
			case 'messageSize': {
				const n = Number(value)
				if (Number.isFinite(n)) out.messageSize = n
				break
			}
			case 'batchSize': {
				const n = Number(value)
				if (Number.isFinite(n)) out.batchSize = n
				break
			}
			case 'outputDir':
				out.outputDir = value
				break
		}
	}

	return out
}

const args = parseArgs(process.argv.slice(2))
const cluster = await startKafkaCluster({ brokerCount: 3 })

try {
	const topicName = uniqueName('heap-producer-kafkats')
	await createTopicWithAssignments(cluster, topicName, 3)

	const client = new KafkaClient({
		brokers: cluster.brokers,
		clientId: 'heap-kafkats-producer',
		logLevel: 'silent',
	})
	await client.connect()

	const testTopic = topic<string, string>(topicName, { key: codec.string(), value: codec.string() })
	const producer = client.producer({
		lingerMs: 0,
		maxBatchBytes: Math.max(16384, args.messageSize * args.batchSize),
		acks: 'all',
		compression: 'none',
		idempotent: false,
		maxInFlight: 5,
		retries: 0,
	})

	const message = generateMessage(args.messageSize)
	for (let i = 0; i < args.messageCount; i += args.batchSize) {
		const batch: Array<{ key: string; value: string }> = []
		for (let j = 0; j < args.batchSize && i + j < args.messageCount; j++) {
			batch.push({ key: String(i + j), value: message })
		}
		await producer.send(testTopic, batch)
	}

	await producer.disconnect()
	await client.disconnect()

	mkdirSync(args.outputDir, { recursive: true })
	;(globalThis as { gc?: () => void }).gc?.()
	const snapshotPath = join(args.outputDir, `heap-${Date.now()}.heapsnapshot`)
	writeHeapSnapshot(snapshotPath)
	console.log(`[kafkats producer heap] wrote ${snapshotPath}`)
} finally {
	await cluster.stop()
}
