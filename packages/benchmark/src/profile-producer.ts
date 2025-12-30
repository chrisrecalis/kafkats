import { KafkaClient, codec, topic } from '@kafkats/client'

import { createTopicWithAssignments, startKafkaCluster } from './kafka-cluster.js'
import { generateMessage, uniqueName } from './utils.js'

interface Args {
	messageCount: number
	messageSize: number
	batchSize: number
}

function parseArgs(argv: string[]): Args {
	const out: Args = {
		messageCount: 50_000,
		messageSize: 1024,
		batchSize: 500,
	}

	for (let i = 0; i < argv.length; i++) {
		const raw = argv[i]
		if (!raw) continue
		if (!raw.startsWith('--')) continue
		const [key, inlineValue] = raw.slice(2).split('=', 2)
		const value = inlineValue ?? argv[i + 1]
		if (!value || value.startsWith('--')) continue

		const n = Number(value)
		if (!Number.isFinite(n)) continue

		switch (key) {
			case 'messageCount':
				out.messageCount = n
				break
			case 'messageSize':
				out.messageSize = n
				break
			case 'batchSize':
				out.batchSize = n
				break
		}
	}

	return out
}

const args = parseArgs(process.argv.slice(2))
const cluster = await startKafkaCluster({ brokerCount: 3 })

try {
	const topicName = uniqueName('profile-producer-kafkats')
	await createTopicWithAssignments(cluster, topicName, 3)

	const client = new KafkaClient({
		brokers: cluster.brokers,
		clientId: 'profile-kafkats-producer',
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
	const start = performance.now()

	for (let i = 0; i < args.messageCount; i += args.batchSize) {
		const batch: Array<{ key: string; value: string }> = []
		for (let j = 0; j < args.batchSize && i + j < args.messageCount; j++) {
			batch.push({ key: String(i + j), value: message })
		}
		await producer.send(testTopic, batch)
	}

	const end = performance.now()
	await producer.disconnect()
	await client.disconnect()

	const durationMs = end - start
	console.log(
		`[kafkats producer profile] ${args.messageCount} msgs (${args.messageSize}B) in ${durationMs.toFixed(2)}ms -> ${Math.round((args.messageCount / durationMs) * 1000)} msg/s`
	)
} finally {
	await cluster.stop()
}
