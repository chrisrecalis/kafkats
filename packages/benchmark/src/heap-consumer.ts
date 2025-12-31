import { mkdirSync } from 'node:fs'
import { join } from 'node:path'
import { writeHeapSnapshot } from 'node:v8'

import { Kafka as KafkaJs } from 'kafkajs'
import { KafkaClient, codec, topic } from '@kafkats/client'

import { createTopicWithAssignments, startKafkaCluster } from './kafka-cluster.js'
import { generateMessage, uniqueName } from './utils.js'

interface Args {
	messageCount: number
	messageSize: number
	partitionConcurrency: number
	outputDir: string
}

function parseArgs(argv: string[]): Args {
	const out: Args = {
		messageCount: 50_000,
		messageSize: 1024,
		partitionConcurrency: 3,
		outputDir: 'heaps/consumer',
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
			case 'partitionConcurrency': {
				const n = Number(value)
				if (Number.isFinite(n)) out.partitionConcurrency = n
				break
			}
			case 'outputDir':
				out.outputDir = value
				break
		}
	}

	return out
}

async function seed(brokers: string[], topicName: string, messageCount: number, messageSize: number): Promise<void> {
	const kafka = new KafkaJs({
		clientId: 'heap-consumer-seeder',
		brokers,
		logLevel: 0,
	})
	const producer = kafka.producer()
	await producer.connect()

	const message = generateMessage(messageSize)
	const batchSize = 1000

	for (let i = 0; i < messageCount; i += batchSize) {
		const messages: { key: string; value: string }[] = []
		for (let j = 0; j < batchSize && i + j < messageCount; j++) {
			messages.push({ key: String(i + j), value: message })
		}
		await producer.send({ topic: topicName, messages })
	}

	await producer.disconnect()
}

const args = parseArgs(process.argv.slice(2))
const cluster = await startKafkaCluster({ brokerCount: 3 })

try {
	const topicName = uniqueName('heap-consumer-kafkats')
	await createTopicWithAssignments(cluster, topicName, 3)
	await seed(cluster.brokers, topicName, args.messageCount, args.messageSize)

	const client = new KafkaClient({
		brokers: cluster.brokers,
		clientId: 'heap-kafkats-consumer',
		logLevel: 'silent',
	})
	await client.connect()

	const testTopic = topic<string>(topicName, { value: codec.string() })
	const consumer = client.consumer({
		groupId: uniqueName('heap-group-kafkats'),
		autoOffsetReset: 'earliest',
		maxWaitMs: 50,
		minBytes: 1,
		maxBytesPerPartition: 1048576,
		isolationLevel: 'read_committed',
	})

	let messagesConsumed = 0
	const abortController = new AbortController()

	const runPromise = consumer.runEach(
		testTopic,
		() => {
			messagesConsumed++
			if (messagesConsumed >= args.messageCount) {
				abortController.abort()
			}
			return Promise.resolve()
		},
		{ signal: abortController.signal, partitionConcurrency: args.partitionConcurrency }
	)

	// Wait for consumer to be running
	await new Promise<void>((resolve, reject) => {
		consumer.once('running', () => resolve())
		consumer.once('error', err => reject(err))
	})

	await runPromise.catch(() => {})
	await client.disconnect()

	mkdirSync(args.outputDir, { recursive: true })
	;(globalThis as { gc?: () => void }).gc?.()
	const snapshotPath = join(args.outputDir, `heap-${Date.now()}.heapsnapshot`)
	writeHeapSnapshot(snapshotPath)
	console.log(`[kafkats consumer heap] wrote ${snapshotPath}`)
} finally {
	await cluster.stop()
}
