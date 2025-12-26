import { Kafka as KafkaJs, CompressionTypes } from 'kafkajs'
import { KafkaClient, codec, topic } from '@kafkats/client'
import { Producer as PlatformaticProducer, ProduceAcks, stringSerializers } from '@platformatic/kafka'

import { createTopicWithAssignments, startKafkaCluster, type StartedKafkaCluster } from './kafka-cluster.js'
import {
	type BenchmarkResult,
	calculatePercentile,
	generateMessage,
	printComparison,
	printResult,
	createTraceCollector,
	startDiagnostics,
	uniqueName,
} from './utils.js'

interface ProducerBenchmarkConfig {
	messageCount: number
	messageSize: number
	batchSize: number
}

const DEFAULT_CONFIG: ProducerBenchmarkConfig = {
	messageCount: 10000,
	messageSize: 1024,
	batchSize: 100,
}

function getAlignedProducerConfig(messageSize: number, batchSize: number) {
	const maxBatchBytes = Math.max(16384, messageSize * batchSize)
	return {
		lingerMs: 0,
		maxBatchBytes,
		acks: 'all' as const,
		compression: 'none' as const,
		idempotent: false,
		maxInFlight: 5,
		retries: 3,
		retryBackoffMs: 100,
		maxRetryBackoffMs: 1000,
		requestTimeoutMs: 30000,
	}
}

async function benchmarkKafkaTsProducer(
	cluster: StartedKafkaCluster,
	config: ProducerBenchmarkConfig
): Promise<BenchmarkResult> {
	const { messageCount, messageSize, batchSize } = config
	const topicName = uniqueName('bench-producer-kafkats')
	const message = generateMessage(messageSize)
	const trace = createTraceCollector()
	const diagnostics = startDiagnostics()
	const aligned = getAlignedProducerConfig(messageSize, batchSize)

	console.log('\n[kafkats] Starting producer benchmark...')

	const client = new KafkaClient({
		brokers: cluster.brokers,
		clientId: 'benchmark-kafkats',
		logLevel: 'silent',
	})

	await client.connect()
	await createTopicWithAssignments(cluster, topicName, 3)

	const testTopic = topic<string, string>(topicName, { key: codec.string(), value: codec.string() })

	const producer = client.producer({
		...aligned,
		trace: trace.trace,
	})

	const latencies: number[] = []
	const startTime = performance.now()

	// Send messages in batches (using array send like kafkajs)
	for (let i = 0; i < messageCount; i += batchSize) {
		const batchStart = performance.now()
		const messages: Array<{ value: string; key: string }> = []

		for (let j = 0; j < batchSize && i + j < messageCount; j++) {
			messages.push({ key: String(i + j), value: message })
		}

		await producer.send(testTopic, messages)
		const batchEnd = performance.now()
		latencies.push((batchEnd - batchStart) / messages.length)
	}

	const endTime = performance.now()
	const durationMs = endTime - startTime

	await producer.disconnect()
	await client.disconnect()

	const sortedLatencies = latencies.sort((a, b) => a - b)
	const avgLatency = latencies.reduce((a, b) => a + b, 0) / latencies.length

	return {
		name: 'Producer Benchmark',
		library: 'kafkats',
		operation: 'produce',
		messageCount,
		messageSize,
		durationMs,
		messagesPerSecond: (messageCount / durationMs) * 1000,
		bytesPerSecond: ((messageCount * messageSize) / durationMs) * 1000,
		avgLatencyMs: avgLatency,
		p50LatencyMs: calculatePercentile(sortedLatencies, 50),
		p95LatencyMs: calculatePercentile(sortedLatencies, 95),
		p99LatencyMs: calculatePercentile(sortedLatencies, 99),
		diagnostics: {
			...diagnostics.stop(),
			traceSummary: trace.getSummary(),
		},
	}
}

async function benchmarkKafkaJsProducer(
	cluster: StartedKafkaCluster,
	config: ProducerBenchmarkConfig
): Promise<BenchmarkResult> {
	const { messageCount, messageSize, batchSize } = config
	const topicName = uniqueName('bench-producer-kafkajs')
	const message = generateMessage(messageSize)
	const diagnostics = startDiagnostics()
	const aligned = getAlignedProducerConfig(messageSize, batchSize)

	console.log('\n[kafkajs] Starting producer benchmark...')

	const kafka = new KafkaJs({
		clientId: 'benchmark-kafkajs',
		brokers: cluster.brokers,
		logLevel: 0, // NOTHING
	})

	await createTopicWithAssignments(cluster, topicName, 3)

	const producer = kafka.producer({
		allowAutoTopicCreation: false,
		idempotent: aligned.idempotent,
		maxInFlightRequests: aligned.maxInFlight,
		retry: { retries: aligned.retries },
	})
	await producer.connect()

	const latencies: number[] = []
	const startTime = performance.now()

	// Send messages in batches
	for (let i = 0; i < messageCount; i += batchSize) {
		const batchStart = performance.now()
		const messages: { key: string; value: string }[] = []

		for (let j = 0; j < batchSize && i + j < messageCount; j++) {
			messages.push({ key: String(i + j), value: message })
		}

		await producer.send({
			topic: topicName,
			messages,
			acks: -1,
			compression: CompressionTypes.None,
		})

		const batchEnd = performance.now()
		latencies.push((batchEnd - batchStart) / messages.length)
	}

	const endTime = performance.now()
	const durationMs = endTime - startTime

	await producer.disconnect()

	const sortedLatencies = latencies.sort((a, b) => a - b)
	const avgLatency = latencies.reduce((a, b) => a + b, 0) / latencies.length

	return {
		name: 'Producer Benchmark',
		library: 'kafkajs',
		operation: 'produce',
		messageCount,
		messageSize,
		durationMs,
		messagesPerSecond: (messageCount / durationMs) * 1000,
		bytesPerSecond: ((messageCount * messageSize) / durationMs) * 1000,
		avgLatencyMs: avgLatency,
		p50LatencyMs: calculatePercentile(sortedLatencies, 50),
		p95LatencyMs: calculatePercentile(sortedLatencies, 95),
		p99LatencyMs: calculatePercentile(sortedLatencies, 99),
		diagnostics: diagnostics.stop(),
	}
}

async function benchmarkPlatformaticProducer(
	cluster: StartedKafkaCluster,
	config: ProducerBenchmarkConfig
): Promise<BenchmarkResult> {
	const { messageCount, messageSize, batchSize } = config
	const topicName = uniqueName('bench-producer-platformatic')
	const message = generateMessage(messageSize)
	const diagnostics = startDiagnostics()

	console.log('\n[platformatic] Starting producer benchmark...')

	await createTopicWithAssignments(cluster, topicName, 3)

	const producer = new PlatformaticProducer({
		clientId: 'benchmark-platformatic',
		bootstrapBrokers: cluster.brokers,
		serializers: stringSerializers,
		acks: ProduceAcks.ALL,
		compression: 'none',
		autocreateTopics: false,
	})

	const latencies: number[] = []
	const startTime = performance.now()

	for (let i = 0; i < messageCount; i += batchSize) {
		const batchStart = performance.now()
		const messages: Array<{ key: string; value: string }> = []

		for (let j = 0; j < batchSize && i + j < messageCount; j++) {
			messages.push({ key: String(i + j), value: message })
		}

		await producer.send({
			messages: messages.map(msg => ({ topic: topicName, ...msg })),
			acks: ProduceAcks.ALL,
		})

		const batchEnd = performance.now()
		latencies.push((batchEnd - batchStart) / messages.length)
	}

	const endTime = performance.now()
	const durationMs = endTime - startTime

	await producer.close()

	const sortedLatencies = latencies.sort((a, b) => a - b)
	const avgLatency = latencies.reduce((a, b) => a + b, 0) / latencies.length

	return {
		name: 'Producer Benchmark',
		library: 'platformatic',
		operation: 'produce',
		messageCount,
		messageSize,
		durationMs,
		messagesPerSecond: (messageCount / durationMs) * 1000,
		bytesPerSecond: ((messageCount * messageSize) / durationMs) * 1000,
		avgLatencyMs: avgLatency,
		p50LatencyMs: calculatePercentile(sortedLatencies, 50),
		p95LatencyMs: calculatePercentile(sortedLatencies, 95),
		p99LatencyMs: calculatePercentile(sortedLatencies, 99),
		diagnostics: diagnostics.stop(),
	}
}

export async function runProducerBenchmark(
	cluster: StartedKafkaCluster,
	config: Partial<ProducerBenchmarkConfig> = {}
): Promise<{ kafkaTs: BenchmarkResult; kafkaJs: BenchmarkResult; platformatic: BenchmarkResult }> {
	const fullConfig = { ...DEFAULT_CONFIG, ...config }

	console.log('\n' + '='.repeat(60))
	console.log('PRODUCER BENCHMARK')
	console.log('='.repeat(60))
	console.log(`  Message Count: ${fullConfig.messageCount}`)
	console.log(`  Message Size:  ${fullConfig.messageSize} bytes`)
	console.log(`  Batch Size:    ${fullConfig.batchSize}`)

	const kafkaTs = await benchmarkKafkaTsProducer(cluster, fullConfig)
	printResult(kafkaTs)

	const kafkaJs = await benchmarkKafkaJsProducer(cluster, fullConfig)
	printResult(kafkaJs)

	const platformatic = await benchmarkPlatformaticProducer(cluster, fullConfig)
	printResult(platformatic)

	printComparison(kafkaTs, kafkaJs, platformatic)

	return { kafkaTs, kafkaJs, platformatic }
}

// Run standalone if executed directly
const isMainModule = import.meta.url === `file://${process.argv[1]}`
if (isMainModule) {
	const cluster = await startKafkaCluster({ brokerCount: 3 })
	try {
		await runProducerBenchmark(cluster)
	} finally {
		await cluster.stop()
	}
}
