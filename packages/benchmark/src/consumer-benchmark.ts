import { Kafka as KafkaJs } from 'kafkajs'
import { KafkaClient } from '@kafkats/client'
import { Consumer as PlatformaticConsumer, MessagesStreamModes } from '@platformatic/kafka'

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

interface ConsumerBenchmarkConfig {
	messageCount: number
	messageSize: number
}

export interface BenchmarkRunOptions {
	print?: boolean
	diagnostics?: boolean
	trace?: boolean
}

const DEFAULT_CONFIG: ConsumerBenchmarkConfig = {
	messageCount: 10000,
	messageSize: 1024,
}

const ALIGNED_CONSUMER_CONFIG = {
	maxBytesPerPartition: 1048576,
	minBytes: 1,
	maxWaitMs: 50,
} as const

async function seedTopic(
	cluster: StartedKafkaCluster,
	topicName: string,
	messageCount: number,
	messageSize: number,
	print: boolean
): Promise<void> {
	if (print) {
		console.log(`  Seeding ${messageCount} messages to ${topicName}...`)
	}

	const kafka = new KafkaJs({
		clientId: 'benchmark-seeder',
		brokers: cluster.brokers,
		logLevel: 0,
	})

	await createTopicWithAssignments(cluster, topicName, 3)

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
	if (print) {
		console.log(`  Seeding complete.`)
	}
}

async function benchmarkKafkaTsConsumer(
	cluster: StartedKafkaCluster,
	config: ConsumerBenchmarkConfig,
	options: BenchmarkRunOptions
): Promise<BenchmarkResult> {
	const { messageCount, messageSize } = config
	const topicName = uniqueName('bench-consumer-kafkats')
	const trace = options.trace ? createTraceCollector() : null
	const shouldPrint = options.print !== false

	if (shouldPrint) {
		console.log('\n[kafkats] Starting consumer benchmark...')
	}

	await seedTopic(cluster, topicName, messageCount, messageSize, shouldPrint)

	const client = new KafkaClient({
		brokers: cluster.brokers,
		clientId: 'benchmark-kafkats-consumer',
		logLevel: 'silent',
	})

	await client.connect()

	const consumer = client.consumer({
		groupId: uniqueName('bench-group-kafkats'),
		autoOffsetReset: 'earliest',
		isolationLevel: 'read_committed',
		maxWaitMs: ALIGNED_CONSUMER_CONFIG.maxWaitMs,
		minBytes: ALIGNED_CONSUMER_CONFIG.minBytes,
		maxBytesPerPartition: ALIGNED_CONSUMER_CONFIG.maxBytesPerPartition,
		trace: trace?.trace,
	})

	let messagesConsumed = 0
	const latencies: number[] = []
	const abortController = new AbortController()
	let startTime = 0
	let finishTime = 0

	const diagnostics = options.diagnostics ? startDiagnostics() : null
	const runPromise = consumer.runEach(
		topicName,
		(): Promise<void> => {
			const handlerStart = performance.now()
			// Start timing from first message (after group join)
			if (startTime === 0) {
				startTime = performance.now()
			}
			const msgTime = performance.now()
			messagesConsumed++
			latencies.push(msgTime - startTime)
			if (trace) {
				trace.trace({
					stage: 'benchmark_handler',
					durationMs: performance.now() - handlerStart,
					recordCount: 1,
					bytes: messageSize,
				})
			}

			if (messagesConsumed >= messageCount) {
				if (finishTime === 0) {
					finishTime = performance.now()
				}
				abortController.abort()
			}
			return Promise.resolve()
		},
		{
			autoCommit: true,
			signal: abortController.signal,
			partitionConcurrency: 3,
		}
	)

	// Wait for consumer to be running
	await new Promise<void>((resolve, reject) => {
		consumer.once('running', () => resolve())
		consumer.once('error', err => reject(err))
	})

	// Wait for all messages or timeout
	const timeout = setTimeout(() => {
		console.log(`  Warning: Timeout reached with ${messagesConsumed}/${messageCount} messages`)
		abortController.abort()
	}, 120000)

	await runPromise.catch(() => {}) // Ignore abort error

	clearTimeout(timeout)

	const endTime = finishTime || performance.now()
	const durationMs = endTime - startTime

	await client.disconnect()

	// Calculate inter-message latencies
	const interLatencies: number[] = []
	for (let i = 1; i < latencies.length; i++) {
		interLatencies.push(latencies[i] - latencies[i - 1])
	}
	const sortedLatencies = interLatencies.sort((a, b) => a - b)
	const avgLatency = interLatencies.length > 0 ? interLatencies.reduce((a, b) => a + b, 0) / interLatencies.length : 0

	const traceSummary = trace?.getSummary()
	if (traceSummary && Object.keys(traceSummary).length === 0 && options.print) {
		console.log('  Warning: No trace events captured for kafkats consumer')
	}

	return {
		name: 'Consumer Benchmark',
		library: 'kafkats',
		operation: 'consume',
		messageCount: messagesConsumed,
		messageSize,
		durationMs,
		messagesPerSecond: (messagesConsumed / durationMs) * 1000,
		bytesPerSecond: ((messagesConsumed * messageSize) / durationMs) * 1000,
		avgLatencyMs: avgLatency,
		p50LatencyMs: calculatePercentile(sortedLatencies, 50),
		p95LatencyMs: calculatePercentile(sortedLatencies, 95),
		p99LatencyMs: calculatePercentile(sortedLatencies, 99),
		diagnostics: diagnostics
			? {
					...diagnostics.stop(),
					traceSummary,
				}
			: traceSummary
				? { traceSummary }
				: undefined,
	}
}

async function benchmarkKafkaJsConsumer(
	cluster: StartedKafkaCluster,
	config: ConsumerBenchmarkConfig,
	options: BenchmarkRunOptions
): Promise<BenchmarkResult> {
	const { messageCount, messageSize } = config
	const topicName = uniqueName('bench-consumer-kafkajs')
	const shouldPrint = options.print !== false

	if (shouldPrint) {
		console.log('\n[kafkajs] Starting consumer benchmark...')
	}

	await seedTopic(cluster, topicName, messageCount, messageSize, shouldPrint)

	const kafka = new KafkaJs({
		clientId: 'benchmark-kafkajs-consumer',
		brokers: cluster.brokers,
		logLevel: 0,
	})

	const consumer = kafka.consumer({
		groupId: uniqueName('bench-group-kafkajs'),
		minBytes: ALIGNED_CONSUMER_CONFIG.minBytes,
		maxBytesPerPartition: ALIGNED_CONSUMER_CONFIG.maxBytesPerPartition,
		maxWaitTimeInMs: ALIGNED_CONSUMER_CONFIG.maxWaitMs,
		readUncommitted: false,
	})

	await consumer.connect()
	await consumer.subscribe({ topic: topicName, fromBeginning: true })

	let messagesConsumed = 0
	const latencies: number[] = []
	let resolveComplete: () => void
	const completePromise = new Promise<void>(resolve => {
		resolveComplete = resolve
	})

	let startTime = 0
	const diagnostics = options.diagnostics ? startDiagnostics() : null

	await consumer.run({
		eachMessage: (): Promise<void> => {
			// Start timing from first message (after group join)
			if (startTime === 0) {
				startTime = performance.now()
			}
			const msgTime = performance.now()
			messagesConsumed++
			latencies.push(msgTime - startTime)

			if (messagesConsumed >= messageCount) {
				resolveComplete()
			}
			return Promise.resolve()
		},
	})

	// Wait for all messages or timeout
	const timeout = setTimeout(() => {
		console.log(`  Warning: Timeout reached with ${messagesConsumed}/${messageCount} messages`)
		resolveComplete()
	}, 120000)

	await completePromise

	clearTimeout(timeout)

	const endTime = performance.now()
	const durationMs = endTime - startTime

	await consumer.stop()
	await consumer.disconnect()

	// Calculate inter-message latencies
	const interLatencies: number[] = []
	for (let i = 1; i < latencies.length; i++) {
		interLatencies.push(latencies[i] - latencies[i - 1])
	}
	const sortedLatencies = interLatencies.sort((a, b) => a - b)
	const avgLatency = interLatencies.length > 0 ? interLatencies.reduce((a, b) => a + b, 0) / interLatencies.length : 0

	return {
		name: 'Consumer Benchmark',
		library: 'kafkajs',
		operation: 'consume',
		messageCount: messagesConsumed,
		messageSize,
		durationMs,
		messagesPerSecond: (messagesConsumed / durationMs) * 1000,
		bytesPerSecond: ((messagesConsumed * messageSize) / durationMs) * 1000,
		avgLatencyMs: avgLatency,
		p50LatencyMs: calculatePercentile(sortedLatencies, 50),
		p95LatencyMs: calculatePercentile(sortedLatencies, 95),
		p99LatencyMs: calculatePercentile(sortedLatencies, 99),
		diagnostics: diagnostics?.stop(),
	}
}

async function benchmarkPlatformaticConsumer(
	cluster: StartedKafkaCluster,
	config: ConsumerBenchmarkConfig,
	options: BenchmarkRunOptions
): Promise<BenchmarkResult> {
	const { messageCount, messageSize } = config
	const topicName = uniqueName('bench-consumer-platformatic')
	const shouldPrint = options.print !== false

	if (shouldPrint) {
		console.log('\n[platformatic] Starting consumer benchmark...')
	}

	await seedTopic(cluster, topicName, messageCount, messageSize, shouldPrint)

	const consumer = new PlatformaticConsumer({
		clientId: 'benchmark-platformatic-consumer',
		groupId: uniqueName('bench-group-platformatic'),
		bootstrapBrokers: cluster.brokers,
		minBytes: ALIGNED_CONSUMER_CONFIG.minBytes,
		maxBytes: ALIGNED_CONSUMER_CONFIG.maxBytesPerPartition,
		maxWaitTime: ALIGNED_CONSUMER_CONFIG.maxWaitMs,
		autocommit: false,
		isolationLevel: 'READ_COMMITTED',
	})

	const stream = await consumer.consume({
		topics: [topicName],
		mode: MessagesStreamModes.EARLIEST,
	})

	let messagesConsumed = 0
	const latencies: number[] = []
	let startTime = 0
	const diagnostics = options.diagnostics ? startDiagnostics() : null

	const completion = new Promise<void>((resolve, reject) => {
		stream.on('data', () => {
			if (startTime === 0) {
				startTime = performance.now()
			}
			const msgTime = performance.now()
			messagesConsumed++
			latencies.push(msgTime - startTime)

			if (messagesConsumed >= messageCount) {
				process.nextTick(() => {
					consumer.close(true, error => {
						if (error) {
							reject(error)
						} else {
							resolve()
						}
					})
				})
			}
		})

		stream.on('error', reject)
	})

	await completion

	const endTime = performance.now()
	const durationMs = endTime - startTime

	const interLatencies: number[] = []
	for (let i = 1; i < latencies.length; i++) {
		interLatencies.push(latencies[i] - latencies[i - 1])
	}
	const sortedLatencies = interLatencies.sort((a, b) => a - b)
	const avgLatency = interLatencies.length > 0 ? interLatencies.reduce((a, b) => a + b, 0) / interLatencies.length : 0

	return {
		name: 'Consumer Benchmark',
		library: 'platformatic',
		operation: 'consume',
		messageCount: messagesConsumed,
		messageSize,
		durationMs,
		messagesPerSecond: (messagesConsumed / durationMs) * 1000,
		bytesPerSecond: ((messagesConsumed * messageSize) / durationMs) * 1000,
		avgLatencyMs: avgLatency,
		p50LatencyMs: calculatePercentile(sortedLatencies, 50),
		p95LatencyMs: calculatePercentile(sortedLatencies, 95),
		p99LatencyMs: calculatePercentile(sortedLatencies, 99),
		diagnostics: diagnostics?.stop(),
	}
}

export async function runConsumerBenchmark(
	cluster: StartedKafkaCluster,
	config: Partial<ConsumerBenchmarkConfig> = {},
	options: BenchmarkRunOptions = {}
): Promise<{ kafkaTs: BenchmarkResult; kafkaJs: BenchmarkResult; platformatic: BenchmarkResult }> {
	const fullConfig = { ...DEFAULT_CONFIG, ...config }
	const opts: BenchmarkRunOptions = {
		print: options.print !== false,
		diagnostics: options.diagnostics === true,
		trace: options.trace === true,
	}

	if (opts.print) {
		console.log('\n' + '='.repeat(60))
		console.log('CONSUMER BENCHMARK')
		console.log('='.repeat(60))
		console.log(`  Message Count: ${fullConfig.messageCount}`)
		console.log(`  Message Size:  ${fullConfig.messageSize} bytes`)
	}

	const kafkaTs = await benchmarkKafkaTsConsumer(cluster, fullConfig, opts)
	if (opts.print) {
		printResult(kafkaTs)
	}

	const kafkaJs = await benchmarkKafkaJsConsumer(cluster, fullConfig, opts)
	if (opts.print) {
		printResult(kafkaJs)
	}

	const platformatic = await benchmarkPlatformaticConsumer(cluster, fullConfig, opts)
	if (opts.print) {
		printResult(platformatic)
	}

	if (opts.print) {
		printComparison(kafkaTs, kafkaJs, platformatic)
	}

	return { kafkaTs, kafkaJs, platformatic }
}

// Run standalone if executed directly
const isMainModule = import.meta.url === `file://${process.argv[1]}`
if (isMainModule) {
	const cluster = await startKafkaCluster({ brokerCount: 3 })
	try {
		await runConsumerBenchmark(cluster)
	} finally {
		await cluster.stop()
	}
}
