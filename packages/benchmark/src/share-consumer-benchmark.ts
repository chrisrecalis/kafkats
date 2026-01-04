import { KafkaClient, codec, topic } from '@kafkats/client'

import { createTopicWithAssignments, startKafkaCluster, type StartedKafkaCluster } from './kafka-cluster.js'
import {
	type BenchmarkResult,
	calculatePercentile,
	generateMessage,
	printResult,
	createTraceCollector,
	startDiagnostics,
	uniqueName,
} from './utils.js'

interface ShareConsumerBenchmarkConfig {
	messageCount: number
	messageSize: number
	consumerCount: number
}

export interface BenchmarkRunOptions {
	print?: boolean
	diagnostics?: boolean
	trace?: boolean
}

const DEFAULT_CONFIG: ShareConsumerBenchmarkConfig = {
	messageCount: 10000,
	messageSize: 1024,
	consumerCount: 2,
}

const ALIGNED_SHARE_CONSUMER_CONFIG = {
	maxWaitMs: 50,
	minBytes: 1,
	maxBytes: 1048576,
	maxRecords: 500,
	batchSize: 100,
} as const

async function benchmarkKafkaTsShareConsumer(
	cluster: StartedKafkaCluster,
	config: ShareConsumerBenchmarkConfig,
	options: BenchmarkRunOptions
): Promise<BenchmarkResult> {
	const { messageCount, messageSize, consumerCount } = config
	const topicName = uniqueName('bench-share-consumer-kafkats')
	const trace = options.trace ? createTraceCollector() : null
	const shouldPrint = options.print !== false

	if (shouldPrint) {
		console.log('\n[kafkats] Starting ShareConsumer benchmark...')
	}

	const client = new KafkaClient({
		brokers: cluster.brokers,
		clientId: 'benchmark-kafkats-share-consumer',
		logLevel: 'silent',
	})

	await client.connect()

	await createTopicWithAssignments(cluster, topicName, 3)

	const groupId = uniqueName('bench-share-group-kafkats')
	const consumers = Array.from({ length: consumerCount }, () =>
		client.shareConsumer({
			groupId,
			maxWaitMs: ALIGNED_SHARE_CONSUMER_CONFIG.maxWaitMs,
			minBytes: ALIGNED_SHARE_CONSUMER_CONFIG.minBytes,
			maxBytes: ALIGNED_SHARE_CONSUMER_CONFIG.maxBytes,
			maxRecords: ALIGNED_SHARE_CONSUMER_CONFIG.maxRecords,
			batchSize: ALIGNED_SHARE_CONSUMER_CONFIG.batchSize,
		})
	)

	let messagesConsumed = 0
	const perConsumerCounts = Array.from({ length: consumerCount }, () => 0)
	const latencies: number[] = []
	const abortController = new AbortController()
	let startTime = 0
	let finishTime = 0

	const diagnostics = options.diagnostics ? startDiagnostics() : null

	const runPromises = consumers.map((consumer, idx) =>
		consumer
			.runEach(
				topicName,
				(): Promise<void> => {
					const handlerStart = performance.now()

					if (startTime === 0) {
						startTime = performance.now()
					}
					const msgTime = performance.now()

					perConsumerCounts[idx] = (perConsumerCounts[idx] ?? 0) + 1
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
					signal: abortController.signal,
					concurrency: 10,
				}
			)
			.catch(() => {})
	)

	// Wait for consumers to have assignments before producing
	await Promise.all(
		consumers.map(
			consumer =>
				new Promise<void>((resolve, reject) => {
					consumer.once('partitionsAssigned', () => resolve())
					consumer.once('error', err => reject(err))
				})
		)
	)

	if (shouldPrint) {
		console.log(`  Seeding ${messageCount} messages to ${topicName}...`)
	}

	const producer = client.producer({ lingerMs: 0 })
	const testTopic = topic<string, string>(topicName, { key: codec.string(), value: codec.string() })

	const message = generateMessage(messageSize)
	const batchSize = 1000

	for (let i = 0; i < messageCount; i += batchSize) {
		const messages: { key: string; value: string }[] = []
		for (let j = 0; j < batchSize && i + j < messageCount; j++) {
			messages.push({ key: String(i + j), value: message })
		}
		await producer.send(testTopic, messages)
	}
	await producer.flush()

	if (shouldPrint) {
		console.log(`  Seeding complete.`)
	}

	const timeout = setTimeout(() => {
		console.log(`  Warning: Timeout reached with ${messagesConsumed}/${messageCount} messages`)
		abortController.abort()
	}, 120000)

	await Promise.allSettled(runPromises)
	clearTimeout(timeout)

	const endTime = finishTime || performance.now()
	const durationMs = endTime - startTime

	await producer.disconnect()
	await client.disconnect()

	const interLatencies: number[] = []
	for (let i = 1; i < latencies.length; i++) {
		interLatencies.push(latencies[i] - latencies[i - 1])
	}
	const sortedLatencies = interLatencies.sort((a, b) => a - b)
	const avgLatency = interLatencies.length > 0 ? interLatencies.reduce((a, b) => a + b, 0) / interLatencies.length : 0

	const traceSummary = trace?.getSummary()

	if (shouldPrint) {
		console.log(`  Consumer distribution: ${perConsumerCounts.join(', ')}`)
	}

	return {
		name: 'ShareConsumer Benchmark',
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

export async function runShareConsumerBenchmark(
	cluster: StartedKafkaCluster,
	config: Partial<ShareConsumerBenchmarkConfig> = {},
	options: BenchmarkRunOptions = {}
): Promise<{ kafkaTs: BenchmarkResult }> {
	const fullConfig = { ...DEFAULT_CONFIG, ...config }
	const opts: BenchmarkRunOptions = {
		print: options.print !== false,
		diagnostics: options.diagnostics === true,
		trace: options.trace === true,
	}

	if (opts.print) {
		console.log('\n' + '='.repeat(60))
		console.log('SHARE CONSUMER BENCHMARK')
		console.log('='.repeat(60))
		console.log(`  Message Count:  ${fullConfig.messageCount}`)
		console.log(`  Message Size:   ${fullConfig.messageSize} bytes`)
		console.log(`  Consumers:      ${fullConfig.consumerCount}`)
	}

	const kafkaTs = await benchmarkKafkaTsShareConsumer(cluster, fullConfig, opts)
	if (opts.print) {
		printResult(kafkaTs)
	}

	return { kafkaTs }
}

// Run standalone if executed directly
const isMainModule = import.meta.url === `file://${process.argv[1]}`
if (isMainModule) {
	const cluster = await startKafkaCluster({ brokerCount: 3 })
	try {
		await runShareConsumerBenchmark(cluster)
	} finally {
		await cluster.stop()
	}
}
