/**
 * Share Consumer vs Regular Consumer benchmark with simulated processing time.
 *
 * This benchmark demonstrates where Share Groups shine: when message processing
 * takes real time (10-100ms), share groups can parallelize beyond partition count.
 */

import { KafkaClient, codec, topic } from '@kafkats/client'
import { createTopicWithAssignments, type StartedKafkaCluster, startKafkaCluster } from './kafka-cluster.js'
import { generateMessage, uniqueName } from './utils.js'

interface BenchmarkConfig {
	messageCount: number
	messageSize: number
	processingTimeMs: number
	partitionCount: number
	shareConsumerConcurrency: number
}

const DEFAULT_CONFIG: BenchmarkConfig = {
	messageCount: 500,
	messageSize: 256,
	processingTimeMs: 20,
	partitionCount: 3,
	shareConsumerConcurrency: 20,
}

function sleep(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms))
}

async function benchmarkRegularConsumer(
	cluster: StartedKafkaCluster,
	config: BenchmarkConfig
): Promise<{ durationMs: number; messagesPerSecond: number }> {
	const topicName = uniqueName('bench-regular-topic')

	const client = new KafkaClient({
		brokers: cluster.brokers,
		clientId: 'bench-regular-consumer',
		logLevel: 'silent',
	})

	await client.connect()
	await createTopicWithAssignments(cluster, topicName, config.partitionCount)

	const groupId = uniqueName('bench-regular-group')
	const consumer = client.consumer({ groupId, autoOffsetReset: 'earliest' })

	const testTopic = topic<string, string>(topicName, { key: codec.string(), value: codec.string() })

	let messagesConsumed = 0
	const abortController = new AbortController()
	let startTime = 0
	let endTime = 0

	// Wait for partition assignment
	const assignedPromise = new Promise<void>(resolve => {
		consumer.once('partitionsAssigned', () => resolve())
	})

	const runPromise = consumer
		.runEach(
			testTopic,
			async () => {
				if (startTime === 0) {
					startTime = performance.now()
				}

				// Simulate processing time
				await sleep(config.processingTimeMs)

				messagesConsumed++

				if (messagesConsumed >= config.messageCount) {
					endTime = performance.now()
					abortController.abort()
				}
			},
			{
				signal: abortController.signal,
			}
		)
		.catch(() => {})

	await assignedPromise

	const producer = client.producer()
	const message = generateMessage(config.messageSize)
	for (let i = 0; i < config.messageCount; i++) {
		await producer.send(testTopic, [{ key: String(i), value: message }])
	}
	await producer.flush()

	const timeout = setTimeout(() => {
		console.log(`  Regular consumer timeout: ${messagesConsumed}/${config.messageCount}`)
		abortController.abort()
	}, 120000)

	await runPromise
	clearTimeout(timeout)

	const durationMs = endTime - startTime
	await producer.disconnect()
	await client.disconnect()

	return {
		durationMs,
		messagesPerSecond: (messagesConsumed / durationMs) * 1000,
	}
}

async function benchmarkShareConsumer(
	cluster: StartedKafkaCluster,
	config: BenchmarkConfig
): Promise<{ durationMs: number; messagesPerSecond: number }> {
	const topicName = uniqueName('bench-share-topic')

	const client = new KafkaClient({
		brokers: cluster.brokers,
		clientId: 'bench-share-consumer',
		logLevel: 'silent',
	})

	await client.connect()
	await createTopicWithAssignments(cluster, topicName, config.partitionCount)

	const groupId = uniqueName('bench-share-group')
	const consumer = client.shareConsumer({ groupId })

	let messagesConsumed = 0
	const abortController = new AbortController()
	let startTime = 0
	let endTime = 0

	// Wait for partition assignment before seeding
	const assignedPromise = new Promise<void>(resolve => {
		consumer.once('partitionsAssigned', () => resolve())
	})

	const runPromise = consumer
		.runEach(
			topicName,
			async () => {
				if (startTime === 0) {
					startTime = performance.now()
				}

				// Simulate processing time
				await sleep(config.processingTimeMs)

				messagesConsumed++

				if (messagesConsumed >= config.messageCount) {
					endTime = performance.now()
					abortController.abort()
				}
			},
			{
				signal: abortController.signal,
				concurrency: config.shareConsumerConcurrency,
			}
		)
		.catch(() => {})

	await assignedPromise

	const producer = client.producer()
	const testTopic = topic<string, string>(topicName, { key: codec.string(), value: codec.string() })
	const message = generateMessage(config.messageSize)
	for (let i = 0; i < config.messageCount; i++) {
		await producer.send(testTopic, [{ key: String(i), value: message }])
	}
	await producer.flush()

	const timeout = setTimeout(() => {
		console.log(`  Share consumer timeout: ${messagesConsumed}/${config.messageCount}`)
		abortController.abort()
	}, 120000)

	await runPromise
	clearTimeout(timeout)

	const durationMs = endTime - startTime
	await producer.disconnect()
	await client.disconnect()

	return {
		durationMs,
		messagesPerSecond: (messagesConsumed / durationMs) * 1000,
	}
}

export async function runShareVsConsumerBenchmark(
	cluster: StartedKafkaCluster,
	config: Partial<BenchmarkConfig> = {}
): Promise<void> {
	const fullConfig = { ...DEFAULT_CONFIG, ...config }

	console.log('\n' + '='.repeat(70))
	console.log('SHARE CONSUMER vs REGULAR CONSUMER (with processing time)')
	console.log('='.repeat(70))
	console.log(`  Messages:           ${fullConfig.messageCount}`)
	console.log(`  Processing Time:    ${fullConfig.processingTimeMs}ms per message`)
	console.log(`  Partitions:         ${fullConfig.partitionCount}`)
	console.log(`  Share Concurrency:  ${fullConfig.shareConsumerConcurrency}`)
	console.log()

	// Theoretical limits
	const theoreticalRegular = (1000 / fullConfig.processingTimeMs) * fullConfig.partitionCount
	const theoreticalShare = (1000 / fullConfig.processingTimeMs) * fullConfig.shareConsumerConcurrency

	console.log(`  Theoretical max (regular):  ${theoreticalRegular.toFixed(0)} msg/s`)
	console.log(`  Theoretical max (share):    ${theoreticalShare.toFixed(0)} msg/s`)
	console.log()

	// Run regular consumer benchmark (seeds after join)
	console.log('[Regular Consumer] Running...')
	const regularResult = await benchmarkRegularConsumer(cluster, fullConfig)
	console.log(`  Duration:    ${regularResult.durationMs.toFixed(0)}ms`)
	console.log(`  Throughput:  ${regularResult.messagesPerSecond.toFixed(0)} msg/s`)
	console.log()

	// Run share consumer benchmark (seeds after join)
	console.log('[Share Consumer] Running...')
	const shareResult = await benchmarkShareConsumer(cluster, fullConfig)
	console.log(`  Duration:    ${shareResult.durationMs.toFixed(0)}ms`)
	console.log(`  Throughput:  ${shareResult.messagesPerSecond.toFixed(0)} msg/s`)
	console.log()

	// Summary
	const speedup = shareResult.messagesPerSecond / regularResult.messagesPerSecond
	console.log('='.repeat(70))
	console.log('SUMMARY')
	console.log('='.repeat(70))
	console.log(`  Regular Consumer:  ${regularResult.messagesPerSecond.toFixed(0)} msg/s`)
	console.log(`  Share Consumer:    ${shareResult.messagesPerSecond.toFixed(0)} msg/s`)
	console.log(`  Speedup:           ${speedup.toFixed(1)}x`)
	console.log()
}

// Run standalone
const isMainModule = import.meta.url === `file://${process.argv[1]}`
if (isMainModule) {
	const cluster = await startKafkaCluster({ brokerCount: 3 })
	try {
		await runShareVsConsumerBenchmark(cluster, {
			messageCount: 300,
			processingTimeMs: 20,
			partitionCount: 3,
			shareConsumerConcurrency: 20,
		})
	} finally {
		await cluster.stop()
	}
}
