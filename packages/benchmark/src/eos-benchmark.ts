import { KafkaClient, codec, topic } from '@kafkats/client'

import { createTopicWithAssignments, startKafkaCluster, type StartedKafkaCluster } from './kafka-cluster.js'
import { calculatePercentile, generateMessage, uniqueName } from './utils.js'

/**
 * Benchmarks the documented EOS consume-transform-produce loop:
 * runBatch(partitionConcurrency: 3) with producer.transaction() per batch,
 * producer left at default settings. Compares transactionConcurrency 1
 * (single transactional ID — every handler queues on the same transaction)
 * against transactionConcurrency 3 (one lane per concurrent handler).
 */

interface EosBenchmarkConfig {
	messageCount: number
	messageSize: number
	partitions: number
	partitionConcurrency: number
}

const DEFAULT_CONFIG: EosBenchmarkConfig = {
	messageCount: 15000,
	messageSize: 512,
	partitions: 3,
	partitionConcurrency: 3,
}

interface EosBenchmarkResult {
	name: string
	messageCount: number
	durationMs: number
	messagesPerSecond: number
	transactionCount: number
	transactionsPerSecond: number
	txnP50Ms: number
	txnP95Ms: number
	txnP99Ms: number
	queuedEvents: number
}

async function runEosLoop(
	cluster: StartedKafkaCluster,
	config: EosBenchmarkConfig,
	transactionConcurrency: number
): Promise<EosBenchmarkResult> {
	const { messageCount, messageSize, partitions, partitionConcurrency } = config

	const inputName = uniqueName('bench-eos-input')
	const outputName = uniqueName('bench-eos-output')
	const input = topic<string, string>(inputName, { key: codec.string(), value: codec.string() })
	const output = topic<string, string>(outputName, { key: codec.string(), value: codec.string() })

	const client = new KafkaClient({
		brokers: cluster.brokers,
		clientId: `bench-eos-tc${transactionConcurrency}`,
		logLevel: 'silent',
	})
	await client.connect()
	await createTopicWithAssignments(cluster, inputName, partitions)
	await createTopicWithAssignments(cluster, outputName, partitions)

	// Seed the input topic (not part of the measured loop).
	const seedProducer = client.producer({ lingerMs: 5 })
	const message = generateMessage(messageSize)
	const seedBatch = 500
	for (let i = 0; i < messageCount; i += seedBatch) {
		const batch = Array.from({ length: Math.min(seedBatch, messageCount - i) }, (_, j) => ({
			key: `key-${i + j}`,
			value: message,
		}))
		await seedProducer.send(input, batch)
	}
	await seedProducer.flush()
	await seedProducer.disconnect()

	const producer = client.producer({
		transactionalId: uniqueName('bench-eos-tx'),
		transactionConcurrency,
	})
	const consumer = client.consumer({
		groupId: uniqueName('bench-eos-group'),
		autoOffsetReset: 'earliest',
		isolationLevel: 'read_committed',
		// Cap fetches so the pre-seeded backlog arrives as live-stream-sized batches,
		// keeping the loop transaction-rate-bound like production.
		maxBytesPerPartition: 64 * 1024,
	})

	let queuedEvents = 0
	producer.on('transaction:queued', () => {
		queuedEvents += 1
	})

	const txnLatencies: number[] = []
	let processed = 0
	let started = performance.now()

	consumer.once('running', () => {
		started = performance.now()
	})

	await consumer.runBatch(
		input,
		async (messages, ctx) => {
			const txnStart = performance.now()
			await producer.transaction(async txn => {
				await txn.send(
					output,
					messages.map(m => ({ key: m.key, value: m.value }))
				)
				await txn.sendOffsets(ctx, [{ topic: ctx.topic, partition: ctx.partition, offset: ctx.offset + 1n }])
			})
			txnLatencies.push(performance.now() - txnStart)

			processed += messages.length
			if (processed >= messageCount) {
				consumer.stop()
			}
		},
		{ autoCommit: false, commitOffsets: false, partitionConcurrency }
	)

	const durationMs = performance.now() - started

	await producer.disconnect()
	await client.disconnect()

	txnLatencies.sort((a, b) => a - b)
	return {
		name: `transactionConcurrency: ${transactionConcurrency}`,
		messageCount: processed,
		durationMs,
		messagesPerSecond: (processed / durationMs) * 1000,
		transactionCount: txnLatencies.length,
		transactionsPerSecond: (txnLatencies.length / durationMs) * 1000,
		txnP50Ms: calculatePercentile(txnLatencies, 50),
		txnP95Ms: calculatePercentile(txnLatencies, 95),
		txnP99Ms: calculatePercentile(txnLatencies, 99),
		queuedEvents,
	}
}

function printEosResult(result: EosBenchmarkResult): void {
	console.log(`\n  ${result.name}`)
	console.log(`    messages:            ${result.messageCount}`)
	console.log(`    duration:            ${(result.durationMs / 1000).toFixed(2)}s`)
	console.log(`    throughput:          ${result.messagesPerSecond.toFixed(0)} msg/s`)
	console.log(`    transactions:        ${result.transactionCount} (${result.transactionsPerSecond.toFixed(1)}/s)`)
	console.log(
		`    txn latency (p50/p95/p99): ${result.txnP50Ms.toFixed(1)}ms / ${result.txnP95Ms.toFixed(1)}ms / ${result.txnP99Ms.toFixed(1)}ms`
	)
	console.log(`    transaction:queued events: ${result.queuedEvents}`)
}

export async function runEosBenchmark(): Promise<void> {
	const config: EosBenchmarkConfig = {
		...DEFAULT_CONFIG,
		messageCount: Number(process.env.EOS_MESSAGE_COUNT ?? DEFAULT_CONFIG.messageCount),
	}

	console.log('\n=== EOS loop benchmark (runBatch + producer.transaction) ===')
	console.log(
		`  ${config.messageCount} messages x ${config.messageSize}B, ${config.partitions} partitions, partitionConcurrency ${config.partitionConcurrency}, producer defaults`
	)

	const cluster = await startKafkaCluster({ brokerCount: 3 })
	try {
		const baseline = await runEosLoop(cluster, config, 1)
		printEosResult(baseline)

		const pooled = await runEosLoop(cluster, config, config.partitionConcurrency)
		printEosResult(pooled)

		const speedup = pooled.messagesPerSecond / baseline.messagesPerSecond
		console.log(
			`\n  speedup: ${speedup.toFixed(2)}x (${baseline.messagesPerSecond.toFixed(0)} -> ${pooled.messagesPerSecond.toFixed(0)} msg/s)`
		)
	} finally {
		await cluster.stop()
	}
}

const isMain = process.argv[1]?.endsWith('eos-benchmark.ts') || process.argv[1]?.endsWith('eos-benchmark.js')
if (isMain) {
	runEosBenchmark().catch(err => {
		console.error(err)
		process.exit(1)
	})
}
