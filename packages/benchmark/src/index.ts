import { startKafkaCluster } from './kafka-cluster.js'
import { runProducerBenchmark } from './producer-benchmark.js'
import { runConsumerBenchmark } from './consumer-benchmark.js'
import { type BenchmarkResult, formatNumber } from './utils.js'

interface BenchmarkSuite {
	producer: { kafkaTs: BenchmarkResult; kafkaJs: BenchmarkResult; platformatic: BenchmarkResult }
	consumer: { kafkaTs: BenchmarkResult; kafkaJs: BenchmarkResult; platformatic: BenchmarkResult }
}

function printSummary(results: BenchmarkSuite): void {
	console.log('\n')
	console.log('╔══════════════════════════════════════════════════════════════════════╗')
	console.log('║                        BENCHMARK SUMMARY                             ║')
	console.log('╠══════════════════════════════════════════════════════════════════════╣')

	const producerRatio = results.producer.kafkaTs.messagesPerSecond / results.producer.kafkaJs.messagesPerSecond
	const consumerRatio = results.consumer.kafkaTs.messagesPerSecond / results.consumer.kafkaJs.messagesPerSecond

	console.log('║                                                                      ║')
	console.log('║  PRODUCER THROUGHPUT                                                 ║')
	console.log(
		`║    kafkats:  ${formatNumber(Math.round(results.producer.kafkaTs.messagesPerSecond)).padEnd(12)} msg/s                              ║`
	)
	console.log(
		`║    kafkajs:   ${formatNumber(Math.round(results.producer.kafkaJs.messagesPerSecond)).padEnd(12)} msg/s                              ║`
	)
	console.log(
		`║    platformatic: ${formatNumber(Math.round(results.producer.platformatic.messagesPerSecond)).padEnd(12)} msg/s                           ║`
	)
	console.log(
		`║    Ratio:     ${producerRatio.toFixed(2)}x ${producerRatio >= 1 ? '(kafkats faster)' : '(kafkajs faster)'}                           ║`
	)
	console.log('║                                                                      ║')
	console.log('║  CONSUMER THROUGHPUT                                                 ║')
	console.log(
		`║    kafkats:  ${formatNumber(Math.round(results.consumer.kafkaTs.messagesPerSecond)).padEnd(12)} msg/s                              ║`
	)
	console.log(
		`║    kafkajs:   ${formatNumber(Math.round(results.consumer.kafkaJs.messagesPerSecond)).padEnd(12)} msg/s                              ║`
	)
	console.log(
		`║    platformatic: ${formatNumber(Math.round(results.consumer.platformatic.messagesPerSecond)).padEnd(12)} msg/s                           ║`
	)
	console.log(
		`║    Ratio:     ${consumerRatio.toFixed(2)}x ${consumerRatio >= 1 ? '(kafkats faster)' : '(kafkajs faster)'}                           ║`
	)
	console.log('║                                                                      ║')
	console.log('╚══════════════════════════════════════════════════════════════════════╝')
}

async function main(): Promise<void> {
	console.log('╔══════════════════════════════════════════════════════════════════════╗')
	console.log('║              kafkats vs kafkajs Benchmark Suite                     ║')
	console.log('║                    3-Node Kafka Cluster                              ║')
	console.log('╚══════════════════════════════════════════════════════════════════════╝')

	const cluster = await startKafkaCluster({ brokerCount: 3 })

	try {
		// Run producer benchmark
		const producerResults = await runProducerBenchmark(cluster, {
			messageCount: 10000,
			messageSize: 1024,
			batchSize: 100,
		})

		// Run consumer benchmark
		const consumerResults = await runConsumerBenchmark(cluster, {
			messageCount: 10000,
			messageSize: 1024,
		})

		// Print final summary
		printSummary({
			producer: producerResults,
			consumer: consumerResults,
		})
	} finally {
		await cluster.stop()
	}
}

main().catch(err => {
	console.error('Benchmark failed:', err)
	process.exit(1)
})
