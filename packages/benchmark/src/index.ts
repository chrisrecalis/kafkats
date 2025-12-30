import { randomUUID } from 'node:crypto'
import { appendFileSync, mkdirSync } from 'node:fs'
import { dirname } from 'node:path'
import os from 'node:os'

import { startKafkaCluster } from './kafka-cluster.js'
import { runProducerBenchmark } from './producer-benchmark.js'
import { runConsumerBenchmark } from './consumer-benchmark.js'
import type { BenchmarkResult } from './utils.js'
import { formatBytes, formatNumber } from './utils.js'
import { summarize } from './statistics.js'

type SuiteResult = {
	kafkaTs: BenchmarkResult
	kafkaJs: BenchmarkResult
	platformatic: BenchmarkResult
}

type Operation = 'producer' | 'consumer' | 'all'

type RunOptions = {
	print?: boolean
	diagnostics?: boolean
	trace?: boolean
}

function parseArgs(argv: string[]): Record<string, string | boolean> {
	const out: Record<string, string | boolean> = {}

	for (let i = 0; i < argv.length; i++) {
		const raw = argv[i]
		if (!raw) continue
		if (raw === '--') continue
		if (!raw.startsWith('--')) continue

		const [key, inlineValue] = raw.slice(2).split('=', 2)
		if (!key) continue
		if (inlineValue !== undefined) {
			out[key] = inlineValue
			continue
		}

		const next = argv[i + 1]
		if (next && !next.startsWith('--')) {
			out[key] = next
			i++
			continue
		}

		out[key] = true
	}

	return out
}

function getNumberArg(args: Record<string, string | boolean>, key: string, fallback: number): number {
	const value = args[key]
	if (typeof value !== 'string') return fallback
	const parsed = Number(value)
	return Number.isFinite(parsed) ? parsed : fallback
}

function getStringArg(args: Record<string, string | boolean>, key: string, fallback: string): string {
	const value = args[key]
	return typeof value === 'string' ? value : fallback
}

function hasFlag(args: Record<string, string | boolean>, key: string): boolean {
	return args[key] === true
}

function writeJsonLine(path: string, payload: unknown): void {
	mkdirSync(dirname(path), { recursive: true })
	appendFileSync(path, JSON.stringify(payload) + '\n')
}

function summarizeSuite(results: SuiteResult[], label: string): void {
	const kafkats = results.map(r => r.kafkaTs.messagesPerSecond)
	const kafkajs = results.map(r => r.kafkaJs.messagesPerSecond)
	const platformatic = results.map(r => r.platformatic.messagesPerSecond)

	const kafkatsStats = summarize(kafkats)
	const kafkajsStats = summarize(kafkajs)
	const platformaticStats = summarize(platformatic)

	const ratioMean = kafkajsStats.mean > 0 ? kafkatsStats.mean / kafkajsStats.mean : 0

	console.log('\n' + '='.repeat(60))
	console.log(`${label.toUpperCase()} SUMMARY (${results.length} iterations)`)
	console.log('='.repeat(60))
	console.log(
		`  kafkats:      mean ${formatNumber(Math.round(kafkatsStats.mean))} msg/s (σ=${formatNumber(Math.round(kafkatsStats.stdev))}, min=${formatNumber(Math.round(kafkatsStats.min))}, max=${formatNumber(Math.round(kafkatsStats.max))})`
	)
	console.log(
		`  kafkajs:      mean ${formatNumber(Math.round(kafkajsStats.mean))} msg/s (σ=${formatNumber(Math.round(kafkajsStats.stdev))}, min=${formatNumber(Math.round(kafkajsStats.min))}, max=${formatNumber(Math.round(kafkajsStats.max))})`
	)
	console.log(
		`  platformatic: mean ${formatNumber(Math.round(platformaticStats.mean))} msg/s (σ=${formatNumber(Math.round(platformaticStats.stdev))}, min=${formatNumber(Math.round(platformaticStats.min))}, max=${formatNumber(Math.round(platformaticStats.max))})`
	)
	console.log(
		`  ratio (kafkats/kafkajs): ${ratioMean.toFixed(2)}x ${ratioMean >= 1 ? '(kafkats faster)' : '(kafkajs faster)'}`
	)
}

async function runOperation(options: {
	label: 'producer' | 'consumer'
	runId: string
	jsonPath: string
	iterations: number
	warmup: number
	quiet: boolean
	verbose: boolean
	run: (opts: RunOptions) => Promise<SuiteResult>
	diagnostics: boolean
	trace: boolean
}): Promise<void> {
	const { label, runId, jsonPath, iterations, warmup, quiet, verbose, run, diagnostics, trace } = options
	const measured: SuiteResult[] = []

	for (let i = 0; i < warmup + iterations; i++) {
		const isWarmup = i < warmup
		const iterationIndex = isWarmup ? i + 1 : i - warmup + 1

		if (!quiet) {
			console.log(
				`[${label}] ${isWarmup ? `warmup ${iterationIndex}/${warmup}` : `iteration ${iterationIndex}/${iterations}`}`
			)
		}

		const result = await run({
			print: verbose && !isWarmup,
			diagnostics: diagnostics && !isWarmup,
			trace: trace && !isWarmup,
		})

		if (jsonPath) {
			writeJsonLine(jsonPath, { type: label, runId, warmup: isWarmup, iteration: i, result })
		}

		if (!isWarmup) {
			measured.push(result)
		}
	}

	summarizeSuite(measured, label)
}

async function main(): Promise<void> {
	const args = parseArgs(process.argv.slice(2))

	const iterations = getNumberArg(args, 'iterations', 5)
	const warmup = getNumberArg(args, 'warmup', 1)
	const operation = getStringArg(args, 'operation', 'all') as Operation

	const messageCount = getNumberArg(args, 'messageCount', 10_000)
	const messageSize = getNumberArg(args, 'messageSize', 1024)
	const batchSize = getNumberArg(args, 'batchSize', 100)

	const jsonPath = getStringArg(args, 'json', '')
	const diagnostics = hasFlag(args, 'diagnostics')
	const trace = hasFlag(args, 'trace')
	const quiet = hasFlag(args, 'quiet')
	const verbose = hasFlag(args, 'verbose')

	const runId = getStringArg(args, 'runId', randomUUID())

	if (!quiet) {
		console.log('╔══════════════════════════════════════════════════════════════════════╗')
		console.log('║                     kafkats Benchmark Suite                          ║')
		console.log('╚══════════════════════════════════════════════════════════════════════╝')
		console.log(`  Run ID:       ${runId}`)
		console.log(`  Iterations:   ${iterations} (+${warmup} warmup)`)
		console.log(`  Operation:    ${operation}`)
		console.log(`  Messages:     ${formatNumber(messageCount)}`)
		console.log(`  Message Size: ${formatBytes(messageSize)}`)
		if (operation !== 'consumer') {
			console.log(`  Batch Size:   ${formatNumber(batchSize)}`)
		}
		console.log(`  Diagnostics:  ${diagnostics ? 'on' : 'off'}`)
		console.log(`  Trace:        ${trace ? 'on' : 'off'}`)
		console.log(`  Verbose:      ${verbose ? 'on' : 'off'}`)
		if (jsonPath) {
			console.log(`  JSON:         ${jsonPath}`)
		}
	}

	if (jsonPath) {
		writeJsonLine(jsonPath, {
			type: 'meta',
			runId,
			timestamp: new Date().toISOString(),
			node: process.version,
			platform: process.platform,
			arch: process.arch,
			cpus: os.cpus()?.[0]?.model,
			totalmem: os.totalmem(),
			config: { iterations, warmup, operation, messageCount, messageSize, batchSize, diagnostics, trace },
		})
	}

	const cluster = await startKafkaCluster({ brokerCount: 3 })

	try {
		if (operation === 'producer' || operation === 'all') {
			await runOperation({
				label: 'producer',
				runId,
				jsonPath,
				iterations,
				warmup,
				quiet,
				verbose,
				diagnostics,
				trace,
				run: opts => runProducerBenchmark(cluster, { messageCount, messageSize, batchSize }, opts),
			})
		}

		if (operation === 'consumer' || operation === 'all') {
			await runOperation({
				label: 'consumer',
				runId,
				jsonPath,
				iterations,
				warmup,
				quiet,
				verbose,
				diagnostics,
				trace,
				run: opts => runConsumerBenchmark(cluster, { messageCount, messageSize }, opts),
			})
		}
	} finally {
		await cluster.stop()
	}
}

main().catch(err => {
	console.error('Benchmark failed:', err)
	process.exit(1)
})
