import { performance, PerformanceObserver } from 'node:perf_hooks'

export interface TraceSummaryItem {
	count: number
	totalMs: number
	avgMs: number
	totalBytes?: number
	totalRecords?: number
}

export interface BenchmarkDiagnostics {
	traceSummary?: Record<string, TraceSummaryItem>
	gcTotalMs?: number
	gcCount?: number
	heapDeltaBytes?: number
	rssDeltaBytes?: number
	elu?: { activeMs: number; idleMs: number; utilization: number }
}

export interface BenchmarkResult {
	name: string
	library: string
	operation: 'produce' | 'consume'
	messageCount: number
	messageSize: number
	durationMs: number
	messagesPerSecond: number
	bytesPerSecond: number
	avgLatencyMs: number
	p50LatencyMs: number
	p95LatencyMs: number
	p99LatencyMs: number
	diagnostics?: BenchmarkDiagnostics
}

export function formatBytes(bytes: number): string {
	if (bytes < 1024) return `${bytes} B`
	if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`
	if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(2)} MB`
	return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`
}

export function formatNumber(num: number): string {
	return num.toLocaleString('en-US')
}

export function calculatePercentile(sortedLatencies: number[], percentile: number): number {
	if (sortedLatencies.length === 0) return 0
	const index = Math.ceil((percentile / 100) * sortedLatencies.length) - 1
	return sortedLatencies[Math.max(0, index)]
}

export function generateMessage(size: number): string {
	const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
	let result = ''
	for (let i = 0; i < size; i++) {
		result += chars.charAt(Math.floor(Math.random() * chars.length))
	}
	return result
}

export function printResult(result: BenchmarkResult): void {
	console.log('\n' + '='.repeat(60))
	console.log(`${result.library} - ${result.name}`)
	console.log('='.repeat(60))
	console.log(`  Operation:          ${result.operation}`)
	console.log(`  Messages:           ${formatNumber(result.messageCount)}`)
	console.log(`  Message Size:       ${formatBytes(result.messageSize)}`)
	console.log(`  Duration:           ${result.durationMs.toFixed(2)} ms`)
	console.log(`  Throughput:         ${formatNumber(Math.round(result.messagesPerSecond))} msg/s`)
	console.log(`  Bandwidth:          ${formatBytes(result.bytesPerSecond)}/s`)
	console.log(`  Avg Latency:        ${result.avgLatencyMs.toFixed(3)} ms`)
	console.log(`  P50 Latency:        ${result.p50LatencyMs.toFixed(3)} ms`)
	console.log(`  P95 Latency:        ${result.p95LatencyMs.toFixed(3)} ms`)
	console.log(`  P99 Latency:        ${result.p99LatencyMs.toFixed(3)} ms`)

	if (result.diagnostics) {
		const diag = result.diagnostics
		if (diag.heapDeltaBytes !== undefined) {
			console.log(`  Heap Delta:         ${formatBytes(diag.heapDeltaBytes)}`)
		}
		if (diag.rssDeltaBytes !== undefined) {
			console.log(`  RSS Delta:          ${formatBytes(diag.rssDeltaBytes)}`)
		}
		if (diag.gcTotalMs !== undefined) {
			console.log(`  GC Time:            ${diag.gcTotalMs.toFixed(2)} ms (${diag.gcCount ?? 0} events)`)
		}
		if (
			diag.elu &&
			Number.isFinite(diag.elu.utilization) &&
			Number.isFinite(diag.elu.activeMs) &&
			Number.isFinite(diag.elu.idleMs)
		) {
			const totalMs = diag.elu.activeMs + diag.elu.idleMs
			console.log(
				`  ELU:                ${(diag.elu.utilization * 100).toFixed(1)}% (${diag.elu.activeMs.toFixed(1)}ms/${totalMs.toFixed(1)}ms)`
			)
		}
		if (diag.traceSummary) {
			console.log('  Trace Summary:')
			for (const [stage, summary] of Object.entries(diag.traceSummary)) {
				const extras: string[] = []
				if (summary.totalRecords !== undefined) {
					extras.push(`${formatNumber(summary.totalRecords)} recs`)
				}
				if (summary.totalBytes !== undefined) {
					extras.push(`${formatBytes(summary.totalBytes)}`)
				}
				console.log(
					`    - ${stage}: ${summary.avgMs.toFixed(3)} ms avg (${summary.count} calls, ${summary.totalMs.toFixed(2)} ms total${extras.length ? `, ${extras.join(', ')}` : ''})`
				)
			}
		}
	}
}

export function printComparison(
	kafkaTsResult: BenchmarkResult,
	kafkaJsResult: BenchmarkResult,
	platformaticResult?: BenchmarkResult
): void {
	const throughputRatio = kafkaTsResult.messagesPerSecond / kafkaJsResult.messagesPerSecond
	const latencyRatio = kafkaJsResult.avgLatencyMs / kafkaTsResult.avgLatencyMs

	console.log('\n' + '='.repeat(60))
	console.log('COMPARISON: kafkats vs kafkajs')
	console.log('='.repeat(60))
	console.log(
		`  Throughput:         kafkats is ${throughputRatio.toFixed(2)}x ${throughputRatio >= 1 ? 'faster' : 'slower'}`
	)
	console.log(
		`  Avg Latency:        kafkats is ${latencyRatio.toFixed(2)}x ${latencyRatio >= 1 ? 'faster' : 'slower'}`
	)

	if (platformaticResult) {
		const platformaticThroughputRatio = kafkaTsResult.messagesPerSecond / platformaticResult.messagesPerSecond
		const platformaticLatencyRatio = platformaticResult.avgLatencyMs / kafkaTsResult.avgLatencyMs
		console.log('\n' + '='.repeat(60))
		console.log('COMPARISON: kafkats vs platformatic')
		console.log('='.repeat(60))
		console.log(
			`  Throughput:         kafkats is ${platformaticThroughputRatio.toFixed(2)}x ${platformaticThroughputRatio >= 1 ? 'faster' : 'slower'}`
		)
		console.log(
			`  Avg Latency:        kafkats is ${platformaticLatencyRatio.toFixed(2)}x ${platformaticLatencyRatio >= 1 ? 'faster' : 'slower'}`
		)
	}
}

export function uniqueName(prefix: string): string {
	return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
}

export async function sleep(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms))
}

export interface TraceEvent {
	stage: string
	durationMs: number
	recordCount?: number
	bytes?: number
}

export function createTraceCollector(): {
	trace: (event: TraceEvent) => void
	getSummary: () => Record<string, TraceSummaryItem>
} {
	const totals = new Map<string, { count: number; totalMs: number; totalBytes?: number; totalRecords?: number }>()

	return {
		trace: event => {
			const entry = totals.get(event.stage) ?? {
				count: 0,
				totalMs: 0,
				totalBytes: 0,
				totalRecords: 0,
			}
			entry.count += 1
			entry.totalMs += event.durationMs
			if (event.bytes !== undefined) {
				entry.totalBytes = (entry.totalBytes ?? 0) + event.bytes
			}
			if (event.recordCount !== undefined) {
				entry.totalRecords = (entry.totalRecords ?? 0) + event.recordCount
			}
			totals.set(event.stage, entry)
		},
		getSummary: () =>
			Object.fromEntries(
				Array.from(totals.entries()).map(([stage, data]) => [
					stage,
					{
						count: data.count,
						totalMs: data.totalMs,
						avgMs: data.count ? data.totalMs / data.count : 0,
						totalBytes: data.totalBytes,
						totalRecords: data.totalRecords,
					},
				])
			),
	}
}

export function startDiagnostics(): { stop: () => BenchmarkDiagnostics } {
	const startMem = process.memoryUsage()
	const startElu = performance.eventLoopUtilization()
	let gcTotalMs = 0
	let gcCount = 0
	let observer: PerformanceObserver | null = null

	try {
		observer = new PerformanceObserver(list => {
			for (const entry of list.getEntries()) {
				gcTotalMs += entry.duration
				gcCount += 1
			}
		})
		observer.observe({ entryTypes: ['gc'] })
	} catch {
		observer = null
	}

	return {
		stop: () => {
			if (observer) {
				observer.disconnect()
			}
			const endMem = process.memoryUsage()
			const endElu = performance.eventLoopUtilization(startElu)
			return {
				gcTotalMs: gcTotalMs || undefined,
				gcCount: gcCount || undefined,
				heapDeltaBytes: endMem.heapUsed - startMem.heapUsed,
				rssDeltaBytes: endMem.rss - startMem.rss,
				elu: {
					activeMs: endElu.active,
					idleMs: endElu.idle,
					utilization: endElu.utilization,
				},
			}
		},
	}
}
