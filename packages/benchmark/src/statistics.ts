export interface SummaryStats {
	count: number
	min: number
	max: number
	mean: number
	median: number
	stdev: number
}

function median(sorted: number[]): number {
	if (sorted.length === 0) return 0
	const mid = Math.floor(sorted.length / 2)
	if (sorted.length % 2 === 0) {
		const left = sorted[mid - 1]
		const right = sorted[mid]
		if (left === undefined || right === undefined) return 0
		return (left + right) / 2
	}
	return sorted[mid] ?? 0
}

export function summarize(values: number[]): SummaryStats {
	if (values.length === 0) {
		return { count: 0, min: 0, max: 0, mean: 0, median: 0, stdev: 0 }
	}

	let sum = 0
	let min = Infinity
	let max = -Infinity
	for (const v of values) {
		sum += v
		if (v < min) min = v
		if (v > max) max = v
	}
	const mean = sum / values.length

	let variance = 0
	for (const v of values) {
		const d = v - mean
		variance += d * d
	}
	const stdev = Math.sqrt(variance / values.length)

	const sorted = [...values].sort((a, b) => a - b)
	return {
		count: values.length,
		min,
		max,
		mean,
		median: median(sorted),
		stdev,
	}
}
