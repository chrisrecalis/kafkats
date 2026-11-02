/**
 * Produce-rate tracking for estimated time lag. Pure; the collector feeds it high watermarks and
 * asks for estimates.
 */

export interface RateTrackerOptions {
	/** Samples kept per partition (default: 10, i.e. 5 minutes at a 30 s interval) */
	samples?: number
	/** Samples older than this are ignored (default: 600000 = 10 minutes) */
	maxAgeMs?: number
	/** Below this produce rate the partition counts as idle and yields no estimate (default: 0.1) */
	minMessagesPerSecond?: number
}

interface Sample {
	at: number
	highWatermark: bigint
}

/**
 * Estimates how far behind a committed offset is in time from how fast the partition is being
 * produced to: `(highWatermark - committed) / rate`. Costs nothing beyond the ListOffsets call the
 * collector already makes; the price is that it is an estimate. Returns `null` while warming up (fewer
 * than two samples), when the producer is idle, or when the samples span no time.
 */
export class RateTracker {
	private readonly samples = new Map<string, Sample[]>()
	private readonly maxSamples: number
	private readonly maxAgeMs: number
	private readonly minPerMs: number

	constructor(options: RateTrackerOptions = {}) {
		this.maxSamples = Math.max(2, options.samples ?? 10)
		this.maxAgeMs = options.maxAgeMs ?? 10 * 60_000
		this.minPerMs = (options.minMessagesPerSecond ?? 0.1) / 1000
	}

	observe(key: string, highWatermark: bigint, now: number): void {
		const list = this.samples.get(key) ?? []
		// A truncated or recreated log invalidates the history
		const last = list[list.length - 1]
		if (last && highWatermark < last.highWatermark) list.length = 0
		list.push({ at: now, highWatermark })
		if (list.length > this.maxSamples) list.splice(0, list.length - this.maxSamples)
		this.samples.set(key, list)
	}

	/** Estimated seconds of lag, or `null` when no estimate can be made */
	estimate(key: string, committedOffset: bigint, now: number): number | null {
		const list = this.samples.get(key)
		if (!list || list.length < 2) return null
		const newest = list[list.length - 1]!
		const oldest = list.find(s => now - s.at <= this.maxAgeMs)
		if (!oldest || oldest === newest) return null

		const elapsedMs = newest.at - oldest.at
		if (elapsedMs <= 0) return null
		const ratePerMs = Number(newest.highWatermark - oldest.highWatermark) / elapsedMs
		if (ratePerMs < this.minPerMs) return null

		const behind = newest.highWatermark - committedOffset
		// Callers settle caught-up / beyond-the-end from the offsets; this only guards the arithmetic
		if (behind <= 0n) return 0
		return Number(behind) / ratePerMs / 1000
	}

	/** Forget partitions not in `keep` */
	retain(keep: ReadonlySet<string>): void {
		for (const key of this.samples.keys()) {
			if (!keep.has(key)) this.samples.delete(key)
		}
	}
}
