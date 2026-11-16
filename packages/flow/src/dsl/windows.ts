/**
 * Window definitions (Kafka Streams' TimeWindows, SessionWindows, SlidingWindows, JoinWindows). Every factory
 * takes a WindowDuration; sizes, gaps and graces are kept in milliseconds. The deprecated factories get Kafka
 * Streams' legacy grace (24 h minus the size), so their windows stay open about a day.
 */
import { TopologyError } from '@/api/errors.js'
import type { WindowDuration } from '@/api/options.js'

const DAY_MS = 86_400_000
const UNIT_MS: Readonly<Record<string, number>> = { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: DAY_MS }

/** Milliseconds of a duration: a number of ms or `<n><unit>` (n may be fractional); a non-negative whole number of ms. */
export function durationMs(duration: WindowDuration): number {
	const ms = typeof duration === 'number' ? duration : parseDuration(duration)
	if (!Number.isSafeInteger(ms) || ms < 0) {
		throw new TopologyError(`A duration must be a non-negative whole number of milliseconds, got ${String(duration)}`)
	}
	return ms
}

function parseDuration(duration: string): number {
	const match = /^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/.exec(duration)
	if (!match) {
		throw new TopologyError(`Invalid duration '${duration}': use milliseconds or a number with a unit (ms, s, m, h, d)`)
	}
	const ms = Number(match[1]) * UNIT_MS[match[2]!]!
	// '1.1s' is 1100.0000000000002 in floating point.
	const rounded = Math.round(ms)
	return Math.abs(ms - rounded) < 1e-6 ? rounded : ms
}

/** Fixed-size windows `[start, start + size)`, starts aligned to multiples of the advance (tumbling when advance = size). */
export class TimeWindows {
	private constructor(
		readonly sizeMs: number,
		readonly advanceMs: number,
		/** How long after its end a window still accepts records. */
		readonly graceMs: number
	) {}

	static ofSizeWithNoGrace(size: WindowDuration): TimeWindows {
		const sizeMs = durationMs(size)
		return new TimeWindows(sizeMs, sizeMs, 0)
	}

	static ofSizeAndGrace(size: WindowDuration, grace: WindowDuration): TimeWindows {
		const sizeMs = durationMs(size)
		return new TimeWindows(sizeMs, sizeMs, durationMs(grace))
	}

	/** @deprecated Use ofSizeWithNoGrace or ofSizeAndGrace: the grace is 24 h minus the size, so windows are kept about a day. */
	static of(size: WindowDuration): TimeWindows {
		const sizeMs = durationMs(size)
		return new TimeWindows(sizeMs, sizeMs, Math.max(DAY_MS - sizeMs, 0))
	}

	/** Hopping windows: a new window every `advance` (0 < advance <= size). */
	advanceBy(advance: WindowDuration): TimeWindows {
		return new TimeWindows(this.sizeMs, durationMs(advance), this.graceMs)
	}
}

/** Sessions `[start, end]` of records less than `gap` apart. */
export class SessionWindows {
	private constructor(
		readonly gapMs: number,
		readonly graceMs: number
	) {}

	static ofInactivityGapWithNoGrace(gap: WindowDuration): SessionWindows {
		return new SessionWindows(durationMs(gap), 0)
	}

	static ofInactivityGapAndGrace(gap: WindowDuration, grace: WindowDuration): SessionWindows {
		return new SessionWindows(durationMs(gap), durationMs(grace))
	}

	/** @deprecated Use ofInactivityGapWithNoGrace or ofInactivityGapAndGrace: the grace is 24 h minus the gap. */
	static withInactivityGap(gap: WindowDuration): SessionWindows {
		const gapMs = durationMs(gap)
		return new SessionWindows(gapMs, Math.max(DAY_MS - gapMs, 0))
	}
}

/** Windows `[start, start + timeDifference]` that slide with the records: one per distinct set of records. */
export class SlidingWindows {
	private constructor(
		readonly timeDifferenceMs: number,
		readonly graceMs: number
	) {}

	static ofTimeDifferenceWithNoGrace(timeDifference: WindowDuration): SlidingWindows {
		return new SlidingWindows(durationMs(timeDifference), 0)
	}

	static ofTimeDifferenceAndGrace(timeDifference: WindowDuration, grace: WindowDuration): SlidingWindows {
		return new SlidingWindows(durationMs(timeDifference), durationMs(grace))
	}

	/** @deprecated Use ofTimeDifferenceWithNoGrace or ofTimeDifferenceAndGrace. */
	static of(timeDifference: WindowDuration): SlidingWindows {
		return new SlidingWindows(durationMs(timeDifference), 0)
	}

	/** @deprecated Use ofTimeDifferenceAndGrace. */
	gracePeriod(grace: WindowDuration): SlidingWindows {
		return new SlidingWindows(this.timeDifferenceMs, durationMs(grace))
	}
}

/** Stream-stream join windows: a left record at `t` joins right records in `[t - before, t + after]`. */
export class JoinWindows {
	private constructor(
		readonly beforeMs: number,
		readonly afterMs: number,
		readonly graceMs: number,
		/**
		 * Kafka Streams' `enableSpuriousResultFix`: left/outer results of unmatched records wait until the
		 * window closed. False only for the deprecated JoinWindows.of, which emits them eagerly.
		 */
		readonly spuriousResultFix: boolean
	) {}

	static ofTimeDifferenceWithNoGrace(timeDifference: WindowDuration): JoinWindows {
		const ms = durationMs(timeDifference)
		return new JoinWindows(ms, ms, 0, true)
	}

	static ofTimeDifferenceAndGrace(timeDifference: WindowDuration, grace: WindowDuration): JoinWindows {
		const ms = durationMs(timeDifference)
		return new JoinWindows(ms, ms, durationMs(grace), true)
	}

	/**
	 * @deprecated Use ofTimeDifferenceWithNoGrace or ofTimeDifferenceAndGrace: the grace is 24 h minus twice
	 * the difference and unmatched left/outer results are emitted eagerly.
	 */
	static of(timeDifference: WindowDuration): JoinWindows {
		const ms = durationMs(timeDifference)
		return new JoinWindows(ms, ms, Math.max(DAY_MS - 2 * ms, 0), false)
	}

	before(before: WindowDuration): JoinWindows {
		return new JoinWindows(durationMs(before), this.afterMs, this.graceMs, this.spuriousResultFix)
	}

	after(after: WindowDuration): JoinWindows {
		return new JoinWindows(this.beforeMs, durationMs(after), this.graceMs, this.spuriousResultFix)
	}
}
