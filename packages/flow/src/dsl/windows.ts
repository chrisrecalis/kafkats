/**
 * Window definitions (Kafka Streams' TimeWindows, SessionWindows, SlidingWindows, JoinWindows; ksdsl§2). Every factory
 * takes a WindowDuration and validates the window as Kafka Streams does (B9); sizes, gaps and graces are kept in
 * milliseconds. The deprecated factories get Kafka Streams' legacy grace (24 h minus the size), so their windows stay
 * open about a day, and warn once per process. The deprecated `Joined.within` maps onto JoinWindows here.
 */
import { TopologyError } from '@/api/errors.js'
import type { WindowDuration } from '@/api/options.js'
import { warnDeprecated } from '@/dsl/deprecation.js'

const DAY_MS = 86_400_000
const UNIT_MS: Readonly<Record<string, number>> = { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: DAY_MS }

/**
 * Milliseconds of a duration: a number of ms or `<n><unit>` (n may be fractional); a non-negative whole number of ms.
 * `name` names the duration in errors.
 */
export function durationMs(duration: WindowDuration, name = 'A duration'): number {
	const ms = signedDurationMs(duration, name)
	if (ms < 0) {
		throw new TopologyError(`${name} must not be negative, got ${String(duration)}`)
	}
	return ms
}

/** Milliseconds of a duration that may be negative (the before or after of join windows). */
function signedDurationMs(duration: WindowDuration, name: string): number {
	const ms = typeof duration === 'number' ? duration : parseDuration(duration, name)
	if (!Number.isSafeInteger(ms)) {
		throw new TopologyError(`${name} must be a whole number of milliseconds, got ${String(duration)}`)
	}
	return ms
}

function parseDuration(duration: string, name: string): number {
	const match = /^(-?\d+(?:\.\d+)?)(ms|s|m|h|d)$/.exec(duration)
	if (!match) {
		throw new TopologyError(
			`${name} '${duration}' is invalid: use milliseconds or a number with a unit (ms, s, m, h, d)`
		)
	}
	const ms = Number(match[1]) * UNIT_MS[match[2]!]!
	// '1.1s' is 1100.0000000000002 in floating point.
	const rounded = Math.round(ms)
	return Math.abs(ms - rounded) < 1e-6 ? rounded : ms
}

/** Kafka Streams' grace of the deprecated factories: what is left of a day after the window itself. */
function legacyGraceMs(windowMs: number): number {
	return Math.max(DAY_MS - windowMs, 0)
}

/** Fixed-size windows `[start, start + size)`, starts aligned to multiples of the advance (tumbling when advance = size). */
export class TimeWindows {
	private constructor(
		readonly sizeMs: number,
		readonly advanceMs: number,
		/** How long after its end a window still accepts records. */
		readonly graceMs: number
	) {
		if (sizeMs <= 0) {
			throw new TopologyError(`TimeWindows size must be positive, got ${sizeMs} ms`)
		}
		if (advanceMs <= 0 || advanceMs > sizeMs) {
			throw new TopologyError(
				`TimeWindows advance must be positive and at most the window size (${sizeMs} ms), got ${advanceMs} ms`
			)
		}
	}

	static ofSizeWithNoGrace(size: WindowDuration): TimeWindows {
		return TimeWindows.ofSizeAndGrace(size, 0)
	}

	static ofSizeAndGrace(size: WindowDuration, grace: WindowDuration): TimeWindows {
		const sizeMs = durationMs(size, 'TimeWindows size')
		return new TimeWindows(sizeMs, sizeMs, durationMs(grace, 'TimeWindows grace'))
	}

	/**
	 * @deprecated Use ofSizeWithNoGrace or ofSizeAndGrace: the grace is 24 h minus the size, so windows accept records
	 * up to a day late and their store keeps about a day of windows.
	 */
	static of(size: WindowDuration): TimeWindows {
		warnDeprecated(
			'TimeWindows.of',
			'its grace period is 24 h minus the window size, so windows accept records up to a day late and their ' +
				'store keeps about a day of windows. Use TimeWindows.ofSizeWithNoGrace(size) or ' +
				'TimeWindows.ofSizeAndGrace(size, grace).'
		)
		const sizeMs = durationMs(size, 'TimeWindows size')
		return new TimeWindows(sizeMs, sizeMs, legacyGraceMs(sizeMs))
	}

	/** Hopping windows: a new window every `advance` (0 < advance <= size). */
	advanceBy(advance: WindowDuration): TimeWindows {
		return new TimeWindows(this.sizeMs, durationMs(advance, 'TimeWindows advance'), this.graceMs)
	}
}

/** Sessions `[start, end]` of records at most `gap` apart. */
export class SessionWindows {
	private constructor(
		readonly gapMs: number,
		readonly graceMs: number
	) {}

	static ofInactivityGapWithNoGrace(gap: WindowDuration): SessionWindows {
		return SessionWindows.ofInactivityGapAndGrace(gap, 0)
	}

	static ofInactivityGapAndGrace(gap: WindowDuration, grace: WindowDuration): SessionWindows {
		return new SessionWindows(durationMs(gap, 'SessionWindows gap'), durationMs(grace, 'SessionWindows grace'))
	}

	/**
	 * @deprecated Use ofInactivityGapWithNoGrace or ofInactivityGapAndGrace: the grace is 24 h minus the gap, so
	 * sessions accept records up to a day late and their store keeps about a day of sessions.
	 */
	static withInactivityGap(gap: WindowDuration): SessionWindows {
		warnDeprecated(
			'SessionWindows.withInactivityGap',
			'its grace period is 24 h minus the gap, so sessions accept records up to a day late and their store ' +
				'keeps about a day of sessions. Use SessionWindows.ofInactivityGapWithNoGrace(gap) or ' +
				'SessionWindows.ofInactivityGapAndGrace(gap, grace).'
		)
		const gapMs = durationMs(gap, 'SessionWindows gap')
		return new SessionWindows(gapMs, legacyGraceMs(gapMs))
	}
}

/** Windows `[start, start + timeDifference]` that slide with the records: one per distinct set of records. */
export class SlidingWindows {
	private constructor(
		readonly timeDifferenceMs: number,
		readonly graceMs: number
	) {}

	static ofTimeDifferenceWithNoGrace(timeDifference: WindowDuration): SlidingWindows {
		return SlidingWindows.ofTimeDifferenceAndGrace(timeDifference, 0)
	}

	static ofTimeDifferenceAndGrace(timeDifference: WindowDuration, grace: WindowDuration): SlidingWindows {
		return new SlidingWindows(
			durationMs(timeDifference, 'SlidingWindows time difference'),
			durationMs(grace, 'SlidingWindows grace')
		)
	}

	/** @deprecated Use ofTimeDifferenceWithNoGrace or ofTimeDifferenceAndGrace (same windows and retention). */
	static of(timeDifference: WindowDuration): SlidingWindows {
		warnDeprecated(
			'SlidingWindows.of',
			'use SlidingWindows.ofTimeDifferenceWithNoGrace(timeDifference) or ' +
				'SlidingWindows.ofTimeDifferenceAndGrace(timeDifference, grace) (same windows and retention).'
		)
		return SlidingWindows.ofTimeDifferenceWithNoGrace(timeDifference)
	}

	/** @deprecated Use ofTimeDifferenceAndGrace (same windows and retention). */
	gracePeriod(grace: WindowDuration): SlidingWindows {
		warnDeprecated(
			'SlidingWindows.gracePeriod',
			'use SlidingWindows.ofTimeDifferenceAndGrace(timeDifference, grace) (same windows and retention).'
		)
		return SlidingWindows.ofTimeDifferenceAndGrace(this.timeDifferenceMs, grace)
	}
}

/**
 * Stream-stream join windows: a left record at `t` joins right records in `[t - before, t + after]`. One of before and
 * after may be negative, as long as the window is not inverted (before + after >= 0).
 */
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
	) {
		if (beforeMs + afterMs < 0) {
			throw new TopologyError(
				`JoinWindows before + after must not be negative, got before ${beforeMs} ms and after ${afterMs} ms`
			)
		}
	}

	static ofTimeDifferenceWithNoGrace(timeDifference: WindowDuration): JoinWindows {
		return JoinWindows.ofTimeDifferenceAndGrace(timeDifference, 0)
	}

	static ofTimeDifferenceAndGrace(timeDifference: WindowDuration, grace: WindowDuration): JoinWindows {
		const ms = durationMs(timeDifference, 'JoinWindows time difference')
		return new JoinWindows(ms, ms, durationMs(grace, 'JoinWindows grace'), true)
	}

	/**
	 * @deprecated Use ofTimeDifferenceWithNoGrace or ofTimeDifferenceAndGrace: the grace is 24 h minus twice the
	 * difference, so both join stores keep about a day of records, and unmatched left/outer results are emitted
	 * eagerly. `Joined.within` maps to this factory.
	 */
	static of(timeDifference: WindowDuration): JoinWindows {
		warnDeprecated(
			'JoinWindows.of (and Joined.within)',
			'its grace period is 24 h minus twice the time difference, so both join stores keep about a day of ' +
				'records, and left and outer joins emit unmatched records at once instead of when their window closes. ' +
				'Pass JoinWindows.ofTimeDifferenceWithNoGrace(timeDifference) or ' +
				'JoinWindows.ofTimeDifferenceAndGrace(timeDifference, grace) as the third argument of the join.'
		)
		const ms = durationMs(timeDifference, 'JoinWindows time difference')
		return new JoinWindows(ms, ms, legacyGraceMs(2 * ms), false)
	}

	/** How long before a left record's timestamp the window starts; negative for a window that starts after it. */
	before(before: WindowDuration): JoinWindows {
		const beforeMs = signedDurationMs(before, 'JoinWindows before')
		return new JoinWindows(beforeMs, this.afterMs, this.graceMs, this.spuriousResultFix)
	}

	/** How long after a left record's timestamp the window ends; negative for a window that ends before it. */
	after(after: WindowDuration): JoinWindows {
		const afterMs = signedDurationMs(after, 'JoinWindows after')
		return new JoinWindows(this.beforeMs, afterMs, this.graceMs, this.spuriousResultFix)
	}
}

/**
 * The JoinWindows of the deprecated `Joined.within` (6.1): JoinWindows as they are; the size of TimeWindows (advance and
 * grace ignored) or the gap of SessionWindows through `JoinWindows.of`, so these joins keep their eager left/outer
 * results. SlidingWindows have no join counterpart. `operator` names the join in errors.
 */
export function joinWindowsWithin(
	within: TimeWindows | SessionWindows | SlidingWindows | JoinWindows | undefined,
	operator: string
): JoinWindows {
	if (within instanceof JoinWindows) {
		return within
	}
	if (within instanceof TimeWindows) {
		return JoinWindows.of(within.sizeMs)
	}
	if (within instanceof SessionWindows) {
		return JoinWindows.of(within.gapMs)
	}
	if (within === undefined) {
		throw new TopologyError(`${operator} with a stream needs JoinWindows as its third argument`)
	}
	throw new TopologyError(`${operator} with a stream cannot use SlidingWindows; pass JoinWindows`)
}
