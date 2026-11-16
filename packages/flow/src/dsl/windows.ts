/**
 * Window definitions (Kafka Streams' TimeWindows, SessionWindows, SlidingWindows, JoinWindows). Every factory
 * takes a WindowDuration; sizes, gaps and graces are kept in milliseconds.
 */
import type { WindowDuration } from '@/api/options.js'
import { notImplemented } from '@/dsl/not-implemented.js'

/** Fixed-size windows `[start, start + size)`, starts aligned to multiples of the advance (tumbling when advance = size). */
export class TimeWindows {
	private constructor(
		readonly sizeMs: number,
		readonly advanceMs: number,
		/** How long after its end a window still accepts records. */
		readonly graceMs: number
	) {}

	static ofSizeWithNoGrace(_size: WindowDuration): TimeWindows {
		return notImplemented('TimeWindows.ofSizeWithNoGrace')
	}

	static ofSizeAndGrace(_size: WindowDuration, _grace: WindowDuration): TimeWindows {
		return notImplemented('TimeWindows.ofSizeAndGrace')
	}

	/** @deprecated Use ofSizeWithNoGrace or ofSizeAndGrace: the grace is 24 h minus the size, so windows are kept about a day. */
	static of(_size: WindowDuration): TimeWindows {
		return notImplemented('TimeWindows.of')
	}

	/** Hopping windows: a new window every `advance` (0 < advance <= size). */
	advanceBy(_advance: WindowDuration): TimeWindows {
		return notImplemented('TimeWindows.advanceBy')
	}
}

/** Sessions `[start, end]` of records less than `gap` apart. */
export class SessionWindows {
	private constructor(
		readonly gapMs: number,
		readonly graceMs: number
	) {}

	static ofInactivityGapWithNoGrace(_gap: WindowDuration): SessionWindows {
		return notImplemented('SessionWindows.ofInactivityGapWithNoGrace')
	}

	static ofInactivityGapAndGrace(_gap: WindowDuration, _grace: WindowDuration): SessionWindows {
		return notImplemented('SessionWindows.ofInactivityGapAndGrace')
	}

	/** @deprecated Use ofInactivityGapWithNoGrace or ofInactivityGapAndGrace: the grace is 24 h minus the gap. */
	static withInactivityGap(_gap: WindowDuration): SessionWindows {
		return notImplemented('SessionWindows.withInactivityGap')
	}
}

/** Windows `[start, start + timeDifference]` that slide with the records: one per distinct set of records. */
export class SlidingWindows {
	private constructor(
		readonly timeDifferenceMs: number,
		readonly graceMs: number
	) {}

	static ofTimeDifferenceWithNoGrace(_timeDifference: WindowDuration): SlidingWindows {
		return notImplemented('SlidingWindows.ofTimeDifferenceWithNoGrace')
	}

	static ofTimeDifferenceAndGrace(_timeDifference: WindowDuration, _grace: WindowDuration): SlidingWindows {
		return notImplemented('SlidingWindows.ofTimeDifferenceAndGrace')
	}

	/** @deprecated Use ofTimeDifferenceWithNoGrace or ofTimeDifferenceAndGrace. */
	static of(_timeDifference: WindowDuration): SlidingWindows {
		return notImplemented('SlidingWindows.of')
	}

	/** @deprecated Use ofTimeDifferenceAndGrace. */
	gracePeriod(_grace: WindowDuration): SlidingWindows {
		return notImplemented('SlidingWindows.gracePeriod')
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

	static ofTimeDifferenceWithNoGrace(_timeDifference: WindowDuration): JoinWindows {
		return notImplemented('JoinWindows.ofTimeDifferenceWithNoGrace')
	}

	static ofTimeDifferenceAndGrace(_timeDifference: WindowDuration, _grace: WindowDuration): JoinWindows {
		return notImplemented('JoinWindows.ofTimeDifferenceAndGrace')
	}

	/**
	 * @deprecated Use ofTimeDifferenceWithNoGrace or ofTimeDifferenceAndGrace: the grace is 24 h minus twice
	 * the difference and unmatched left/outer results are emitted eagerly.
	 */
	static of(_timeDifference: WindowDuration): JoinWindows {
		return notImplemented('JoinWindows.of')
	}

	before(_before: WindowDuration): JoinWindows {
		return notImplemented('JoinWindows.before')
	}

	after(_after: WindowDuration): JoinWindows {
		return notImplemented('JoinWindows.after')
	}
}
