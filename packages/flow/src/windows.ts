import type { WindowDuration } from '@/types.js'

export class TimeWindows {
	readonly size: WindowDuration
	readonly advance?: WindowDuration

	private constructor(size: WindowDuration, advance?: WindowDuration) {
		this.size = size
		this.advance = advance
	}

	static of(size: WindowDuration): TimeWindows {
		return new TimeWindows(size)
	}

	advanceBy(advance: WindowDuration): TimeWindows {
		return new TimeWindows(this.size, advance)
	}
}

export class SessionWindows {
	readonly gap: WindowDuration

	private constructor(gap: WindowDuration) {
		this.gap = gap
	}

	static withInactivityGap(gap: WindowDuration): SessionWindows {
		return new SessionWindows(gap)
	}
}

export class SlidingWindows {
	readonly size: WindowDuration
	readonly grace?: WindowDuration

	private constructor(size: WindowDuration, grace?: WindowDuration) {
		this.size = size
		this.grace = grace
	}

	static of(size: WindowDuration): SlidingWindows {
		return new SlidingWindows(size)
	}

	gracePeriod(grace: WindowDuration): SlidingWindows {
		return new SlidingWindows(this.size, grace)
	}
}
