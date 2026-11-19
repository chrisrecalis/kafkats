import type { Clock } from '@/runtime/io.js'

interface Timer {
	readonly at: number
	readonly fn: () => void
}

/**
 * A clock the test moves. Timers fire only from `advance()`, each at its own time (the clock reads `at` while its
 * callback runs), in time order, ties in the order they were set; a timer set for a time already passed fires at
 * the next advance, `advance(0)` included.
 */
export class ManualClock implements Clock {
	private current: number
	/** In the order they were set. */
	private timers: Timer[] = []

	constructor(time = 0) {
		this.current = time
	}

	get time(): number {
		return this.current
	}

	/** Timers set and neither fired nor cancelled. */
	get pendingTimers(): number {
		return this.timers.length
	}

	now(): number {
		return this.current
	}

	setTimer(at: number, fn: () => void): () => void {
		const timer = { at, fn }
		this.timers.push(timer)
		return () => {
			this.timers = this.timers.filter(t => t !== timer)
		}
	}

	/** Moves the clock `ms` forward, firing every timer due by then, those set by the callbacks included. */
	advance(ms = 0): void {
		const target = this.current + ms
		for (let due = this.firstDue(target); due; due = this.firstDue(target)) {
			this.timers = this.timers.filter(t => t !== due)
			this.current = Math.max(this.current, due.at)
			due.fn()
		}
		this.current = target
	}

	/** The earliest timer due by `time`; of equal ones the first set. */
	private firstDue(time: number): Timer | undefined {
		let first: Timer | undefined
		for (const timer of this.timers) {
			if (timer.at <= time && (!first || timer.at < first.at)) first = timer
		}
		return first
	}
}
