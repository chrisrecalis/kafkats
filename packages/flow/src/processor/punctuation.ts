/**
 * Punctuation schedules of one task (KS `PunctuationQueue` / `PunctuationSchedule`): one queue for stream time,
 * one for the wall clock.
 */
import type { Cancellable, MaybePromise } from '@/processor/api.js'

/** Runs the punctuator of one schedule with its node's punctuation context and error handling. */
export type Punctuate = (timestamp: number) => MaybePromise<void>

export class PunctuationSchedule implements Cancellable {
	private cancelled = false

	constructor(
		private readonly queue: PunctuationQueue,
		/** Next due time. */
		public due: number,
		readonly interval: number,
		readonly punctuate: Punctuate
	) {}

	get isCancelled(): boolean {
		return this.cancelled
	}

	/** Also from inside its own punctuator: the schedule is then not re-added. */
	cancel(): void {
		this.cancelled = true
		this.queue.remove(this)
	}

	/** Moves `due` past `time`; missed intervals are skipped, never replayed (KS). */
	advance(time: number): void {
		const next = this.due + this.interval
		this.due = time >= next ? this.due + (Math.floor((time - this.due) / this.interval) + 1) * this.interval : next
	}
}

export class PunctuationQueue {
	/** Sorted by due time; equal due times in insertion order. */
	private readonly schedules: PunctuationSchedule[] = []

	get size(): number {
		return this.schedules.length
	}

	schedule(due: number, interval: number, punctuate: Punctuate): PunctuationSchedule {
		const schedule = new PunctuationSchedule(this, due, interval, punctuate)
		this.add(schedule)
		return schedule
	}

	/** Earliest due time, undefined when nothing is scheduled. */
	nextDue(): number | undefined {
		return this.schedules[0]?.due
	}

	/** Whether a schedule is due at `time`. */
	canPunctuate(time: number): boolean {
		const next = this.nextDue()
		return next !== undefined && next <= time
	}

	/**
	 * Runs every schedule due at `time` in due order with `time` (the current stream time or wall clock, not the due
	 * time), re-adding each that was not cancelled meanwhile. Returns whether anything ran. A failing punctuator
	 * rejects (its error was not handled to 'continue'); its schedule is then dropped with the task.
	 */
	async maybePunctuate(time: number): Promise<boolean> {
		let punctuated = false
		for (let next = this.schedules[0]; next !== undefined && next.due <= time; next = this.schedules[0]) {
			this.schedules.shift()
			await next.punctuate(time)
			if (!next.isCancelled) {
				next.advance(time)
				this.add(next)
			}
			punctuated = true
		}
		return punctuated
	}

	remove(schedule: PunctuationSchedule): void {
		const index = this.schedules.indexOf(schedule)
		if (index >= 0) {
			this.schedules.splice(index, 1)
		}
	}

	clear(): void {
		this.schedules.length = 0
	}

	private add(schedule: PunctuationSchedule): void {
		let index = this.schedules.length
		while (index > 0 && this.schedules[index - 1]!.due > schedule.due) {
			index--
		}
		this.schedules.splice(index, 0, schedule)
	}
}
