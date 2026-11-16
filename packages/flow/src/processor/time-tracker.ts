import type { InternalProcessorContext } from '@/processor/api.js'

/** A time-based processor expires its store when its clock moved more than this past the last expiry. */
export const CLEANUP_INTERVAL_MS = 60_000

/**
 * A processor clock (KS observed stream time): the max timestamp the processor has seen, persisted in the task's
 * processor metadata under `key` (the name of the store the processor owns) and seeded from it when the task starts,
 * so lateness decisions are the same after a restart. Clocks with the same key share their time (the two sides of a
 * stream-stream join).
 */
export class ObservedStreamTime {
	private readonly metadata: InternalProcessorContext['metadata']
	private lastExpiry = 0

	constructor(
		context: Pick<InternalProcessorContext, 'metadata'>,
		readonly key: string
	) {
		this.metadata = context.metadata
	}

	/** -1 while unknown. */
	get(): number {
		return this.metadata.get(this.key) ?? -1
	}

	/** Raises the clock to `timestamp` (an increase marks the task's metadata for commit); returns the clock. */
	advance(timestamp: number): number {
		const current = this.get()
		if (timestamp <= current) {
			return current
		}
		this.metadata.put(this.key, timestamp)
		return timestamp
	}

	/** Whether the clock moved more than CLEANUP_INTERVAL_MS past the last expiry; if so, this expiry is noted. */
	expiryDue(): boolean {
		const time = this.get()
		if (time - this.lastExpiry <= CLEANUP_INTERVAL_MS) {
			return false
		}
		this.lastExpiry = time
		return true
	}
}
