/**
 * The clock of a window or session(-layout) store of one task (6.2): the clipping layer of the store's stack keeps it
 * and persists it as `<store>.observed` in the task's processor metadata, so it survives in the commit metadata; the
 * task seeds it when it starts running (4.4).
 */
export interface StoreClock {
	/** Max key time written (window start, session end, entry timestamp); -1 unknown. */
	readonly time: number
	/** Processor writes through the clipping layer only (restore writes go to the provider store). */
	advance(keyTime: number): void
	/** `time = max(time, seed)`, when the task starts running. */
	seed(time: number): void
	/** Lowest key time a read may return: `time - retention + 1` (unbounded under emit-final, 6.8). */
	readonly readFloor: number
	/** Expiry cutoff: `min(time - retention + 1, emit-final bound)`. */
	readonly expiryCutoff: number
}
