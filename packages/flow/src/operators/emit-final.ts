/**
 * Emit on window close (KS `EmitStrategy.onWindowClose()`, KIP-825; 6.8): a windowed aggregation forwards only the
 * final result of each window, once the window closed, instead of each update
 * (`AbstractKStreamTimeWindowAggregateProcessor.java:143-246`, `KStreamSessionWindowAggregate.java:241-323`).
 *
 * After each record, at most once per `emitIntervalMs` of wall clock, the processor reads the windows that closed since
 * the last emission from its store and forwards them. The close time of the last emission is the task's processor
 * metadata `<store>.emit`, committed with the offsets, so a window is emitted once across restarts (at-least-once may
 * emit it again after a failure), and the store's expiry keeps every window the next emission still reads
 * (`state/time-clipping.ts`).
 */
import type { Windowed } from '@/api/grouped.js'
import type { Change } from '@/operators/change.js'
import type { InternalProcessorContext } from '@/processor/api.js'
import { StreamRecord, type RecordHeaders } from '@/processor/record.js'
import { yieldIfDue } from '@/runtime/signals.js'
import type { SessionStoreReads } from '@/state/time-clipping.js'
import type { ValueAndTimestamp } from '@/state.js'

/** The final result of one window. */
export interface FinalResult<K, A> {
	readonly key: Windowed<K>
	readonly value: A
	readonly timestamp: number
}

/**
 * The windows that closed after the close time of the last emission (`last`, -1 before the first) and by `closeTime`,
 * in emit order; undefined when no window can have closed in between (no read, and the progress stays).
 */
export type ClosedWindows<K, A> = (last: number, closeTime: number) => Promise<FinalResult<K, A>[]> | undefined

export class EmitFinal<K, A> {
	/** Wall clock time from which the next emission may run (KS `TimeTracker.nextTimeToEmit`). */
	private nextTimeToEmit = 0
	private readonly progressKey: string

	constructor(
		private readonly context: InternalProcessorContext<Windowed<K>, Change<A>>,
		store: string,
		private readonly closedWindows: ClosedWindows<K, A>
	) {
		this.progressKey = `${store}.emit`
	}

	/** After each record: forwards the windows closed by `closeTime`, with the headers of the record. */
	async maybeEmit(closeTime: number, headers: RecordHeaders): Promise<void> {
		const now = this.context.currentSystemTimeMs()
		if (now < this.nextTimeToEmit) {
			return
		}
		// From now, not from the last due time: after a jump of the wall clock, one emission, not one per interval.
		this.nextTimeToEmit = now + this.context.config.emitIntervalMs
		const last = this.context.metadata.get(this.progressKey) ?? -1
		if (last !== -1 && last >= closeTime) {
			return
		}
		const closed = this.closedWindows(last, closeTime)
		if (closed === undefined) {
			return
		}
		for (const { key, value, timestamp } of await closed) {
			await this.context.forward(new StreamRecord(key, { newValue: value, oldValue: null }, timestamp, headers))
		}
		this.context.metadata.put(this.progressKey, closeTime)
	}
}

/** Entries a scan of a whole store reads between two yields to the event loop. */
const YIELD_EVERY = 1000

/**
 * The final results of the windows of a session(-layout) store ending in `[lowerEnd, upperEnd]`, by end, each stored
 * value given as a value and timestamp by `read`. Such stores are ordered by key, so this scans all of it (B16), below
 * its read floor too, yielding to the event loop on the way.
 */
export async function scanClosedWindows<K, S, A>(
	store: Pick<SessionStoreReads<K, S>, 'all'>,
	lowerEnd: number,
	upperEnd: number,
	read: (stored: S, window: Windowed<K>['window']) => ValueAndTimestamp<A>
): Promise<FinalResult<K, A>[]> {
	const closed: FinalResult<K, A>[] = []
	let scanned = 0
	for await (const [{ key, windowStart: start, windowEnd: end }, stored] of store.all()) {
		if (end >= lowerEnd && end <= upperEnd) {
			const window = { start, end }
			closed.push({ key: { key, window }, ...read(stored, window) })
		}
		if (++scanned % YIELD_EVERY === 0) await yieldIfDue()
	}
	// By end, as Kafka Streams' time-ordered session stores return them.
	return closed.sort((a, b) => a.key.window.end - b.key.window.end)
}
