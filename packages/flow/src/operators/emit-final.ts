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
