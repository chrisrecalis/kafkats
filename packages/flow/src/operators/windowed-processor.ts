/**
 * What the time, session and sliding window aggregations share (KS `AbstractKStreamTimeWindowAggregateProcessor`): the
 * value getter of their tables and the frame of their processors. A processor keeps its windows in a clocked store
 * and its processor clock under the store's name; it drops records with a null key, aggregates the others, then
 * forwards the final results of the windows that closed (emit on window close, `operators/emit-final.ts`) and runs
 * the expiry cadence (6.0).
 */
import type { Windowed } from '@/api/grouped.js'
import type { Change } from '@/operators/change.js'
import { EmitFinal, type FinalResult } from '@/operators/emit-final.js'
import type { KTableValueGetterSupplier } from '@/operators/ktable-node.js'
import type { InternalProcessorContext, Processor, ProcessorContext } from '@/processor/api.js'
import { StreamRecord, type RecordHeaders } from '@/processor/record.js'
import { ObservedStreamTime } from '@/processor/time-tracker.js'
import type { KeyValueStore, StateStore, ValueAndTimestamp, WindowedKey } from '@/state.js'
import type { EmitFinalWindows } from '@/topology/nodes.js'

export type Window = Windowed<unknown>['window']

/**
 * The value getter of a windowed aggregation (KS `view()`): a point read of one window or session of its store, the
 * stored value given as a value and timestamp by `read`.
 */
export function windowedValueGetter<K, S, A>(
	storeName: string,
	read: (stored: S, window: Window) => ValueAndTimestamp<A>
): KTableValueGetterSupplier<Windowed<K>, A> {
	return {
		storeNames: [storeName],
		create: () => {
			let store: KeyValueStore<WindowedKey<K>, S>
			return {
				init: context => {
					store = context.getStateStore(storeName)
				},
				get: async ({ key, window }) => {
					const stored = await store.get({ key, windowStart: window.start, windowEnd: window.end })
					return stored === undefined ? null : read(stored, window)
				},
			}
		},
	}
}

export abstract class WindowedAggregateProcessor<
	K,
	V,
	A,
	S extends StateStore & { expire(): Promise<void> },
> implements Processor<K | null, V | null, Windowed<K>, Change<A>> {
	protected context!: InternalProcessorContext<Windowed<K>, Change<A>>
	protected store!: S
	protected clock!: ObservedStreamTime
	private final: EmitFinal<K, A> | undefined

	constructor(
		protected readonly storeName: string,
		/** With emit on window close, the windows as its scans read them; else every update is forwarded. */
		private readonly emitFinal: EmitFinalWindows | undefined,
		protected readonly sendOldValues: () => boolean
	) {}

	init(context: ProcessorContext<Windowed<K>, Change<A>>): void {
		this.context = context as InternalProcessorContext<Windowed<K>, Change<A>>
		this.store = context.getStateStore<S>(this.storeName)
		this.clock = new ObservedStreamTime(this.context, this.storeName)
		if (this.emitFinal !== undefined) {
			this.final = new EmitFinal(this.context, this.storeName, this.emitFinal, (lowerEnd, closeTime) =>
				this.closedWindows(lowerEnd, closeTime)
			)
		}
	}

	async process(record: StreamRecord<K | null, V | null>): Promise<void> {
		if (record.key === null || record.key === undefined) {
			this.context.recordDropped('null-key', { store: this.storeName })
			return
		}
		const closeTime = await this.aggregate(record as StreamRecord<K, V | null>)
		if (closeTime === undefined) {
			return
		}
		await this.final?.maybeEmit(closeTime, record.headers)
		if (this.clock.expiryDue()) {
			await this.store.expire()
		}
	}

	/** Aggregates a record of a key; returns the close time it was judged by, undefined when it was dropped unseen. */
	protected abstract aggregate(record: StreamRecord<K, V | null>): Promise<number | undefined>

	/**
	 * The windows of every key that closed by `closeTime` and end at `lowerEnd` or later (the first end the last
	 * emission did not cover), in emit order; undefined when none can have closed (no read, and the progress stays).
	 */
	protected abstract closedWindows(lowerEnd: number, closeTime: number): Promise<FinalResult<K, A>[]> | undefined

	/** Forwards an update of a window, unless only final results are emitted. */
	protected async forward(
		key: K,
		window: Window,
		change: Change<A>,
		timestamp: number,
		headers: RecordHeaders
	): Promise<void> {
		if (this.final === undefined) {
			await this.context.forward(new StreamRecord({ key, window }, change, timestamp, headers))
		}
	}
}
