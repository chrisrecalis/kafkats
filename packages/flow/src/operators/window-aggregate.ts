/**
 * Time-window count, reduce and aggregate (KS KStreamWindowAggregate; 6.4), ported with today's lateness and
 * retention: a store keeps windows for 24 window sizes, and a window whose end is older than the processor clock minus
 * that retention is expired, so a late record does not re-create it. The clock counts window starts (the newest window
 * a record opened) and is persisted in the commit metadata under the store's name. Results carry the timestamp of the
 * record. Stage 3 switches to grace periods, timestamp clocks and the max timestamp of a window (WP3.4).
 */
import type { Windowed } from '@/api/grouped.js'
import type { Change } from '@/operators/change.js'
import { aggregatable, type Aggregation } from '@/operators/kstream-aggregate.js'
import type { KTableNode, KTableValueGetterSupplier } from '@/operators/ktable-node.js'
import type { InternalProcessorContext, Processor, ProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import { StreamRecord } from '@/processor/record.js'
import { ObservedStreamTime } from '@/processor/time-tracker.js'
import type { KeyValueStore, WindowedKey, WindowStore } from '@/state.js'

/** Window and session stores keep this many window sizes / session gaps (today's retention; 6.2 replaces it). */
export const WINDOW_STORE_RETENTION_MULTIPLIER = 24

export interface TimeWindowSizes {
	readonly sizeMs: number
	/** 0 < advance <= size; tumbling windows when equal. */
	readonly advanceMs: number
}

export type Window = Windowed<unknown>['window']

/** The windows `[start, start + size)` that contain `timestamp`, by ascending start, none negative (KS `windowsFor`). */
export function windowsFor(timestamp: number, { sizeMs, advanceMs }: TimeWindowSizes): Window[] {
	const windows: Window[] = []
	const first = Math.floor(Math.max(0, timestamp - sizeMs + advanceMs) / advanceMs) * advanceMs
	for (let start = first; start <= timestamp; start += advanceMs) {
		windows.push({ start, end: start + sizeMs })
	}
	return windows
}

/**
 * The value getter of a windowed aggregation: a point read of its window or session store. The stores of this build
 * keep no timestamps: a value is reported with the timestamp `timestampOf(window)`.
 */
export function windowedValueGetter<K, A>(
	storeName: string,
	timestampOf: (window: Window) => number
): KTableValueGetterSupplier<Windowed<K>, A> {
	return {
		storeNames: [storeName],
		create: () => {
			let store: KeyValueStore<WindowedKey<K>, A>
			return {
				init: context => {
					store = context.getStateStore(storeName)
				},
				get: async ({ key, window }) => {
					const value = await store.get({ key, windowStart: window.start, windowEnd: window.end })
					return value === undefined ? null : { value, timestamp: timestampOf(window) }
				},
			}
		},
	}
}

export class TimeWindowAggregate<K, V, A> implements KTableNode<Windowed<K>, A> {
	readonly supplier: ProcessorSupplier<K | null, V | null, Windowed<K>, Change<A>>
	readonly valueGetter: KTableValueGetterSupplier<Windowed<K>, A>
	private sendOldValues = false

	constructor(storeName: string, windows: TimeWindowSizes, aggregation: Aggregation<K, V, A>) {
		this.supplier = () =>
			new TimeWindowAggregateProcessor(storeName, windows, aggregation, () => this.sendOldValues)
		// The window's start: the earliest timestamp it holds.
		this.valueGetter = windowedValueGetter(storeName, window => window.start)
	}

	/** Always materialized: the store has the old values. */
	enableSendingOldValues(): boolean {
		this.sendOldValues = true
		return true
	}
}

class TimeWindowAggregateProcessor<K, V, A> implements Processor<K | null, V | null, Windowed<K>, Change<A>> {
	private context!: InternalProcessorContext<Windowed<K>, Change<A>>
	private store!: WindowStore<K, A>
	private clock!: ObservedStreamTime

	constructor(
		private readonly storeName: string,
		private readonly windows: TimeWindowSizes,
		private readonly aggregation: Aggregation<K, V, A>,
		private readonly sendOldValues: () => boolean
	) {}

	init(context: ProcessorContext<Windowed<K>, Change<A>>): void {
		this.context = context as InternalProcessorContext<Windowed<K>, Change<A>>
		this.store = context.getStateStore(this.storeName)
		this.clock = new ObservedStreamTime(this.context, this.storeName)
	}

	async process(record: StreamRecord<K | null, V | null>): Promise<void> {
		if (!aggregatable(this.context, this.storeName, record)) {
			return
		}
		const { key, value, timestamp, headers } = record
		const expiredBefore = this.clock.get() - this.windows.sizeMs * WINDOW_STORE_RETENTION_MULTIPLIER
		const windows = windowsFor(timestamp, this.windows)
		for (const window of windows) {
			// Re-creating an expired window from scratch would emit a wrong partial result.
			if (window.end < expiredBefore) {
				this.context.recordDropped('expired-window', { store: this.storeName, window })
				continue
			}
			const windowKey: WindowedKey<K> = { key, windowStart: window.start, windowEnd: window.end }
			const old = await this.store.get(windowKey)
			const aggregate = this.aggregation(key, value, old)
			if (aggregate === null || aggregate === undefined) {
				await this.store.delete(windowKey)
			} else {
				await this.store.put(windowKey, aggregate)
			}
			const change = { newValue: aggregate ?? null, oldValue: this.sendOldValues() ? (old ?? null) : null }
			await this.context.forward(new StreamRecord({ key, window }, change, timestamp, headers))
		}
		this.clock.advance(windows[windows.length - 1]!.start)
		if (this.clock.expiryDue()) {
			await this.store.expireOldWindows(this.clock.get())
		}
	}
}
