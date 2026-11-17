/**
 * Time-window count, reduce and aggregate (KS KStreamWindowAggregate; 6.4, `KStreamWindowAggregate.java:107-161`).
 *
 * The processor clock is the max record timestamp the processor saw, persisted in the commit metadata under the
 * store's name. It is advanced by each record before its late check: a record updates each of its windows that ends
 * after `streamTime - grace` and is dropped for the others, so a hopping record may update its newer windows only. A
 * window keeps the max timestamp of its records, which every result of it carries. Null keys are dropped; null values
 * reach the aggregator, whatever its declared value type (count counts them, KS). With emit on window close, only the
 * final result of each window is forwarded (`operators/emit-final.ts`).
 */
import type { Windowed } from '@/api/grouped.js'
import type { Change } from '@/operators/change.js'
import type { FinalResult } from '@/operators/emit-final.js'
import type { Aggregation } from '@/operators/kstream-aggregate.js'
import { MaterializedTableNode, type KTableValueGetterSupplier } from '@/operators/ktable-node.js'
import { windowedValueGetter, WindowedAggregateProcessor, type Window } from '@/operators/windowed-processor.js'
import type { ProcessorSupplier } from '@/processor/api.js'
import type { RecordHeaders, StreamRecord } from '@/processor/record.js'
import type { ValueAndTimestamp, WindowedKey } from '@/state.js'
import type { ClockedWindowStore } from '@/state/time-clipping.js'

export interface TimeWindowSpec {
	readonly sizeMs: number
	/** 0 < advance <= size; tumbling windows when equal. */
	readonly advanceMs: number
	/** How long after its end a window still takes records. */
	readonly graceMs: number
}

/** The windows `[start, start + size)` that contain `timestamp`, by ascending start, none negative (KS `windowsFor`). */
function windowsFor(timestamp: number, { sizeMs, advanceMs }: TimeWindowSpec): Window[] {
	const windows: Window[] = []
	const first = Math.floor(Math.max(0, timestamp - sizeMs + advanceMs) / advanceMs) * advanceMs
	for (let start = first; start <= timestamp; start += advanceMs) {
		windows.push({ start, end: start + sizeMs })
	}
	return windows
}

export class TimeWindowAggregate<K, V, A> extends MaterializedTableNode<Windowed<K>, A> {
	readonly supplier: ProcessorSupplier<K | null, V | null, Windowed<K>, Change<A>>
	readonly valueGetter: KTableValueGetterSupplier<Windowed<K>, A>

	constructor(storeName: string, windows: TimeWindowSpec, aggregation: Aggregation<K, V, A>, emitFinal: boolean) {
		super()
		this.supplier = () =>
			new TimeWindowAggregateProcessor(storeName, windows, aggregation, emitFinal, this.sendOldValues)
		this.valueGetter = windowedValueGetter<K, ValueAndTimestamp<A>, A>(storeName, stored => stored)
	}
}

class TimeWindowAggregateProcessor<K, V, A> extends WindowedAggregateProcessor<
	K,
	V,
	A,
	ClockedWindowStore<K, ValueAndTimestamp<A>>
> {
	constructor(
		storeName: string,
		private readonly windows: TimeWindowSpec,
		private readonly aggregation: Aggregation<K, V, A>,
		emitFinal: boolean,
		sendOldValues: () => boolean
	) {
		super(storeName, emitFinal ? { kind: 'time', sizeMs: windows.sizeMs } : undefined, sendOldValues)
	}

	protected async aggregate({ key, value, timestamp, headers }: StreamRecord<K, V | null>): Promise<number> {
		// Advanced before the late check (6.0): the record is judged by a clock that includes it.
		const closeTime = this.clock.advance(timestamp) - this.windows.graceMs
		for (const window of windowsFor(timestamp, this.windows)) {
			if (window.end > closeTime) {
				await this.update(key, value as V, timestamp, window, headers)
			} else {
				this.context.recordDropped('expired-window', { store: this.storeName, window, closeTime })
			}
		}
		return closeTime
	}

	private async update(key: K, value: V, timestamp: number, window: Window, headers: RecordHeaders): Promise<void> {
		const windowKey: WindowedKey<K> = { key, windowStart: window.start, windowEnd: window.end }
		const old = await this.store.get(windowKey)
		const aggregate = this.aggregation(key, value, old?.value)
		// The max timestamp of the window's records.
		const resultTimestamp = old === undefined ? timestamp : Math.max(timestamp, old.timestamp)
		if (aggregate === null || aggregate === undefined) {
			await this.store.delete(windowKey)
		} else {
			await this.store.put(windowKey, { value: aggregate, timestamp: resultTimestamp })
		}
		const change = { newValue: aggregate ?? null, oldValue: this.sendOldValues() ? (old?.value ?? null) : null }
		await this.forward(key, window, change, resultTimestamp, headers)
	}

	/**
	 * The windows of every key that closed since the last emission: those starting in `[lower, upper]`, where `upper`
	 * is the start of the newest window ended by `closeTime` and `lower` the first start the last emission did not
	 * cover (KS `emitRangeLowerBound`, `emitRangeUpperBound`, `shouldRangeFetch`). Read below the store's read floor.
	 */
	protected closedWindows(lowerEnd: number, closeTime: number): Promise<FinalResult<K, A>[]> | undefined {
		const upper = closeTime - this.windows.sizeMs
		if (upper < 0) {
			return undefined
		}
		const lower = lowerEnd - this.windows.sizeMs
		if (sameWindows(windowsFor(upper, this.windows), windowsFor(lower - 1, this.windows))) {
			return undefined
		}
		return this.fetchAll(lower, upper)
	}

	private async fetchAll(lower: number, upper: number): Promise<FinalResult<K, A>[]> {
		const closed: FinalResult<K, A>[] = []
		for await (const [{ key, windowStart: start, windowEnd: end }, stored] of this.store.unclipped.fetchAll(
			lower,
			upper
		)) {
			closed.push({ key: { key, window: { start, end } }, value: stored.value, timestamp: stored.timestamp })
		}
		// By start, as Kafka Streams' time-ordered stores return them (providers order fetchAll as they like).
		return closed.sort((a, b) => a.key.window.start - b.key.window.start)
	}
}

function sameWindows(a: readonly Window[], b: readonly Window[]): boolean {
	return a.length === b.length && a.every((window, i) => window.start === b[i]!.start)
}
