/**
 * Sliding-window count, reduce and aggregate (KS KStreamSlidingWindowAggregate; 6.6, ksdsl§5).
 *
 * A sliding window `[start, start + d]` (both ends inclusive) holds records at most `d` apart. Every record has a left
 * window ending at it and, when a later record is at most `d + 1` after it, a right window starting just after it: the
 * windows of a key are those in which a record enters or leaves the slide (KS `SlidingWindows` javadoc). A record is
 * folded into the windows of its key that contain it, then creates the windows it brings (KS `createWindows`): the
 * previous record's right window, its own left window (over the window before it when the previous record is inside)
 * and its own right window, a copy of the window holding the records after it, without it. Records before `d`, whose
 * left windows would start below 0, share the window `[0, d]` (KS `processEarly`). Windows are iterated forward only
 * (KS `processInOrder`); an aggregator must return a new aggregate rather than change the one it is given, since that
 * one may also be copied into the record's right window (as in Kafka Streams).
 *
 * The windows a record may touch start in `[t - 2d, t + 1]`. Kept in a session-layout store (key first, windows as
 * sessions `[start, start + d]`), they are the sessions of one key ending at `t - d` or later and starting at `t + 1`
 * or earlier (B15); each stored aggregate carries the max timestamp of its records, which every result of it carries.
 * The processor clock is the max record timestamp, persisted under the store's name and advanced before the late
 * check: a record whose windows all closed (`t + d < streamTime - grace`) is dropped, and a window takes records only
 * while `end >= streamTime - grace`. Null keys and values are dropped. Results carry the headers of the record. With
 * emit on window close, only the final result of each window is forwarded (`operators/emit-final.ts`).
 *
 * Two deliberate differences from Kafka Streams: the first update of a window carries no old value (KS sends the
 * aggregate it started from, the initializer's or a copy of the window before, which no table ever held), and final
 * results are looked for after every record (KS skips that after a dropped or early record, which only matters while
 * the wall-clock throttle holds results back).
 */
import type { Windowed } from '@/api/grouped.js'
import type { Change } from '@/operators/change.js'
import { scanClosedWindows, type FinalResult } from '@/operators/emit-final.js'
import type { Aggregation } from '@/operators/kstream-aggregate.js'
import { MaterializedTableNode, type KTableValueGetterSupplier } from '@/operators/ktable-node.js'
import { windowedValueGetter, WindowedAggregateProcessor } from '@/operators/windowed-processor.js'
import type { ProcessorSupplier } from '@/processor/api.js'
import type { RecordHeaders, StreamRecord } from '@/processor/record.js'
import type { ValueAndTimestamp } from '@/state.js'
import type { ClockedSessionStore } from '@/state/time-clipping.js'

export interface SlidingWindowSpec {
	/** `d`: the max time between two records of one window. */
	readonly timeDifferenceMs: number
	/** How long after its end a window still takes records. */
	readonly graceMs: number
}

/** The record being processed and the close time it is judged by. */
interface Current<K, V> {
	readonly key: K
	readonly value: V
	readonly timestamp: number
	readonly headers: RecordHeaders
	readonly closeTime: number
}

/** What a record at `t` found among the windows of its key starting in `[t - 2d, t + 1]`. */
interface Found<A> {
	/** The starts of those windows. */
	readonly starts: ReadonlySet<number>
	/** The window ending before `t` with the largest start: the records before the record. */
	readonly before: ValueAndTimestamp<A> | undefined
	/** The window containing `t` with the largest start, as it was before the record: the records after it. */
	readonly after: ValueAndTimestamp<A> | undefined
	/** The timestamp of the latest record before `t` (KS `previousRecordTimestamp`). */
	readonly previous: number | undefined
	readonly leftExists: boolean
	readonly rightExists: boolean
}

export class SlidingWindowAggregate<K, V, A> extends MaterializedTableNode<Windowed<K>, A> {
	readonly supplier: ProcessorSupplier<K | null, V | null, Windowed<K>, Change<A>>
	readonly valueGetter: KTableValueGetterSupplier<Windowed<K>, A>

	constructor(storeName: string, windows: SlidingWindowSpec, aggregation: Aggregation<K, V, A>, emitFinal: boolean) {
		super()
		this.supplier = () =>
			new SlidingWindowAggregateProcessor(storeName, windows, aggregation, emitFinal, this.sendOldValues)
		this.valueGetter = windowedValueGetter<K, ValueAndTimestamp<A>, A>(storeName, stored => stored)
	}
}

class SlidingWindowAggregateProcessor<K, V, A> extends WindowedAggregateProcessor<
	K,
	V,
	A,
	ClockedSessionStore<K, ValueAndTimestamp<A>>
> {
	private readonly d: number

	constructor(
		storeName: string,
		private readonly windows: SlidingWindowSpec,
		private readonly aggregation: Aggregation<K, V, A>,
		emitFinal: boolean,
		sendOldValues: () => boolean
	) {
		super(storeName, emitFinal ? { kind: 'sliding', sizeMs: windows.timeDifferenceMs } : undefined, sendOldValues)
		this.d = windows.timeDifferenceMs
	}

	protected async aggregate({
		key,
		value,
		timestamp,
		headers,
	}: StreamRecord<K, V | null>): Promise<number | undefined> {
		if (value === null || value === undefined) {
			this.context.recordDropped('null-value', { store: this.storeName })
			return undefined
		}
		// Advanced before the late check (6.0): the record is judged by a clock that includes it.
		const closeTime = this.clock.advance(timestamp) - this.windows.graceMs
		const current: Current<K, V> = { key, value, timestamp, headers, closeTime }
		if (timestamp + this.d < closeTime) {
			const window = { start: timestamp, end: timestamp + this.d }
			this.context.recordDropped('expired-window', { store: this.storeName, window, closeTime })
		} else if (timestamp < this.d) {
			await this.processEarly(current)
		} else {
			await this.processInOrder(current)
		}
		return closeTime
	}

	/**
	 * KS `processInOrder`: by start, the windows found end before the record, are its left window, contain it, or are
	 * its right window.
	 */
	private async processInOrder(r: Current<K, V>): Promise<void> {
		const t = r.timestamp
		const starts = new Set<number>()
		let before: ValueAndTimestamp<A> | undefined
		let after: ValueAndTimestamp<A> | undefined
		let previous: number | undefined
		let leftExists = false
		let rightExists = false
		for (const [start, stored] of await this.windowsAround(r)) {
			starts.add(start)
			const end = start + this.d
			if (end < t) {
				before = stored
				previous = stored.timestamp
			} else if (end === t) {
				leftExists = true
				if (stored.timestamp < t) previous = stored.timestamp
				await this.fold(r, start, stored)
			} else if (start <= t) {
				after = stored
				await this.fold(r, start, stored)
			} else {
				rightExists = true
			}
		}
		await this.createWindows(r, { starts, before, after, previous, leftExists, rightExists })
	}

	/**
	 * KS `processEarly`, for a record before `d`: `[0, d]` is the left window of every such record; the windows found
	 * are that one, windows containing the record (right windows of earlier records), or its own right window.
	 */
	private async processEarly(r: Current<K, V>): Promise<void> {
		const t = r.timestamp
		const starts = new Set<number>()
		let combined: ValueAndTimestamp<A> | undefined
		let after: ValueAndTimestamp<A> | undefined
		let previous: number | undefined
		let rightExists = false
		for (const [start, stored] of await this.windowsAround(r)) {
			starts.add(start)
			if (start === 0) {
				combined = stored
				// When it holds a record at t or later, that one made the previous record's right window (KS).
				if (stored.timestamp < t) previous = stored.timestamp
			} else if (start <= t) {
				after = stored
				await this.fold(r, start, stored)
			} else {
				rightExists = true
			}
		}
		// Without a right window of an earlier record containing it, the records after the record are those of [0, d]
		// when its latest one is after the record: with a record before it too, there would be such a right window.
		if (after === undefined && combined !== undefined && combined.timestamp > t) {
			after = combined
		}
		await this.createRightWindow(r, after, rightExists)
		await this.createPreviousRightWindow(r, previous, starts)
		await this.fold(r, 0, combined)
	}

	/** KS `createWindows`: the previous record's right window, the record's left window, then its right window. */
	private async createWindows(r: Current<K, V>, found: Found<A>): Promise<void> {
		await this.createPreviousRightWindow(r, found.previous, found.starts)
		if (!found.leftExists) {
			// It holds the records before the record when the latest of them is inside (KS `leftWindowNotEmpty`).
			const { before } = found
			const seed = before !== undefined && r.timestamp - this.d <= before.timestamp ? before.value : undefined
			await this.fold(r, r.timestamp - this.d, undefined, seed)
		}
		await this.createRightWindow(r, found.after, found.rightExists)
	}

	/**
	 * The previous record's right window, if it does not exist and the record falls into it: the record alone. One
	 * ending at the record is the record's left window, which `createWindows` creates next: Kafka Streams writes that
	 * window twice (`>=`), which its record cache merges into one update, where flow would forward both.
	 */
	private async createPreviousRightWindow(
		r: Current<K, V>,
		previous: number | undefined,
		starts: ReadonlySet<number>
	): Promise<void> {
		if (previous !== undefined && !starts.has(previous + 1) && previous + 1 + this.d > r.timestamp) {
			await this.fold(r, previous + 1, undefined)
		}
	}

	/**
	 * The record's right window, if it does not exist and a record follows within it: a copy of the records after the
	 * record (KS `createCurrentRecordRightWindow`). It ends after the record's left window, so it is never closed.
	 */
	private async createRightWindow(
		r: Current<K, V>,
		after: ValueAndTimestamp<A> | undefined,
		exists: boolean
	): Promise<void> {
		if (!exists && after !== undefined && after.timestamp > r.timestamp) {
			await this.write(r, r.timestamp + 1, after.value, after.timestamp, undefined)
		}
	}

	/**
	 * Folds the record into the window starting at `start` unless it closed (KS `updateWindowAndForward`): into its
	 * stored aggregate, or for a new window (`stored` undefined) into `seed` (undefined: the initializer's).
	 */
	private async fold(
		r: Current<K, V>,
		start: number,
		stored: ValueAndTimestamp<A> | undefined,
		seed: A | undefined = stored?.value
	): Promise<void> {
		const end = start + this.d
		if (end < r.closeTime) {
			const window = { start, end }
			this.context.recordDropped('expired-window', { store: this.storeName, window, closeTime: r.closeTime })
			return
		}
		const aggregate = this.aggregation(r.key, r.value, seed)
		// The max timestamp of the window's records.
		const timestamp = stored === undefined ? r.timestamp : Math.max(r.timestamp, stored.timestamp)
		await this.write(r, start, aggregate, timestamp, stored?.value)
	}

	/** Stores a window (a null aggregate deletes it) and forwards it, unless only final results are emitted. */
	private async write(
		r: Current<K, V>,
		start: number,
		aggregate: A | null,
		timestamp: number,
		old: A | undefined
	): Promise<void> {
		const end = start + this.d
		const windowKey = { key: r.key, windowStart: start, windowEnd: end }
		if (aggregate === null || aggregate === undefined) {
			await this.store.delete(windowKey)
		} else {
			await this.store.put(windowKey, { value: aggregate, timestamp })
		}
		const change = { newValue: aggregate ?? null, oldValue: this.sendOldValues() ? (old ?? null) : null }
		await this.forward(r.key, { start, end }, change, timestamp, r.headers)
	}

	/** The starts and aggregates of the windows of the record's key starting in `[t - 2d, t + 1]`, by start. */
	private async windowsAround({ key, timestamp }: Current<K, V>): Promise<[number, ValueAndTimestamp<A>][]> {
		const windows: [number, ValueAndTimestamp<A>][] = []
		const sessions = this.store.findSessions(key, timestamp - this.d, timestamp + 1)
		for await (const [{ windowStart }, stored] of sessions) windows.push([windowStart, stored])
		return windows
	}

	/**
	 * The windows of every key that closed since the last emission: those ending in `[lowerEnd, closeTime - 1]` (both
	 * ends of a window are inclusive; KS `emitRangeLowerBound`, `emitRangeUpperBound`, `shouldRangeFetch`).
	 */
	protected closedWindows(lowerEnd: number, closeTime: number): Promise<FinalResult<K, A>[]> | undefined {
		const upper = closeTime - 1
		return upper < lowerEnd ? undefined : scanClosedWindows(this.store.unclipped, lowerEnd, upper, stored => stored)
	}
}
