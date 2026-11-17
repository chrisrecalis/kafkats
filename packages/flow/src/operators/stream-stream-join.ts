/**
 * One side of a windowed stream-stream join (6.10; KS `KStreamKStreamJoin`, ksdsl§7.2-7.4). A record joins every record
 * of the other side's buffer within its window `[ts - before, ts + after]` (inclusive, clipped by that buffer's store
 * clock), each result at `max(ts, otherTs)`, then is added to its own side's buffer, so it never joins itself on its own
 * side. A record of an outer side (the left side of a left join, both sides of an outer join) that matched nothing is
 * joined with null: at once without an outer-join store (the deprecated JoinWindows.of) or when its window cannot
 * receive a match any more; otherwise it waits in the shared outer-join store until its window and grace closed, unless
 * a match removes it first. Both sides share one processor clock; each expires both buffers on its cadence.
 */
import type { Codec } from '@/codec.js'
import { JoinBuffer } from '@/operators/join-buffer.js'
import { OuterJoinStore, type JoinSide } from '@/operators/outer-join-store.js'
import type { InternalProcessorContext, Processor, ProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import type { StreamRecord } from '@/processor/record.js'
import { ObservedStreamTime } from '@/processor/time-tracker.js'
import type { ClockedSessionStore } from '@/state/time-clipping.js'

export interface JoinSideSpec<K, V, VO, VR> {
	readonly side: JoinSide
	/** This side's buffer, written. */
	readonly thisStore: string
	/** The other side's buffer, probed. */
	readonly otherStore: string
	/** Metadata key of the processor clock both sides share: the left buffer's store name. */
	readonly clock: string
	/** A record at `ts` matches other-side records in `[ts - beforeMs, ts + afterMs]`: the right side's are swapped. */
	readonly beforeMs: number
	readonly afterMs: number
	readonly graceMs: number
	/** This side's value first; null for the missing side of a left or outer result. */
	readonly joiner: (value: V | null, otherValue: VO | null) => VR
	/** Whether unmatched records of this side are joined with null. */
	readonly outer: boolean
	/** Whether those of the other side are: only then may the outer-join store hold records of the other side. */
	readonly otherOuter: boolean
	/** The outer-join store of the join and the codecs of its entries; absent for inner and legacy joins. */
	readonly outerStore?: {
		readonly name: string
		readonly key: Codec<K>
		readonly value: Codec<V>
		readonly otherValue: Codec<VO>
	}
}

export function streamStreamJoinProcessor<K, V, VO, VR>(
	spec: JoinSideSpec<K, V, VO, VR>
): ProcessorSupplier<K | null, V | null, K | null, VR> {
	return () => new StreamStreamJoinProcessor<K, V, VO, VR>(spec)
}

class StreamStreamJoinProcessor<K, V, VO, VR> implements Processor<K | null, V | null, K | null, VR> {
	private context!: InternalProcessorContext<K | null, VR>
	private buffer!: JoinBuffer<K, V>
	private other!: JoinBuffer<K, VO>
	private clock!: ObservedStreamTime
	private outerStore: OuterJoinStore<K, V, VO> | undefined

	constructor(private readonly spec: JoinSideSpec<K, V, VO, VR>) {}

	async init(context: ProcessorContext<K | null, VR>): Promise<void> {
		const { thisStore, otherStore, outerStore, side } = this.spec
		this.context = context as InternalProcessorContext<K | null, VR>
		this.buffer = new JoinBuffer(context.getStateStore<ClockedSessionStore<K, V[]>>(thisStore))
		this.other = new JoinBuffer(context.getStateStore<ClockedSessionStore<K, VO[]>>(otherStore))
		this.clock = new ObservedStreamTime(this.context, this.spec.clock)
		if (outerStore !== undefined) {
			this.outerStore = new OuterJoinStore(context.getStateStore(outerStore.name), side, outerStore)
			await this.outerStore.init()
		}
	}

	async process(record: StreamRecord<K | null, V | null>): Promise<void> {
		const { key, value, timestamp } = record
		const streamTime = this.clock.advance(timestamp)
		const { beforeMs, afterMs, joiner, outer, otherOuter } = this.spec
		if (key === null || value === null) {
			// KIP-962: an outer side joins a null-key record with null at once, without storing it.
			if (outer && value !== null) {
				await this.context.forward(record.withValue(joiner(value, null)))
			} else {
				this.context.recordDropped(key === null ? 'null-key' : 'null-value')
			}
			return
		}
		const keyed = record as StreamRecord<K, V>
		if (this.outerStore !== undefined && timestamp === streamTime) {
			await this.emitClosed(this.outerStore, keyed, streamTime)
		}
		let matched = false
		const window = this.other.fetch(key, Math.max(0, timestamp - beforeMs), Math.max(0, timestamp + afterMs))
		for await (const [otherTimestamp, otherValues] of window) {
			matched = true
			if (otherOuter) {
				await this.outerStore?.removeOther(key, otherTimestamp)
			}
			const result = keyed.withTimestamp(Math.max(timestamp, otherTimestamp))
			for (const otherValue of otherValues) {
				await this.context.forward(result.withValue(joiner(value, otherValue)))
			}
		}
		if (outer && !matched) {
			await this.unmatched(keyed, streamTime)
		}
		await this.buffer.put(key, timestamp, value)
		if (this.clock.expiryDue()) {
			await this.buffer.expire()
			await this.other.expire()
		}
	}

	/**
	 * Joined with null at once without an outer-join store, or when no record can match it any more: its window ended
	 * before the stream time (an out-of-order record, KS `testLowerWindowBound`). Otherwise it waits.
	 */
	private async unmatched(record: StreamRecord<K, V>, streamTime: number): Promise<void> {
		const outerStore = this.outerStore
		if (outerStore === undefined || Math.max(0, record.timestamp + this.spec.afterMs) < streamTime) {
			await this.context.forward(record.withValue(this.spec.joiner(record.value, null)))
			return
		}
		outerStore.emitState.minTime = Math.min(outerStore.emitState.minTime, record.timestamp)
		await outerStore.add(record.key, record.timestamp, record.value)
	}

	/**
	 * Joins the pending records whose window and grace closed with null, oldest first, each at its own timestamp with the
	 * headers of the record that triggered the scan (KS `emitNonJoinedOuterRecords`, ksdsl§7.4, KF11), and deletes each
	 * entry once all its values were forwarded. A record of this side waits `after` past its timestamp, one of the other
	 * side `before` (this side's view). Skipped while the oldest pending record cannot have closed, and throttled to one
	 * scan per `emitIntervalMs` of wall clock per task.
	 */
	private async emitClosed(
		outerStore: OuterJoinStore<K, V, VO>,
		trigger: StreamRecord<K, V>,
		streamTime: number
	): Promise<void> {
		const { beforeMs, afterMs, graceMs, joiner, outer, otherOuter } = this.spec
		const state = outerStore.emitState
		if (state.minTime + afterMs + graceMs >= streamTime) return
		const now = this.context.currentSystemTimeMs()
		if (now < state.nextEmit) return
		state.nextEmit = now + this.context.config.emitIntervalMs
		// The oldest entry left (Kafka Streams keeps the last one it looked at, which can delay the next scan).
		state.minTime = Number.POSITIVE_INFINITY
		// A side without unmatched records to wait (the right side of a left join) is open from the start.
		const open = { own: !outer, other: !otherOuter }
		for await (const entry of outerStore.pending()) {
			if (entry.timestamp + (entry.own ? afterMs : beforeMs) + graceMs >= streamTime) {
				// Later entries of the same side are newer: open too.
				state.minTime = Math.min(state.minTime, entry.timestamp)
				open[entry.own ? 'own' : 'other'] = true
				if (open.own && open.other) break
				continue
			}
			const results = entry.own
				? entry.values().map(v => joiner(v, null))
				: entry.values().map(v => joiner(null, v))
			const result = trigger.withKey(entry.key()).withTimestamp(entry.timestamp)
			for (const joined of results) {
				await this.context.forward(result.withValue(joined))
			}
			await outerStore.delete(entry)
		}
	}
}
