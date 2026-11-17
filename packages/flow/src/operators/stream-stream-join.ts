/**
 * One side of a windowed stream-stream join (KS KStreamKStreamJoin, eager variant): a record is joined with every
 * record of the other side's buffer within the join window, then added to its own side's buffer. A record of an
 * outer side (the left side of a left join, both sides of an outer join) that matched nothing is joined with null at
 * once, as KS does for the deprecated JoinWindows.of; deferring those results is Stage 3 (6.10). Results keep the
 * key, timestamp and headers of the record. The two sides share one processor clock, and each expires both buffers
 * by it (runtime§6.21).
 */
import { JoinBuffer } from '@/operators/join-buffer.js'
import type { InternalProcessorContext, Processor, ProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import type { StreamRecord } from '@/processor/record.js'
import { ObservedStreamTime } from '@/processor/time-tracker.js'

export interface JoinSide<V, VO, VR> {
	/** This side's buffer, written. */
	readonly thisStore: string
	/** The other side's buffer, probed. */
	readonly otherStore: string
	/** Metadata key of the clock both sides share: the left buffer's store name. */
	readonly clock: string
	/** A record at `ts` matches other-side records in `[ts - beforeMs, ts + afterMs]` (swapped for the right side). */
	readonly beforeMs: number
	readonly afterMs: number
	/** The window of the buffers' layout (`joinBufferWindow`). */
	readonly windowMs: number
	/** Takes this side's value first; the other value is null for an unmatched record of an outer side. */
	readonly joiner: (value: V, otherValue: VO | null) => VR
	readonly outer: boolean
}

export function streamStreamJoinProcessor<K, V, VO, VR>(
	side: JoinSide<V, VO, VR>
): ProcessorSupplier<K | null, V | null, K, VR> {
	return () => new StreamStreamJoinProcessor<K, V, VO, VR>(side)
}

class StreamStreamJoinProcessor<K, V, VO, VR> implements Processor<K | null, V | null, K, VR> {
	private context!: InternalProcessorContext<K, VR>
	private buffer!: JoinBuffer<K, V>
	private other!: JoinBuffer<K, VO>
	private clock!: ObservedStreamTime

	constructor(private readonly side: JoinSide<V, VO, VR>) {}

	init(context: ProcessorContext<K, VR>): void {
		const { thisStore, otherStore, windowMs } = this.side
		this.context = context as InternalProcessorContext<K, VR>
		this.buffer = new JoinBuffer(context.getStateStore(thisStore), windowMs)
		this.other = new JoinBuffer(context.getStateStore(otherStore), windowMs)
		this.clock = new ObservedStreamTime(this.context, this.side.clock)
	}

	async process(record: StreamRecord<K | null, V | null>): Promise<void> {
		const { key, value, timestamp } = record
		this.clock.advance(timestamp)
		if (key === null || value === null) {
			this.context.recordDropped(key === null ? 'null-key' : 'null-value')
			return
		}
		const { beforeMs, afterMs, joiner, outer } = this.side
		const keyed = record as StreamRecord<K, V>
		let matched = false
		for await (const otherValue of this.other.fetch(key, timestamp - beforeMs, timestamp + afterMs)) {
			matched = true
			await this.context.forward(keyed.withValue(joiner(value, otherValue)))
		}
		if (!matched && outer) {
			await this.context.forward(keyed.withValue(joiner(value, null)))
		}
		await this.buffer.put(key, timestamp, value)
		if (this.clock.expiryDue()) {
			const streamTime = this.clock.get()
			await this.buffer.expire(streamTime)
			await this.other.expire(streamTime)
		}
	}
}
