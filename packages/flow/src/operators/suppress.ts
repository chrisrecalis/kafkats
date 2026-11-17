/**
 * KTable.suppress (KS KTableSuppressProcessorSupplier, ksdsl§12; 6.16): holds back a table's updates in a buffer
 * (`suppress-buffer.ts`) and forwards each key's latest update once its processor clock passed the key's buffer time
 * by the suppress duration. Until a window closes: the buffer time is the window's end, the duration the grace of the
 * windowed aggregation upstream, a deleted window is not forwarded, and a full buffer stops the app. Until a time
 * limit: the buffer time is the timestamp of the key's first buffered update, the duration the limit, and a full buffer
 * emits its earliest keys early unless told to stop the app. The clock (KS observed stream time) spans every partition
 * of the task and is persisted with the commits, so a restarted task evicts as the old one would have.
 *
 * Deviation (KF14): an emitted update's old value is the key's prior value, what downstream last saw of it, where Kafka
 * Streams sends the old value of the latest buffered update, which downstream never saw; a subtractor downstream (a
 * KGroupedTable aggregation) then stays consistent.
 */
import { StreamsError } from '@/api/errors.js'
import type { Windowed } from '@/api/grouped.js'
import type { Change } from '@/operators/change.js'
import type { KTableNode, KTableValueGetterSupplier } from '@/operators/ktable-node.js'
import { SuppressBuffer, type BufferCodecs, type BufferStore } from '@/operators/suppress-buffer.js'
import { priorValueOf, type BufferedChange } from '@/operators/suppress-entry.js'
import type {
	ForwardResult,
	InternalProcessorContext,
	Processor,
	ProcessorContext,
	ProcessorSupplier,
} from '@/processor/api.js'
import { StreamRecord } from '@/processor/record.js'
import { ObservedStreamTime } from '@/processor/time-tracker.js'

/** How a table is suppressed (KS SuppressedInternal). */
export interface Suppression {
	readonly until: 'window-close' | 'time-limit'
	/** How long after its buffer time a key is forwarded: the windows' grace, or the time limit. */
	readonly durationMs: number
	/** Infinity: unbounded. */
	readonly maxRecords: number
	readonly maxBytes: number
	readonly whenFull: 'emit-early' | 'shutdown'
}

/** The timestamp of a buffered prior value: unknown, as in Kafka Streams (`RecordQueue.UNKNOWN`). */
const UNKNOWN_TIMESTAMP = -1

export class KTableSuppress<K, V> implements KTableNode<K, V> {
	readonly supplier: ProcessorSupplier<K, Change<V>, K, Change<V>>
	readonly valueGetter: KTableValueGetterSupplier<K, V>

	constructor(
		private readonly parent: KTableNode<K, V>,
		storeName: string,
		suppression: Suppression,
		codecs: BufferCodecs<K, V>
	) {
		// The old value of a key's first buffered update is its prior value.
		parent.enableSendingOldValues(true)
		this.supplier = () => new KTableSuppressProcessor(storeName, suppression, codecs)
		this.valueGetter = suppressedValueGetter(parent.valueGetter, storeName, codecs)
	}

	enableSendingOldValues(force: boolean): boolean {
		return this.parent.enableSendingOldValues(force)
	}
}

/**
 * The getter of a suppressed table (KS `view()`): what downstream last saw of a key, the prior value of a buffered key
 * (null when it had none), else the parent's value. Read from the buffer's store, which matches the buffer.
 */
function suppressedValueGetter<K, V>(
	parent: KTableValueGetterSupplier<K, V>,
	storeName: string,
	codecs: BufferCodecs<K, V>
): KTableValueGetterSupplier<K, V> {
	return {
		storeNames: [storeName, ...parent.storeNames],
		create: () => {
			const parentValues = parent.create()
			let store: BufferStore
			return {
				init: context => {
					store = context.getStateStore(storeName)
					return parentValues.init(context)
				},
				get: async key => {
					const buffered = await store.get(codecs.key.encode(key))
					if (buffered === undefined) {
						return parentValues.get(key)
					}
					const value = priorValueOf(codecs.value, buffered)
					return value === null ? null : { value, timestamp: UNKNOWN_TIMESTAMP }
				},
				close: () => parentValues.close?.(),
			}
		},
	}
}

class KTableSuppressProcessor<K, V> implements Processor<K, Change<V>, K, Change<V>> {
	private context!: InternalProcessorContext<K, Change<V>>
	private clock!: ObservedStreamTime
	private buffer!: SuppressBuffer<K, V>

	constructor(
		private readonly storeName: string,
		private readonly suppression: Suppression,
		private readonly codecs: BufferCodecs<K, V>
	) {}

	async init(context: ProcessorContext<K, Change<V>>): Promise<void> {
		this.context = context as InternalProcessorContext<K, Change<V>>
		this.clock = new ObservedStreamTime(this.context, this.storeName)
		this.buffer = new SuppressBuffer(context.getStateStore(this.storeName), this.codecs)
		await this.buffer.load()
	}

	async process(record: StreamRecord<K, Change<V>>): Promise<void> {
		const streamTime = this.clock.advance(record.timestamp)
		const { buffer, suppression } = this
		await buffer.put(this.bufferTime(record), record)
		const expiryTime = streamTime - suppression.durationMs
		await buffer.evictWhile(() => buffer.minTime <= expiryTime, this.emit)
		if (!this.overCapacity()) {
			return
		}
		if (suppression.whenFull === 'shutdown') {
			throw new StreamsError(
				`${this.context.processorName} buffer exceeded its max capacity. Currently ` +
					`[${buffer.numRecords}/${suppression.maxRecords}] records and ` +
					`[${buffer.bytes}/${suppression.maxBytes}] bytes.`,
				{ taskId: this.context.taskId }
			)
		}
		await buffer.evictWhile(() => this.overCapacity(), this.emit)
	}

	private bufferTime(record: StreamRecord<K, Change<V>>): number {
		if (this.suppression.until === 'time-limit') {
			return record.timestamp
		}
		const end = (record.key as Windowed<unknown> | null)?.window?.end
		if (typeof end !== 'number') {
			throw new StreamsError(
				`${this.context.processorName} suppresses until windows close, but its table's keys are not windowed`
			)
		}
		return end
	}

	private overCapacity(): boolean {
		const { buffer, suppression } = this
		return buffer.numRecords > suppression.maxRecords || buffer.bytes > suppression.maxBytes
	}

	/** Final results never include a deleted window (KS safeToDropTombstones); time-limited updates do. */
	private readonly emit = (key: K, change: BufferedChange<V>): ForwardResult => {
		if (change.newValue === null && this.suppression.until === 'window-close') {
			return
		}
		const value: Change<V> = { newValue: change.newValue, oldValue: change.priorValue }
		return this.context.forward(new StreamRecord(key, value, change.timestamp, change.headers))
	}
}
