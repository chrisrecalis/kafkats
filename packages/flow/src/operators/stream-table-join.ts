/**
 * KStream-KTable and KStream-GlobalKTable joins (KS KStreamKTableJoinProcessor): a stream record looks up the table
 * value of the key it maps to (its own key for a KTable, the selector's key for a GlobalKTable) through the table's
 * value getter and is joined with it (6.14, ksdsl§8). A left join joins records without a table value with null; an
 * inner join drops them. Records with a null value are dropped; so are records without a lookup key in an inner join,
 * while a left join joins them with null without a lookup (KIP-962). Only stream records produce results, with their
 * key, timestamp and headers; the table is read as it is when the record is processed.
 */
import type { ValueJoiner } from '@/api/kstream.js'
import type { KTableValueGetter, KTableValueGetterSupplier } from '@/operators/ktable-node.js'
import type {
	InternalProcessorContext,
	MaybePromise,
	Processor,
	ProcessorContext,
	ProcessorSupplier,
} from '@/processor/api.js'
import type { StreamRecord } from '@/processor/record.js'

export interface StreamTableJoin<K, V, KT, VT, VR> {
	readonly valueGetter: KTableValueGetterSupplier<KT, VT>
	/** The table key a record looks up. */
	readonly lookupKey: (key: K | null, value: V) => KT | null
	readonly joiner: (key: K | null, value: V, tableValue: VT | null) => VR
	readonly left: boolean
}

/** A ValueJoiner as a function of the key too: joiners declaring three or more parameters get the key (6.20). */
export function joinerWithKey<K, V, VO, VR>(
	joiner: ValueJoiner<K, V, VO, VR>
): (key: K, value: V, otherValue: VO) => VR {
	return joiner.length >= 3
		? (joiner as (key: K, value: V, otherValue: VO) => VR)
		: (_key, value, otherValue) => (joiner as (value: V, otherValue: VO) => VR)(value, otherValue)
}

export function streamTableJoinProcessor<K, V, KT, VT, VR>(
	join: StreamTableJoin<K, V, KT, VT, VR>
): ProcessorSupplier<K | null, V | null, K | null, VR> {
	return () => new StreamTableJoinProcessor(join)
}

class StreamTableJoinProcessor<K, V, KT, VT, VR> implements Processor<K | null, V | null, K | null, VR> {
	private context!: InternalProcessorContext<K | null, VR>
	private readonly getter: KTableValueGetter<KT, VT>

	constructor(private readonly join: StreamTableJoin<K, V, KT, VT, VR>) {
		this.getter = join.valueGetter.create()
	}

	init(context: ProcessorContext<K | null, VR>): MaybePromise<void> {
		this.context = context as InternalProcessorContext<K | null, VR>
		return this.getter.init(this.context as InternalProcessorContext<unknown, unknown>)
	}

	async process(record: StreamRecord<K | null, V | null>): Promise<void> {
		const { key, value } = record
		if (value === null) {
			this.context.recordDropped('null-value')
			return
		}
		const lookupKey = this.join.lookupKey(key, value)
		const keyless = lookupKey === null || lookupKey === undefined
		if (keyless && !this.join.left) {
			this.context.recordDropped('null-key')
			return
		}
		const tableValue = keyless ? null : await this.getter.get(lookupKey)
		if (tableValue === null && !this.join.left) {
			return
		}
		await this.context.forward(record.withValue(this.join.joiner(key, value, tableValue?.value ?? null)))
	}

	close(): MaybePromise<void> {
		return this.getter.close?.()
	}
}
