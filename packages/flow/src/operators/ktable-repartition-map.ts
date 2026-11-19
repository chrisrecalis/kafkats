/**
 * KTable.groupBy's repartition map (KS `KTableRepartitionMap.java:130-167`, 6.15): maps both values of a row's update
 * (groupBy forces its table to send old values) to the groups the row leaves and joins. A mapped pair counts only
 * when its key and value are not null (B19). A row that stays in its group (the same encoded grouped key) gives the
 * group one record `Change(new, old)` (KIP-904); otherwise the group it leaves gets `Change(null, old)` first, then
 * the group it joins `Change(new, null)`. Records keep the update's timestamp and headers.
 */
import type { KeyValue } from '@/api/kstream.js'
import type { Codec } from '@/codec.js'
import type { Change } from '@/operators/change.js'
import type {
	ForwardResult,
	InternalProcessorContext,
	Processor,
	ProcessorContext,
	ProcessorSupplier,
} from '@/processor/api.js'
import type { StreamRecord } from '@/processor/record.js'

export function repartitionMapProcessor<K, V, KR, VR>(
	selector: (key: K, value: V) => KeyValue<KR, VR>,
	keyCodec: Codec<KR>
): ProcessorSupplier<K, Change<V>, KR, Change<VR>> {
	return () => new KTableRepartitionMapProcessor(selector, keyCodec)
}

class KTableRepartitionMapProcessor<K, V, KR, VR> implements Processor<K, Change<V>, KR, Change<VR>> {
	private context!: InternalProcessorContext<KR, Change<VR>>

	constructor(
		private readonly selector: (key: K, value: V) => KeyValue<KR, VR>,
		private readonly keyCodec: Codec<KR>
	) {}

	init(context: ProcessorContext<KR, Change<VR>>): void {
		this.context = context as InternalProcessorContext<KR, Change<VR>>
	}

	/** Synchronous when the records' subtrees are. */
	process(record: StreamRecord<K, Change<V>>): ForwardResult {
		const { key, value } = record
		if (key === null || key === undefined) {
			this.context.recordDropped('null-key')
			return
		}
		const newPair = this.map(key, value.newValue)
		const oldPair = this.map(key, value.oldValue)
		if (newPair !== undefined && oldPair !== undefined && this.sameGroup(newPair[0], oldPair[0])) {
			return this.forward(record, newPair[0], { newValue: newPair[1], oldValue: oldPair[1] })
		}
		const join = () =>
			newPair === undefined
				? undefined
				: this.forward(record, newPair[0], { newValue: newPair[1], oldValue: null })
		if (oldPair === undefined) {
			return join()
		}
		const leave = this.forward(record, oldPair[0], { newValue: null, oldValue: oldPair[1] })
		return leave === undefined ? join() : leave.then(join)
	}

	/** The group and value of a row's value; undefined without a value, or when the group or value is null. */
	private map(key: K, value: V | null): KeyValue<KR, VR> | undefined {
		const pair = value === null ? undefined : this.selector(key, value)
		return isPresent(pair) && isPresent(pair[0]) && isPresent(pair[1]) ? pair : undefined
	}

	private sameGroup(a: KR, b: KR): boolean {
		return a === b || this.keyCodec.encode(a).equals(this.keyCodec.encode(b))
	}

	private forward(record: StreamRecord<K, Change<V>>, group: KR, change: Change<VR>): ForwardResult {
		return this.context.forward(record.withKey(group).withValue(change))
	}
}

function isPresent<T>(value: T | null | undefined): value is T {
	return value !== null && value !== undefined
}
