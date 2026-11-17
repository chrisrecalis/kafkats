/**
 * KTable.groupBy (KS KTableRepartitionMap, 6.15), ported with today's mapping-store semantics (WP3.8 replaces them
 * with Change pairs): every update of a source row retracts the row's previous contribution to its group and adds
 * the new one, as two records for the repartition topic marked by the `__kafkats_delta_op` header (`sub`, then
 * `add`; a deleted row only retracts). The previous contribution (grouped key and value) of each source key is kept
 * in a mapping store. Both deltas are forwarded before the mapping is updated: a replay after a crash in between
 * emits them again (at least once) instead of losing the change.
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
import { StreamRecord, type RecordHeaders } from '@/processor/record.js'
import type { KeyValueStore } from '@/state.js'

/** Marks a grouped record as the addition (`add`) or the retraction (`sub`) of a row's value. */
export const DELTA_OP_HEADER = '__kafkats_delta_op'
const ADD = Buffer.from('add')
const SUB = Buffer.from('sub')

/** Whether a grouped record retracts a row's value from its group. */
export function isRetraction(headers: RecordHeaders): boolean {
	return headers[DELTA_OP_HEADER]?.equals(SUB) ?? false
}

/** A row's contribution to its group: the grouped key and the mapped value. */
export interface GroupedRow<K, V> {
	readonly groupedKey: K
	readonly value: V
}

/** `[u32BE key length][grouped key][value]`. */
export function groupedRowCodec<K, V>(keyCodec: Codec<K>, valueCodec: Codec<V>): Codec<GroupedRow<K, V>> {
	return {
		encode: row => {
			const key = keyCodec.encode(row.groupedKey)
			const length = Buffer.alloc(4)
			length.writeUInt32BE(key.length, 0)
			return Buffer.concat([length, key, valueCodec.encode(row.value)])
		},
		decode: encoded => {
			const end = 4 + encoded.readUInt32BE(0)
			return {
				groupedKey: keyCodec.decode(encoded.subarray(4, end)),
				value: valueCodec.decode(encoded.subarray(end)),
			}
		},
	}
}

export function repartitionMapProcessor<K, V, KR, VR>(
	selector: (key: K, value: V) => KeyValue<KR, VR>,
	mappingStore: string
): ProcessorSupplier<K, Change<V>, KR, VR> {
	return () => new KTableRepartitionMapProcessor(selector, mappingStore)
}

class KTableRepartitionMapProcessor<K, V, KR, VR> implements Processor<K, Change<V>, KR, VR> {
	private context!: InternalProcessorContext<KR, VR>
	private mapping!: KeyValueStore<K, GroupedRow<KR, VR>>

	constructor(
		private readonly selector: (key: K, value: V) => KeyValue<KR, VR>,
		private readonly mappingStore: string
	) {}

	init(context: ProcessorContext<KR, VR>): void {
		this.context = context as InternalProcessorContext<KR, VR>
		this.mapping = context.getStateStore(this.mappingStore)
	}

	async process(record: StreamRecord<K, Change<V>>): Promise<void> {
		const { key } = record
		if (key === null || key === undefined) {
			this.context.recordDropped('null-key', { store: this.mappingStore })
			return
		}
		const { newValue } = record.value
		const added = newValue === null ? undefined : this.selector(key, newValue)
		const previous = await this.mapping.get(key)
		if (previous !== undefined) {
			await this.forward(record, previous.groupedKey, previous.value, SUB)
		}
		if (added === undefined) {
			if (previous !== undefined) {
				await this.mapping.delete(key)
			}
			return
		}
		const [groupedKey, value] = added
		await this.forward(record, groupedKey, value, ADD)
		await this.mapping.put(key, { groupedKey, value })
	}

	private forward(record: StreamRecord<K, unknown>, key: KR, value: VR, op: Buffer): ForwardResult {
		const headers = { ...record.headers, [DELTA_OP_HEADER]: op }
		return this.context.forward(new StreamRecord(key, value, record.timestamp, headers))
	}
}
