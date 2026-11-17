import { Processor, requireStore, type StoreRef, type StreamRecord } from '@/legacy/processors/base.js'
import type { KeyValueStore } from '@/state.js'

/**
 * Stream-table join: each stream record is joined with the table's current value for its key.
 * A record whose key is not in the table is passed to `unmatched` when given (left join),
 * otherwise dropped (inner join).
 */
export class StreamTableJoinNode<K, V1, V2, VR> extends Processor<K, V1, K, VR> {
	constructor(
		private readonly tableStoreRef: StoreRef<KeyValueStore<K, V2>>,
		private readonly joiner: (value: V1, otherValue: V2) => VR,
		private readonly unmatched?: (value: V1) => VR
	) {
		super()
	}

	clone(): Processor<K, V1, K, VR> {
		return new StreamTableJoinNode(this.tableStoreRef, this.joiner, this.unmatched)
	}

	async process(record: StreamRecord<K, V1>): Promise<void> {
		const store = requireStore(this.tableStoreRef, 'stream-table join lookup')
		const { key, value } = record
		// Null keys cannot be looked up; stream-side tombstones are not joined.
		if (key === null || value === null) return

		const tableValue = await store.get(key)
		if (tableValue !== undefined) {
			await this.forward({ ...record, value: this.joiner(value, tableValue) })
		} else if (this.unmatched) {
			await this.forward({ ...record, value: this.unmatched(value) })
		}
	}
}

export class StreamTableLeftJoinNode<K, V1, V2, VR> extends StreamTableJoinNode<K, V1, V2, VR> {
	constructor(tableStoreRef: StoreRef<KeyValueStore<K, V2>>, joiner: (value: V1, otherValue: V2 | null) => VR) {
		super(tableStoreRef, joiner, value => joiner(value, null))
	}
}
