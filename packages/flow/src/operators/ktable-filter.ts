/**
 * KTable.filter and filterNot (KS KTableFilter, 6.12): the derived table keeps the values the predicate accepts
 * (filterNot: rejects); the predicate never sees null. A key whose value stops matching is deleted with a tombstone.
 * Like Kafka Streams, the filter asks its parent for old values when the parent has them without materializing
 * anything (a materialized filter reads its own store), so an update without a value before and after is not
 * forwarded; without old values every non-matching update forwards a tombstone.
 */
import type { KTableNode } from '@/operators/ktable-node.js'
import { KTableMapValues } from '@/operators/ktable-map-values.js'

export function ktableFilter<K, V>(
	parent: KTableNode<K, V>,
	predicate: (key: K, value: V) => boolean,
	negate: boolean,
	storeName?: string
): KTableNode<K, V> {
	const keep = (key: K, value: V) => (predicate(key, value) !== negate ? value : null)
	const filter = new KTableMapValues(parent, keep, { storeName, dropAbsent: true })
	filter.enableSendingOldValues(false)
	return filter
}
