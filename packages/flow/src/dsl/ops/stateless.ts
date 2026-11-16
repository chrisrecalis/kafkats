/** Stateless operators: filter, map, flatMap, peek, foreach, selectKey, merge, split and KTable.toStream (6.20). */
import type { BranchedKStream, KStream, KeyValue, ValueMapper } from '@/api/kstream.js'
import type { BranchOptions, NamedOptions, SplitOptions } from '@/api/options.js'
import type { BranchedKStreamImpl } from '@/dsl/branched.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import { notImplemented } from '@/dsl/not-implemented.js'

type Predicate<K, V> = (key: K | null, value: V | null) => boolean
type Action<K, V> = (key: K | null, value: V | null) => void

export function filter<K, V>(
	_stream: KStreamImpl<K, V>,
	_predicate: Predicate<K, V>,
	_options?: NamedOptions
): KStream<K, V> {
	return notImplemented('KStream.filter')
}

export function filterNot<K, V>(
	_stream: KStreamImpl<K, V>,
	_predicate: Predicate<K, V>,
	_options?: NamedOptions
): KStream<K, V> {
	return notImplemented('KStream.filterNot')
}

export function map<K, V, KR, VR>(
	_stream: KStreamImpl<K, V>,
	_mapper: (key: K | null, value: V | null) => KeyValue<KR, VR>,
	_options?: NamedOptions
): KStream<KR, VR> {
	return notImplemented('KStream.map')
}

export function mapValues<K, V, VR>(
	_stream: KStreamImpl<K, V>,
	_mapper: ValueMapper<K | null, V | null, VR>,
	_options?: NamedOptions
): KStream<K, VR> {
	return notImplemented('KStream.mapValues')
}

export function flatMap<K, V, KR, VR>(
	_stream: KStreamImpl<K, V>,
	_mapper: (key: K | null, value: V | null) => Iterable<KeyValue<KR, VR>>,
	_options?: NamedOptions
): KStream<KR, VR> {
	return notImplemented('KStream.flatMap')
}

export function flatMapValues<K, V, VR>(
	_stream: KStreamImpl<K, V>,
	_mapper: ValueMapper<K | null, V | null, Iterable<VR>>,
	_options?: NamedOptions
): KStream<K, VR> {
	return notImplemented('KStream.flatMapValues')
}

export function peek<K, V>(_stream: KStreamImpl<K, V>, _action: Action<K, V>, _options?: NamedOptions): KStream<K, V> {
	return notImplemented('KStream.peek')
}

export function foreach<K, V>(_stream: KStreamImpl<K, V>, _action: Action<K, V>, _options?: NamedOptions): void {
	notImplemented('KStream.foreach')
}

export function selectKey<K, V, KR>(
	_stream: KStreamImpl<K, V>,
	_mapper: (key: K | null, value: V | null) => KR,
	_options?: NamedOptions
): KStream<KR, V> {
	return notImplemented('KStream.selectKey')
}

export function merge<K, V>(_stream: KStreamImpl<K, V>, _other: KStream<K, V>, _options?: NamedOptions): KStream<K, V> {
	return notImplemented('KStream.merge')
}

export function split<K, V>(_stream: KStreamImpl<K, V>, _options?: SplitOptions): BranchedKStream<K, V> {
	return notImplemented('KStream.split')
}

/** The deprecated KStream.branch(...predicates). */
export function branch<K, V>(_stream: KStreamImpl<K, V>, _predicates: readonly Predicate<K, V>[]): KStream<K, V>[] {
	return notImplemented('KStream.branch')
}

export function addBranch<K, V>(
	_branched: BranchedKStreamImpl<K, V>,
	_predicate: Predicate<K, V>,
	_options?: BranchOptions<K, V>
): void {
	notImplemented('BranchedKStream.branch')
}

export function defaultBranch<K, V>(
	_branched: BranchedKStreamImpl<K, V>,
	_options?: BranchOptions<K, V>
): Record<string, KStream<K, V>> {
	return notImplemented('BranchedKStream.defaultBranch')
}

export function noDefaultBranch<K, V>(_branched: BranchedKStreamImpl<K, V>): Record<string, KStream<K, V>> {
	return notImplemented('BranchedKStream.noDefaultBranch')
}

/** KTable.toStream: `change.newValue` of every update, re-keyed by the mapper if one is given. */
export function toStream<K, V, KR>(
	_table: KTableImpl<K, V>,
	_mapperOrOptions?: ((key: K, value: V | null) => KR) | NamedOptions,
	_options?: NamedOptions
): KStream<K | KR, V> {
	return notImplemented('KTable.toStream')
}
