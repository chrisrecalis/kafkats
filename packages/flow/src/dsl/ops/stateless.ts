/**
 * Stateless operators: filter, map, flatMap, peek, foreach, selectKey, merge, branch and KTable.toStream (6.0, 6.20).
 * Key-changing operators flag the stream for repartitioning and forget the key codec; value-changing ones forget the
 * value codec.
 */
import { TopologyError } from '@/api/errors.js'
import type { BranchedKStream, KStream, KeyValue, ValueMapper } from '@/api/kstream.js'
import type { BranchOptions, NamedOptions, SplitOptions } from '@/api/options.js'
import type { BranchedKStreamImpl } from '@/dsl/branched.js'
import { mergedFormat, withNewKeys, withNewValues } from '@/dsl/format.js'
import { KStreamImpl } from '@/dsl/kstream.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import { NodePrefix } from '@/dsl/naming.js'
import { notImplemented } from '@/dsl/not-implemented.js'
import {
	branchProcessor,
	filterProcessor,
	flatMapProcessor,
	flatMapValuesProcessor,
	foreachProcessor,
	mapProcessor,
	mapValuesProcessor,
	passThroughProcessor,
	peekProcessor,
	selectKeyProcessor,
	toStreamProcessor,
} from '@/operators/stateless.js'
import type { ProcessorSupplier } from '@/processor/api.js'
import type { NodeName } from '@/topology/nodes.js'

type Predicate<K, V> = (key: K | null, value: V | null) => boolean
type Action<K, V> = (key: K | null, value: V | null) => void

/** Registers a processor under `stream`; returns its name. */
function add<K, V>(
	stream: KStreamImpl<K, V>,
	prefix: string,
	options: NamedOptions | undefined,
	supplier: ProcessorSupplier<K | null, V | null, unknown, unknown>
): NodeName {
	return stream.ctx.processor(prefix, options?.name, [stream.node], supplier)
}

export function filter<K, V>(
	stream: KStreamImpl<K, V>,
	predicate: Predicate<K, V>,
	options?: NamedOptions
): KStream<K, V> {
	const node = add(stream, NodePrefix.FILTER, options, filterProcessor(predicate, false))
	return new KStreamImpl<K, V>(stream.ctx, node, stream.format, stream.repartitionRequired)
}

export function filterNot<K, V>(
	stream: KStreamImpl<K, V>,
	predicate: Predicate<K, V>,
	options?: NamedOptions
): KStream<K, V> {
	const node = add(stream, NodePrefix.FILTER, options, filterProcessor(predicate, true))
	return new KStreamImpl<K, V>(stream.ctx, node, stream.format, stream.repartitionRequired)
}

export function map<K, V, KR, VR>(
	stream: KStreamImpl<K, V>,
	mapper: (key: K | null, value: V | null) => KeyValue<KR, VR>,
	options?: NamedOptions
): KStream<KR, VR> {
	const node = add(stream, NodePrefix.MAP, options, mapProcessor(mapper))
	return new KStreamImpl<KR, VR>(stream.ctx, node, {}, true)
}

export function mapValues<K, V, VR>(
	stream: KStreamImpl<K, V>,
	mapper: ValueMapper<K | null, V | null, VR>,
	options?: NamedOptions
): KStream<K, VR> {
	const node = add(stream, NodePrefix.MAP_VALUES, options, mapValuesProcessor(mapper))
	return new KStreamImpl<K, VR>(stream.ctx, node, withNewValues(stream.format), stream.repartitionRequired)
}

export function flatMap<K, V, KR, VR>(
	stream: KStreamImpl<K, V>,
	mapper: (key: K | null, value: V | null) => Iterable<KeyValue<KR, VR>>,
	options?: NamedOptions
): KStream<KR, VR> {
	const node = add(stream, NodePrefix.FLAT_MAP, options, flatMapProcessor(mapper))
	return new KStreamImpl<KR, VR>(stream.ctx, node, {}, true)
}

export function flatMapValues<K, V, VR>(
	stream: KStreamImpl<K, V>,
	mapper: ValueMapper<K | null, V | null, Iterable<VR>>,
	options?: NamedOptions
): KStream<K, VR> {
	const node = add(stream, NodePrefix.FLAT_MAP_VALUES, options, flatMapValuesProcessor(mapper))
	return new KStreamImpl<K, VR>(stream.ctx, node, withNewValues(stream.format), stream.repartitionRequired)
}

export function peek<K, V>(stream: KStreamImpl<K, V>, action: Action<K, V>, options?: NamedOptions): KStream<K, V> {
	const node = add(stream, NodePrefix.PEEK, options, peekProcessor(action))
	return new KStreamImpl<K, V>(stream.ctx, node, stream.format, stream.repartitionRequired)
}

export function foreach<K, V>(stream: KStreamImpl<K, V>, action: Action<K, V>, options?: NamedOptions): void {
	add(stream, NodePrefix.FOREACH, options, foreachProcessor(action))
}

export function selectKey<K, V, KR>(
	stream: KStreamImpl<K, V>,
	mapper: (key: K | null, value: V | null) => KR,
	options?: NamedOptions
): KStream<KR, V> {
	const node = add(stream, NodePrefix.KEY_SELECT, options, selectKeyProcessor(mapper))
	return new KStreamImpl<KR, V>(stream.ctx, node, withNewKeys(stream.format), true)
}

export function merge<K, V>(stream: KStreamImpl<K, V>, other: KStream<K, V>, options?: NamedOptions): KStream<K, V> {
	if (!(other instanceof KStreamImpl) || other.ctx !== stream.ctx) {
		throw new TopologyError('merge() needs a stream of the same app')
	}
	const right = other as KStreamImpl<K, V>
	const { ctx } = stream
	const parents = [stream.node, right.node]
	const node = ctx.processor(NodePrefix.MERGE, options?.name, parents, passThroughProcessor<K | null, V | null>())
	const format = mergedFormat(stream.format, right.format)
	return new KStreamImpl<K, V>(ctx, node, format, stream.repartitionRequired || right.repartitionRequired)
}

export function split<K, V>(_stream: KStreamImpl<K, V>, _options?: SplitOptions): BranchedKStream<K, V> {
	return notImplemented('KStream.split')
}

/** The deprecated KStream.branch(...predicates): records go to the first matching predicate's stream only. */
export function branch<K, V>(stream: KStreamImpl<K, V>, predicates: readonly Predicate<K, V>[]): KStream<K, V>[] {
	const { ctx } = stream
	const branchNode = ctx.builder.nodeName(NodePrefix.BRANCH)
	const children = predicates.map(() => ctx.builder.nodeName(NodePrefix.BRANCH_CHILD))
	ctx.processor(NodePrefix.BRANCH, branchNode, [stream.node], branchProcessor(predicates, children))
	return children.map(child => {
		ctx.processor(NodePrefix.BRANCH_CHILD, child, [branchNode], passThroughProcessor<K | null, V | null>())
		return new KStreamImpl<K, V>(ctx, child, stream.format, stream.repartitionRequired)
	})
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

/**
 * KTable.toStream: `change.newValue` of every update. Without a mapper the records stay in the partition of their key
 * (windowed results in the partition of their base key, where key-based operations on them find them, BC10); a mapper
 * re-keys them, so a key-based operation downstream repartitions first.
 */
export function toStream<K, V, KR>(
	table: KTableImpl<K, V>,
	mapperOrOptions?: ((key: K, value: V | null) => KR) | NamedOptions,
	options?: NamedOptions
): KStream<K | KR, V> {
	const mapper =
		typeof mapperOrOptions === 'function' ? (mapperOrOptions as (key: K, value: V | null) => KR) : undefined
	const named = mapper === undefined ? (mapperOrOptions as NamedOptions | undefined) : options
	const { ctx } = table
	const node = ctx.processor(NodePrefix.TABLE_TO_STREAM, named?.name, [table.node], toStreamProcessor(mapper))
	const format = mapper === undefined ? table.format : withNewKeys(table.format)
	return new KStreamImpl<K | KR, V>(ctx, node, format, mapper !== undefined)
}
