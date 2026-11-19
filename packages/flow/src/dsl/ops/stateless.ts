/**
 * Stateless operators: filter, map, flatMap, peek, foreach, selectKey, merge, split, branch and KTable.toStream (6.0,
 * 6.20). Key-changing operators flag the stream for repartitioning and forget the key codec; value-changing ones forget
 * the value codec.
 */
import { TopologyError } from '@/api/errors.js'
import type { BranchedKStream, KStream, KeyValue, ValueMapperWithKey } from '@/api/kstream.js'
import type { BranchOptions, NamedOptions, SplitOptions } from '@/api/options.js'
import { BranchedKStreamImpl } from '@/dsl/branched.js'
import { mergedFormat, withNewKeys, withNewValues } from '@/dsl/format.js'
import { KStreamImpl } from '@/dsl/kstream.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import { NodePrefix } from '@/dsl/naming.js'
import {
	branchProcessor,
	type BranchRoute,
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
	mapper: ValueMapperWithKey<K | null, V | null, VR>,
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
	mapper: ValueMapperWithKey<K | null, V | null, Iterable<VR>>,
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

/** KStream.split (KS BranchedKStreamImpl): registers the splitter `options.name ?? KSTREAM-BRANCH-<n>`. */
export function split<K, V>(stream: KStreamImpl<K, V>, options?: SplitOptions): BranchedKStream<K, V> {
	const routes: BranchRoute<K | null, V | null>[] = []
	const node = add(stream, NodePrefix.BRANCH, options, branchProcessor(routes))
	return new BranchedKStreamImpl(stream, node, routes)
}

/** A branch of the split, named by its 1-based position unless `options.name` names it (KS). */
export function addBranch<K, V>(
	branched: BranchedKStreamImpl<K, V>,
	predicate: Predicate<K, V>,
	options?: BranchOptions<K, V>
): void {
	addRoute(branched, predicate, branched.routes.length + 1, options)
}

/** Ends the split with a branch for the records no branch matched, named `0` unless `options.name` names it (KS). */
export function defaultBranch<K, V>(
	branched: BranchedKStreamImpl<K, V>,
	options?: BranchOptions<K, V>
): Record<string, KStream<K, V>> {
	addRoute(branched, undefined, 0, options)
	return noDefaultBranch(branched)
}

/** Ends the split; records no branch matched are dropped. */
export function noDefaultBranch<K, V>(branched: BranchedKStreamImpl<K, V>): Record<string, KStream<K, V>> {
	checkOpen(branched)
	branched.ended = true
	return branched.branches
}

/** The deprecated KStream.branch(...predicates): records go to the first matching predicate's stream only. */
export function branch<K, V>(stream: KStreamImpl<K, V>, predicates: readonly Predicate<K, V>[]): KStream<K, V>[] {
	const { builder } = stream.ctx
	const splitter = builder.nodeName(NodePrefix.BRANCH)
	const routes = predicates.map(predicate => ({ child: builder.nodeName(NodePrefix.BRANCH_CHILD), predicate }))
	add(stream, NodePrefix.BRANCH, { name: splitter }, branchProcessor(routes))
	return routes.map(({ child }) => branchStream(stream, splitter, child))
}

/**
 * Registers the branch `<splitter><options.name ?? position>` after the branches added so far; the branch, or what its
 * chain returns, joins the result under that name (nothing, if the chain returns nothing).
 */
function addRoute<K, V>(
	branched: BranchedKStreamImpl<K, V>,
	predicate: Predicate<K, V> | undefined,
	position: number,
	options: BranchOptions<K, V> | undefined
): void {
	checkOpen(branched)
	const branch = branchStream(branched.stream, branched.node, `${branched.node}${options?.name ?? position}`)
	branched.routes.push({ child: branch.node, predicate })
	const result = options?.chain ? options.chain(branch) : branch
	if (result) {
		branched.branches[branch.node] = result
	}
}

function checkOpen<K, V>(branched: BranchedKStreamImpl<K, V>): void {
	if (branched.ended) {
		throw new TopologyError(
			`split() '${branched.node}' already returned its branches: add every branch before defaultBranch() or noDefaultBranch()`
		)
	}
}

/** A branch of a splitter: a pass-through node keeping the stream's format and repartition flag (KS). */
function branchStream<K, V>(stream: KStreamImpl<K, V>, splitter: NodeName, name: NodeName): KStreamImpl<K, V> {
	const { ctx } = stream
	const node = ctx.processor(NodePrefix.BRANCH_CHILD, name, [splitter], passThroughProcessor<K | null, V | null>())
	return new KStreamImpl<K, V>(ctx, node, stream.format, stream.repartitionRequired)
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
