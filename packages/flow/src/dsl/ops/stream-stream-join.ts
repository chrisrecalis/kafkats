/**
 * Windowed stream-stream joins: inner, left and outer (6.10). Each side is repartitioned if its keys changed upstream
 * and buffered in its own join buffer, kept `before + after + grace`; a join processor per side probes the other side's
 * buffer and a pass-through node merges their results. Left and outer joins keep their unmatched records in a shared
 * outer-join store until their window closed, except with the deprecated JoinWindows.of (and `Joined.within`), which
 * joins them with null at once (KS `enableSpuriousResultFix`). Both processors are connected to both buffers, so the
 * sides end in one subtopology and their topics must be co-partitioned (4.1.3). Everything is validated before anything
 * is registered (B22).
 */
import { TopologyError } from '@/api/errors.js'
import type { KStream } from '@/api/kstream.js'
import type { Joined, StreamJoined } from '@/api/options.js'
import type { Codec } from '@/codec.js'
import type { DslContext } from '@/dsl/context.js'
import { requireCodec } from '@/dsl/format.js'
import { KStreamImpl } from '@/dsl/kstream.js'
import { NodePrefix, storeName, StorePrefix, suffixed } from '@/dsl/naming.js'
import { repartitionIfRequired } from '@/dsl/ops/repartition.js'
import { joinRetentionMs } from '@/dsl/retention.js'
import { JoinWindows, joinWindowsWithin } from '@/dsl/windows.js'
import { joinBufferBuilder } from '@/operators/join-buffer.js'
import { outerJoinStoreBuilder } from '@/operators/outer-join-store.js'
import { passThroughProcessor } from '@/operators/stateless.js'
import { streamStreamJoinProcessor } from '@/operators/stream-stream-join.js'
import type { StoreBuilder } from '@/topology/nodes.js'

/**
 * `(joiner, windows, options?)`, or the deprecated `(joiner, joined)` whose `within` gives the window. The
 * joiner receives null for the missing side of left and outer results.
 */
export type StreamStreamJoinArgs<K, V, VO, J> =
	| [joiner: J, windows: JoinWindows, options?: StreamJoined<K, V, VO>]
	| [joiner: J, joined: Joined<K, V, VO>]

/**
 * A joiner as the join processors call it (Kafka Streams' `ValueJoinerWithKey`): the record's key, null in a KIP-962
 * result of a null-key record, then the two values, null for the missing side of a left or outer result.
 */
export type KeyedStreamJoiner<K, V, VO, VR> = (key: K | null, value: V | null, otherValue: VO | null) => VR

type JoinKind = 'inner' | 'left' | 'outer'

const STORE_PREFIX: Readonly<Record<JoinKind, string>> = {
	inner: StorePrefix.STREAM_JOIN,
	left: StorePrefix.STREAM_LEFT_JOIN,
	outer: StorePrefix.STREAM_OUTER_JOIN,
}

/** `operator`: the method called (`join()`, `leftJoinWithKey()`, ...), as the topology errors name it. */
export function streamStreamJoin<K, V, VO, VR>(
	stream: KStreamImpl<K, V>,
	kind: JoinKind,
	operator: string,
	other: KStream<K, VO>,
	...args: StreamStreamJoinArgs<K, V, VO, KeyedStreamJoiner<K, V, VO, VR>>
): KStream<K, VR> {
	if (!(other instanceof KStreamImpl) || other.ctx !== stream.ctx) {
		throw new TopologyError(`${operator} needs a stream of the same app`)
	}
	const right = other as KStreamImpl<K, VO>
	const { joiner, windows, options } = joinArguments(operator, args)
	// The codecs do not depend on whether a side is repartitioned (B2).
	const key = requireCodec(options.key, stream.format.keyCodec, 'key', operator)
	const value = requireCodec(options.value, stream.format.valueCodec, 'value', operator)
	const otherValue = requireCodec(
		options.otherValue,
		right.format.valueCodec,
		'value',
		`${operator} of the other stream`
	)

	const named = (suffix: string) => suffixed(options.name, suffix)
	const thisSide = repartitionIfRequired(stream, { name: named('left'), owner: {}, codecs: { key, value }, operator })
	const otherSide = repartitionIfRequired(right, {
		name: named('right'),
		owner: {},
		codecs: { key, value: otherValue },
		operator,
	})

	const { ctx } = stream
	const { thisStore, otherStore, outerStore } = addStores(ctx, kind, windows, options, { key, value, otherValue })
	const outer = outerStore === undefined ? [] : [outerStore]
	// One processor clock for both sides, under the left buffer's name (4.8.5).
	const shared = { clock: thisStore, graceMs: windows.graceMs }
	// Left and outer joins pass null keys, so repartition topics before either side keep them (4.1.6, KF12).
	const marks = kind === 'inner' ? {} : { relaxedNullKeyJoin: true as const }
	// Kafka Streams' node names (KStreamImplJoin): "outer" marks this side's node in an outer join and the other
	// side's node in left and outer joins.
	const thisJoin = ctx.processor(
		kind === 'outer' ? NodePrefix.OUTER_THIS : NodePrefix.JOIN_THIS,
		named(kind === 'outer' ? 'outer-this-join' : 'this-join'),
		[thisSide.node],
		streamStreamJoinProcessor<K, V, VO, VR>({
			...shared,
			side: 'left',
			thisStore,
			otherStore,
			beforeMs: windows.beforeMs,
			afterMs: windows.afterMs,
			joiner,
			outer: kind !== 'inner',
			otherOuter: kind === 'outer',
			outerStore: outerStore === undefined ? undefined : { name: outerStore, key, value, otherValue },
		}),
		[thisStore, otherStore, ...outer],
		marks
	)
	const otherJoin = ctx.processor(
		kind === 'inner' ? NodePrefix.JOIN_OTHER : NodePrefix.OUTER_OTHER,
		named(kind === 'inner' ? 'other-join' : 'outer-other-join'),
		[otherSide.node],
		streamStreamJoinProcessor<K, VO, V, VR>({
			...shared,
			side: 'right',
			thisStore: otherStore,
			otherStore: thisStore,
			beforeMs: windows.afterMs,
			afterMs: windows.beforeMs,
			joiner: (key, otherValue, thisValue) => joiner(key, thisValue, otherValue),
			outer: kind === 'outer',
			otherOuter: kind !== 'inner',
			outerStore:
				outerStore === undefined ? undefined : { name: outerStore, key, value: otherValue, otherValue: value },
		}),
		[otherStore, thisStore, ...outer],
		marks
	)
	const merge = ctx.processor(NodePrefix.MERGE, named('merge'), [thisJoin, otherJoin], passThroughProcessor())
	const windowed = options.key === undefined ? thisSide.format.windowed : undefined
	return new KStreamImpl<K, VR>(ctx, merge, { keyCodec: key, windowed }, false)
}

/**
 * Registers the buffers of both sides and, for left and outer joins that defer their unmatched records (all but the
 * deprecated JoinWindows.of: KS `enableSpuriousResultFix`), the outer-join store; returns their names.
 */
function addStores<K, V, VO>(
	ctx: DslContext,
	kind: JoinKind,
	windows: JoinWindows,
	options: StreamJoined<K, V, VO> | Joined<K, V, VO>,
	codecs: { readonly key: Codec<K>; readonly value: Codec<V>; readonly otherValue: Codec<VO> }
): { thisStore: string; otherStore: string; outerStore?: string } {
	const base = (options as StreamJoined<K, V, VO>).storeName ?? options.name
	/** `<base>-<suffix>`, or a generated `<prefix>-<generated>-<n>`. */
	const name = (suffix: string, generated: string) =>
		suffixed(base, suffix) ?? storeName(ctx.builder, `${STORE_PREFIX[kind]}-${generated}`)
	const logging = options.changelog
	const add = (builder: StoreBuilder): string => {
		ctx.builder.addStateStore(builder)
		return builder.name
	}
	const buffer = <T>(suffix: string, generated: string, valueCodec: Codec<T>) =>
		add(
			joinBufferBuilder(ctx.changelog, {
				name: name(suffix, generated),
				keyCodec: codecs.key,
				valueCodec,
				retentionMs: joinRetentionMs(windows),
				logging,
			})
		)
	const thisStore = buffer('this-join-store', 'left', codecs.value)
	const otherStore = buffer('other-join-store', 'right', codecs.otherValue)
	if (kind === 'inner' || !windows.spuriousResultFix) {
		return { thisStore, otherStore }
	}
	const outerStore = add(
		outerJoinStoreBuilder(ctx.changelog, { name: name(`${kind}-shared-join-store`, 'shared'), logging })
	)
	return { thisStore, otherStore, outerStore }
}

interface JoinArguments<K, V, VO, VR> {
	readonly joiner: KeyedStreamJoiner<K, V, VO, VR>
	readonly windows: JoinWindows
	readonly options: StreamJoined<K, V, VO> | Joined<K, V, VO>
}

function joinArguments<K, V, VO, VR>(
	operator: string,
	[joiner, windows, options]: StreamStreamJoinArgs<K, V, VO, KeyedStreamJoiner<K, V, VO, VR>>
): JoinArguments<K, V, VO, VR> {
	if (windows instanceof JoinWindows) {
		return { joiner, windows, options: options ?? {} }
	}
	return { joiner, windows: joinWindowsWithin(windows?.within, operator), options: windows ?? {} }
}
