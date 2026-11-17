/**
 * Windowed stream-stream joins: inner, left and outer (6.10). Each side is repartitioned if its keys changed upstream
 * and buffered in its own window store; a join processor per side probes the other side's buffer and a pass-through
 * node merges their results. Both processors are connected to both buffers, so the sides end in one subtopology and
 * their topics must be co-partitioned (4.1.3). Everything is validated before anything is registered (B22).
 */
import { TopologyError } from '@/api/errors.js'
import type { KStream } from '@/api/kstream.js'
import type { Joined, StreamJoined } from '@/api/options.js'
import type { Codec } from '@/codec.js'
import type { DslContext } from '@/dsl/context.js'
import { requireCodec } from '@/dsl/format.js'
import { KStreamImpl } from '@/dsl/kstream.js'
import { NodePrefix, storeName, StorePrefix } from '@/dsl/naming.js'
import { repartitionIfRequired } from '@/dsl/ops/repartition.js'
import { JoinWindows, joinWindowsWithin } from '@/dsl/windows.js'
import { joinBufferWindow } from '@/operators/join-buffer.js'
import { passThroughProcessor } from '@/operators/stateless.js'
import { streamStreamJoinProcessor } from '@/operators/stream-stream-join.js'
import { windowStoreBuilder, type WindowStoreBuilderOptions } from '@/state/store-builders.js'

/**
 * `(joiner, windows, options?)`, or the deprecated `(joiner, joined)` whose `within` gives the window. The
 * joiner receives null for the missing side of left and outer results.
 */
export type StreamStreamJoinArgs<K, V, VO, VR> =
	| [joiner: (value: V, otherValue: VO) => VR, windows: JoinWindows, options?: StreamJoined<K, V, VO>]
	| [joiner: (value: V, otherValue: VO) => VR, joined: Joined<K, V, VO>]

type JoinKind = 'inner' | 'left' | 'outer'

const OPERATOR: Readonly<Record<JoinKind, string>> = { inner: 'join()', left: 'leftJoin()', outer: 'outerJoin()' }
const STORE_PREFIX: Readonly<Record<JoinKind, string>> = {
	inner: StorePrefix.STREAM_JOIN,
	left: StorePrefix.STREAM_LEFT_JOIN,
	outer: StorePrefix.STREAM_OUTER_JOIN,
}

export function streamStreamJoin<K, V, VO, VR>(
	stream: KStreamImpl<K, V>,
	kind: JoinKind,
	other: KStream<K, VO>,
	...args: StreamStreamJoinArgs<K, V, VO, VR>
): KStream<K, VR> {
	const operator = OPERATOR[kind]
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

	const { name } = options
	const named = (suffix: string) => (name === undefined ? undefined : `${name}-${suffix}`)
	const thisSide = repartitionIfRequired(stream, { name: named('left'), owner: {}, codecs: { key, value }, operator })
	const otherSide = repartitionIfRequired(right, {
		name: named('right'),
		owner: {},
		codecs: { key, value: otherValue },
		operator,
	})

	const { ctx } = stream
	const windowMs = Math.max(windows.beforeMs, windows.afterMs)
	const base = (options as StreamJoined<K, V, VO>).storeName ?? name
	const buffer = (side: 'left' | 'right', suffix: string, valueCodec: Codec<unknown>) =>
		addBuffer(ctx, {
			name: base === undefined ? storeName(ctx.builder, `${STORE_PREFIX[kind]}-${side}`) : `${base}-${suffix}`,
			keyCodec: key,
			valueCodec,
			logging: options.changelog,
			...joinBufferWindow(windowMs),
		})
	const thisStore = buffer('left', 'this-join-store', value)
	const otherStore = buffer('right', 'other-join-store', otherValue)

	// One clock for both sides, under the left buffer's name (4.8.5).
	const shared = { clock: thisStore, windowMs }
	// Kafka Streams' node names (KStreamImplJoin): "outer" marks this side's node in an outer join and the other
	// side's node in left and outer joins.
	const thisJoin = ctx.processor(
		kind === 'outer' ? NodePrefix.OUTER_THIS : NodePrefix.JOIN_THIS,
		named(kind === 'outer' ? 'outer-this-join' : 'this-join'),
		[thisSide.node],
		streamStreamJoinProcessor<K, V, VO, VR>({
			...shared,
			thisStore,
			otherStore,
			beforeMs: windows.beforeMs,
			afterMs: windows.afterMs,
			joiner,
			outer: kind !== 'inner',
		}),
		[thisStore, otherStore]
	)
	const otherJoin = ctx.processor(
		kind === 'inner' ? NodePrefix.JOIN_OTHER : NodePrefix.OUTER_OTHER,
		named(kind === 'inner' ? 'other-join' : 'outer-other-join'),
		[otherSide.node],
		streamStreamJoinProcessor<K, VO, V, VR>({
			...shared,
			thisStore: otherStore,
			otherStore: thisStore,
			beforeMs: windows.afterMs,
			afterMs: windows.beforeMs,
			joiner: (otherValue, thisValue) => joiner(thisValue, otherValue),
			outer: kind === 'outer',
		}),
		[otherStore, thisStore]
	)
	const merge = ctx.processor(NodePrefix.MERGE, named('merge'), [thisJoin, otherJoin], passThroughProcessor())
	const windowed = options.key === undefined ? thisSide.format.windowed : undefined
	return new KStreamImpl<K, VR>(ctx, merge, { keyCodec: key, windowed }, false)
}

/** Registers the window store of one side's buffer; returns its name. */
function addBuffer(ctx: DslContext, options: WindowStoreBuilderOptions<unknown, unknown>): string {
	ctx.builder.addStateStore(windowStoreBuilder(ctx.changelog, options))
	return options.name
}

interface JoinArguments<K, V, VO, VR> {
	/** Called with null for the missing side of left and outer results. */
	readonly joiner: (value: V | null, otherValue: VO | null) => VR
	readonly windows: JoinWindows
	readonly options: StreamJoined<K, V, VO> | Joined<K, V, VO>
}

function joinArguments<K, V, VO, VR>(
	operator: string,
	[joiner, windows, options]: StreamStreamJoinArgs<K, V, VO, VR>
): JoinArguments<K, V, VO, VR> {
	const join = joiner as JoinArguments<K, V, VO, VR>['joiner']
	if (windows instanceof JoinWindows) {
		return { joiner: join, windows, options: options ?? {} }
	}
	return { joiner: join, windows: joinWindowsWithin(windows?.within, operator), options: windows ?? {} }
}
