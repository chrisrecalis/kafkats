/** Windowed stream-stream joins: inner, left and outer (6.10). */
import type { KStream } from '@/api/kstream.js'
import type { Joined, StreamJoined } from '@/api/options.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import { notImplemented } from '@/dsl/not-implemented.js'
import type { JoinWindows } from '@/dsl/windows.js'

/**
 * `(joiner, windows, options?)`, or the deprecated `(joiner, joined)` whose `within` gives the window. The
 * joiner receives null for the missing side of left and outer results.
 */
export type StreamStreamJoinArgs<K, V, VO, VR> =
	| [joiner: (value: V, otherValue: VO) => VR, windows: JoinWindows, options?: StreamJoined<K, V, VO>]
	| [joiner: (value: V, otherValue: VO) => VR, joined: Joined<K, V, VO>]

export function streamStreamJoin<K, V, VO, VR>(
	_stream: KStreamImpl<K, V>,
	kind: 'inner' | 'left' | 'outer',
	_other: KStream<K, VO>,
	..._args: StreamStreamJoinArgs<K, V, VO, VR>
): KStream<K, VR> {
	return notImplemented(`KStream ${kind} join with a KStream`)
}
