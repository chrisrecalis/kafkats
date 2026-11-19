/**
 * The codecs known for a stream or table at one point of the topology, and how operators change and resolve them
 * (6.0 "Formats"): an operator option wins over the format; a codec an operator needs and nobody gave is a
 * TopologyError naming the operator.
 */
import { TopologyError } from '@/api/errors.js'
import { buffer, type Codec } from '@/codec.js'

/** What windowed results are: time windows of `sizeMs`, sessions, or sliding windows of `sizeMs` (the time difference). */
export type WindowedFormat =
	| { kind: 'time'; sizeMs: number }
	| { kind: 'session' }
	| { kind: 'sliding'; sizeMs: number }

/** The codecs known for a stream or table at one point of the topology (undefined: unknown). */
export interface StreamFormat {
	readonly keyCodec?: Codec<unknown>
	readonly valueCodec?: Codec<unknown>
	readonly windowed?: WindowedFormat
}

const RAW: Codec<unknown> = buffer() as Codec<unknown>

/** A source reads raw Buffers unless codecs are given. */
export function sourceFormat(codecs: { readonly key?: Codec<unknown>; readonly value?: Codec<unknown> }): StreamFormat {
	return { keyCodec: codecs.key ?? RAW, valueCodec: codecs.value ?? RAW }
}

/** After an operator that changes values: the value codec is unknown. */
export function withNewValues(format: StreamFormat): StreamFormat {
	return { keyCodec: format.keyCodec, windowed: format.windowed }
}

/** After an operator that changes keys: the key codec (and with it the windowing) is unknown. */
export function withNewKeys(format: StreamFormat): StreamFormat {
	return { valueCodec: format.valueCodec }
}

/** The format of merged streams: what both agree on (the same codec objects, the same windows). */
export function mergedFormat(a: StreamFormat, b: StreamFormat): StreamFormat {
	return {
		keyCodec: a.keyCodec === b.keyCodec ? a.keyCodec : undefined,
		valueCodec: a.valueCodec === b.valueCodec ? a.valueCodec : undefined,
		windowed: sameWindows(a.windowed, b.windowed) ? a.windowed : undefined,
	}
}

/** `option ?? known`, or a TopologyError naming the operator that needs it. */
export function requireCodec<T>(
	option: Codec<T> | undefined,
	known: Codec<unknown> | undefined,
	part: 'key' | 'value',
	operator: string
): Codec<T> {
	const codec = option ?? (known as Codec<T> | undefined)
	if (codec === undefined) {
		throw new TopologyError(
			`${operator} needs a ${part} codec: the ${part}s were changed upstream, so pass one in its options`
		)
	}
	return codec
}

function sameWindows(a: WindowedFormat | undefined, b: WindowedFormat | undefined): boolean {
	const size = (w: WindowedFormat | undefined) => (w !== undefined && 'sizeMs' in w ? w.sizeMs : undefined)
	return a?.kind === b?.kind && size(a) === size(b)
}
