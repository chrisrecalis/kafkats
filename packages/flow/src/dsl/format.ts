import type { Codec } from '@/codec.js'

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
