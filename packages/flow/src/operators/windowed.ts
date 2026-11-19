/**
 * Windowed keys on topics, in Kafka Streams' formats (6.9, ksdsl§14), and the partitioner that keeps them in the
 * partition of their base key. Window and session stores and their changelogs use another key format
 * (`windowedKeyCodec`, 5.1), which these codecs do not change.
 */
import { murmur2, toPositive } from '@kafkats/client'
import type { Windowed } from '@/api/grouped.js'
import type { Codec } from '@/codec.js'
import type { StreamPartitioner } from '@/topology/nodes.js'

/** A codec of windowed keys that knows the codec of their base key: sinks partition its keys by the base key. */
export interface WindowedCodec<K> extends Codec<Windowed<K>> {
	readonly inner: Codec<K>
	readonly kind: 'time' | 'session'
}

const TIME_BYTES = 8

/** Codecs of windowed keys, byte-compatible with Kafka Streams' `WindowedSerdes`. */
export const WindowedCodecs = {
	/**
	 * Time and sliding windows (KS `TimeWindowedSerde`): `[key][start int64BE]`. The end is not written: decoding
	 * gives `start + windowSizeMs`, at most `Number.MAX_SAFE_INTEGER` (KS `WindowKeySchema.timeWindowForSize`).
	 */
	timeWindowed<K>(inner: Codec<K>, windowSizeMs: number): WindowedCodec<K> {
		if (!Number.isSafeInteger(windowSizeMs) || windowSizeMs < 0) {
			throw new RangeError(`window size must be a non-negative integer of ms, got ${windowSizeMs}`)
		}
		return {
			inner,
			kind: 'time',
			encode: windowed => withTimes(inner.encode(windowed.key), [windowed.window.start]),
			decode: buffer => {
				const key = inner.decode(baseKey(buffer, 1))
				const start = timeAt(buffer, 1)
				return { key, window: { start, end: Math.min(start + windowSizeMs, Number.MAX_SAFE_INTEGER) } }
			},
		}
	},

	/** Sessions (KS `SessionWindowedSerde`): `[key][end int64BE][start int64BE]`. */
	sessionWindowed<K>(inner: Codec<K>): WindowedCodec<K> {
		return {
			inner,
			kind: 'session',
			encode: windowed => withTimes(inner.encode(windowed.key), [windowed.window.end, windowed.window.start]),
			decode: buffer => ({
				key: inner.decode(baseKey(buffer, 2)),
				window: { start: timeAt(buffer, 1), end: timeAt(buffer, 2) },
			}),
		}
	},
}

/** Whether `codec` is a windowed codec: one of `WindowedCodecs` or any other implementation of the interface. */
export function isWindowedCodec(codec: Codec<unknown> | undefined): codec is WindowedCodec<unknown> {
	const windowed = codec as Partial<WindowedCodec<unknown>> | undefined
	return (windowed?.kind === 'time' || windowed?.kind === 'session') && typeof windowed.inner?.encode === 'function'
}

/**
 * KS `WindowedStreamPartitioner`: the partition the producer's default partitioner picks for the base key, so a
 * windowed record lands where its key does. A null key is left to the producer.
 */
export function windowedPartitioner<K>(codec: WindowedCodec<K>): StreamPartitioner<Windowed<K> | null, unknown> {
	const { inner } = codec
	return (_topic, windowed, _value, partitionCount) =>
		windowed == null ? undefined : toPositive(murmur2(inner.encode(windowed.key))) % partitionCount
}

/** `key` followed by each time as int64 big-endian. */
function withTimes(key: Buffer, times: readonly number[]): Buffer {
	const buffer = Buffer.allocUnsafe(key.length + times.length * TIME_BYTES)
	key.copy(buffer)
	times.forEach((time, i) => buffer.writeBigInt64BE(BigInt(time), key.length + i * TIME_BYTES))
	return buffer
}

/** The bytes before the last `times` int64s. */
function baseKey(buffer: Buffer, times: number): Buffer {
	const length = buffer.length - times * TIME_BYTES
	if (length < 0) {
		throw new RangeError(`a windowed key has at least ${times * TIME_BYTES} bytes, got ${buffer.length}`)
	}
	return buffer.subarray(0, length)
}

/** The `n`-th int64 big-endian from the end of `buffer` (1: the last one). */
function timeAt(buffer: Buffer, n: number): number {
	return Number(buffer.readBigInt64BE(buffer.length - n * TIME_BYTES))
}
