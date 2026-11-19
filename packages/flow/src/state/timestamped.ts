import type { Codec } from '@/codec.js'
import type { ValueAndTimestamp } from '@/state.js'

const TIMESTAMP_BYTES = 8

/**
 * `[int64BE timestamp][inner bytes]`: the value format of Kafka Streams' timestamped stores and their changelogs
 * (`ValueAndTimestampSerializer`).
 */
export function valueAndTimestampCodec<V>(inner: Codec<V>): Codec<ValueAndTimestamp<V>> {
	return {
		encode({ value, timestamp }) {
			const bytes = inner.encode(value)
			const encoded = Buffer.allocUnsafe(TIMESTAMP_BYTES + bytes.length)
			encoded.writeBigInt64BE(BigInt(timestamp), 0)
			bytes.copy(encoded, TIMESTAMP_BYTES)
			return encoded
		},
		decode: bytes => ({
			value: inner.decode(bytes.subarray(TIMESTAMP_BYTES)),
			timestamp: Number(bytes.readBigInt64BE(0)),
		}),
	}
}
