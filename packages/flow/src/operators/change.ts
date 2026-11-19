/**
 * A table update (6.11), and how one is written to the repartition topic of KTable.groupBy (6.15) in Kafka Streams'
 * format (KS `ChangedSerializer.java:88-131`, `ChangedDeserializer.java`): `[old][0x00]` for an old value only,
 * `[new][0x01]` for a new value only, `[varint newLength][new][old][0x02]` for both (Kafka's zigzag varint). An update
 * on that topic has at least one of them.
 */
import { decodeVarInt, encodeVarInt } from '@kafkats/client'
import type { Codec } from '@/codec.js'

/** A table update. The old value is populated only where sending old values is enabled. */
export interface Change<V> {
	readonly newValue: V | null
	readonly oldValue: V | null
}

const OLD_ONLY = 0
const NEW_ONLY = 1
const BOTH = 2

/** The codec of the updates of a table whose values `inner` encodes. */
export function changeCodec<V>(inner: Codec<V>): Codec<Change<V>> {
	return {
		encode({ newValue, oldValue }) {
			if (newValue === null) {
				if (oldValue === null) {
					throw new TypeError('a Change needs a new or an old value')
				}
				return Buffer.concat([inner.encode(oldValue), Buffer.of(OLD_ONLY)])
			}
			const encodedNew = inner.encode(newValue)
			if (oldValue === null) {
				return Buffer.concat([encodedNew, Buffer.of(NEW_ONLY)])
			}
			const length = encodeVarInt(encodedNew.length)
			return Buffer.concat([length, encodedNew, inner.encode(oldValue), Buffer.of(BOTH)])
		},
		decode(encoded) {
			const values = encoded.subarray(0, -1)
			const flag = encoded[encoded.length - 1]
			switch (flag) {
				case OLD_ONLY:
					return { newValue: null, oldValue: inner.decode(values) }
				case NEW_ONLY:
					return { newValue: inner.decode(values), oldValue: null }
				case BOTH: {
					const [newValue, oldValue] = splitValues(values)
					return { newValue: inner.decode(newValue), oldValue: inner.decode(oldValue) }
				}
				default:
					throw new RangeError(`unknown encoding flag ${flag} of a Change`)
			}
		},
	}
}

/** The encoded new and old value of `[varint newLength][new][old]`. */
function splitValues(values: Buffer): [newValue: Buffer, oldValue: Buffer] {
	const { value: length, bytesRead } = decodeVarInt(values)
	const end = bytesRead + length
	if (length < 0 || end > values.length) {
		throw new RangeError(`a Change's new value has ${length} bytes, ${values.length - bytesRead} follow`)
	}
	return [values.subarray(bytesRead, end), values.subarray(end)]
}
