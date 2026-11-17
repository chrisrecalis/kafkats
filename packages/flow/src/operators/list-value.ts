/**
 * The list values of the stream-stream join stores (6.10, 6.25): the values of one entry, in the order they were added,
 * as `[u32 count]([u32 length][bytes])*`. They keep the records that share a key and a timestamp apart, as Kafka
 * Streams' join window stores retain duplicates and its outer-join store is a `ListValueStore` (ksdsl§7.1, §7.3).
 */
import type { Codec } from '@/codec.js'

const U32 = 4

export function listCodec<V>(element: Codec<V>): Codec<V[]> {
	return {
		encode(values) {
			const parts = values.map(value => element.encode(value))
			const encoded = Buffer.allocUnsafe(parts.reduce((size, part) => size + U32 + part.length, U32))
			encoded.writeUInt32BE(parts.length, 0)
			let offset = U32
			for (const part of parts) {
				encoded.writeUInt32BE(part.length, offset)
				part.copy(encoded, offset + U32)
				offset += U32 + part.length
			}
			return encoded
		},
		decode(bytes) {
			const values: V[] = []
			let offset = U32
			for (let count = bytes.readUInt32BE(0); count > 0; count--) {
				const end = offset + U32 + bytes.readUInt32BE(offset)
				values.push(element.decode(bytes.subarray(offset + U32, end)))
				offset = end
			}
			return values
		},
	}
}
