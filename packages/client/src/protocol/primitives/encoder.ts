import type { IEncoder, TaggedField } from '@/protocol/primitives/types.js'
import { zigZagEncode32, zigZagEncode64 } from '@/protocol/primitives/varint.js'

/**
 * Return the next power of 2 >= value
 */
function nextPowerOfTwo(value: number): number {
	if (value <= 0) return 1
	value--
	value |= value >> 1
	value |= value >> 2
	value |= value >> 4
	value |= value >> 8
	value |= value >> 16
	return value + 1
}

/**
 * Binary encoder for Kafka protocol messages
 * Uses dynamic buffer expansion with power-of-2 growth
 */
export class Encoder implements IEncoder {
	private buffer: Buffer
	private position: number

	constructor(initialSize: number = 256) {
		this.buffer = Buffer.allocUnsafe(nextPowerOfTwo(initialSize))
		this.position = 0
	}

	/**
	 * Ensure the buffer has space for the specified number of bytes
	 */
	private ensureCapacity(bytes: number): void {
		const required = this.position + bytes
		if (required > this.buffer.length) {
			const newSize = nextPowerOfTwo(required)
			const newBuffer = Buffer.allocUnsafe(newSize)
			this.buffer.copy(newBuffer, 0, 0, this.position)
			this.buffer = newBuffer
		}
	}

	writeInt8(value: number): this {
		this.ensureCapacity(1)
		this.buffer.writeInt8(value, this.position)
		this.position += 1
		return this
	}

	writeInt16(value: number): this {
		this.ensureCapacity(2)
		this.buffer.writeInt16BE(value, this.position)
		this.position += 2
		return this
	}

	writeInt32(value: number): this {
		this.ensureCapacity(4)
		this.buffer.writeInt32BE(value, this.position)
		this.position += 4
		return this
	}

	writeInt64(value: bigint): this {
		this.ensureCapacity(8)
		this.buffer.writeBigInt64BE(value, this.position)
		this.position += 8
		return this
	}

	writeUInt8(value: number): this {
		this.ensureCapacity(1)
		this.buffer.writeUInt8(value, this.position)
		this.position += 1
		return this
	}

	writeUInt16(value: number): this {
		this.ensureCapacity(2)
		this.buffer.writeUInt16BE(value, this.position)
		this.position += 2
		return this
	}

	writeUInt32(value: number): this {
		this.ensureCapacity(4)
		this.buffer.writeUInt32BE(value, this.position)
		this.position += 4
		return this
	}

	writeVarInt(value: number): this {
		return this.writeUVarInt(zigZagEncode32(value))
	}

	writeVarLong(value: bigint): this {
		let remaining = zigZagEncode64(value)

		// Ensure capacity for max 10 bytes
		this.ensureCapacity(10)

		while (remaining > 0x7fn) {
			this.buffer[this.position++] = Number(remaining & 0x7fn) | 0x80
			remaining >>= 7n
		}
		this.buffer[this.position++] = Number(remaining & 0x7fn)
		return this
	}

	writeUVarInt(value: number): this {
		if (value < 0) {
			throw new Error('UVARINT cannot encode negative numbers')
		}

		// Ensure capacity for max 5 bytes (32-bit value)
		this.ensureCapacity(5)

		while (value > 0x7f) {
			this.buffer[this.position++] = (value & 0x7f) | 0x80
			value >>>= 7
		}
		this.buffer[this.position++] = value & 0x7f
		return this
	}

	writeString(value: string): this {
		const bytes = Buffer.from(value, 'utf-8')
		this.writeInt16(bytes.length)
		this.writeRaw(bytes)
		return this
	}

	writeNullableString(value: string | null): this {
		if (value === null) {
			this.writeInt16(-1)
			return this
		}
		return this.writeString(value)
	}

	writeCompactString(value: string): this {
		const bytes = Buffer.from(value, 'utf-8')
		// Length + 1 as UVARINT (0 would mean null in nullable variant)
		this.writeUVarInt(bytes.length + 1)
		this.writeRaw(bytes)
		return this
	}

	writeCompactNullableString(value: string | null): this {
		if (value === null) {
			this.writeUVarInt(0) // 0 means null
			return this
		}
		return this.writeCompactString(value)
	}

	writeBytes(value: Buffer): this {
		this.writeInt32(value.length)
		this.writeRaw(value)
		return this
	}

	writeNullableBytes(value: Buffer | null): this {
		if (value === null) {
			this.writeInt32(-1)
			return this
		}
		return this.writeBytes(value)
	}

	writeCompactBytes(value: Buffer): this {
		// Length + 1 as UVARINT
		this.writeUVarInt(value.length + 1)
		this.writeRaw(value)
		return this
	}

	writeCompactNullableBytes(value: Buffer | null): this {
		if (value === null) {
			this.writeUVarInt(0)
			return this
		}
		return this.writeCompactBytes(value)
	}

	writeUUID(value: string): this {
		// Remove hyphens and convert to bytes
		const hex = value.replace(/-/g, '')
		if (hex.length !== 32) {
			throw new Error(`Invalid UUID format: ${value}`)
		}
		const bytes = Buffer.from(hex, 'hex')
		this.writeRaw(bytes)
		return this
	}

	writeBoolean(value: boolean): this {
		this.writeInt8(value ? 1 : 0)
		return this
	}

	writeArray<T>(items: T[], writeItem: (item: T, encoder: IEncoder) => void): this {
		this.writeInt32(items.length)
		for (const item of items) {
			writeItem(item, this)
		}
		return this
	}

	writeNullableArray<T>(items: T[] | null, writeItem: (item: T, encoder: IEncoder) => void): this {
		if (items === null) {
			this.writeInt32(-1)
			return this
		}
		return this.writeArray(items, writeItem)
	}

	writeCompactArray<T>(items: T[], writeItem: (item: T, encoder: IEncoder) => void): this {
		// Length + 1 as UVARINT
		this.writeUVarInt(items.length + 1)
		for (const item of items) {
			writeItem(item, this)
		}
		return this
	}

	writeCompactNullableArray<T>(items: T[] | null, writeItem: (item: T, encoder: IEncoder) => void): this {
		if (items === null) {
			this.writeUVarInt(0)
			return this
		}
		return this.writeCompactArray(items, writeItem)
	}

	writeTaggedFields(fields: TaggedField[]): this {
		// Sort by tag number (required by protocol)
		const sorted = [...fields].sort((a, b) => a.tag - b.tag)
		this.writeUVarInt(sorted.length)
		for (const field of sorted) {
			this.writeUVarInt(field.tag)
			this.writeUVarInt(field.data.length)
			this.writeRaw(field.data)
		}
		return this
	}

	writeEmptyTaggedFields(): this {
		this.writeUVarInt(0)
		return this
	}

	writeRaw(data: Buffer): this {
		this.ensureCapacity(data.length)
		data.copy(this.buffer, this.position)
		this.position += data.length
		return this
	}

	toBuffer(): Buffer {
		return this.buffer.subarray(0, this.position)
	}

	size(): number {
		return this.position
	}
}
