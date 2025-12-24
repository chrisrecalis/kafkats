import type { IDecoder, TaggedField } from '@/protocol/primitives/types.js'
import { zigZagDecode32, zigZagDecode64 } from '@/protocol/primitives/varint.js'

/**
 * Binary decoder for Kafka protocol messages
 * Reads sequentially from a buffer with position tracking
 */
export class Decoder implements IDecoder {
	private readonly buffer: Buffer
	private position: number

	constructor(buffer: Buffer, initialOffset: number = 0) {
		this.buffer = buffer
		this.position = initialOffset
	}

	/**
	 * Check that we have enough bytes remaining
	 */
	private ensureAvailable(bytes: number): void {
		if (this.position + bytes > this.buffer.length) {
			throw new Error(
				`Buffer underflow: need ${bytes} bytes but only ${this.buffer.length - this.position} remaining`
			)
		}
	}

	readInt8(): number {
		this.ensureAvailable(1)
		const value = this.buffer.readInt8(this.position)
		this.position += 1
		return value
	}

	readInt16(): number {
		this.ensureAvailable(2)
		const value = this.buffer.readInt16BE(this.position)
		this.position += 2
		return value
	}

	readInt32(): number {
		this.ensureAvailable(4)
		const value = this.buffer.readInt32BE(this.position)
		this.position += 4
		return value
	}

	readInt64(): bigint {
		this.ensureAvailable(8)
		const value = this.buffer.readBigInt64BE(this.position)
		this.position += 8
		return value
	}

	readUInt8(): number {
		this.ensureAvailable(1)
		const value = this.buffer.readUInt8(this.position)
		this.position += 1
		return value
	}

	readUInt16(): number {
		this.ensureAvailable(2)
		const value = this.buffer.readUInt16BE(this.position)
		this.position += 2
		return value
	}

	readUInt32(): number {
		this.ensureAvailable(4)
		const value = this.buffer.readUInt32BE(this.position)
		this.position += 4
		return value
	}

	readVarInt(): number {
		let value = 0
		let shift = 0
		let byte: number

		do {
			if (this.position >= this.buffer.length) {
				throw new Error('Buffer underflow while reading VARINT')
			}
			byte = this.buffer[this.position++]!
			value |= (byte & 0x7f) << shift
			shift += 7
		} while ((byte & 0x80) !== 0)

		return zigZagDecode32(value)
	}

	readVarLong(): bigint {
		let value = 0n
		let shift = 0n
		let byte: number

		do {
			if (this.position >= this.buffer.length) {
				throw new Error('Buffer underflow while reading VARLONG')
			}
			byte = this.buffer[this.position++]!
			value |= BigInt(byte & 0x7f) << shift
			shift += 7n
		} while ((byte & 0x80) !== 0)

		return zigZagDecode64(value)
	}

	readUVarInt(): number {
		let value = 0
		let shift = 0
		let byte: number

		do {
			if (this.position >= this.buffer.length) {
				throw new Error('Buffer underflow while reading UVARINT')
			}
			byte = this.buffer[this.position++]!
			value |= (byte & 0x7f) << shift
			shift += 7
		} while ((byte & 0x80) !== 0)

		return value
	}

	readString(): string {
		const length = this.readInt16()
		if (length < 0) {
			throw new Error('Unexpected null in non-nullable string')
		}
		this.ensureAvailable(length)
		const value = this.buffer.toString('utf-8', this.position, this.position + length)
		this.position += length
		return value
	}

	readNullableString(): string | null {
		const length = this.readInt16()
		if (length < 0) {
			return null
		}
		this.ensureAvailable(length)
		const value = this.buffer.toString('utf-8', this.position, this.position + length)
		this.position += length
		return value
	}

	readCompactString(): string {
		const length = this.readUVarInt() - 1 // Stored as length + 1
		if (length < 0) {
			throw new Error('Unexpected null in non-nullable compact string')
		}
		this.ensureAvailable(length)
		const value = this.buffer.toString('utf-8', this.position, this.position + length)
		this.position += length
		return value
	}

	readCompactNullableString(): string | null {
		const length = this.readUVarInt() - 1
		if (length < 0) {
			return null
		}
		this.ensureAvailable(length)
		const value = this.buffer.toString('utf-8', this.position, this.position + length)
		this.position += length
		return value
	}

	readBytes(): Buffer {
		const length = this.readInt32()
		if (length < 0) {
			throw new Error('Unexpected null in non-nullable bytes')
		}
		this.ensureAvailable(length)
		const value = this.buffer.subarray(this.position, this.position + length)
		this.position += length
		return value
	}

	readNullableBytes(): Buffer | null {
		const length = this.readInt32()
		if (length < 0) {
			return null
		}
		this.ensureAvailable(length)
		const value = this.buffer.subarray(this.position, this.position + length)
		this.position += length
		return value
	}

	readCompactBytes(): Buffer {
		const length = this.readUVarInt() - 1
		if (length < 0) {
			throw new Error('Unexpected null in non-nullable compact bytes')
		}
		this.ensureAvailable(length)
		const value = this.buffer.subarray(this.position, this.position + length)
		this.position += length
		return value
	}

	readCompactNullableBytes(): Buffer | null {
		const length = this.readUVarInt() - 1
		if (length < 0) {
			return null
		}
		this.ensureAvailable(length)
		const value = this.buffer.subarray(this.position, this.position + length)
		this.position += length
		return value
	}

	readUUID(): string {
		this.ensureAvailable(16)
		const bytes = this.buffer.subarray(this.position, this.position + 16)
		this.position += 16

		// Format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
		const hex = bytes.toString('hex')
		return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`
	}

	readBoolean(): boolean {
		return this.readInt8() !== 0
	}

	readArray<T>(readItem: (decoder: IDecoder) => T): T[] {
		const length = this.readInt32()
		if (length < 0) {
			throw new Error('Unexpected null in non-nullable array')
		}
		const items: T[] = []
		for (let i = 0; i < length; i++) {
			items.push(readItem(this))
		}
		return items
	}

	readNullableArray<T>(readItem: (decoder: IDecoder) => T): T[] | null {
		const length = this.readInt32()
		if (length < 0) {
			return null
		}
		const items: T[] = []
		for (let i = 0; i < length; i++) {
			items.push(readItem(this))
		}
		return items
	}

	readCompactArray<T>(readItem: (decoder: IDecoder) => T): T[] {
		const length = this.readUVarInt() - 1
		if (length < 0) {
			throw new Error('Unexpected null in non-nullable compact array')
		}
		const items: T[] = []
		for (let i = 0; i < length; i++) {
			items.push(readItem(this))
		}
		return items
	}

	readCompactNullableArray<T>(readItem: (decoder: IDecoder) => T): T[] | null {
		const length = this.readUVarInt() - 1
		if (length < 0) {
			return null
		}
		const items: T[] = []
		for (let i = 0; i < length; i++) {
			items.push(readItem(this))
		}
		return items
	}

	readTaggedFields(): TaggedField[] {
		const count = this.readUVarInt()
		const fields: TaggedField[] = []
		for (let i = 0; i < count; i++) {
			const tag = this.readUVarInt()
			const length = this.readUVarInt()
			const data = this.readRaw(length)
			fields.push({ tag, data })
		}
		return fields
	}

	skipTaggedFields(): void {
		const count = this.readUVarInt()
		for (let i = 0; i < count; i++) {
			this.readUVarInt() // Skip tag
			const length = this.readUVarInt()
			this.skip(length) // Skip data
		}
	}

	readRaw(length: number): Buffer {
		this.ensureAvailable(length)
		const value = this.buffer.subarray(this.position, this.position + length)
		this.position += length
		return value
	}

	remaining(): number {
		return this.buffer.length - this.position
	}

	offset(): number {
		return this.position
	}

	seek(position: number): void {
		if (position < 0 || position > this.buffer.length) {
			throw new Error(`Invalid seek position: ${position}`)
		}
		this.position = position
	}

	skip(length: number): void {
		this.ensureAvailable(length)
		this.position += length
	}
}
