/**
 * Tagged field structure for flexible protocol versions
 */
export interface TaggedField {
	tag: number
	data: Buffer
}

/**
 * Binary encoder interface for Kafka protocol serialization
 * All methods return `this` for fluent chaining
 */
export interface IEncoder {
	// Fixed-width integers (big-endian)
	writeInt8(value: number): this
	writeInt16(value: number): this
	writeInt32(value: number): this
	writeInt64(value: bigint): this
	writeUInt8(value: number): this
	writeUInt16(value: number): this
	writeUInt32(value: number): this

	// Variable-length integers
	writeVarInt(value: number): this // Signed, ZigZag encoded
	writeVarLong(value: bigint): this // Signed 64-bit, ZigZag encoded
	writeUVarInt(value: number): this // Unsigned varint (LEB128)

	// Strings (non-flexible versions)
	writeString(value: string): this // INT16 length + UTF-8
	writeNullableString(value: string | null): this // -1 for null

	// Strings (flexible versions - COMPACT)
	writeCompactString(value: string): this // UVARINT(len+1) + UTF-8
	writeCompactNullableString(value: string | null): this // 0 for null

	// Bytes (non-flexible versions)
	writeBytes(value: Buffer): this // INT32 length + data
	writeNullableBytes(value: Buffer | null): this // -1 for null

	// Bytes (flexible versions - COMPACT)
	writeCompactBytes(value: Buffer): this // UVARINT(len+1) + data
	writeCompactNullableBytes(value: Buffer | null): this // 0 for null

	// UUID (16 bytes, big-endian)
	writeUUID(value: string): this

	// Boolean
	writeBoolean(value: boolean): this

	// Arrays (non-flexible versions)
	writeArray<T>(items: T[], writeItem: (item: T, encoder: IEncoder) => void): this
	writeNullableArray<T>(items: T[] | null, writeItem: (item: T, encoder: IEncoder) => void): this

	// Arrays (flexible versions - COMPACT)
	writeCompactArray<T>(items: T[], writeItem: (item: T, encoder: IEncoder) => void): this
	writeCompactNullableArray<T>(items: T[] | null, writeItem: (item: T, encoder: IEncoder) => void): this

	// Tagged fields (flexible versions only)
	writeTaggedFields(fields: TaggedField[]): this
	writeEmptyTaggedFields(): this // Convenience for empty tag buffer

	// Raw bytes and buffer management
	writeRaw(data: Buffer): this
	toBuffer(): Buffer
	size(): number
}

/**
 * Binary decoder interface for Kafka protocol deserialization
 */
export interface IDecoder {
	// Fixed-width integers (big-endian)
	readInt8(): number
	readInt16(): number
	readInt32(): number
	readInt64(): bigint
	readUInt8(): number
	readUInt16(): number
	readUInt32(): number

	// Variable-length integers
	readVarInt(): number
	readVarLong(): bigint
	readUVarInt(): number

	// Strings (non-flexible)
	readString(): string
	readNullableString(): string | null

	// Strings (flexible/compact)
	readCompactString(): string
	readCompactNullableString(): string | null

	// Bytes (non-flexible)
	readBytes(): Buffer
	readNullableBytes(): Buffer | null

	// Bytes (flexible/compact)
	readCompactBytes(): Buffer
	readCompactNullableBytes(): Buffer | null

	// UUID
	readUUID(): string

	// Boolean
	readBoolean(): boolean

	// Arrays (non-flexible)
	readArray<T>(readItem: (decoder: IDecoder) => T): T[]
	readNullableArray<T>(readItem: (decoder: IDecoder) => T): T[] | null

	// Arrays (flexible/compact)
	readCompactArray<T>(readItem: (decoder: IDecoder) => T): T[]
	readCompactNullableArray<T>(readItem: (decoder: IDecoder) => T): T[] | null

	// Tagged fields
	readTaggedFields(): TaggedField[]
	skipTaggedFields(): void // For when tags are not needed

	// Raw bytes and buffer management
	readRaw(length: number): Buffer
	remaining(): number
	offset(): number
	seek(position: number): void
	skip(length: number): void
}
