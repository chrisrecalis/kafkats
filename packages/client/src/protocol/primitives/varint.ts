/**
 * Variable-length integer encoding for Kafka protocol
 *
 * UVARINT: Unsigned variable-length integer using LEB128 encoding
 * VARINT: Signed variable-length integer using ZigZag + LEB128 encoding
 * VARLONG: Signed 64-bit variable-length integer using ZigZag + LEB128 encoding
 */

// ============================================================================
// ZigZag Encoding
// Converts signed integers to unsigned by interleaving negative and positive
// values: 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, ...
// ============================================================================

/**
 * ZigZag encode a 32-bit signed integer to unsigned
 */
export function zigZagEncode32(value: number): number {
	return (value << 1) ^ (value >> 31)
}

/**
 * ZigZag decode an unsigned integer back to 32-bit signed
 */
export function zigZagDecode32(value: number): number {
	return (value >>> 1) ^ -(value & 1)
}

/**
 * ZigZag encode a 64-bit signed bigint to unsigned
 */
export function zigZagEncode64(value: bigint): bigint {
	return (value << 1n) ^ (value >> 63n)
}

/**
 * ZigZag decode an unsigned bigint back to 64-bit signed
 */
export function zigZagDecode64(value: bigint): bigint {
	return (value >> 1n) ^ -(value & 1n)
}

// ============================================================================
// LEB128 Encoding (Little Endian Base 128)
// Each byte has 7 data bits + 1 continuation bit (MSB)
// MSB = 1 means more bytes follow; MSB = 0 means final byte
// ============================================================================

/**
 * Encode an unsigned integer as UVARINT (LEB128)
 */
export function encodeUVarInt(value: number): Buffer {
	if (value < 0) {
		throw new Error('UVARINT cannot encode negative numbers')
	}

	const bytes: number[] = []

	while (value > 0x7f) {
		bytes.push((value & 0x7f) | 0x80) // Set continuation bit
		value >>>= 7
	}
	bytes.push(value & 0x7f) // Final byte without continuation bit

	return Buffer.from(bytes)
}

/**
 * Decode a UVARINT from a buffer at the given offset
 * Returns the decoded value and number of bytes read
 */
export function decodeUVarInt(buffer: Buffer, offset: number = 0): { value: number; bytesRead: number } {
	let value = 0
	let shift = 0
	let bytesRead = 0

	while (true) {
		if (offset + bytesRead >= buffer.length) {
			throw new Error('Buffer underflow while reading UVARINT')
		}

		const byte = buffer[offset + bytesRead]!
		bytesRead++

		value |= (byte & 0x7f) << shift

		if ((byte & 0x80) === 0) {
			break // No continuation bit, we're done
		}

		shift += 7
		if (shift > 28) {
			throw new Error('UVARINT is too long for 32-bit integer')
		}
	}

	return { value, bytesRead }
}

/**
 * Encode a signed 32-bit integer as VARINT (ZigZag + LEB128)
 */
export function encodeVarInt(value: number): Buffer {
	return encodeUVarInt(zigZagEncode32(value))
}

/**
 * Decode a VARINT from a buffer at the given offset
 * Returns the decoded value and number of bytes read
 */
export function decodeVarInt(buffer: Buffer, offset: number = 0): { value: number; bytesRead: number } {
	const result = decodeUVarInt(buffer, offset)
	return {
		value: zigZagDecode32(result.value),
		bytesRead: result.bytesRead,
	}
}

/**
 * Encode a signed 64-bit bigint as VARLONG (ZigZag + LEB128)
 */
export function encodeVarLong(value: bigint): Buffer {
	const encoded = zigZagEncode64(value)
	const bytes: number[] = []

	let remaining = encoded

	while (remaining > 0x7fn) {
		bytes.push(Number(remaining & 0x7fn) | 0x80)
		remaining >>= 7n
	}
	bytes.push(Number(remaining & 0x7fn))

	return Buffer.from(bytes)
}

/**
 * Decode a VARLONG from a buffer at the given offset
 * Returns the decoded value and number of bytes read
 */
export function decodeVarLong(buffer: Buffer, offset: number = 0): { value: bigint; bytesRead: number } {
	let value = 0n
	let shift = 0n
	let bytesRead = 0

	while (true) {
		if (offset + bytesRead >= buffer.length) {
			throw new Error('Buffer underflow while reading VARLONG')
		}

		const byte = buffer[offset + bytesRead]!
		bytesRead++

		value |= BigInt(byte & 0x7f) << shift

		if ((byte & 0x80) === 0) {
			break
		}

		shift += 7n
		if (shift > 63n) {
			throw new Error('VARLONG is too long for 64-bit integer')
		}
	}

	return {
		value: zigZagDecode64(value),
		bytesRead,
	}
}

/**
 * Calculate the encoded size of a UVARINT without actually encoding
 */
export function uvarIntSize(value: number): number {
	if (value < 0) {
		throw new Error('UVARINT cannot encode negative numbers')
	}

	let size = 1
	while (value > 0x7f) {
		size++
		value >>>= 7
	}
	return size
}

/**
 * Calculate the encoded size of a VARINT without actually encoding
 */
export function varIntSize(value: number): number {
	return uvarIntSize(zigZagEncode32(value))
}

/**
 * Calculate the encoded size of a VARLONG without actually encoding
 */
export function varLongSize(value: bigint): number {
	const encoded = zigZagEncode64(value)
	let size = 1
	let remaining = encoded
	while (remaining > 0x7fn) {
		size++
		remaining >>= 7n
	}
	return size
}
