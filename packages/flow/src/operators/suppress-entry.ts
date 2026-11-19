/**
 * What the suppress buffer keeps of a key (6.16; KS BufferValue with its record context) and how its store holds it
 * (6.25): `[int64BE bufferTime][int64BE timestamp][i32 length | -1 prior][prior][i32 length | -1 new][new][u16 header
 * count]([u16 key length][key][u32 value length][value])*`, values in the table's value codec, header keys in UTF-8.
 */
import type { Codec } from '@/codec.js'
import type { RecordHeaders } from '@/processor/record.js'

export interface BufferedChange<V> {
	/** When the key is due: set when it is first buffered, never moved by later updates (non-resetting). */
	readonly bufferTime: number
	/** Timestamp of the latest update; the emitted record carries it and the update's headers. */
	readonly timestamp: number
	readonly headers: RecordHeaders
	/** What downstream last saw of the key: the old value of its first buffered update (null: nothing). */
	readonly priorValue: V | null
	/** The latest update's value (null: the key was deleted). */
	readonly newValue: V | null
}

const I64 = 8
const I32 = 4
const U16 = 2
const U32 = 4
const NULL_LENGTH = -1
/** Where the prior value starts. */
const PRIOR = 2 * I64

export function bufferedChangeCodec<V>(values: Codec<V>): Codec<BufferedChange<V>> {
	return {
		encode(change) {
			const prior = encodeValue(values, change.priorValue)
			const value = encodeValue(values, change.newValue)
			const headers = Object.entries(change.headers).map(([key, header]) => [Buffer.from(key), header] as const)
			const size = headers.reduce(
				(total, [key, header]) => total + U16 + key.length + U32 + header.length,
				PRIOR + I32 + (prior?.length ?? 0) + I32 + (value?.length ?? 0) + U16
			)
			const bytes = Buffer.allocUnsafe(size)
			bytes.writeBigInt64BE(BigInt(change.bufferTime), 0)
			bytes.writeBigInt64BE(BigInt(change.timestamp), I64)
			let offset = writeValue(bytes, PRIOR, prior)
			offset = writeValue(bytes, offset, value)
			offset = bytes.writeUInt16BE(headers.length, offset)
			for (const [key, header] of headers) {
				offset = bytes.writeUInt16BE(key.length, offset)
				offset += key.copy(bytes, offset)
				offset = bytes.writeUInt32BE(header.length, offset)
				offset += header.copy(bytes, offset)
			}
			return bytes
		},
		decode(bytes) {
			const [priorValue, valueOffset] = readValue(values, bytes, PRIOR)
			const [newValue, headersOffset] = readValue(values, bytes, valueOffset)
			const headers: RecordHeaders = {}
			let offset = headersOffset + U16
			for (let count = bytes.readUInt16BE(headersOffset); count > 0; count--) {
				const keyEnd = offset + U16 + bytes.readUInt16BE(offset)
				const key = bytes.toString('utf8', offset + U16, keyEnd)
				offset = keyEnd + U32 + bytes.readUInt32BE(keyEnd)
				headers[key] = bytes.subarray(keyEnd + U32, offset)
			}
			return {
				bufferTime: Number(bytes.readBigInt64BE(0)),
				timestamp: Number(bytes.readBigInt64BE(I64)),
				headers,
				priorValue,
				newValue,
			}
		},
	}
}

/** The prior value of a stored entry, without decoding the rest (the value getter of a suppressed table). */
export function priorValueOf<V>(values: Codec<V>, bytes: Buffer): V | null {
	return readValue(values, bytes, PRIOR)[0]
}

function encodeValue<V>(values: Codec<V>, value: V | null): Buffer | null {
	return value === null ? null : values.encode(value)
}

/** Writes `[i32 length | -1][bytes]` at `offset`; returns the offset after it. */
function writeValue(bytes: Buffer, offset: number, value: Buffer | null): number {
	if (value === null) {
		return bytes.writeInt32BE(NULL_LENGTH, offset)
	}
	const start = bytes.writeInt32BE(value.length, offset)
	return start + value.copy(bytes, start)
}

/** Reads `[i32 length | -1][bytes]` at `offset`: the value and the offset after it. */
function readValue<V>(values: Codec<V>, bytes: Buffer, offset: number): [V | null, number] {
	const length = bytes.readInt32BE(offset)
	if (length === NULL_LENGTH) {
		return [null, offset + I32]
	}
	const end = offset + I32 + length
	return [values.decode(bytes.subarray(offset + I32, end)), end]
}
