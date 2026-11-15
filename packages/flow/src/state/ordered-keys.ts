import type { Codec } from '@/codec.js'
import type { WindowedKey } from '@/state.js'

/**
 * How window and session stores lay out their keys in a byte-ordered engine, and the key ranges
 * their queries scan. Times are 8 bytes each, biased so that byte order is numeric order.
 */

/** Bounds of a scan: `start <= key < end`. */
export type KeyRange = { start: Buffer; end: Buffer }

// Bias signed times by 2^63 so unsigned byte order matches signed numeric order.
const SIGNED_BIAS = 0x8000000000000000n
const TIME_BYTES = 8
/** A window's start and end. */
const TIMES_BYTES = 2 * TIME_BYTES

export function writeSignedTime(buf: Buffer, value: number, offset: number): void {
	buf.writeBigUInt64BE(BigInt(value) + SIGNED_BIAS, offset)
}

export function readSignedTime(buf: Buffer, offset: number): number {
	return Number(buf.readBigUInt64BE(offset) - SIGNED_BIAS)
}

/**
 * Window store keys: `[windowStart][windowEnd][key]`. Time comes first, so the windows of a time
 * range are adjacent and expiry scans only the oldest keys.
 */
export function windowKeys<K>(keyCodec: Codec<K>) {
	/** The first possible key of windows starting at `time`. */
	const startingAt = (time: number): Buffer => {
		const bound = Buffer.alloc(TIMES_BYTES)
		writeSignedTime(bound, time, 0)
		return bound
	}
	const codec: Codec<WindowedKey<K>> = {
		encode({ key, windowStart, windowEnd }) {
			const keyBytes = keyCodec.encode(key)
			const buf = Buffer.alloc(TIMES_BYTES + keyBytes.length)
			writeSignedTime(buf, windowStart, 0)
			writeSignedTime(buf, windowEnd, TIME_BYTES)
			keyBytes.copy(buf, TIMES_BYTES)
			return buf
		},
		decode: buf => ({
			key: keyCodec.decode(buf.subarray(TIMES_BYTES)),
			windowStart: readSignedTime(buf, 0),
			windowEnd: readSignedTime(buf, TIME_BYTES),
		}),
	}
	return {
		codec,
		/** Windows with `from <= windowStart <= to`. */
		startingIn: (from: number, to: number): KeyRange => ({ start: startingAt(from), end: startingAt(to + 1) }),
		/** The user key part of a stored key, without decoding it. */
		userKey: (stored: Buffer): Buffer => stored.subarray(TIMES_BYTES),
	}
}

/**
 * Session store keys: `[key][sessionStart][sessionEnd]`. The key comes first, so the sessions of
 * one key are adjacent.
 */
export function sessionKeys<K>(keyCodec: Codec<K>) {
	const codec: Codec<WindowedKey<K>> = {
		encode({ key, windowStart, windowEnd }) {
			const keyBytes = keyCodec.encode(key)
			const buf = Buffer.alloc(keyBytes.length + TIMES_BYTES)
			keyBytes.copy(buf, 0)
			writeSignedTime(buf, windowStart, keyBytes.length)
			writeSignedTime(buf, windowEnd, keyBytes.length + TIME_BYTES)
			return buf
		},
		decode: buf => ({
			key: keyCodec.decode(buf.subarray(0, buf.length - TIMES_BYTES)),
			windowStart: readSignedTime(buf, buf.length - TIMES_BYTES),
			windowEnd: readSignedTime(buf, buf.length - TIME_BYTES),
		}),
	}
	return {
		codec,
		/**
		 * Every session of the key `keyBytes`, from the smallest to the largest pair of times. Keys
		 * vary in length, so a longer key sharing the prefix (`ka` for `k`) can sort inside the
		 * range: filter with {@link isOf}.
		 */
		of: (keyBytes: Buffer): KeyRange => ({
			start: Buffer.concat([keyBytes, Buffer.alloc(TIMES_BYTES, 0x00)]),
			end: Buffer.concat([keyBytes, Buffer.alloc(TIMES_BYTES, 0xff)]),
		}),
		/** Whether a stored key is a session of exactly the key `keyBytes`. */
		isOf: (stored: Buffer, keyBytes: Buffer): boolean =>
			stored.length === keyBytes.length + TIMES_BYTES && stored.subarray(0, keyBytes.length).equals(keyBytes),
	}
}
