/**
 * CRC32C (Castagnoli) checksum implementation for Kafka protocol
 *
 * Used in RecordBatch v2 format for message integrity verification.
 * Uses the Castagnoli polynomial (0x1EDC6F41) which is optimized for
 * hardware acceleration on modern CPUs.
 *
 * This module uses native hardware-accelerated CRC32C via @node-rs/crc32
 * when available, with a fallback to a pure JS "slice-by-8" implementation.
 */

import { createRequire } from 'node:module'

type NativeCrc32c = (data: Uint8Array) => number

let nativeCrc32c: NativeCrc32c | null = null
try {
	const require = createRequire(import.meta.url)
	const mod = require('@node-rs/crc32') as { crc32c?: unknown }
	if (typeof mod.crc32c === 'function') {
		nativeCrc32c = mod.crc32c as NativeCrc32c
	}
} catch {
	// Optional native implementation not installed/available.
}

// CRC32C polynomial (Castagnoli): 0x1EDC6F41
// Reversed/reflected polynomial for table generation: 0x82F63B78
const CRC32C_POLYNOMIAL = 0x82f63b78

/**
 * Pre-computed lookup tables for slice-by-8 CRC32C calculation
 * 8 tables of 256 entries each, allowing 8-byte-at-a-time processing
 */
const CRC32C_TABLES: Uint32Array[] = generateCrc32cTables()

function generateCrc32cTables(): Uint32Array[] {
	const tables: Uint32Array[] = []

	// Generate base table (table 0)
	const table0 = new Uint32Array(256)
	for (let i = 0; i < 256; i++) {
		let crc = i
		for (let j = 0; j < 8; j++) {
			if (crc & 1) {
				crc = (crc >>> 1) ^ CRC32C_POLYNOMIAL
			} else {
				crc = crc >>> 1
			}
		}
		table0[i] = crc
	}
	tables.push(table0)

	// Generate tables 1-7 by extending table 0
	for (let t = 1; t < 8; t++) {
		const table = new Uint32Array(256)
		for (let i = 0; i < 256; i++) {
			table[i] = (tables[t - 1]![i]! >>> 8) ^ table0[tables[t - 1]![i]! & 0xff]!
		}
		tables.push(table)
	}

	return tables
}

// Extract tables for faster access (avoid array indexing in hot loop)
const T0 = CRC32C_TABLES[0]!
const T1 = CRC32C_TABLES[1]!
const T2 = CRC32C_TABLES[2]!
const T3 = CRC32C_TABLES[3]!
const T4 = CRC32C_TABLES[4]!
const T5 = CRC32C_TABLES[5]!
const T6 = CRC32C_TABLES[6]!
const T7 = CRC32C_TABLES[7]!

/**
 * Calculate CRC32C checksum using pure JS slice-by-8 algorithm.
 */
function crc32cJS(data: Buffer, start: number, end: number): number {
	let crc = 0xffffffff
	let i = start

	// Process 8 bytes at a time using slice-by-8
	const endMain = start + ((end - start) & ~7) // Round down to multiple of 8
	while (i < endMain) {
		// XOR CRC with first 4 bytes
		const lo = (data[i]! | (data[i + 1]! << 8) | (data[i + 2]! << 16) | (data[i + 3]! << 24)) ^ crc
		const hi = data[i + 4]! | (data[i + 5]! << 8) | (data[i + 6]! << 16) | (data[i + 7]! << 24)

		crc =
			T7[lo & 0xff]! ^
			T6[(lo >>> 8) & 0xff]! ^
			T5[(lo >>> 16) & 0xff]! ^
			T4[(lo >>> 24) & 0xff]! ^
			T3[hi & 0xff]! ^
			T2[(hi >>> 8) & 0xff]! ^
			T1[(hi >>> 16) & 0xff]! ^
			T0[(hi >>> 24) & 0xff]!

		i += 8
	}

	// Process remaining bytes one at a time
	while (i < end) {
		crc = T0[(crc ^ data[i]!) & 0xff]! ^ (crc >>> 8)
		i++
	}

	// Final XOR and ensure unsigned
	return (crc ^ 0xffffffff) >>> 0
}

/**
 * Calculate CRC32C checksum of a buffer.
 *
 * Uses native hardware-accelerated CRC32C when available, falling back to the
 * pure JS slice-by-8 implementation otherwise.
 *
 * @param data - The buffer to calculate checksum for
 * @param start - Optional start offset (default: 0)
 * @param length - Optional length (default: data.length - start)
 * @returns The CRC32C checksum as an unsigned 32-bit integer
 */
export function crc32c(data: Buffer, start: number = 0, length?: number): number {
	const end = length !== undefined ? start + length : data.length

	if (nativeCrc32c !== null) {
		const view = start === 0 && end === data.length ? data : data.subarray(start, end)
		return nativeCrc32c(view) >>> 0
	}

	return crc32cJS(data, start, end)
}

/**
 * Verify a CRC32C checksum matches expected value
 *
 * @param data - The buffer to verify
 * @param expected - The expected CRC32C value
 * @param start - Optional start offset
 * @param length - Optional length
 * @returns true if checksum matches
 */
export function verifyCrc32c(data: Buffer, expected: number, start: number = 0, length?: number): boolean {
	return crc32c(data, start, length) === expected
}

/**
 * Calculate CRC32C checksum and return as a signed 32-bit integer
 * (matching Java's int representation used in Kafka protocol)
 *
 * @param data - The buffer to calculate checksum for
 * @param start - Optional start offset
 * @param length - Optional length
 * @returns The CRC32C checksum as a signed 32-bit integer
 */
export function crc32cSigned(data: Buffer, start: number = 0, length?: number): number {
	const unsigned = crc32c(data, start, length)
	// Convert to signed if necessary (values > 0x7FFFFFFF become negative)
	return unsigned > 0x7fffffff ? unsigned - 0x100000000 : unsigned
}
