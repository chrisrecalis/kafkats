/**
 * Compression codec support for Kafka RecordBatch
 *
 * Supports pluggable compression implementations:
 * - GZIP: Built-in using Node.js zlib
 * - Snappy, LZ4, Zstd: Pluggable via external libraries
 */

import { gzipSync, gunzipSync } from 'node:zlib'

/**
 * Compression type identifiers (stored in RecordBatch attributes)
 */
export enum CompressionType {
	None = 0,
	Gzip = 1,
	Snappy = 2,
	Lz4 = 3,
	Zstd = 4,
}

/**
 * Compression codec interface
 */
export interface CompressionCodec {
	/**
	 * Compress data
	 */
	compress(data: Buffer): Promise<Buffer>

	/**
	 * Decompress data
	 */
	decompress(data: Buffer): Promise<Buffer>
}

/**
 * Synchronous compression codec interface (for codecs that support sync operations)
 */
export interface SyncCompressionCodec extends CompressionCodec {
	compressSync(data: Buffer): Buffer
	decompressSync(data: Buffer): Buffer
}

/**
 * GZIP codec using Node.js built-in zlib
 */
export const gzipCodec: SyncCompressionCodec = {
	compress(data: Buffer): Promise<Buffer> {
		return Promise.resolve(gzipSync(data))
	},

	decompress(data: Buffer): Promise<Buffer> {
		return Promise.resolve(gunzipSync(data))
	},

	compressSync(data: Buffer): Buffer {
		return gzipSync(data)
	},

	decompressSync(data: Buffer): Buffer {
		return gunzipSync(data)
	},
}

/**
 * Compression codec registry
 */
export interface CompressionCodecRegistry {
	/**
	 * Get a codec for a compression type
	 *
	 * @param type - The compression type
	 * @returns The codec or undefined if not registered
	 */
	get(type: CompressionType): CompressionCodec | undefined

	/**
	 * Register a codec for a compression type
	 *
	 * @param type - The compression type
	 * @param codec - The codec implementation
	 */
	register(type: CompressionType, codec: CompressionCodec): void

	/**
	 * Check if a codec is registered
	 *
	 * @param type - The compression type
	 * @returns true if registered
	 */
	has(type: CompressionType): boolean
}

class CodecRegistry implements CompressionCodecRegistry {
	private codecs = new Map<CompressionType, CompressionCodec>()

	constructor() {
		// Register built-in GZIP codec
		this.codecs.set(CompressionType.Gzip, gzipCodec)
	}

	get(type: CompressionType): CompressionCodec | undefined {
		if (type === CompressionType.None) {
			return undefined
		}
		return this.codecs.get(type)
	}

	register(type: CompressionType, codec: CompressionCodec): void {
		if (type === CompressionType.None) {
			throw new Error('Cannot register codec for CompressionType.None')
		}
		this.codecs.set(type, codec)
	}

	has(type: CompressionType): boolean {
		if (type === CompressionType.None) {
			return true // None doesn't need a codec
		}
		return this.codecs.has(type)
	}
}

/**
 * Global compression codec registry
 */
export const compressionCodecs = new CodecRegistry()

/**
 * Factory function to create a Snappy codec from an external library
 *
 * @param snappy - The snappy library (e.g., 'snappy' npm package)
 * @returns A compression codec
 *
 * @example
 * ```typescript
 * import snappy from 'snappy'
 * compressionCodecs.register(CompressionType.Snappy, createSnappyCodec(snappy))
 * ```
 */
export function createSnappyCodec(snappy: {
	compress: (data: Buffer) => Promise<Buffer>
	uncompress: (data: Buffer) => Promise<Buffer>
}): CompressionCodec {
	return {
		compress: snappy.compress,
		decompress: snappy.uncompress,
	}
}

/**
 * Factory function to create an LZ4 codec from an external library
 *
 * @param lz4 - The lz4 library (e.g., 'lz4' npm package)
 * @returns A compression codec
 *
 * @example
 * ```typescript
 * import lz4 from 'lz4'
 * compressionCodecs.register(CompressionType.Lz4, createLz4Codec(lz4))
 * ```
 */
export function createLz4Codec(lz4: {
	encode: (data: Buffer) => Buffer
	decode: (data: Buffer) => Buffer
}): CompressionCodec {
	return {
		compress(data: Buffer): Promise<Buffer> {
			return Promise.resolve(lz4.encode(data))
		},
		decompress(data: Buffer): Promise<Buffer> {
			return Promise.resolve(lz4.decode(data))
		},
	}
}

/**
 * Factory function to create a Zstd codec from an external library
 *
 * @param zstd - The zstd library (e.g., '@mongodb-js/zstd' npm package)
 * @returns A compression codec
 *
 * @example
 * ```typescript
 * import { compress, decompress } from '@mongodb-js/zstd'
 * compressionCodecs.register(CompressionType.Zstd, createZstdCodec({ compress, decompress }))
 * ```
 */
export function createZstdCodec(zstd: {
	compress: (data: Buffer, level?: number) => Promise<Buffer>
	decompress: (data: Buffer) => Promise<Buffer>
}): CompressionCodec {
	return {
		compress: (data: Buffer) => zstd.compress(data),
		decompress: zstd.decompress,
	}
}

/**
 * Get the name of a compression type
 */
export function getCompressionTypeName(type: CompressionType): string {
	return CompressionType[type] ?? `Unknown(${type})`
}
