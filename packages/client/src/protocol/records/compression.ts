/**
 * Compression codec support for Kafka RecordBatch
 *
 * Supports pluggable compression implementations:
 * - GZIP: Built-in using Node.js zlib
 * - Snappy, LZ4, Zstd: Pluggable via external libraries
 */

import { gzipSync, gunzipSync } from 'node:zlib'

type NodeCallbackCompressionFn = (data: Buffer, callback: (error: unknown, output: Buffer) => void) => unknown
type CompressionImpl = ((data: Buffer) => Buffer | Promise<Buffer> | undefined) | NodeCallbackCompressionFn

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

function normalizeCompressionFn(fn: CompressionImpl, context: unknown): (data: Buffer) => Promise<Buffer> {
	return async (data: Buffer) => {
		try {
			const result = (fn as (data: Buffer) => Buffer | Promise<Buffer> | undefined).call(context, data)
			if (result !== undefined) {
				return await Promise.resolve(result)
			}
		} catch (error) {
			if (fn.length < 2) {
				throw error
			}
			return new Promise<Buffer>((resolve, reject) => {
				try {
					;(fn as NodeCallbackCompressionFn).call(context, data, (callbackError, output) => {
						if (callbackError) {
							reject(callbackError)
							return
						}
						resolve(output)
					})
				} catch (callbackError) {
					reject(callbackError)
				}
			})
		}

		if (fn.length >= 2) {
			return new Promise<Buffer>((resolve, reject) => {
				try {
					;(fn as NodeCallbackCompressionFn).call(context, data, (error, output) => {
						if (error) {
							reject(error)
							return
						}
						resolve(output)
					})
				} catch (error) {
					reject(error)
				}
			})
		}

		throw new Error('Compression function returned undefined')
	}
}

function pickCompressionFn(
	implementation: unknown,
	candidates: string[],
	codecName: string,
	role: 'compress' | 'decompress'
): (data: Buffer) => Promise<Buffer> {
	for (const target of [implementation, (implementation as { default?: unknown })?.default]) {
		if (!target || typeof target !== 'object' && typeof target !== 'function') {
			continue
		}

		for (const name of candidates) {
			const candidate = (target as Record<string, unknown>)[name]
			if (typeof candidate === 'function') {
				return normalizeCompressionFn(candidate as CompressionImpl, target)
			}
		}
	}

	throw new Error(`${codecName} implementation is missing a ${role} function`)
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
 *
 * Supports callback-based implementations (e.g., 'snappy') and N-API builds such as '@kafkajs/snappy'.
 */
export function createSnappyCodec(
	snappy: {
		compress?: CompressionImpl
		compressAsync?: CompressionImpl
		compressSync?: CompressionImpl
		decompress?: CompressionImpl
		decompressSync?: CompressionImpl
		uncompress?: CompressionImpl
		uncompressSync?: CompressionImpl
	} = {}
): CompressionCodec {
	const compress = pickCompressionFn(
		snappy,
		['compress', 'compressAsync', 'compressSync'],
		'Snappy',
		'compress'
	)
	const decompress = pickCompressionFn(
		snappy,
		['uncompress', 'decompress', 'uncompressAsync', 'decompressAsync', 'decompressSync', 'uncompressSync'],
		'Snappy',
		'decompress'
	)

	return {
		compress,
		decompress,
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
 *
 * Works with sync JavaScript implementations as well as native/N-API builds like 'lz4-napi'.
 */
export function createLz4Codec(
	lz4: {
		compress?: CompressionImpl
		compressSync?: CompressionImpl
		decompress?: CompressionImpl
		decompressSync?: CompressionImpl
		decompressBlock?: CompressionImpl
		uncompress?: CompressionImpl
		uncompressSync?: CompressionImpl
		encode?: CompressionImpl
		decode?: CompressionImpl
	} = {}
): CompressionCodec {
	const compress = pickCompressionFn(
		lz4,
		['encode', 'compress', 'compressSync'],
		'LZ4',
		'compress'
	)
	const decompress = pickCompressionFn(
		lz4,
		['decode', 'decompress', 'uncompress', 'decompressSync', 'uncompressSync', 'decompressBlock'],
		'LZ4',
		'decompress'
	)

	return {
		compress,
		decompress,
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
 *
 * Compatible with WASM implementations (e.g., '@mongodb-js/zstd') and N-API libraries such as '@kafkajs/zstd'.
 */
export function createZstdCodec(
	zstd: {
		compress?: CompressionImpl
		compressSync?: CompressionImpl
		compressAsync?: CompressionImpl
		decompress?: CompressionImpl
		decompressSync?: CompressionImpl
		decompressAsync?: CompressionImpl
	} = {}
): CompressionCodec {
	const compress = pickCompressionFn(
		zstd,
		['compress', 'compressAsync', 'compressSync'],
		'Zstd',
		'compress'
	)
	const decompress = pickCompressionFn(
		zstd,
		['decompress', 'decompressAsync', 'decompressSync'],
		'Zstd',
		'decompress'
	)

	return {
		compress,
		decompress,
	}
}

/**
 * Get the name of a compression type
 */
export function getCompressionTypeName(type: CompressionType): string {
	return CompressionType[type] ?? `Unknown(${type})`
}
