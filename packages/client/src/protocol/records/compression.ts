/**
 * Compression codec support for Kafka RecordBatch
 *
 * Supports pluggable compression implementations:
 * - GZIP: Built-in using Node.js zlib
 * - Snappy, LZ4, Zstd: Auto-registered when a supported library is installed,
 *   or pluggable via manual registration
 */

import { createRequire } from 'node:module'
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

/**
 * Loads a module by id, returning its exports. Injectable for testing.
 */
export type ModuleLoader = (id: string) => unknown

let nodeRequire: NodeJS.Require | undefined
const defaultModuleLoader: ModuleLoader = id => {
	nodeRequire ??= createRequire(import.meta.url)
	return nodeRequire(id)
}

/**
 * A known compression library that can be auto-registered when installed
 */
interface AutoCodecSource {
	module: string
	create: (mod: unknown) => CompressionCodec
}

/**
 * Return the export object (the module itself or its `default`) that exposes
 * all of the given functions. Throws if neither does, so auto-registration
 * moves on to the next candidate library.
 */
function pickExports<T>(mod: unknown, fns: string[]): T {
	for (const candidate of [mod, (mod as { default?: unknown } | undefined)?.default]) {
		if (candidate && fns.every(fn => typeof (candidate as Record<string, unknown>)[fn] === 'function')) {
			return candidate as T
		}
	}
	throw new Error(`module does not expose the expected functions: ${fns.join(', ')}`)
}

/**
 * Known libraries per compression type, in preference order (fastest first).
 *
 * `zstd-codec` (WASM) is intentionally absent: it requires asynchronous
 * initialization via ZstdCodec.run() and must be registered manually.
 */
const autoCodecSources: Partial<Record<CompressionType, readonly AutoCodecSource[]>> = {
	[CompressionType.Snappy]: [
		{ module: 'snappy', create: mod => createSnappyCodec(pickExports<SnappyLib>(mod, ['compress', 'uncompress'])) },
		{
			module: 'snappyjs',
			create: mod => createSnappyCodec(pickExports<SnappyLib>(mod, ['compress', 'uncompress'])),
		},
	],
	[CompressionType.Lz4]: [
		{ module: 'lz4-napi', create: mod => createLz4Codec(pickExports<Lz4Lib>(mod, ['compress', 'uncompress'])) },
		{ module: 'lz4', create: mod => createLz4Codec(pickExports<Lz4Lib>(mod, ['encode', 'decode'])) },
		{ module: 'lz4js', create: mod => createLz4Codec(pickExports<Lz4Lib>(mod, ['compress', 'decompress'])) },
	],
	[CompressionType.Zstd]: [
		{
			module: '@mongodb-js/zstd',
			create: mod => createZstdCodec(pickExports<ZstdLib>(mod, ['compress', 'decompress'])),
		},
		{ module: 'zstd-napi', create: mod => createZstdCodec(pickExports<ZstdLib>(mod, ['compress', 'decompress'])) },
	],
}

/**
 * Compression codec registry with automatic codec discovery.
 *
 * When a codec is looked up for a type that has no registered codec, the
 * registry tries to load a known compression library (see the compression
 * docs) and registers it automatically. Manual registration always takes
 * precedence and remains available for custom codecs.
 */
export class CodecRegistry implements CompressionCodecRegistry {
	/**
	 * Whether missing codecs may be auto-registered from installed libraries.
	 * Set to false to require explicit registration via register().
	 */
	autoRegister = true

	private codecs = new Map<CompressionType, CompressionCodec>()
	private autoLoadAttempted = new Set<CompressionType>()

	constructor(private moduleLoader: ModuleLoader = defaultModuleLoader) {
		// Register built-in GZIP codec
		this.codecs.set(CompressionType.Gzip, gzipCodec)
	}

	get(type: CompressionType): CompressionCodec | undefined {
		if (type === CompressionType.None) {
			return undefined
		}
		return this.codecs.get(type) ?? this.tryAutoRegister(type)
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
		return this.codecs.has(type) || this.tryAutoRegister(type) !== undefined
	}

	/**
	 * Try each known library for the type once. Missing libraries are skipped
	 * silently; an installed library that fails to produce a codec emits a
	 * process warning (so e.g. an incompatible lz4-napi < 2.x doesn't fail
	 * completely silently) and the next candidate is tried.
	 */
	private tryAutoRegister(type: CompressionType): CompressionCodec | undefined {
		if (!this.autoRegister || this.autoLoadAttempted.has(type)) {
			return undefined
		}
		this.autoLoadAttempted.add(type)

		for (const source of autoCodecSources[type] ?? []) {
			let mod: unknown
			try {
				mod = this.moduleLoader(source.module)
			} catch {
				continue // Not installed
			}
			try {
				const codec = source.create(mod)
				this.codecs.set(type, codec)
				return codec
			} catch (error) {
				process.emitWarning(
					`kafkats: found '${source.module}' but could not use it for ${getCompressionTypeName(type)} compression: ` +
						`${error instanceof Error ? error.message : String(error)}`
				)
			}
		}
		return undefined
	}
}

/**
 * Global compression codec registry
 */
export const compressionCodecs = new CodecRegistry()

/**
 * Snappy library interface for native async libraries (e.g., 'snappy' npm package)
 */
export interface SnappyNativeLib {
	compress: (data: Buffer | Uint8Array | string) => Promise<Buffer>
	uncompress: (data: Buffer) => Promise<Buffer>
}

/**
 * Snappy library interface for pure JS libraries (e.g., 'snappyjs' npm package)
 */
export interface SnappyJsLib {
	compress: (data: ArrayBuffer | Buffer | Uint8Array) => ArrayBuffer | Uint8Array
	uncompress: (data: ArrayBuffer | Buffer | Uint8Array) => ArrayBuffer | Uint8Array
}

/**
 * Union type for all supported Snappy library interfaces
 */
export type SnappyLib = SnappyNativeLib | SnappyJsLib

/**
 * Factory function to create a Snappy codec from an external library
 *
 * Supports the following libraries:
 * - **Native**: `snappy` - Fastest Snappy compression library using napi-rs
 * - **Pure JS**: `snappyjs` - Pure JavaScript implementation
 *
 * @param snappy - The snappy library instance
 * @returns A compression codec
 *
 * @example Native snappy (async)
 * ```typescript
 * import snappy from 'snappy'
 * compressionCodecs.register(CompressionType.Snappy, createSnappyCodec(snappy))
 * ```
 *
 * @example Pure JS snappyjs (sync)
 * ```typescript
 * import * as SnappyJS from 'snappyjs'
 * compressionCodecs.register(CompressionType.Snappy, createSnappyCodec(SnappyJS))
 * ```
 */
export function createSnappyCodec(snappy: SnappyLib): CompressionCodec {
	// Detect if this is an async library by checking if the result is a Promise
	// We do this by calling compress with a tiny buffer and checking the result type
	const testResult = snappy.compress(Buffer.alloc(1))
	const isAsync = testResult instanceof Promise

	if (isAsync) {
		// The probe's result is discarded; swallow any rejection so it can't surface as an
		// unhandled promise rejection (which crashes the process under the default handler).
		testResult.catch(() => {})

		// Native async library (snappy)
		const asyncLib = snappy as SnappyNativeLib
		return {
			compress: asyncLib.compress,
			decompress: asyncLib.uncompress,
		}
	} else {
		// Pure JS sync library (snappyjs)
		const syncLib = snappy as SnappyJsLib
		return {
			compress(data: Buffer): Promise<Buffer> {
				const result = syncLib.compress(data)
				return Promise.resolve(Buffer.from(result as ArrayBuffer))
			},
			decompress(data: Buffer): Promise<Buffer> {
				const result = syncLib.uncompress(data)
				return Promise.resolve(Buffer.from(result as ArrayBuffer))
			},
		}
	}
}

/**
 * LZ4 library interface for node-lz4 (encode/decode API)
 */
export interface Lz4NodeLib {
	encode: (data: Buffer) => Buffer
	decode: (data: Buffer) => Buffer
}

/**
 * LZ4 library interface for lz4js (pure JS, compress/decompress API)
 */
export interface Lz4JsLib {
	compress: (data: ArrayLike<number> | Uint8Array) => Uint8Array
	decompress: (data: ArrayLike<number> | Uint8Array) => Uint8Array
}

/**
 * LZ4 library interface for lz4-napi (native async, compress/uncompress API)
 *
 * Note: lz4-napi exposes both raw-block (compress/uncompress) and framed
 * (compressFrame/decompressFrame) APIs.  Kafka's RecordBatch v2 requires the
 * LZ4 *framing* format (magic bytes 0x184D2204), so createLz4Codec detects
 * the presence of compressFrame and prefers the framed API when available.
 */
export interface Lz4NapiLib {
	compress: (data: Buffer | Uint8Array | string) => Promise<Buffer>
	uncompress: (data: Buffer | Uint8Array | string) => Promise<Buffer>
	/** Present in lz4-napi ≥ 2.x — produces the LZ4 frame format required by Kafka */
	compressFrame?: (data: Buffer | Uint8Array | string) => Promise<Buffer>
	/** Present in lz4-napi ≥ 2.x — decompresses LZ4 frame format */
	decompressFrame?: (data: Buffer | Uint8Array | string) => Promise<Buffer>
}

/**
 * Union type for all supported LZ4 library interfaces
 */
export type Lz4Lib = Lz4NodeLib | Lz4JsLib | Lz4NapiLib

/**
 * Factory function to create an LZ4 codec from an external library
 *
 * Supports the following libraries:
 * - **Native (node-lz4)**: `lz4` - encode/decode API
 * - **Pure JS**: `lz4js` - Pure JavaScript implementation
 * - **Native (napi)**: `lz4-napi` - Fastest LZ4 library using napi-rs
 *
 * @param lz4 - The LZ4 library instance
 * @returns A compression codec
 *
 * @example node-lz4 (encode/decode API)
 * ```typescript
 * import lz4 from 'lz4'
 * compressionCodecs.register(CompressionType.Lz4, createLz4Codec(lz4))
 * ```
 *
 * @example lz4js (pure JS)
 * ```typescript
 * import * as lz4js from 'lz4js'
 * compressionCodecs.register(CompressionType.Lz4, createLz4Codec(lz4js))
 * ```
 *
 * @example lz4-napi (native async)
 * ```typescript
 * import * as lz4 from 'lz4-napi'
 * compressionCodecs.register(CompressionType.Lz4, createLz4Codec(lz4))
 * ```
 */
export function createLz4Codec(lz4: Lz4Lib): CompressionCodec {
	// Detect library type by checking available methods
	if ('encode' in lz4 && 'decode' in lz4) {
		// node-lz4 with encode/decode API
		return {
			compress(data: Buffer): Promise<Buffer> {
				return Promise.resolve(lz4.encode(data))
			},
			decompress(data: Buffer): Promise<Buffer> {
				return Promise.resolve(lz4.decode(data))
			},
		}
	}

	if ('uncompress' in lz4) {
		// lz4-napi exposes both raw-block (compress/uncompress) and framed
		// (compressFrame/decompressFrame) APIs.  Kafka RecordBatch v2 requires the
		// LZ4 *framing* format (magic bytes 0x184D2204), so prefer the framed API.
		const { compressFrame, decompressFrame } = lz4
		if (compressFrame && decompressFrame) {
			return {
				compress: (data: Buffer) => compressFrame(data),
				decompress: (data: Buffer) => decompressFrame(data),
			}
		}
		// Raw-block lz4-napi (< 2.x): compressFrame/decompressFrame are absent.
		// Raw-block LZ4 lacks the magic-bytes framing (0x184D2204) that Kafka's
		// RecordBatch v2 requires — every produce will be rejected by the broker
		// with UnknownServerError.  Silently falling back would turn a dependency
		// mismatch into completely opaque broker failures, so we fail fast here.
		throw new Error(
			'lz4-napi >= 2.x exposing compressFrame/decompressFrame is required for Kafka LZ4 ' +
				'(raw-block LZ4 is rejected by the broker). ' +
				'Install lz4-napi@2.x or later.'
		)
	}

	// lz4js with sync compress/decompress API
	return {
		compress(data: Buffer): Promise<Buffer> {
			const result = lz4.compress(data)
			return Promise.resolve(Buffer.from(result))
		},
		decompress(data: Buffer): Promise<Buffer> {
			const result = lz4.decompress(data)
			return Promise.resolve(Buffer.from(result))
		},
	}
}

/**
 * Zstd library interface for async native libraries (e.g., '@mongodb-js/zstd', 'zstd-napi')
 */
export interface ZstdNativeAsyncLib {
	compress: (data: Buffer, level?: number) => Promise<Buffer>
	decompress: (data: Buffer) => Promise<Buffer>
}

/**
 * Zstd library interface for zstd-codec (Simple API after initialization)
 */
export interface ZstdCodecSimpleLib {
	compress: (data: Uint8Array, level?: number) => Uint8Array
	decompress: (data: Uint8Array, maxSize?: number) => Uint8Array
}

/**
 * Union type for all supported Zstd library interfaces
 */
export type ZstdLib = ZstdNativeAsyncLib | ZstdCodecSimpleLib

/**
 * Options for creating a Zstd codec
 */
export interface ZstdCodecOptions {
	/**
	 * Compression level (1-22, default: 3)
	 * Lower = faster, higher = better compression
	 */
	level?: number
}

/**
 * Factory function to create a Zstd codec from an external library
 *
 * Supports the following libraries:
 * - **Native**: `@mongodb-js/zstd` - MongoDB's native Zstd binding
 * - **Native**: `zstd-napi` - Native Zstd using Node-API
 * - **WASM**: `zstd-codec` - Zstd codec powered by Emscripten
 *
 * @param zstd - The Zstd library instance
 * @param options - Optional configuration
 * @returns A compression codec
 *
 * @example @mongodb-js/zstd (native async)
 * ```typescript
 * import { compress, decompress } from '@mongodb-js/zstd'
 * compressionCodecs.register(CompressionType.Zstd, createZstdCodec({ compress, decompress }))
 * ```
 *
 * @example zstd-napi (native async)
 * ```typescript
 * import { compress, decompress } from 'zstd-napi'
 * compressionCodecs.register(CompressionType.Zstd, createZstdCodec({ compress, decompress }))
 * ```
 *
 * @example zstd-codec (WASM)
 * ```typescript
 * import { ZstdCodec } from 'zstd-codec'
 * ZstdCodec.run((zstd) => {
 *   const simple = new zstd.Simple()
 *   compressionCodecs.register(CompressionType.Zstd, createZstdCodec(simple))
 * })
 * ```
 */
export function createZstdCodec(zstd: ZstdLib, options?: ZstdCodecOptions): CompressionCodec {
	const level = options?.level ?? 3

	// Detect if this is an async library by checking if the result is a Promise
	const testResult = zstd.compress(Buffer.alloc(1), level)
	const isAsync = testResult instanceof Promise

	if (isAsync) {
		// The probe's result is discarded; swallow any rejection so it can't surface as an
		// unhandled promise rejection (which crashes the process under the default handler).
		testResult.catch(() => {})

		// Native async library (@mongodb-js/zstd, zstd-napi)
		const asyncLib = zstd as ZstdNativeAsyncLib
		return {
			compress: (data: Buffer) => asyncLib.compress(data, level),
			decompress: asyncLib.decompress,
		}
	} else {
		// WASM/sync library (zstd-codec)
		const syncLib = zstd as ZstdCodecSimpleLib
		return {
			compress(data: Buffer): Promise<Buffer> {
				const result = syncLib.compress(data, level)
				return Promise.resolve(Buffer.from(result))
			},
			decompress(data: Buffer): Promise<Buffer> {
				const result = syncLib.decompress(data)
				return Promise.resolve(Buffer.from(result))
			},
		}
	}
}

/**
 * Get the name of a compression type
 */
export function getCompressionTypeName(type: CompressionType): string {
	return CompressionType[type] ?? `Unknown(${type})`
}

/**
 * Build the error thrown when no codec is available for a compression type,
 * including an install hint for types that support auto-registration.
 */
export function missingCodecError(type: CompressionType): Error {
	const sources = autoCodecSources[type]
	const hint = sources?.length
		? ` Install one of: ${sources.map(s => s.module).join(', ')} (used automatically when installed), ` +
			`or register a codec via compressionCodecs.register().`
		: ''
	return new Error(`Compression codec not registered: ${getCompressionTypeName(type)}.${hint}`)
}
