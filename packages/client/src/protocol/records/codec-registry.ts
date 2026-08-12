/**
 * Compression codec registry with automatic codec discovery. Internal module:
 * only the `compressionCodecs` singleton is re-exported from the barrel.
 */

import { createRequire } from 'node:module'

import {
	type CompressionCodec,
	type CompressionCodecRegistry,
	CompressionType,
	createLz4Codec,
	createSnappyCodec,
	createZstdCodec,
	getCompressionTypeName,
	gzipCodec,
	type Lz4Lib,
	type SnappyLib,
	type ZstdLib,
} from '@/protocol/records/compression.js'

export type ModuleLoader = (id: string) => unknown

let nodeRequire: NodeJS.Require | undefined
const defaultModuleLoader: ModuleLoader = id => {
	nodeRequire ??= createRequire(import.meta.url)
	return nodeRequire(id)
}

interface AutoCodecSource {
	module: string
	create: (mod: unknown) => CompressionCodec
}

/** Pick the export object (module or its `default`) exposing all given functions. */
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

export class CodecRegistry implements CompressionCodecRegistry {
	/**
	 * Whether missing codecs may be auto-registered from installed libraries.
	 * Set to false to require explicit registration via register().
	 */
	autoRegister = true

	private codecs = new Map<CompressionType, CompressionCodec>()
	private autoLoadAttempted = new Set<CompressionType>()

	constructor(private moduleLoader: ModuleLoader = defaultModuleLoader) {
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

	private tryAutoRegister(type: CompressionType): CompressionCodec | undefined {
		if (!this.autoRegister || this.autoLoadAttempted.has(type)) {
			return undefined
		}
		this.autoLoadAttempted.add(type)

		for (const source of autoCodecSources[type] ?? []) {
			let mod: unknown
			try {
				mod = this.moduleLoader(source.module)
			} catch (error) {
				// Installed but broken (native binding, Node version mismatch, ...) must
				// not be silently conflated with "not installed"
				if (!isModuleNotFound(error, source.module)) {
					warnUnusable(source.module, type, error)
				}
				continue
			}
			try {
				const codec = source.create(mod)
				this.codecs.set(type, codec)
				return codec
			} catch (error) {
				// Installed but unusable (e.g. lz4-napi < 2.x): warn and try the next candidate
				warnUnusable(source.module, type, error)
			}
		}
		return undefined
	}
}

function isModuleNotFound(error: unknown, id: string): boolean {
	const code = (error as NodeJS.ErrnoException | null)?.code
	return (
		(code === 'MODULE_NOT_FOUND' || code === 'ERR_MODULE_NOT_FOUND') &&
		error instanceof Error &&
		error.message.includes(`'${id}'`)
	)
}

function warnUnusable(module: string, type: CompressionType, error: unknown): void {
	process.emitWarning(
		`kafkats: found '${module}' but could not use it for ${getCompressionTypeName(type)} compression: ` +
			`${error instanceof Error ? error.message : String(error)}`
	)
}

/**
 * Global compression codec registry
 */
export const compressionCodecs: CompressionCodecRegistry = new CodecRegistry()

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
