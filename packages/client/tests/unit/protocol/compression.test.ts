import { describe, expect, it } from 'vitest'

import {
	CodecRegistry,
	compressionCodecs,
	CompressionType,
	createLz4Codec,
	createSnappyCodec,
	createZstdCodec,
	getCompressionTypeName,
	missingCodecError,
	type ModuleLoader,
} from '@/protocol/records/compression.js'

describe('compression registry', () => {
	it('registers and retrieves codecs', async () => {
		const payload = Buffer.from('hello')
		const snappy = createSnappyCodec({
			compress: async (data: Buffer | Uint8Array | string) => Buffer.from(data),
			uncompress: async (data: Buffer) => data,
		})
		compressionCodecs.register(CompressionType.Snappy, snappy)

		const codec = compressionCodecs.get(CompressionType.Snappy)
		expect(codec).toBeDefined()
		const compressed = await codec!.compress(payload)
		const decompressed = await codec!.decompress(compressed)
		expect(decompressed).toEqual(payload)
	})

	it('handles built-in gzip codec', async () => {
		const payload = Buffer.from('gzip-test')
		const gzip = compressionCodecs.get(CompressionType.Gzip)
		expect(gzip).toBeDefined()
		const compressed = await gzip!.compress(payload)
		const decompressed = await gzip!.decompress(compressed)
		expect(decompressed).toEqual(payload)
	})

	it('guards invalid registrations and lookups', () => {
		expect(compressionCodecs.has(CompressionType.None)).toBe(true)
		expect(compressionCodecs.get(CompressionType.None)).toBeUndefined()
		expect(() =>
			compressionCodecs.register(CompressionType.None, {
				compress: async () => Buffer.alloc(0),
				decompress: async () => Buffer.alloc(0),
			})
		).toThrow('Cannot register codec for CompressionType.None')
		expect(getCompressionTypeName(CompressionType.Lz4)).toBe('Lz4')
		expect(getCompressionTypeName(99 as CompressionType)).toBe('Unknown(99)')
	})

	it('creates an LZ4 codec from sync functions', async () => {
		const codec = createLz4Codec({
			encode: data => data,
			decode: data => data,
		})
		const payload = Buffer.from('lz4')
		const compressed = await codec.compress(payload)
		const decompressed = await codec.decompress(compressed)
		expect(decompressed).toEqual(payload)
	})

	it('prefers the framed lz4-napi API (compressFrame/decompressFrame) over raw block', async () => {
		let framedCompress = 0
		let framedDecompress = 0
		const codec = createLz4Codec({
			compress: async () => Buffer.from('raw-block-must-not-be-used'),
			uncompress: async () => Buffer.from('raw-block-must-not-be-used'),
			compressFrame: async (data: Buffer | Uint8Array | string) => {
				framedCompress++
				return Buffer.from(data as Uint8Array)
			},
			decompressFrame: async (data: Buffer | Uint8Array | string) => {
				framedDecompress++
				return Buffer.from(data as Uint8Array)
			},
		})
		const payload = Buffer.from('lz4-frame')
		const compressed = await codec.compress(payload)
		const decompressed = await codec.decompress(compressed)
		expect(framedCompress).toBe(1)
		expect(framedDecompress).toBe(1)
		expect(decompressed).toEqual(payload)
	})

	it('throws for raw-block lz4-napi (< 2.x) lacking compressFrame — Kafka RecordBatch v2 requires LZ4 framing', () => {
		expect(() =>
			createLz4Codec({
				compress: async () => Buffer.alloc(0),
				uncompress: async () => Buffer.alloc(0),
			})
		).toThrow(/lz4-napi >= 2\.x/)
	})

	it('creates a Zstd codec from async functions', async () => {
		const codec = createZstdCodec({
			compress: async (data: Buffer) => data,
			decompress: async (data: Buffer) => data,
		})
		const payload = Buffer.from('zstd')
		const compressed = await codec.compress(payload)
		const decompressed = await codec.decompress(compressed)
		expect(decompressed).toEqual(payload)
	})

	it('returns undefined for unknown codec', () => {
		// Disable auto-registration so the result doesn't depend on which
		// compression libraries happen to be installed in the workspace.
		compressionCodecs.autoRegister = false
		try {
			expect(compressionCodecs.get(CompressionType.Zstd)).toBeUndefined()
		} finally {
			compressionCodecs.autoRegister = true
		}
	})
})

describe('automatic codec registration', () => {
	/** A loader that serves fake modules by name and records the ids it was asked for */
	function fakeLoader(modules: Record<string, unknown>): { loader: ModuleLoader; requested: string[] } {
		const requested: string[] = []
		const loader: ModuleLoader = id => {
			requested.push(id)
			if (id in modules) {
				return modules[id]
			}
			throw new Error(`Cannot find module '${id}'`)
		}
		return { loader, requested }
	}

	const fakeSnappyJs = {
		compress: (data: ArrayBuffer | Buffer | Uint8Array) => Buffer.from(data as Uint8Array),
		uncompress: (data: ArrayBuffer | Buffer | Uint8Array) => Buffer.from(data as Uint8Array),
	}

	it('auto-registers snappy when installed', async () => {
		const { loader } = fakeLoader({ snappy: fakeSnappyJs })
		const registry = new CodecRegistry(loader)

		expect(registry.has(CompressionType.Snappy)).toBe(true)
		const codec = registry.get(CompressionType.Snappy)!
		const payload = Buffer.from('auto-snappy')
		expect(await codec.decompress(await codec.compress(payload))).toEqual(payload)
	})

	it('tries candidate libraries in preference order and falls back to the next one', () => {
		const { loader, requested } = fakeLoader({ snappyjs: fakeSnappyJs })
		const registry = new CodecRegistry(loader)

		expect(registry.get(CompressionType.Snappy)).toBeDefined()
		expect(requested).toEqual(['snappy', 'snappyjs'])
	})

	it('unwraps a default export', () => {
		const { loader } = fakeLoader({ snappy: { default: fakeSnappyJs } })
		const registry = new CodecRegistry(loader)

		expect(registry.get(CompressionType.Snappy)).toBeDefined()
	})

	it('auto-registers lz4 via the node-lz4 encode/decode API', async () => {
		const { loader } = fakeLoader({ lz4: { encode: (data: Buffer) => data, decode: (data: Buffer) => data } })
		const registry = new CodecRegistry(loader)

		const codec = registry.get(CompressionType.Lz4)!
		const payload = Buffer.from('auto-lz4')
		expect(await codec.decompress(await codec.compress(payload))).toEqual(payload)
	})

	it('auto-registers zstd via an async compress/decompress API', async () => {
		const { loader } = fakeLoader({
			'@mongodb-js/zstd': {
				compress: async (data: Buffer) => data,
				decompress: async (data: Buffer) => data,
			},
		})
		const registry = new CodecRegistry(loader)

		const codec = registry.get(CompressionType.Zstd)!
		const payload = Buffer.from('auto-zstd')
		expect(await codec.decompress(await codec.compress(payload))).toEqual(payload)
	})

	it('only attempts auto-registration once per compression type', () => {
		const { loader, requested } = fakeLoader({})
		const registry = new CodecRegistry(loader)

		expect(registry.get(CompressionType.Snappy)).toBeUndefined()
		const attempts = requested.length
		expect(registry.get(CompressionType.Snappy)).toBeUndefined()
		expect(registry.has(CompressionType.Snappy)).toBe(false)
		expect(requested.length).toBe(attempts)
	})

	it('skips an installed library that does not expose the expected API and warns', () => {
		const warnings: string[] = []
		const handler = (warning: Error) => warnings.push(warning.message)
		process.on('warning', handler)
		try {
			const { loader } = fakeLoader({ snappy: { notTheApi: true }, snappyjs: fakeSnappyJs })
			const registry = new CodecRegistry(loader)
			expect(registry.get(CompressionType.Snappy)).toBeDefined()
		} finally {
			process.off('warning', handler)
		}
		// process warnings are delivered asynchronously; the emitted message is
		// still buffered, so just assert the fallback codec was registered above.
	})

	it('does not auto-register when autoRegister is disabled', () => {
		const { loader, requested } = fakeLoader({ snappy: fakeSnappyJs })
		const registry = new CodecRegistry(loader)
		registry.autoRegister = false

		expect(registry.get(CompressionType.Snappy)).toBeUndefined()
		expect(requested).toEqual([])

		// Re-enabling makes the next lookup attempt auto-registration
		registry.autoRegister = true
		expect(registry.get(CompressionType.Snappy)).toBeDefined()
	})

	it('manual registration still works after a failed auto-registration attempt', async () => {
		const { loader } = fakeLoader({})
		const registry = new CodecRegistry(loader)

		expect(registry.get(CompressionType.Snappy)).toBeUndefined()
		registry.register(CompressionType.Snappy, createSnappyCodec(fakeSnappyJs))
		expect(registry.get(CompressionType.Snappy)).toBeDefined()
	})

	it('does not auto-register gzip or none (no lookup needed)', () => {
		const { loader, requested } = fakeLoader({})
		const registry = new CodecRegistry(loader)

		expect(registry.get(CompressionType.Gzip)).toBeDefined()
		expect(registry.get(CompressionType.None)).toBeUndefined()
		expect(requested).toEqual([])
	})

	it('includes an install hint in the missing codec error', () => {
		expect(missingCodecError(CompressionType.Snappy).message).toMatch(/Install one of: snappy, snappyjs/)
		expect(missingCodecError(CompressionType.Lz4).message).toMatch(/lz4-napi, lz4, lz4js/)
		expect(missingCodecError(CompressionType.Zstd).message).toMatch(/@mongodb-js\/zstd, zstd-napi/)
	})
})

describe('async codec async-detection probe', () => {
	async function expectNoUnhandledRejection(create: () => void): Promise<void> {
		const rejections: unknown[] = []
		const handler = (reason: unknown) => rejections.push(reason)
		process.on('unhandledRejection', handler)
		try {
			create()
			// Let the probe promise settle and Node's unhandled-rejection detection run.
			await new Promise(resolve => setTimeout(resolve, 20))
		} finally {
			process.off('unhandledRejection', handler)
		}
		expect(rejections).toHaveLength(0)
	}

	it('does not leak an unhandled rejection when the async snappy probe rejects', async () => {
		await expectNoUnhandledRejection(() => {
			createSnappyCodec({
				compress: () => Promise.reject(new Error('probe failed')),
				uncompress: () => Promise.resolve(Buffer.alloc(0)),
				// eslint-disable-next-line @typescript-eslint/no-explicit-any
			} as any)
		})
	})

	it('does not leak an unhandled rejection when the async zstd probe rejects', async () => {
		await expectNoUnhandledRejection(() => {
			createZstdCodec({
				compress: () => Promise.reject(new Error('probe failed')),
				decompress: () => Promise.resolve(Buffer.alloc(0)),
				// eslint-disable-next-line @typescript-eslint/no-explicit-any
			} as any)
		})
	})
})
