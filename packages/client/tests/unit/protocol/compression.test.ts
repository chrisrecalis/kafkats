import { describe, expect, it } from 'vitest'

import {
	compressionCodecs,
	CompressionType,
	createLz4Codec,
	createSnappyCodec,
	createZstdCodec,
	getCompressionTypeName,
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
		expect(compressionCodecs.get(CompressionType.Zstd)).toBeUndefined()
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
