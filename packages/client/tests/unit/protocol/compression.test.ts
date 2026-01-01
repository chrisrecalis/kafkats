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
			compress: async data => data,
			uncompress: async data => data,
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

	it('creates a Snappy codec from callback-style functions', async () => {
		const payload = Buffer.from('callback')
		const codec = createSnappyCodec({
			compress: (data, callback) => callback(null, Buffer.concat([Buffer.from('c:'), data])),
			uncompress: (data, callback) =>
				callback(null, Buffer.from(data.toString().replace(/^c:/, ''))),
		})
		const compressed = await codec.compress(payload)
		const decompressed = await codec.decompress(compressed)
		expect(compressed.toString()).toContain('c:')
		expect(decompressed).toEqual(payload)
	})

	it('creates an LZ4 codec from sync or napi-style functions', async () => {
		const codec = createLz4Codec({
			compressSync: data => Buffer.concat([Buffer.from('lz4:'), data]),
			decompress: data => Buffer.from(data.subarray(4)),
			decompressBlock: () => Buffer.from('ignored'),
		})
		const payload = Buffer.from('lz4')
		const compressed = await codec.compress(payload)
		const decompressed = await codec.decompress(compressed)
		expect(decompressed).toEqual(payload)
	})

	it('creates a Zstd codec from async and sync functions', async () => {
		const codec = createZstdCodec({
			compress: async data => data,
			decompressSync: data => data,
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
