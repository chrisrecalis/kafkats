import { describe, expect, it } from 'vitest'
import { CompressionType, compressionCodecs } from '@kafkats/client'

/**
 * The exporter decodes whatever compression producers use, so every codec it relies on must actually
 * load (native bindings included) and round-trip. Zstd comes from Node's zlib (22.15+); a codec that
 * fails to load would otherwise surface only as a per-partition decode error in production.
 */
describe('compression codecs', () => {
	for (const type of [CompressionType.Snappy, CompressionType.Lz4, CompressionType.Zstd]) {
		it(`loads and round-trips ${CompressionType[type]}`, async () => {
			const codec = compressionCodecs.get(type)
			expect(codec).toBeDefined()
			const input = Buffer.from('lag exporter codec smoke test '.repeat(50))
			const compressed = await codec!.compress(input)
			expect(compressed.length).toBeLessThan(input.length)
			expect(await codec!.decompress(compressed)).toEqual(input)
		})
	}
})
