import { describe, expect, it } from 'vitest'
import { decodeCommitMetadata, encodeCommitMetadata, ProcessorMetadataImpl } from '@/processor/metadata.js'

const hex = (encoded: string) => Buffer.from(encoded, 'base64').toString('hex')
const fromHex = (bytes: string) => Buffer.from(bytes, 'hex').toString('base64')
const int64 = (value: number) => BigInt.asUintN(64, BigInt(value)).toString(16).padStart(16, '0')
const int32 = (value: number) => (value >>> 0).toString(16).padStart(8, '0')

function metadata(entries: Record<string, number>): ProcessorMetadataImpl {
	const result = new ProcessorMetadataImpl()
	for (const [key, value] of Object.entries(entries)) {
		result.put(key, value)
	}
	return result
}

describe('encodeCommitMetadata', () => {
	it('writes magic 2 and the partition time, without meta bytes when empty', () => {
		expect(hex(encodeCommitMetadata(7, new ProcessorMetadataImpl()))).toBe(`02${int64(7)}`)
		expect(hex(encodeCommitMetadata(-1, new ProcessorMetadataImpl()))).toBe(`02${'ff'.repeat(8)}`)
	})

	it('writes the entries sorted by key', () => {
		const encoded = encodeCommitMetadata(7, metadata({ 'b.emit': 2, a: -1 }))
		expect(hex(encoded)).toBe(
			`02${int64(7)}${int32(2)}${int32(1)}61${int64(-1)}${int32(6)}${Buffer.from('b.emit').toString('hex')}${int64(2)}`
		)
	})

	it('reflects a put after an earlier encode', () => {
		const meta = metadata({ a: 1 })
		const before = encodeCommitMetadata(0, meta)
		meta.put('a', 2)
		expect(decodeCommitMetadata(encodeCommitMetadata(0, meta)).metadata).toEqual(new Map([['a', 2]]))
		expect(decodeCommitMetadata(before).metadata).toEqual(new Map([['a', 1]]))
	})
})

describe('decodeCommitMetadata', () => {
	it('round-trips', () => {
		const decoded = decodeCommitMetadata(
			encodeCommitMetadata(1_700_000_000_000, metadata({ store: 5, 'store.observed': 9 }))
		)
		expect(decoded).toEqual({
			partitionTime: 1_700_000_000_000,
			metadata: new Map([
				['store', 5],
				['store.observed', 9],
			]),
		})
	})

	it.each([null, undefined, ''])('reads %j as unknown, without a warning', encoded => {
		expect(decodeCommitMetadata(encoded)).toEqual({ partitionTime: -1, metadata: new Map() })
	})

	it('reads magic 1 (partition time only) and magic 2 without meta bytes', () => {
		expect(decodeCommitMetadata(fromHex(`01${int64(42)}`))).toEqual({ partitionTime: 42, metadata: new Map() })
		expect(decodeCommitMetadata(fromHex(`02${int64(42)}`))).toEqual({ partitionTime: 42, metadata: new Map() })
	})

	it.each([
		['not base64', 'garbage!'],
		['an unknown version', fromHex(`03${int64(42)}`)],
		['a truncated partition time', fromHex('0200000000')],
		['a count larger than the bytes', fromHex(`02${int64(42)}${int32(5)}${'00'.repeat(12)}`)],
		['a negative count', fromHex(`02${int64(42)}${int32(-1)}`)],
		['a negative key length', fromHex(`02${int64(42)}${int32(1)}${int32(-1)}${'00'.repeat(8)}`)],
		['a key longer than the bytes', fromHex(`02${int64(42)}${int32(1)}${int32(16)}61${'00'.repeat(8)}`)],
		['a missing value', fromHex(`02${int64(42)}${int32(1)}${int32(1)}61`)],
	])('reads %s as unknown with a warning', (_, encoded) => {
		const decoded = decodeCommitMetadata(encoded)
		expect(decoded).toMatchObject({ partitionTime: -1, metadata: new Map() })
		expect(decoded.invalid).toEqual(expect.any(String))
	})
})

describe('ProcessorMetadataImpl', () => {
	it('needs a commit after a put, until committed', () => {
		const meta = new ProcessorMetadataImpl()
		expect(meta.needsCommit).toBe(false)
		meta.put('a', 1)
		expect(meta.needsCommit).toBe(true)
		meta.markCommitted()
		expect(meta.needsCommit).toBe(false)
		expect(meta.get('a')).toBe(1)
	})

	it('keeps the values of the last commit, or of the seed before the first', () => {
		const meta = new ProcessorMetadataImpl()
		meta.replaceAll([new Map([['a', 1]])], new Set(['a', 'b']))
		meta.put('a', 2)
		meta.put('b', 5)
		expect([meta.committed('a'), meta.committed('b')]).toEqual([1, undefined])
		meta.markCommitted()
		meta.put('a', 3)
		expect([meta.committed('a'), meta.committed('b')]).toEqual([2, 5])
		meta.replaceAll([new Map([['a', 1]])], new Set(['a', 'b']))
		expect([meta.committed('a'), meta.committed('b')]).toEqual([1, undefined])
	})

	it.each([1.5, Number.NaN, 2 ** 53])('rejects the value %s', value => {
		expect(() => new ProcessorMetadataImpl().put('a', value)).toThrow(TypeError)
	})

	it('seeding replaces the entries with the max-merge of the commits, restricted to the keys', () => {
		const meta = metadata({ a: 100, stale: 1 })
		meta.replaceAll(
			[
				new Map([
					['a', 1],
					['b', 5],
				]),
				new Map([
					['a', 3],
					['removed', 9],
				]),
			],
			new Set(['a', 'b', 'never'])
		)
		expect(meta.get('a')).toBe(3)
		expect(meta.get('b')).toBe(5)
		expect(meta.get('stale')).toBeUndefined()
		expect(meta.get('removed')).toBeUndefined()
		expect(meta.needsCommit).toBe(false)
		expect(decodeCommitMetadata(encodeCommitMetadata(0, meta)).metadata).toEqual(
			new Map([
				['a', 3],
				['b', 5],
			])
		)
	})
})
