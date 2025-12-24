import { describe, expect, it } from 'vitest'

import { crc32c, crc32cSigned, verifyCrc32c } from '@/utils/crc32c.js'

describe('crc32c', () => {
	it('matches known CRC32C vector', () => {
		const data = Buffer.from('123456789', 'utf-8')
		const checksum = crc32c(data)
		expect(checksum).toBe(0xe3069283)
		expect(verifyCrc32c(data, checksum)).toBe(true)
	})

	it('supports slicing with start and length', () => {
		const data = Buffer.from('xx123456789yy', 'utf-8')
		const checksum = crc32c(data, 2, 9)
		expect(checksum).toBe(0xe3069283)
	})

	it('returns signed values when requested', () => {
		const data = Buffer.from('123456789', 'utf-8')
		const signed = crc32cSigned(data)
		expect(signed).toBe(0xe3069283 - 0x100000000)
	})

	it('verifies mismatched checksums', () => {
		const data = Buffer.from('123456789', 'utf-8')
		expect(verifyCrc32c(data, 0)).toBe(false)
	})

	it('handles empty buffers', () => {
		const checksum = crc32c(Buffer.alloc(0))
		expect(checksum).toBe(0)
	})

	it('supports checksum of partial buffer', () => {
		const data = Buffer.from('abcdef', 'utf-8')
		const checksum = crc32c(data, 1, 3)
		expect(checksum).toBe(crc32c(Buffer.from('bcd', 'utf-8')))
	})
})
