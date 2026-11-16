import { describe, expect, it } from 'vitest'
import { FixedKeyRecord, StreamRecord } from '@/processor/record.js'

describe('StreamRecord', () => {
	it('is immutable: with* return new records', () => {
		const headers = { h: Buffer.from('v') }
		const record = new StreamRecord('k', 1, 10, headers)
		const changed = record.withKey(2).withValue('v').withTimestamp(20).withHeaders({})
		expect(record).toMatchObject({ key: 'k', value: 1, timestamp: 10, headers })
		expect(changed).toMatchObject({ key: 2, value: 'v', timestamp: 20, headers: {} })
		expect(new StreamRecord(null, null, 0).headers).toEqual({})
	})

	it.each([-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY])('rejects the timestamp %s', timestamp => {
		expect(() => new StreamRecord('k', 1, timestamp)).toThrow(TypeError)
		expect(() => new StreamRecord('k', 1, 0).withTimestamp(timestamp)).toThrow(TypeError)
	})
})

describe('FixedKeyRecord', () => {
	it('keeps its key and validates timestamps', () => {
		const record = new FixedKeyRecord('k', 1, 10)
		expect(record.withValue('v').withTimestamp(11)).toMatchObject({ key: 'k', value: 'v', timestamp: 11 })
		expect(() => record.withTimestamp(-1)).toThrow(TypeError)
	})
})
