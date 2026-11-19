import { describe, expect, expectTypeOf, it } from 'vitest'
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
		const headers = { h: Buffer.from('v') }
		const record = FixedKeyRecord.from(new StreamRecord('k', 1, 10, headers))
		expect(record).toMatchObject({ key: 'k', value: 1, timestamp: 10, headers })
		expect(record.withValue('v').withTimestamp(11)).toMatchObject({ key: 'k', value: 'v', timestamp: 11 })
		expect(() => record.withTimestamp(-1)).toThrow(TypeError)
	})

	it('is derived from received records only, never constructed (Kafka Streams)', async () => {
		expect('FixedKeyRecord' in (await import('@/index.js'))).toBe(false)
		// @ts-expect-error the constructor is private: a record built from scratch could carry another key
		const construct = () => new FixedKeyRecord('other', 1, 10, {})
		expectTypeOf(construct).toBeFunction()
		type LookAlike = Pick<FixedKeyRecord<string, number>, keyof FixedKeyRecord<string, number>>
		expectTypeOf<LookAlike>().not.toMatchTypeOf<FixedKeyRecord<string, number>>()
	})
})
