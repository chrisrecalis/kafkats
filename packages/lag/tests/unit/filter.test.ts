import { describe, expect, it } from 'vitest'
import { isSelected, parseNameFilter } from '../../src/filter.js'

describe('name filters', () => {
	it('parses comma-separated names', () => {
		expect(parseNameFilter('orders, billing')).toEqual(['orders', 'billing'])
	})

	it('parses regular expressions', () => {
		const filter = parseNameFilter('/^orders-/')
		expect(isSelected('orders-worker', filter, undefined)).toBe(true)
		expect(isSelected('billing-worker', filter, undefined)).toBe(false)
	})

	it('applies exclusions after inclusions', () => {
		expect(isSelected('orders-replay', /^orders-/, /-replay$/)).toBe(false)
	})
})
