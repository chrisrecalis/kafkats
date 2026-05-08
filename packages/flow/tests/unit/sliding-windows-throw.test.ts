import { describe, it, expect } from 'vitest'

import { codec, SlidingWindows } from '../../src/index.js'
import { TestDriver } from '../../src/testing.js'

describe('SlidingWindows throws instead of silently misbehaving', () => {
	it('throws when used as an aggregation window', () => {
		const driver = new TestDriver()
		const stream = driver.input('input', { key: codec.string(), value: codec.json<number>() })

		expect(() => stream.groupByKey().windowedBy(SlidingWindows.of(1000)).count()).toThrow(
			/SlidingWindows is not yet implemented/
		)
	})
})
