import { describe, expect, it } from 'vitest'
import { codec } from '@/codec.js'
import { NotImplementedError } from '@/dsl/not-implemented.js'
import { SlidingWindows } from '@/dsl/windows.js'
import { TestDriver } from '@/testing/driver.js'

// Sliding-window aggregations come with Stage 3 (WP3.5, which rewrites this test).
describe('SlidingWindows throws instead of silently misbehaving', () => {
	it('throws when used as an aggregation window', () => {
		const driver = new TestDriver()
		const stream = driver.input('input', { key: codec.string(), value: codec.json<number>() })

		expect(() => stream.groupByKey().windowedBy(SlidingWindows.of(1000)).count()).toThrow(NotImplementedError)
	})
})
