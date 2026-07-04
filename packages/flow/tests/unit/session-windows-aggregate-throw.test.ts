import { describe, it, expect } from 'vitest'

import { codec, SessionWindows } from '../../src/index.js'
import { TestDriver } from '../../src/testing.js'

const numberCodec = {
	encode: (n: number) => Buffer.from(String(n)),
	decode: (b: Buffer) => Number(b.toString()),
}

describe('SessionWindows aggregate() throws instead of silently building tumbling windows', () => {
	it('throws a clear error because the aggregate() signature has no session merger', () => {
		const driver = new TestDriver()
		const stream = driver.input('input', { key: codec.string(), value: codec.json<number>() })

		// Pre-fix this silently built a tumbling WindowStore with size = the session gap,
		// producing wrong aggregates. It must throw like the SlidingWindows branch does.
		expect(() =>
			stream
				.groupByKey()
				.windowedBy(SessionWindows.withInactivityGap('30s'))
				.aggregate(
					() => 0,
					(_key, _value, aggregate) => aggregate + 1,
					{ value: numberCodec }
				)
		).toThrow(/[Ss]ession/)
	})
})
