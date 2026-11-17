/**
 * The case of the legacy operator-semantics suite that is not ported yet (the others moved to tests/unit/dsl/*):
 * WP3.6 un-skips it with `JoinWindows.ofTimeDifferenceWithNoGrace(1000)` (deferred left/outer results, KF8).
 */
import { describe, expect, it } from 'vitest'
import { codec } from '@/codec.js'
import { TimeWindows } from '@/dsl/windows.js'
import { TestDriver, type TestContextApi } from '@/testing/driver.js'

const str = codec.string()

const send = (ctx: TestContextApi, topic: string, key: string, value: string, ts: number) =>
	ctx.send(topic, Buffer.from(value), { key: Buffer.from(key), timestamp: BigInt(ts) })

describe('operator semantics: stream-stream join', () => {
	// Known divergence: left/outer stream joins emit the unmatched result eagerly (pre-3.1 Kafka Streams semantics).
	it.skip('left join does not emit (l,null) when a matching right record is already in the window... and not both null and match for same left', async () => {
		// Kafka Streams >= 3.? defers the unmatched-left result until the window closes, so a left record that
		// later finds a match never produces a (l, null) result.
		const driver = new TestDriver()
		const l = driver.input('l', { key: str, value: str })
		const r = driver.input('r', { key: str, value: str })
		l.leftJoin(r, (a, b) => `${a}+${b}`, { within: TimeWindows.of(1000) }).to('out', { key: str, value: str })
		await driver.run(async ctx => {
			await send(ctx, 'l', 'k', 'L', 5000)
			await send(ctx, 'r', 'k', 'R', 5500)
			const vals = ctx.output('out', { key: str, value: str }).map(o => o.value)
			expect(vals).toEqual(['L+R'])
		})
	})
})
