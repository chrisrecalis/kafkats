import { describe, expect, it } from 'vitest'
import { ManualClock } from './manual-clock.js'

describe('ManualClock', () => {
	it('fires due timers only when advanced, in time order, each at its own time; ties in the order set', () => {
		const clock = new ManualClock(100)
		const fired: string[] = []
		clock.setTimer(130, () => fired.push(`c@${clock.now()}`))
		clock.setTimer(110, () => fired.push(`a@${clock.now()}`))
		clock.setTimer(110, () => fired.push(`b@${clock.now()}`))
		clock.setTimer(200, () => fired.push('late'))
		expect(fired).toEqual([])
		clock.advance(50)
		expect(fired).toEqual(['a@110', 'b@110', 'c@130'])
		expect([clock.now(), clock.time, clock.pendingTimers]).toEqual([150, 150, 1])
	})

	it('fires timers set by a callback that fall due within the same advance; cancelled ones never fire', () => {
		const clock = new ManualClock()
		const fired: number[] = []
		const retry = () => {
			fired.push(clock.now())
			clock.setTimer(clock.now() + 100, retry)
		}
		clock.setTimer(100, retry)
		const cancel = clock.setTimer(150, () => fired.push(-1))
		cancel()
		clock.advance(350)
		expect(fired).toEqual([100, 200, 300])
		expect(clock.pendingTimers).toBe(1)
	})

	it('a timer set for a time already passed fires at the next advance, advance(0) included', () => {
		const clock = new ManualClock(1000)
		let fired = 0
		clock.setTimer(500, () => fired++)
		expect(fired).toBe(0)
		clock.advance()
		expect([fired, clock.now()]).toEqual([1, 1000])
	})
})
