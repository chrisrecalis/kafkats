import { describe, expect, it, vi } from 'vitest'

import { ReconnectionStrategy, ReconnectingConnection } from '@/network/reconnection.js'

describe('ReconnectionStrategy', () => {
	it('computes exponential backoff without jitter', () => {
		const strategy = new ReconnectionStrategy({ initialDelayMs: 10, maxDelayMs: 100, multiplier: 2, jitter: 0 })
		expect(strategy.nextDelay()).toBe(10)
		strategy.recordFailure()
		expect(strategy.nextDelay()).toBe(20)
		strategy.recordFailure()
		expect(strategy.nextDelay()).toBe(40)
	})

	it('caps delay at maxDelayMs', () => {
		const strategy = new ReconnectionStrategy({ initialDelayMs: 50, maxDelayMs: 60, multiplier: 3, jitter: 0 })
		expect(strategy.nextDelay()).toBe(50)
		strategy.recordFailure()
		expect(strategy.nextDelay()).toBe(60)
	})

	it('respects maxAttempts', () => {
		const strategy = new ReconnectionStrategy({ maxAttempts: 2 })
		expect(strategy.shouldReconnect()).toBe(true)
		strategy.recordFailure()
		expect(strategy.shouldReconnect()).toBe(true)
		strategy.recordFailure()
		expect(strategy.shouldReconnect()).toBe(false)
	})

	it('records success time and resets attempts', () => {
		vi.useFakeTimers()
		const strategy = new ReconnectionStrategy({ maxAttempts: 2 })
		strategy.recordFailure()
		vi.setSystemTime(new Date('2024-01-01T00:00:00Z'))
		strategy.recordSuccess()
		expect(strategy.currentAttempts).toBe(0)
		const since = strategy.timeSinceLastSuccess
		expect(typeof since).toBe('number')
		vi.useRealTimers()
	})

	it('reset clears attempts without touching success time', () => {
		const strategy = new ReconnectionStrategy({ maxAttempts: 2 })
		strategy.recordFailure()
		strategy.reset()
		expect(strategy.currentAttempts).toBe(0)
	})
})

describe('ReconnectingConnection', () => {
	it('attemptReconnect calls onReconnect on success', async () => {
		const onReconnect = vi.fn()
		const connection = new ReconnectingConnection(async () => {}, onReconnect, undefined, { maxAttempts: 1 })
		const result = await connection.attemptReconnect()
		expect(result).toBe(true)
		expect(onReconnect).toHaveBeenCalledTimes(1)
	})

	it('attemptReconnect calls onReconnectFailed on failure', async () => {
		const onReconnectFailed = vi.fn()
		const connection = new ReconnectingConnection(
			async () => {
				throw new Error('fail')
			},
			undefined,
			onReconnectFailed,
			{ maxAttempts: 1 }
		)
		const result = await connection.attemptReconnect()
		expect(result).toBe(false)
		expect(onReconnectFailed).toHaveBeenCalledTimes(1)
	})

	it('startReconnecting stops when stop() is called', async () => {
		vi.useFakeTimers()
		let attempts = 0
		const connection = new ReconnectingConnection(
			async () => {
				attempts++
				throw new Error('fail')
			},
			undefined,
			undefined,
			{ initialDelayMs: 1, maxAttempts: 10, jitter: 0 }
		)

		const promise = connection.startReconnecting()
		await vi.advanceTimersByTimeAsync(2)
		connection.stop()
		await vi.advanceTimersByTimeAsync(5)

		const result = await promise
		expect(result).toBe(false)
		expect(attempts).toBeGreaterThan(0)
		vi.useRealTimers()
	})

	it('startReconnecting succeeds after retry', async () => {
		vi.useFakeTimers()
		let attempts = 0
		const connection = new ReconnectingConnection(
			async () => {
				attempts++
				if (attempts < 2) {
					throw new Error('fail')
				}
			},
			undefined,
			undefined,
			{ initialDelayMs: 1, maxAttempts: 3, jitter: 0 }
		)

		const promise = connection.startReconnecting()
		await vi.advanceTimersByTimeAsync(2)
		await vi.advanceTimersByTimeAsync(2)

		const result = await promise
		expect(result).toBe(true)
		expect(attempts).toBe(2)
		vi.useRealTimers()
	})
})
