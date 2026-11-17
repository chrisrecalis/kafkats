import { describe, expect, it, vi } from 'vitest'
import { YIELD_AFTER_MS, cap, raceSignal, sleep, yieldIfDue } from '@/runtime/signals.js'

describe('cap', () => {
	it('aborts when the parent aborts', () => {
		const parent = new AbortController()
		const signal = cap(parent.signal, 60_000)
		expect(signal.aborted).toBe(false)
		parent.abort(new Error('stop'))
		expect(signal.aborted).toBe(true)
	})

	it('aborts after the cap', async () => {
		const signal = cap(new AbortController().signal, 5)
		await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }))
		expect(signal.reason).toBeInstanceOf(DOMException)
		expect((signal.reason as DOMException).name).toBe('TimeoutError')
	})
})

describe('raceSignal', () => {
	it('settles like the promise while the signal is not aborted', async () => {
		const controller = new AbortController()
		await expect(raceSignal(Promise.resolve(1), controller.signal)).resolves.toBe(1)
		await expect(raceSignal(Promise.reject(new Error('boom')), controller.signal)).rejects.toThrow('boom')
	})

	it('rejects with an AbortError on abort and never leaves the abandoned promise unhandled', async () => {
		const unhandled = vi.fn()
		process.on('unhandledRejection', unhandled)
		try {
			const controller = new AbortController()
			let rejectLater!: (error: Error) => void
			const pending = new Promise<number>((_, reject) => (rejectLater = reject))
			const raced = raceSignal(pending, controller.signal)
			controller.abort()
			await expect(raced).rejects.toMatchObject({ name: 'AbortError' })
			rejectLater(new Error('late'))

			const aborted = AbortSignal.abort()
			await expect(raceSignal(Promise.reject(new Error('ignored')), aborted)).rejects.toMatchObject({
				name: 'AbortError',
			})
			await new Promise(resolve => setImmediate(resolve))
			expect(unhandled).not.toHaveBeenCalled()
		} finally {
			process.off('unhandledRejection', unhandled)
		}
	})
})

describe('sleep', () => {
	it('resolves after the delay', async () => {
		const started = performance.now()
		await sleep(10, new AbortController().signal)
		expect(performance.now() - started).toBeGreaterThanOrEqual(9)
	})

	it('rejects with an AbortError on abort', async () => {
		const controller = new AbortController()
		const sleeping = sleep(60_000, controller.signal)
		controller.abort()
		await expect(sleeping).rejects.toMatchObject({ name: 'AbortError' })
	})
})

describe('yieldIfDue', () => {
	it('yields only once YIELD_AFTER_MS passed since the last yield', async () => {
		const now = vi.spyOn(performance, 'now')
		try {
			now.mockReturnValue(1e9)
			await yieldIfDue()
			expect(yieldIfDue()).toBeUndefined()
			now.mockReturnValue(1e9 + YIELD_AFTER_MS)
			const yielded = yieldIfDue()
			expect(yielded).toBeInstanceOf(Promise)
			await yielded
			expect(yieldIfDue()).toBeUndefined()
		} finally {
			now.mockRestore()
		}
	})
})
