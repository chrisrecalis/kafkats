import { describe, expect, it } from 'vitest'

import { abortable } from '@/utils/abortable.js'

describe('abortable', () => {
	it('returns the promise itself without a signal', () => {
		const promise = Promise.resolve(1)
		expect(abortable(promise, undefined)).toBe(promise)
	})

	it('settles as the promise does while the signal is not aborted', async () => {
		const signal = new AbortController().signal
		await expect(abortable(Promise.resolve('done'), signal)).resolves.toBe('done')
		await expect(abortable(Promise.reject(new Error('boom')), signal)).rejects.toThrow('boom')
	})

	it('rejects with the reason of an already aborted signal', async () => {
		const reason = new Error('stopped')
		await expect(abortable(new Promise(() => {}), AbortSignal.abort(reason))).rejects.toBe(reason)
	})

	it('rejects with the reason once the signal aborts, and leaves a later rejection handled', async () => {
		const controller = new AbortController()
		let fail!: (error: Error) => void
		const pending = abortable(new Promise<never>((_, reject) => (fail = reject)), controller.signal)

		controller.abort()
		await expect(pending).rejects.toMatchObject({ name: 'AbortError' })

		const unhandled: unknown[] = []
		const onUnhandled = (error: unknown) => unhandled.push(error)
		process.on('unhandledRejection', onUnhandled)
		fail(new Error('late'))
		await new Promise(resolve => setImmediate(resolve))
		process.off('unhandledRejection', onUnhandled)
		expect(unhandled).toEqual([])
	})
})
