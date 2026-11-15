import { describe, expect, it } from 'vitest'
import { PendingSends } from '@/runtime/pending-sends.js'

const deferred = <T>() => {
	let resolve!: (value: T) => void
	let reject!: (error: unknown) => void
	const promise = new Promise<T>((res, rej) => {
		resolve = res
		reject = rej
	})
	return { promise, resolve, reject }
}

describe('PendingSends', () => {
	it('flush waits for every tracked send', async () => {
		const sends = new PendingSends()
		const a = deferred<void>()
		const b = deferred<void>()
		sends.track(a.promise)
		sends.track(b.promise)

		let flushed = false
		const flushing = sends.flush().then(() => {
			flushed = true
		})
		a.resolve()
		await Promise.resolve()
		expect(flushed).toBe(false)
		b.resolve()
		await flushing
		expect(flushed).toBe(true)
	})

	it('flush throws the first failure, after all sends have settled, until discarded', async () => {
		const sends = new PendingSends()
		const slow = deferred<void>()
		sends.track(Promise.reject(new Error('first')))
		sends.track(Promise.reject(new Error('second')))
		sends.track(slow.promise)

		let settled = false
		const flushing = sends.flush().finally(() => {
			settled = true
		})
		await new Promise(resolve => setTimeout(resolve, 5))
		expect(settled).toBe(false)
		slow.resolve()
		await expect(flushing).rejects.toThrow('first')

		// Every flush reports it (batches of several partitions flush the same sends) ...
		await expect(sends.flush()).rejects.toThrow('first')
		// ... until the failed work has been rolled back.
		await sends.discard()
		await expect(sends.flush()).resolves.toBeUndefined()
	})

	it('concurrent flushes all wait for all sends', async () => {
		const sends = new PendingSends()
		const first = deferred<void>()
		const second = deferred<void>()
		sends.track(first.promise)
		const flushA = sends.flush()
		sends.track(second.promise)
		let doneB = false
		const flushB = sends.flush().then(() => {
			doneB = true
		})

		first.resolve()
		await new Promise(resolve => setTimeout(resolve, 5))
		expect(doneB).toBe(false)
		second.resolve()
		await Promise.all([flushA, flushB])
	})

	it('a failing acknowledgement callback fails the flush only after the other sends have settled', async () => {
		const sends = new PendingSends()
		const slow = deferred<void>()
		sends.track(Promise.resolve(), () => {
			throw new Error('ack failed')
		})
		sends.track(slow.promise)

		let settled = false
		const flushing = sends.flush().finally(() => {
			settled = true
		})
		await new Promise(resolve => setTimeout(resolve, 5))
		expect(settled).toBe(false)
		slow.resolve()
		await expect(flushing).rejects.toThrow('ack failed')
		await expect(sends.discard()).resolves.toBeUndefined()
	})

	it('a failed send does not surface as an unhandled rejection before the flush', async () => {
		const sends = new PendingSends()
		const unhandled: unknown[] = []
		const onUnhandled = (reason: unknown) => unhandled.push(reason)
		process.on('unhandledRejection', onUnhandled)
		try {
			sends.track(Promise.reject(new Error('boom')))
			await new Promise(resolve => setTimeout(resolve, 10))
			expect(unhandled).toEqual([])
			await expect(sends.flush()).rejects.toThrow('boom')
		} finally {
			process.off('unhandledRejection', onUnhandled)
		}
	})

	it('flush also waits for sends tracked while it is flushing', async () => {
		const sends = new PendingSends()
		const late = deferred<void>()
		const first = deferred<void>()
		// An acknowledgement callback that produces something else.
		sends.track(first.promise.then(() => sends.track(late.promise)))

		let flushed = false
		const flushing = sends.flush().then(() => {
			flushed = true
		})
		first.resolve()
		await new Promise(resolve => setTimeout(resolve, 5))
		expect(flushed).toBe(false)
		late.resolve()
		await flushing
	})

	it('discard forgets pending sends and failures', async () => {
		const sends = new PendingSends()
		sends.track(Promise.reject(new Error('dropped')))
		sends.track(new Promise(() => {}))
		sends.discard()
		await expect(sends.flush()).resolves.toBeUndefined()
	})

	it('a discarded send that fails later does not fail the next flush', async () => {
		const sends = new PendingSends()
		const aborted = deferred<void>()
		sends.track(aborted.promise)
		sends.discard()

		// The work was discarded (transaction aborted); its send is rejected afterwards.
		aborted.reject(new Error('aborted'))
		await new Promise(resolve => setTimeout(resolve, 5))

		sends.track(Promise.resolve())
		await expect(sends.flush()).resolves.toBeUndefined()
	})
})
