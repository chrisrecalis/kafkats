import { describe, expect, it, vi } from 'vitest'
import { IllegalStateError } from '@/api/errors.js'
import { AppLifecycle } from '@/app/lifecycle.js'

function deferred(): { promise: Promise<void>; resolve(): void; reject(error: unknown): void } {
	let resolve!: () => void
	let reject!: (error: unknown) => void
	const promise = new Promise<void>((res, rej) => {
		resolve = res
		reject = rej
	})
	return { promise, resolve, reject }
}

async function started(): Promise<AppLifecycle> {
	const lifecycle = new AppLifecycle()
	await lifecycle.start(async () => {})
	return lifecycle
}

describe('AppLifecycle', () => {
	it('is CREATED, then RUNNING once a start succeeded', async () => {
		const lifecycle = new AppLifecycle()
		expect(lifecycle.state).toBe('CREATED')
		expect(lifecycle.error).toBeNull()
		const start = deferred()
		const starting = lifecycle.start(() => start.promise)
		expect(lifecycle.state).toBe('CREATED')
		start.resolve()
		await starting
		expect(lifecycle.state).toBe('RUNNING')
		expect(lifecycle.started).toBe(true)
	})

	it('runs one start at a time: concurrent and later starts share its promise', async () => {
		const lifecycle = new AppLifecycle()
		const start = vi.fn(async () => {})
		const first = lifecycle.start(start)
		expect(lifecycle.start(start)).toBe(first)
		await first
		expect(lifecycle.start(start)).toBe(first)
		expect(start).toHaveBeenCalledTimes(1)
	})

	it('a failed start leaves ERROR with the error and can be retried; a new start clears the error', async () => {
		const lifecycle = new AppLifecycle()
		const failure = new Error('no brokers')
		await expect(lifecycle.start(() => Promise.reject(failure))).rejects.toBe(failure)
		expect(lifecycle.state).toBe('ERROR')
		expect(lifecycle.error).toBe(failure)
		expect(lifecycle.started).toBe(false)
		const start = deferred()
		const retry = lifecycle.start(() => start.promise)
		expect(lifecycle.error).toBeNull()
		expect(lifecycle.state).toBe('CREATED')
		start.resolve()
		await retry
		expect(lifecycle.state).toBe('RUNNING')
	})

	it('a start that throws synchronously fails like one that rejects', async () => {
		const lifecycle = new AppLifecycle()
		await expect(
			lifecycle.start(() => {
				throw new Error('sync')
			})
		).rejects.toThrow('sync')
		expect(lifecycle.state).toBe('ERROR')
	})

	it('closing an app that never started only marks it STOPPED', async () => {
		const lifecycle = new AppLifecycle()
		const close = vi.fn(async () => {})
		await lifecycle.close(close)
		expect(close).not.toHaveBeenCalled()
		expect(lifecycle.state).toBe('STOPPED')
	})

	it('runs one close (later calls share it) and ends STOPPED even if it failed', async () => {
		const lifecycle = await started()
		const close = vi.fn(() => Promise.reject(new Error('close failed')))
		const closing = lifecycle.close(close)
		expect(lifecycle.closing).toBe(true)
		expect(lifecycle.close(close)).toBe(closing)
		await expect(closing).rejects.toThrow('close failed')
		expect(close).toHaveBeenCalledTimes(1)
		expect(lifecycle.state).toBe('STOPPED')
		expect(lifecycle.started).toBe(false)
	})

	it('a close during a start waits for the start; a start failing meanwhile ends STOPPED', async () => {
		const lifecycle = new AppLifecycle()
		const start = deferred()
		const starting = lifecycle.start(() => start.promise)
		let closed = false
		const closing = lifecycle
			.close(async () => {})
			.then(() => {
				closed = true
			})
		await Promise.resolve()
		expect(closed).toBe(false)
		start.reject(new Error('interrupted'))
		await expect(starting).rejects.toThrow('interrupted')
		await closing
		expect(lifecycle.state).toBe('STOPPED')
	})

	it('start() during a close rejects with IllegalStateError and leaves the close to end STOPPED', async () => {
		const cases: Array<() => Promise<AppLifecycle>> = [
			started,
			async () => {
				const lifecycle = await started()
				lifecycle.halt(new Error('fatal'))
				return lifecycle
			},
			async () => {
				const lifecycle = new AppLifecycle()
				await lifecycle.start(() => Promise.reject(new Error('no brokers'))).catch(() => {})
				return lifecycle
			},
		]
		for (const setup of cases) {
			const lifecycle = await setup()
			const close = deferred()
			const closing = lifecycle.close(() => close.promise)
			const second = vi.fn(async () => {})
			await expect(lifecycle.start(second)).rejects.toBeInstanceOf(IllegalStateError)
			expect(lifecycle.closing).toBe(true)
			close.resolve()
			await closing
			expect(second).not.toHaveBeenCalled()
			expect(lifecycle.state).toBe('STOPPED')
			expect(lifecycle.closing).toBe(false)
			await lifecycle.start(second)
			expect(second).toHaveBeenCalledTimes(1)
			expect(lifecycle.state).toBe('RUNNING')
		}
	})

	it('can start again after a close', async () => {
		const lifecycle = await started()
		await lifecycle.close(async () => {})
		await lifecycle.start(async () => {})
		expect(lifecycle.state).toBe('RUNNING')
	})

	it('moves between RUNNING and REBALANCING only once started and not closing', async () => {
		const lifecycle = new AppLifecycle()
		lifecycle.setRebalancing()
		expect(lifecycle.state).toBe('CREATED')
		await lifecycle.start(async () => {})
		lifecycle.setRebalancing()
		expect(lifecycle.state).toBe('REBALANCING')
		lifecycle.setRunning()
		expect(lifecycle.state).toBe('RUNNING')
		lifecycle.setRebalancing()
		const closing = lifecycle.close(async () => {})
		lifecycle.setRunning()
		expect(lifecycle.state).toBe('REBALANCING')
		await closing
		expect(lifecycle.state).toBe('STOPPED')
	})

	it('stays RUNNING while closing: the tasks closing do not make it REBALANCING', async () => {
		const lifecycle = await started()
		const close = deferred()
		const closing = lifecycle.close(() => close.promise)
		lifecycle.setRebalancing()
		expect(lifecycle.state).toBe('RUNNING')
		close.resolve()
		await closing
		expect(lifecycle.state).toBe('STOPPED')
	})

	it('a start ends REBALANCING when a rebalance or recovery began after its tasks ran, RUNNING otherwise', async () => {
		const lifecycle = new AppLifecycle()
		await lifecycle.start(async () => {
			lifecycle.setRebalancing()
			lifecycle.setRunning()
		})
		expect(lifecycle.state).toBe('RUNNING')
		await lifecycle.close(async () => {})
		await lifecycle.start(async () => {
			lifecycle.setRunning()
			lifecycle.setRebalancing()
		})
		expect(lifecycle.state).toBe('REBALANCING')
		lifecycle.setRunning()
		expect(lifecycle.state).toBe('RUNNING')
	})

	it('reports a recoverable error without changing the state', async () => {
		const lifecycle = await started()
		const error = new Error('migrated')
		lifecycle.recordError(error)
		expect(lifecycle.state).toBe('RUNNING')
		expect(lifecycle.error).toBe(error)
	})

	it('a fatal failure moves to ERROR; the first one wins and outranks recoverable errors until close', async () => {
		const lifecycle = await started()
		const fatal = new Error('fatal')
		expect(lifecycle.halt(fatal)).toBe(true)
		expect(lifecycle.halt(new Error('second'))).toBe(false)
		lifecycle.recordError(new Error('recoverable'))
		lifecycle.setRunning()
		expect(lifecycle.state).toBe('ERROR')
		expect(lifecycle.error).toBe(fatal)
		await lifecycle.start(() => Promise.reject(new Error('not called')))
		expect(lifecycle.state).toBe('ERROR')
		await lifecycle.close(async () => {})
		expect(lifecycle.state).toBe('STOPPED')
		await lifecycle.start(async () => {})
		expect(lifecycle.error).toBeNull()
	})

	it('a fatal failure while closing is reported but leaves the state to the close', async () => {
		const lifecycle = await started()
		const close = deferred()
		const closing = lifecycle.close(() => close.promise)
		lifecycle.halt('lost')
		expect(lifecycle.state).toBe('RUNNING')
		expect(lifecycle.error).toEqual(new Error('lost'))
		close.resolve()
		await closing
		expect(lifecycle.state).toBe('STOPPED')
	})
})
