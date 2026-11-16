import { describe, expect, it, vi } from 'vitest'
import type { ChangelogCheckpointStore } from '@/state.js'
import { Checkpoints } from '@/state/checkpoints.js'

function failing(overrides: Partial<ChangelogCheckpointStore>): ChangelogCheckpointStore {
	return {
		get: vi.fn().mockResolvedValue(undefined),
		set: vi.fn().mockResolvedValue(undefined),
		flush: vi.fn().mockResolvedValue(undefined),
		...overrides,
	}
}

describe('checkpoint persistence errors surface, not swallow', () => {
	it('a failing set() reaches onError and does not throw', async () => {
		const setError = new Error('disk full')
		const store = failing({ set: vi.fn().mockRejectedValue(setError) })
		const onError = vi.fn()
		const checkpoints = new Checkpoints(store, onError)

		await expect(checkpoints.set('t', 0, 42n)).resolves.toBeUndefined()
		await expect(checkpoints.flush()).resolves.toBeUndefined()

		expect(store.set).toHaveBeenCalledWith('t', 0, 42n)
		expect(onError).toHaveBeenCalledWith(setError)
	})

	it('a failing flush() reaches onError and does not throw', async () => {
		const flushError = new Error('fsync failed')
		const store = failing({ flush: vi.fn().mockRejectedValue(flushError) })
		const onError = vi.fn()
		const checkpoints = new Checkpoints(store, onError)

		await checkpoints.set('t', 0, 1n)
		await expect(checkpoints.flush()).resolves.toBeUndefined()

		expect(store.flush).toHaveBeenCalled()
		expect(onError).toHaveBeenCalledWith(flushError)
	})

	it('reports a repeated failure once per message, and again after the checkpoints persisted', async () => {
		let failure: string | null = 'disk full'
		const store = failing({
			set: vi.fn(() => (failure === null ? Promise.resolve() : Promise.reject(new Error(failure)))),
		})
		const onError = vi.fn()
		const checkpoints = new Checkpoints(store, onError)
		const commit = async () => {
			await Promise.all([checkpoints.set('t', 0, 1n), checkpoints.set('t', 1, 1n)])
			await checkpoints.flush()
		}

		await commit()
		await commit()
		expect(onError).toHaveBeenCalledTimes(1)
		expect(onError.mock.calls[0]![0].message).toBe('disk full')

		failure = 'read-only file system'
		await commit()
		expect(onError).toHaveBeenCalledTimes(2)
		expect(onError.mock.calls[1]![0].message).toBe('read-only file system')

		// The checkpoints of a commit persisted: the next failure is news again.
		failure = null
		await commit()
		failure = 'read-only file system'
		await commit()
		expect(onError).toHaveBeenCalledTimes(3)
	})

	it('a failing invalidate() throws, since a rebuild must not start over a trusted checkpoint', async () => {
		const onError = vi.fn()
		const setFails = new Checkpoints(failing({ set: vi.fn().mockRejectedValue(new Error('disk full')) }), onError)
		await expect(setFails.invalidate('t', 0)).rejects.toThrow('disk full')

		const flushFails = new Checkpoints(
			failing({ flush: vi.fn().mockRejectedValue(new Error('fsync failed')) }),
			onError
		)
		await expect(flushFails.invalidate('t', 0)).rejects.toThrow('fsync failed')

		expect(onError).not.toHaveBeenCalled()
	})
})
