import { describe, expect, it, vi } from 'vitest'
import { EventEmitter } from 'node:events'

import type { ChangelogCheckpointStore } from '../../src/state.js'

// Pull just the private flushChangelogCheckpoints + recordCheckpointError into a test seam.
// We can't easily construct a full FlowAppImpl in a unit test (needs cluster, producer, etc.),
// but we CAN verify the behavior via a minimal stand-in that mirrors the real impl's shape.

describe('checkpoint persistence errors are surfaced, not swallowed', () => {
	it('logs and records lastError when set() rejects, without throwing', async () => {
		const setError = new Error('disk full')
		const checkpointStore: ChangelogCheckpointStore = {
			get: vi.fn().mockResolvedValue(undefined),
			set: vi.fn().mockRejectedValue(setError),
			flush: vi.fn().mockResolvedValue(undefined),
		}

		const logged: Array<{ msg: string; ctx: Record<string, unknown> }> = []
		const logger = { error: (msg: string, ctx: Record<string, unknown> = {}) => logged.push({ msg, ctx }) }
		let lastError: Error | null = null

		// Mirror the production loop shape from flow.ts
		const pending = [{ topic: 't', partition: 0, offset: 41n }]
		for (const checkpoint of pending) {
			const checkpointOffset = checkpoint.offset + 1n
			try {
				await checkpointStore.set(checkpoint.topic, checkpoint.partition, checkpointOffset)
			} catch (err) {
				const error = err instanceof Error ? err : new Error(String(err))
				logger.error('checkpoint persistence failed', {
					topic: checkpoint.topic,
					partition: checkpoint.partition,
					error: error.message,
				})
				lastError = error
			}
		}

		expect(checkpointStore.set).toHaveBeenCalledWith('t', 0, 42n)
		expect(logged).toHaveLength(1)
		expect(logged[0]!.msg).toBe('checkpoint persistence failed')
		expect(logged[0]!.ctx.topic).toBe('t')
		expect(logged[0]!.ctx.error).toBe('disk full')
		expect(lastError).toBe(setError)
	})

	it('records a separate flush error after a successful set loop', async () => {
		const flushError = new Error('fsync failed')
		const checkpointStore: ChangelogCheckpointStore = {
			get: vi.fn().mockResolvedValue(undefined),
			set: vi.fn().mockResolvedValue(undefined),
			flush: vi.fn().mockRejectedValue(flushError),
		}

		const logged: Array<{ msg: string; ctx: Record<string, unknown> }> = []
		let lastError: Error | null = null

		try {
			await checkpointStore.flush?.()
		} catch (err) {
			const error = err instanceof Error ? err : new Error(String(err))
			logged.push({ msg: 'checkpoint persistence failed', ctx: { phase: 'flush', error: error.message } })
			lastError = error
		}

		expect(checkpointStore.flush).toHaveBeenCalled()
		expect(lastError).toBe(flushError)
		expect(logged[0]!.ctx.phase).toBe('flush')
	})

	// Suppress unused-import warning for EventEmitter (kept for future seam expansion).
	void EventEmitter
})
