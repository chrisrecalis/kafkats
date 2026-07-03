import { describe, expect, it, vi } from 'vitest'

import { ShareConsumer } from '@/share-consumer/share-consumer.js'

describe('ShareConsumer runEach shutdown ordering on a fatal loop error', () => {
	it("emits 'stopped' only after BOTH loops have settled", async () => {
		const cluster = { getLogger: () => null }
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const c = new ShareConsumer(cluster as any, { groupId: 'g1' }) as any
		c.refreshTopicIdMaps = vi.fn().mockResolvedValue(undefined)
		c.ensureCoordinator = vi.fn().mockResolvedValue(undefined)
		c.joinGroup = vi.fn().mockResolvedValue(undefined)

		// The heartbeat loop is still winding down when the fetch loop fails fatally.
		let settleHeartbeat!: () => void
		c.heartbeatLoop = vi.fn(
			() =>
				new Promise<void>(resolve => {
					settleHeartbeat = resolve
				})
		)
		c.fetchLoop = vi.fn().mockRejectedValue(new Error('fatal loop failure'))

		const stopped = vi.fn()
		c.on('stopped', stopped)
		c.on('error', () => {}) // consume the emitted fatal error

		const run = (c.runEach('t', async () => {}) as Promise<void>).catch((e: unknown) => e)

		// Let the fatal fetch-loop rejection propagate. The heartbeat loop has not settled yet, so
		// finalizeRun/leaveGroup must not have run and 'stopped' must not have been emitted —
		// otherwise the consumer reports quiescence while a loop is still active.
		await new Promise(resolve => setTimeout(resolve, 25))
		expect(stopped).not.toHaveBeenCalled()

		settleHeartbeat()
		const err = await run
		expect((err as Error).message).toBe('fatal loop failure')
		expect(stopped).toHaveBeenCalledTimes(1)
	})
})
