import { describe, it, expect, vi } from 'vitest'

import type { Broker } from '@/client/broker.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { noopLogger } from '@/logger.js'

import { ShareConsumer } from '@/share-consumer/share-consumer.js'
import { AckManager, ACK_ACCEPT } from '@/share-consumer/ack-manager.js'

const TOPIC_ID = '00000000-0000-0000-0000-000000000001'
const TOPIC_NAME = 't'

const settledState = (p: Promise<void>) =>
	p.then(
		() => 'resolved' as const,
		() => 'rejected' as const
	)

describe('AckManager promise settlement when the between-attempts metadata refresh fails', () => {
	it('settles (rejects) the enqueue promise instead of leaking it', async () => {
		// First attempt: leader resolution fails, queueing the partition for retry. The metadata
		// refresh before the second attempt then ALSO fails. The enqueued ack promise must still
		// settle — leaking it deadlocks callers awaiting ack() and stop().
		const resolveLeader = vi.fn().mockRejectedValue(new Error('leader lookup failed'))
		const refreshMetadata = vi.fn().mockRejectedValue(new Error('metadata refresh failed'))
		const sendAcknowledge = vi.fn()

		const ackManager = new AckManager(
			'g1',
			() => 'm1',
			sendAcknowledge,
			resolveLeader,
			refreshMetadata,
			() => undefined,
			noopLogger,
			1000
		)

		const settled = settledState(ackManager.enqueue(TOPIC_NAME, TOPIC_ID, 0, 0n, ACK_ACCEPT))

		await ackManager.flushAll().catch(() => undefined)

		const outcome = await Promise.race([
			settled,
			new Promise<'unsettled'>(resolve => setTimeout(() => resolve('unsettled'), 500)),
		])
		expect(outcome).toBe('rejected')
		expect(refreshMetadata).toHaveBeenCalledTimes(1)
	})
})

describe('AckManager share-session error handling (a ShareAcknowledge can never open a session)', () => {
	function makeSessionErrorBroker(requests: Array<{ shareSessionEpoch: number }>) {
		return {
			nodeId: 1,
			shareAcknowledge: vi.fn(async (req: { shareSessionEpoch: number }) => {
				requests.push(req)
				return {
					throttleTimeMs: 0,
					errorCode: ErrorCode.ShareSessionNotFound,
					errorMessage: null,
					acquisitionLockTimeoutMs: 0,
					topics: [],
					nodeEndpoints: [],
				}
			}),
		}
	}

	it('fails fast on a session error instead of retrying with shareSessionEpoch=0', async () => {
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const consumer = new ShareConsumer({ getLogger: () => null } as any, { groupId: 'g1' }) as any
		consumer.shareSessionEpochByBrokerId.set(1, 5)

		const requests: Array<{ shareSessionEpoch: number }> = []
		const broker = makeSessionErrorBroker(requests)

		const ackManager = new AckManager(
			'g1',
			() => 'm1',
			(_b, req) => consumer.shareAcknowledge(broker, req),
			async () => broker as unknown as Broker,
			async () => undefined,
			id => consumer.resetShareSessionEpoch(id),
			noopLogger,
			1000
		)

		const settled = settledState(ackManager.enqueue(TOPIC_NAME, TOPIC_ID, 0, 0n, ACK_ACCEPT))
		await ackManager.flushAll().catch(() => undefined)

		// The ack fails (the record redelivers after the lock timeout, by design)...
		expect(await settled).toBe('rejected')
		// ...and there is NO second attempt: per KIP-932 a ShareAcknowledge cannot open a session,
		// so a retry after the session reset would carry shareSessionEpoch=0 and the broker would
		// always reject it with InvalidShareSessionEpoch.
		expect(requests).toHaveLength(1)
		expect(requests[0]?.shareSessionEpoch).toBe(5)
		// The session epoch was still reset so the next ShareFetch re-establishes the session.
		expect(consumer.shareSessionEpochByBrokerId.get(1)).toBe(0)
	})
})
