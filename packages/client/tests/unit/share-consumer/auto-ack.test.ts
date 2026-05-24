import { describe, it, expect, vi } from 'vitest'

import { ShareConsumer } from '@/share-consumer/share-consumer.js'

const TOPIC_ID = '00000000-0000-0000-0000-000000000001'

describe('ShareConsumer auto-ack failure handling', () => {
	it('surfaces an auto-ack failure instead of swallowing it', async () => {
		const cluster = { getLogger: () => null }
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const sc = new ShareConsumer(cluster as any, { groupId: 'g' })
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const scAny = sc as any
		scAny.abortController = new AbortController()
		scAny.state = 'running'

		// The auto-ack enqueue fails (e.g. ShareAcknowledge exhausted its retries).
		const ackManager = { enqueue: vi.fn().mockRejectedValue(new Error('ack flush failed')) }

		const item = {
			topicName: 't',
			topicId: TOPIC_ID,
			partitionIndex: 0,
			record: { offset: 0n, timestamp: 0n, key: null, value: Buffer.from('v'), headers: [] },
			keyDecoder: (b: Buffer | null) => b,
			decoder: (b: Buffer) => b.toString('utf-8'),
		}
		// A handler that does not ack/release/reject leaves the message for auto-ack.
		const handler = async () => {}

		// autoAckOnSuccess = true: the framework must surface the failed auto-ack rather
		// than silently dropping it (which would leave the record to be redelivered with
		// no signal to the application).
		await expect(scAny.processShareFetchWorkItem(item, handler, ackManager, true)).rejects.toThrow(
			'ack flush failed'
		)
	})
})
