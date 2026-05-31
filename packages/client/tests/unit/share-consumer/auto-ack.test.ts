import { describe, it, expect, vi } from 'vitest'

import { ShareConsumer } from '@/share-consumer/share-consumer.js'

const TOPIC_ID = '00000000-0000-0000-0000-000000000001'

describe('ShareConsumer auto-ack failure handling', () => {
	it('does not tear down the consumer when an auto-ack fails; logs and lets the record redeliver', async () => {
		const warn = vi.fn()
		const logger = { warn, info: vi.fn(), error: vi.fn(), debug: vi.fn(), child: () => logger }
		const cluster = { getLogger: () => logger }
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const sc = new ShareConsumer(cluster as any, { groupId: 'g' })
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const scAny = sc as any
		scAny.abortController = new AbortController()
		scAny.state = 'running'

		// The auto-ack enqueue fails (e.g. the acquisition lock expired -> InvalidRecordState).
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

		// A failed auto-ack must NOT throw (which would kill the fetch loop). The record will be
		// redelivered after its acquisition lock expires; the failure is surfaced via the log.
		await expect(scAny.processShareFetchWorkItem(item, handler, ackManager, true)).resolves.toBe(1)
		expect(ackManager.enqueue).toHaveBeenCalledTimes(1)
		expect(warn).toHaveBeenCalledTimes(1)
	})
})
