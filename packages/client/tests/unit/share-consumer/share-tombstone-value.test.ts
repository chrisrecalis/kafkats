import { describe, expect, expectTypeOf, it, vi } from 'vitest'

import { ShareConsumer } from '@/share-consumer/share-consumer.js'
import type { ShareMessage } from '@/share-consumer/types.js'

const TOPIC_ID = '00000000-0000-0000-0000-000000000001'

describe('ShareMessage tombstone values', () => {
	it('types value as V | null (tombstones carry a null value)', () => {
		expectTypeOf<ShareMessage<string, string>['value']>().toEqualTypeOf<string | null>()
		expectTypeOf<ShareMessage['value']>().toEqualTypeOf<Buffer | null>()
	})

	it('delivers a tombstone with value === null without invoking the typed value decoder', async () => {
		const cluster = { getLogger: () => null }
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const sc = new ShareConsumer(cluster as any, { groupId: 'g1' }) as any
		sc.abortController = new AbortController()
		sc.state = 'running'

		const decoder = vi.fn((b: Buffer) => b.toString('utf-8'))
		const item = {
			topicName: 't',
			topicId: TOPIC_ID,
			partitionIndex: 0,
			record: { offset: 0n, timestamp: 0n, key: Buffer.from('k'), value: null, headers: [] },
			keyDecoder: (b: Buffer) => b,
			decoder,
		}

		let seen: unknown = 'unset'
		const handler = async (message: ShareMessage<unknown, unknown>) => {
			seen = message.value
		}

		const ackManager = { enqueue: vi.fn().mockResolvedValue(undefined) }
		await expect(sc.processShareFetchWorkItem(item, handler, ackManager, true)).resolves.toBe(1)

		expect(seen).toBeNull()
		expect(decoder).not.toHaveBeenCalled()
	})
})
