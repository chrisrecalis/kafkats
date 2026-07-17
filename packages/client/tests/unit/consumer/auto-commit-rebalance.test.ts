import { describe, expect, it, vi } from 'vitest'

import { Consumer } from '@/consumer/consumer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { KafkaProtocolError } from '@/client/errors.js'

// An auto-commit that fails with REBALANCE_IN_PROGRESS must NOT drop the consumed
// offsets: they are retried by the revoke-time commit during the rejoin. Wiping them
// here means the revoke-time commit has nothing to commit, so the next owner resumes
// from a stale offset and reprocesses records.
function buildConsumer() {
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const cluster = { getLogger: () => null } as any
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const consumer = new Consumer(cluster, { groupId: 'g' } as any)
	// Swallow 'error' events emitted for generation-lost codes.
	consumer.on('error', () => {})
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const consumerAny = consumer as any

	const offsetManager = { clearConsumedOffsets: vi.fn() }
	const consumerGroup = { emit: vi.fn() }
	consumerAny.offsetManager = offsetManager
	consumerAny.consumerGroup = consumerGroup

	return { consumerAny, offsetManager, consumerGroup }
}

describe('Consumer auto-commit error handling', () => {
	it('does NOT clear consumed offsets on RebalanceInProgress (retried at revoke time)', () => {
		const { consumerAny, offsetManager, consumerGroup } = buildConsumer()

		consumerAny.handleAutoCommitError(new KafkaProtocolError(ErrorCode.RebalanceInProgress, 'rebalancing'))

		expect(offsetManager.clearConsumedOffsets).not.toHaveBeenCalled()
		expect(consumerGroup.emit).toHaveBeenCalledWith('rebalance')
	})

	it('clears consumed offsets on generation-lost codes (cannot be committed anymore)', () => {
		const { consumerAny, offsetManager, consumerGroup } = buildConsumer()

		consumerAny.handleAutoCommitError(new KafkaProtocolError(ErrorCode.IllegalGeneration, 'stale generation'))

		expect(offsetManager.clearConsumedOffsets).toHaveBeenCalledTimes(1)
		expect(consumerGroup.emit).toHaveBeenCalledWith('rebalance')
	})
})
