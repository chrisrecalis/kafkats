import { describe, expect, it } from 'vitest'

import type { Consumer } from '@/consumer/consumer.js'
import type { ConsumerGroupMetadata } from '@/consumer/types.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { deferred, fakeBroker, groupConsumer, TOPIC, tp, until } from './group-harness.js'

const member = (generationId: number, memberId: string): ConsumerGroupMetadata => ({
	groupId: 'g',
	generationId,
	memberId,
	groupInstanceId: null,
})

/** Start a run and wait for its first assignment. */
async function running(consumer: Consumer) {
	const assigned = new Promise(resolve => consumer.once('partitionsAssigned', resolve))
	const run = consumer.runBatch(TOPIC, async () => {}, { commitOffsets: false })
	await assigned
	return { run }
}

describe('Consumer.groupMetadata()', () => {
	it('is the last completed join, already in onPartitionsAssigned, and the new one after a rejoin', async () => {
		const broker = fakeBroker()
		const seen: Array<ConsumerGroupMetadata | undefined> = []
		const consumer: Consumer = groupConsumer(broker, {
			onPartitionsAssigned: async () => {
				seen.push(consumer.groupMetadata())
			},
		})
		expect(consumer.groupMetadata()).toBeUndefined()
		const { run } = await running(consumer)
		expect(consumer.groupMetadata()).toEqual(member(1, 'member-1'))

		const rebalanceComplete = new Promise<void>(resolve => consumer.once('rebalanceComplete', () => resolve()))
		broker.heartbeatErrors.push(ErrorCode.RebalanceInProgress)
		await rebalanceComplete

		expect(seen).toEqual([member(1, 'member-1'), member(2, 'member-1')])
		expect(consumer.groupMetadata()).toEqual(member(2, 'member-1'))

		consumer.stop()
		await run
		expect(consumer.groupMetadata()).toBeUndefined()
	})

	it('is undefined from an UNKNOWN_MEMBER_ID heartbeat until the next join completes', async () => {
		const broker = fakeBroker()
		const syncGroup = deferred()
		const consumer = groupConsumer(broker)
		const { run } = await running(consumer)

		broker.gates.syncGroup = syncGroup.promise
		broker.heartbeatErrors.push(ErrorCode.UnknownMemberId)
		await until(() => broker.calls.filter(call => call === 'SyncGroup').length === 2, 'the rejoin')
		expect(consumer.groupMetadata()).toBeUndefined()

		syncGroup.resolve()
		await until(() => consumer.groupMetadata() !== undefined, 'the join')
		expect(consumer.groupMetadata()).toEqual(member(2, 'member-2'))

		consumer.stop()
		await run
	})

	it('is undefined after an ILLEGAL_GENERATION heartbeat until the next join completes', async () => {
		const broker = fakeBroker()
		const syncGroup = deferred()
		const consumer = groupConsumer(broker)
		const { run } = await running(consumer)

		broker.gates.syncGroup = syncGroup.promise
		broker.heartbeatErrors.push(ErrorCode.IllegalGeneration)
		await until(() => consumer.groupMetadata() === undefined, 'the reset')
		await until(() => broker.calls.filter(call => call === 'SyncGroup').length === 2, 'the rejoin')
		expect(consumer.groupMetadata()).toBeUndefined()

		syncGroup.resolve()
		await until(() => consumer.groupMetadata() !== undefined, 'the join')
		expect(consumer.groupMetadata()).toEqual(member(2, 'member-1'))

		consumer.stop()
		await run
	})

	it('is undefined after ILLEGAL_GENERATION on JoinGroup until SyncGroup completed', async () => {
		const broker = fakeBroker()
		const syncGroup = deferred()
		const consumer = groupConsumer(broker)
		const { run } = await running(consumer)

		broker.joinErrors.push(ErrorCode.IllegalGeneration)
		broker.gates.syncGroup = syncGroup.promise
		broker.heartbeatErrors.push(ErrorCode.RebalanceInProgress)
		await until(() => broker.calls.filter(call => call === 'SyncGroup').length === 2, 'the rejoin')
		// The second JoinGroup answered with a new generation; the join is not complete yet.
		expect(broker.generationId).toBe(2)
		expect(consumer.groupMetadata()).toBeUndefined()

		syncGroup.resolve()
		await until(() => consumer.groupMetadata() !== undefined, 'the join')
		expect(consumer.groupMetadata()).toEqual(member(2, 'member-2'))

		consumer.stop()
		await run
	})

	it('is undefined with a manual assignment', async () => {
		const consumer = groupConsumer(fakeBroker())
		const run = consumer.runBatch(TOPIC, async () => {}, { assignment: [{ ...tp(0), offset: 0n }] })
		await new Promise<void>(resolve => consumer.once('running', () => resolve()))

		expect(consumer.groupMetadata()).toBeUndefined()

		consumer.stop()
		await run
	})
})
