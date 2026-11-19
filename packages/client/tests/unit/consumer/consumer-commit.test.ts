import { describe, expect, it, vi } from 'vitest'

import { CommitFailedError, RebalanceInProgressError, TimeoutError } from '@/client/errors.js'
import type { Consumer } from '@/consumer/consumer.js'
import type { AssignedPartition } from '@/consumer/types.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { deferred, fakeBroker, groupConsumer, sleep, TOPIC, tp, until } from './group-harness.js'

/** Start a run that commits nothing by itself, and wait for its first assignment. */
async function running(consumer: Consumer) {
	const assigned = new Promise<AssignedPartition[]>(resolve => consumer.once('partitionsAssigned', resolve))
	const run = consumer.runBatch(TOPIC, async () => {}, { commitOffsets: false })
	return { run, assigned: await assigned }
}

describe('Consumer.commit() and committed()', () => {
	it('throws when the consumer is not running', async () => {
		const consumer = groupConsumer(fakeBroker())

		await expect(consumer.commit([{ ...tp(0), offset: 1n }])).rejects.toThrow('Consumer is not running')
		await expect(consumer.committed([tp(0)])).rejects.toThrow('Consumer is not running')
		expect(consumer.groupMetadata()).toBeUndefined()
		expect(consumer.rebalancePending).toBe(false)
	})

	it('commits offsets with metadata outside any handler, under the generation, and reads them back', async () => {
		const broker = fakeBroker()
		const consumer = groupConsumer(broker)
		const { run } = await running(consumer)

		await consumer.commit([{ ...tp(0), offset: 3n, metadata: 'AgAAAAAAAAAB' }])

		expect(broker.commits).toEqual([{ generationId: 1, partition: 0, offset: 3n, metadata: 'AgAAAAAAAAAB' }])
		expect(await consumer.committed([tp(0), tp(1)])).toEqual(
			new Map([
				[`${TOPIC}:0`, { offset: 3n, metadata: 'AgAAAAAAAAAB' }],
				[`${TOPIC}:1`, null],
			])
		)

		consumer.stop()
		await run
	})

	it('skips partitions that are not assigned to it', async () => {
		const broker = fakeBroker()
		const consumer = groupConsumer(broker)
		const { run } = await running(consumer)

		await consumer.commit([{ ...tp(7), offset: 3n }])

		expect(broker.calls).not.toContain('OffsetCommit')

		consumer.stop()
		await run
	})

	it('rejects with RebalanceInProgressError while the group rebalances, without rejoining', async () => {
		const broker = fakeBroker()
		const consumer = groupConsumer(broker)
		const { run } = await running(consumer)
		const rebalancePending = vi.fn()
		consumer.on('rebalancePending', rebalancePending)

		broker.commitErrors.push(ErrorCode.RebalanceInProgress)

		await expect(consumer.commit([{ ...tp(0), offset: 1n }])).rejects.toBeInstanceOf(RebalanceInProgressError)
		expect(rebalancePending).not.toHaveBeenCalled()

		consumer.stop()
		await run
	})

	it('rejects with CommitFailedError when the generation is gone, and rejoins the group', async () => {
		const broker = fakeBroker()
		const consumer = groupConsumer(broker)
		const { run } = await running(consumer)
		const rebalanceComplete = new Promise<void>(resolve => consumer.once('rebalanceComplete', () => resolve()))

		broker.commitErrors.push(ErrorCode.IllegalGeneration)

		await expect(consumer.commit([{ ...tp(0), offset: 1n }])).rejects.toBeInstanceOf(CommitFailedError)
		expect(consumer.rebalancePending).toBe(true)
		await rebalanceComplete
		expect(consumer.groupMetadata()?.generationId).toBe(2)

		consumer.stop()
		await run
	})

	it('fails a commit still retried after a rebalance, without committing under the new generation', async () => {
		const broker = fakeBroker()
		const consumer = groupConsumer(broker)
		const { run } = await running(consumer)
		const answer = deferred()
		broker.gates.offsetCommit = answer.promise
		broker.commitErrors.push(ErrorCode.NotCoordinator)

		const commit = consumer.commit([{ ...tp(0), offset: 100n, metadata: 'stale' }]).catch(e => e)
		await until(() => broker.calls.includes('OffsetCommit'), 'the first attempt')
		// While the coordinator is slow to answer, the group rebalances: the partition may have another owner now.
		const rebalanced = new Promise<void>(resolve => consumer.once('rebalanceComplete', () => resolve()))
		broker.heartbeatErrors.push(ErrorCode.RebalanceInProgress)
		await rebalanced
		answer.resolve()

		expect(await commit).toBeInstanceOf(CommitFailedError)
		expect(broker.commits).toEqual([{ generationId: 1, partition: 0, offset: 100n, metadata: 'stale' }])
		expect(broker.committed.size).toBe(0)
		// The member is in the new generation already: no other rebalance.
		await sleep(50)
		expect(broker.calls.filter(call => call === 'JoinGroup')).toHaveLength(2)
		expect(consumer.rebalancePending).toBe(false)

		consumer.stop()
		await run
	})

	it('committed() rejects with a TimeoutError once timeoutMs passed while the coordinator does not answer', async () => {
		const broker = fakeBroker()
		const consumer = groupConsumer(broker)
		const { run } = await running(consumer)
		const answer = deferred()
		broker.gates.offsetFetch = answer.promise

		const startedAt = Date.now()
		const error = await consumer.committed([tp(0)], { timeoutMs: 50 }).catch((e: unknown) => e)
		expect(error).toBeInstanceOf(TimeoutError)
		expect(Date.now() - startedAt).toBeLessThan(1_000)

		answer.resolve()
		consumer.stop()
		await run
	})

	it('gives each assigned partition its committed offset and metadata, or null', async () => {
		const broker = fakeBroker()
		broker.produce(1, 4)
		broker.committed.set(0, { offset: 2n, metadata: 'meta' })
		const onPartitionsAssigned = vi.fn(async () => {})
		const consumer = groupConsumer(broker, { onPartitionsAssigned })
		const { run, assigned } = await running(consumer)

		const expected = [
			{ ...tp(0), offset: 2n, committed: { offset: 2n, metadata: 'meta' } },
			{ ...tp(1), offset: 0n, committed: null },
		]
		expect(onPartitionsAssigned).toHaveBeenCalledWith(expected)
		expect(assigned).toEqual(expected)

		consumer.stop()
		await run
	})
})

describe('runBatch marking on return', () => {
	it('marks nothing of a batch whose handler returns after the run was stopped', async () => {
		const broker = fakeBroker()
		broker.produce(0, 3)
		const consumer = groupConsumer(broker)

		await consumer.runBatch(TOPIC, async (_messages, ctx) => {
			consumer.stop()
			// Returns early, as a handler that checks its signal between records does.
			if (ctx.signal.aborted) return
		})

		expect(broker.commits).toEqual([])
	})

	it('commits what the handler resolved itself before the run was stopped', async () => {
		const broker = fakeBroker()
		broker.produce(0, 3)
		const consumer = groupConsumer(broker)

		await consumer.runBatch(TOPIC, async (messages, ctx) => {
			ctx.resolveOffset(messages[0]!.offset)
			consumer.stop()
		})

		expect(broker.commits).toEqual([{ generationId: 1, partition: 0, offset: 1n, metadata: null }])
	})

	it('marks the whole batch of a handler that returns while the run goes on', async () => {
		const broker = fakeBroker()
		broker.produce(0, 3)
		const consumer = groupConsumer(broker)
		let batches = 0

		const run = consumer.runBatch(TOPIC, async () => {
			batches++
		})
		await until(() => batches === 1, 'the batch')
		consumer.stop()
		await run

		expect(broker.commits).toEqual([{ generationId: 1, partition: 0, offset: 3n, metadata: null }])
	})
})
