import { describe, expect, it, vi } from 'vitest'

import type { Consumer } from '@/consumer/consumer.js'
import type { ConsumerEvents, TopicPartition } from '@/consumer/types.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { deferred, fakeBroker, groupConsumer, sleep, TOPIC, until, type FakeBroker } from './group-harness.js'

const partitionsOf = (partitions: TopicPartition[]) => partitions.map(tp => tp.partition).join(',')
const notFetch = (call: string) => !call.startsWith('Fetch')

/** Record the consumer's rebalance events on the broker's timeline. */
function recordEvents(consumer: Consumer, broker: FakeBroker): void {
	const events: Array<keyof ConsumerEvents> = [
		'rebalancePending',
		'rebalance',
		'partitionsRevoked',
		'partitionsLost',
		'partitionsAssigned',
		'rebalanceComplete',
	]
	for (const event of events) consumer.on(event, () => broker.calls.push(event))
}

/** Start a batch run that commits nothing by itself, and wait until it delivered `records` records. */
async function started(consumer: Consumer, records: number) {
	const delivered: string[] = []
	const run = consumer.runBatch(
		TOPIC,
		async messages => {
			delivered.push(...messages.map(message => `${message.partition}:${message.offset}`))
		},
		{ commitOffsets: false }
	)
	await until(() => delivered.length === records, 'the first records')
	return { run, delivered }
}

describe('Consumer rebalance hooks', () => {
	it('awaits onPartitionsAssigned of the first join before the consumer runs and before any fetch', async () => {
		const broker = fakeBroker()
		broker.produce(0, 1)
		const gate = deferred()
		const consumer = groupConsumer(broker, {
			onPartitionsAssigned: async partitions => {
				broker.calls.push(`assigned ${partitionsOf(partitions)}`)
				await gate.promise
			},
		})
		recordEvents(consumer, broker)
		consumer.on('running', () => broker.calls.push('running'))
		const run = consumer.runBatch(TOPIC, async () => {}, { commitOffsets: false })

		await until(() => broker.calls.includes('assigned 0,1'), 'the assignment hook')
		await sleep(30)
		expect(broker.calls).toEqual(['JoinGroup', 'SyncGroup', 'OffsetFetch', 'assigned 0,1'])

		gate.resolve()
		await until(() => broker.calls.includes('Fetch 0,1'), 'the first fetch')
		expect(broker.calls.slice(0, 7)).toEqual([
			'JoinGroup',
			'SyncGroup',
			'OffsetFetch',
			'assigned 0,1',
			'partitionsAssigned',
			'running',
			'Fetch 0,1',
		])

		consumer.stop()
		await run
	})

	it('runs an eager rebalance in order: the revocation (and its commit) before JoinGroup, the assignment before any fetch', async () => {
		const broker = fakeBroker()
		broker.produce(0, 1)
		broker.produce(1, 1)
		const assignedGate = deferred()
		let rebalanced = false
		const consumer: Consumer = groupConsumer(broker, {
			onBeforeRebalance: async () => {
				broker.calls.push('onBeforeRebalance')
			},
			onPartitionsRevoked: async partitions => {
				broker.calls.push(`revoked ${partitionsOf(partitions)} pending=${consumer.rebalancePending}`)
				await consumer.commit(partitions.map(tp => ({ ...tp, offset: 1n, metadata: 'm' })))
			},
			onPartitionsAssigned: async partitions => {
				broker.calls.push(`assigned ${partitionsOf(partitions)}`)
				if (rebalanced) await assignedGate.promise
			},
		})
		recordEvents(consumer, broker)
		const { run } = await started(consumer, 2)

		rebalanced = true
		broker.calls.length = 0
		broker.heartbeatErrors.push(ErrorCode.RebalanceInProgress)
		await until(() => broker.calls.includes('assigned 0,1'), 'the assignment hook')
		await sleep(30)
		// Held while the hook runs: nothing is fetched.
		expect(broker.calls.slice(broker.calls.indexOf('assigned 0,1')).filter(call => !notFetch(call))).toEqual([])
		expect(consumer.rebalancePending).toBe(true)

		assignedGate.resolve()
		await until(() => broker.calls.includes('rebalanceComplete'), 'the end of the rebalance')
		await until(() => broker.calls.includes('Fetch 0,1'), 'a fetch')

		expect(broker.calls.filter(notFetch)).toEqual([
			'rebalancePending',
			'onBeforeRebalance',
			'rebalance',
			'revoked 0,1 pending=true',
			'OffsetCommit',
			'partitionsRevoked',
			'JoinGroup',
			'SyncGroup',
			'OffsetFetch',
			'assigned 0,1',
			'partitionsAssigned',
			'rebalanceComplete',
		])
		// Committed under the generation that owned the partitions, before the member rejoined.
		expect(broker.commits).toEqual([
			{ generationId: 1, partition: 0, offset: 1n, metadata: 'm' },
			{ generationId: 1, partition: 1, offset: 1n, metadata: 'm' },
		])
		expect(consumer.rebalancePending).toBe(false)

		consumer.stop()
		await run
	})

	it('invokes onPartitionsLost synchronously, rejoins once it resolved, and revokes nothing afterwards', async () => {
		const broker = fakeBroker()
		broker.produce(0, 1)
		broker.produce(1, 1)
		const lostGate = deferred()
		const onPartitionsRevoked = vi.fn(async () => {})
		const consumer: Consumer = groupConsumer(broker, {
			onPartitionsRevoked,
			onPartitionsLost: async partitions => {
				broker.calls.push(`lost ${partitionsOf(partitions)} metadata=${String(consumer.groupMetadata())}`)
				await lostGate.promise
			},
			onPartitionsAssigned: async partitions => {
				broker.calls.push(`assigned ${partitionsOf(partitions)}`)
			},
		})
		recordEvents(consumer, broker)
		const { run } = await started(consumer, 2)

		// Registered after the consumer's own listener: by the time it runs, the hook's synchronous part ran.
		const group = (consumer as unknown as { consumerGroup: NodeJS.EventEmitter }).consumerGroup
		group.on('sessionLost', () => broker.calls.push('sessionLost returned'))
		broker.calls.length = 0
		broker.heartbeatErrors.push(ErrorCode.UnknownMemberId)
		await until(() => broker.calls.includes('sessionLost returned'), 'the loss')
		await sleep(30)
		expect(broker.calls).not.toContain('JoinGroup')

		lostGate.resolve()
		await until(() => broker.calls.includes('rebalanceComplete'), 'the end of the rebalance')

		expect(broker.calls.filter(notFetch)).toEqual([
			'lost 0,1 metadata=undefined',
			'partitionsLost',
			'sessionLost returned',
			'rebalancePending',
			'rebalance',
			'JoinGroup',
			'SyncGroup',
			'OffsetFetch',
			'assigned 0,1',
			'partitionsAssigned',
			'rebalanceComplete',
		])
		expect(onPartitionsRevoked).not.toHaveBeenCalled()
		expect(consumer.groupMetadata()).toEqual({
			groupId: 'g',
			generationId: 2,
			memberId: 'member-2',
			groupInstanceId: null,
		})

		consumer.stop()
		await run
	})

	it('does not report partitions lost while onPartitionsAssigned ran as assigned', async () => {
		const broker = fakeBroker()
		const assignedGate = deferred()
		let joins = 0
		const consumer: Consumer = groupConsumer(broker, {
			onPartitionsAssigned: async partitions => {
				broker.calls.push(`assigned ${partitionsOf(partitions)}`)
				if (++joins === 1) await assignedGate.promise
			},
		})
		recordEvents(consumer, broker)
		const positions: Array<bigint | undefined> = []
		consumer.on('partitionsAssigned', partitions => {
			positions.push(...partitions.map(tp => consumer.position(tp.topic, tp.partition)))
		})
		const run = consumer.runBatch(TOPIC, async () => {}, { commitOffsets: false })

		await until(() => broker.calls.includes('assigned 0,1'), 'the assignment hook')
		broker.heartbeatErrors.push(ErrorCode.UnknownMemberId)
		await until(() => broker.calls.includes('partitionsLost'), 'the loss')
		assignedGate.resolve()
		await until(() => broker.calls.includes('rebalanceComplete'), 'the rejoin')

		expect(broker.calls.filter(notFetch)).toEqual([
			'JoinGroup',
			'SyncGroup',
			'OffsetFetch',
			'assigned 0,1',
			'partitionsLost',
			'rebalancePending',
			'rebalance',
			'JoinGroup',
			'SyncGroup',
			'OffsetFetch',
			'assigned 0,1',
			'partitionsAssigned',
			'rebalanceComplete',
		])
		expect(positions).toEqual([0n, 0n])

		consumer.stop()
		await run
	})

	it('completes one rebalance while heartbeats keep reporting it during a slow revocation', async () => {
		const broker = fakeBroker()
		broker.produce(0, 1)
		const revocation = deferred()
		const consumer = groupConsumer(broker, {
			onPartitionsRevoked: () => revocation.promise,
		})
		recordEvents(consumer, broker)
		const { run } = await started(consumer, 1)

		broker.calls.length = 0
		broker.heartbeatErrors.push(...Array<ErrorCode>(5).fill(ErrorCode.RebalanceInProgress))
		// The revocation lasts until every heartbeat answer reporting the rebalance was read.
		await until(() => broker.heartbeatErrors.length === 0, 'the heartbeats')
		revocation.resolve()
		await until(() => broker.calls.includes('rebalanceComplete'), 'the end of the rebalance')
		await sleep(100)

		expect(broker.heartbeatErrors).toEqual([])
		const count = (call: string) => broker.calls.filter(entry => entry === call).length
		expect(count('JoinGroup')).toBe(1)
		expect(count('rebalancePending')).toBe(1)
		expect(count('rebalanceComplete')).toBe(1)

		consumer.stop()
		await run
	})

	it('completes a cooperative rebalance that moves no partition without calling the hooks', async () => {
		const broker = fakeBroker()
		broker.produce(0, 1)
		const onPartitionsRevoked = vi.fn(async () => {})
		const onPartitionsAssigned = vi.fn(async () => {})
		const consumer = groupConsumer(broker, {
			partitionAssignmentStrategy: 'cooperative-sticky',
			onPartitionsRevoked,
			onPartitionsAssigned,
		})
		recordEvents(consumer, broker)
		const { run } = await started(consumer, 1)
		expect(onPartitionsAssigned).toHaveBeenCalledOnce()

		broker.calls.length = 0
		broker.heartbeatErrors.push(ErrorCode.RebalanceInProgress)
		await until(() => broker.calls.includes('rebalanceComplete'), 'the end of the rebalance')

		expect(broker.calls.filter(notFetch)).toEqual([
			'rebalancePending',
			'rebalance',
			'JoinGroup',
			'SyncGroup',
			'rebalanceComplete',
		])
		expect(onPartitionsRevoked).not.toHaveBeenCalled()
		expect(onPartitionsAssigned).toHaveBeenCalledOnce()

		consumer.stop()
		await run
	})

	it('keeps a partition paused in onPartitionsAssigned from being fetched until it is resumed', async () => {
		const broker = fakeBroker()
		broker.produce(0, 2)
		broker.produce(1, 2)
		const consumer: Consumer = groupConsumer(broker, {
			onPartitionsAssigned: async () => {
				consumer.pause([{ topic: TOPIC, partition: 1 }])
			},
		})
		const { run, delivered } = await started(consumer, 2)
		await sleep(50)

		expect(delivered).toEqual(['0:0', '0:1'])
		expect(broker.calls.filter(call => !notFetch(call) && call !== 'Fetch 0')).toEqual([])

		consumer.resume([{ topic: TOPIC, partition: 1 }])
		await until(() => delivered.length === 4, 'the resumed partition')
		expect(delivered).toEqual(['0:0', '0:1', '1:0', '1:1'])

		consumer.stop()
		await run
	})

	describe('a hook that rejects fails the run', () => {
		const failure = new Error('hook failed')

		it('onPartitionsAssigned on the first join', async () => {
			const broker = fakeBroker()
			const consumer = groupConsumer(broker, { onPartitionsAssigned: () => Promise.reject(failure) })
			consumer.on('error', () => {})

			await expect(consumer.runBatch(TOPIC, async () => {})).rejects.toBe(failure)
		})

		it('onBeforeRebalance: the member leaves the group instead of joining again', async () => {
			const broker = fakeBroker()
			broker.produce(0, 1)
			const consumer = groupConsumer(broker, { onBeforeRebalance: () => Promise.reject(failure) })
			consumer.on('error', () => {})
			const { run } = await started(consumer, 1)
			broker.calls.length = 0

			broker.heartbeatErrors.push(ErrorCode.RebalanceInProgress)

			await expect(run).rejects.toBe(failure)
			expect(broker.calls).toContain('LeaveGroup')
			expect(broker.calls).not.toContain('JoinGroup')
		})

		it('onPartitionsRevoked', async () => {
			const broker = fakeBroker()
			broker.produce(0, 1)
			const consumer = groupConsumer(broker, { onPartitionsRevoked: () => Promise.reject(failure) })
			consumer.on('error', () => {})
			const { run } = await started(consumer, 1)

			broker.heartbeatErrors.push(ErrorCode.RebalanceInProgress)

			await expect(run).rejects.toBe(failure)
			expect(broker.calls.filter(call => call === 'JoinGroup')).toHaveLength(1)
		})

		it('onPartitionsLost, before the member rejoins', async () => {
			const broker = fakeBroker()
			broker.produce(0, 1)
			const consumer = groupConsumer(broker, { onPartitionsLost: () => Promise.reject(failure) })
			consumer.on('error', () => {})
			const { run } = await started(consumer, 1)

			broker.heartbeatErrors.push(ErrorCode.UnknownMemberId)

			await expect(run).rejects.toBe(failure)
			expect(broker.calls.filter(call => call === 'JoinGroup')).toHaveLength(1)
		})
	})
})
