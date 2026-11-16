import { describe, expect, it, vi } from 'vitest'

import { NoOffsetForPartitionError } from '@/client/errors.js'
import type { AssignedPartition, AutoOffsetReset, OffsetResetEvent, TopicPartitionOffset } from '@/consumer/types.js'
import { fakeBroker, groupConsumer, TOPIC, tp, until } from './group-harness.js'

describe('Consumer offset reset policy', () => {
	it('evaluates a policy function for each partition without a committed offset, and reports each reset', async () => {
		const broker = fakeBroker({ partitions: 3 })
		broker.produce(0, 3)
		broker.produce(1, 5)
		broker.produce(2, 4)
		broker.committed.set(2, { offset: 1n, metadata: null })
		const policy = vi.fn((_topic: string, partition: number): AutoOffsetReset =>
			partition === 0 ? 'earliest' : 'latest'
		)
		const consumer = groupConsumer(broker, { autoOffsetReset: policy })
		const resets: OffsetResetEvent[] = []
		consumer.on('offsetReset', reset => resets.push(reset))
		const assigned = new Promise<AssignedPartition[]>(resolve => consumer.once('partitionsAssigned', resolve))

		const run = consumer.runBatch(TOPIC, async () => {}, { commitOffsets: false })

		expect((await assigned).map(partition => partition.offset)).toEqual([0n, 5n, 1n])
		expect(policy.mock.calls).toEqual([
			[TOPIC, 0],
			[TOPIC, 1],
		])
		expect(resets).toEqual([
			{ ...tp(0), from: null, to: 0n, reason: 'no-commit' },
			{ ...tp(1), from: null, to: 5n, reason: 'no-commit' },
		])

		consumer.stop()
		await run
	})

	it("fails the run with NoOffsetForPartitionError for a partition whose policy is 'none' and that has no committed offset", async () => {
		const broker = fakeBroker()
		broker.committed.set(0, { offset: 0n, metadata: null })
		const consumer = groupConsumer(broker, { autoOffsetReset: 'none' })
		consumer.on('error', () => {})

		const failure = await consumer.runBatch(TOPIC, async () => {}).catch((error: unknown) => error)

		expect(failure).toBeInstanceOf(NoOffsetForPartitionError)
		expect(failure).toMatchObject({ topic: TOPIC, partition: 1 })
	})

	it("pauses a partition whose position is out of range under 'none', reports it, and goes on", async () => {
		const broker = fakeBroker()
		broker.produce(0, 5)
		broker.produce(1, 2)
		broker.committed.set(0, { offset: 1n, metadata: null })
		broker.committed.set(1, { offset: 0n, metadata: null })
		broker.deleteBefore(0, 3n)
		const consumer = groupConsumer(broker, { autoOffsetReset: 'none' })
		const outOfRange: TopicPartitionOffset[] = []
		consumer.on('offsetOutOfRange', position => outOfRange.push(position))
		const delivered: string[] = []
		const run = consumer.runBatch(
			TOPIC,
			async messages => {
				delivered.push(...messages.map(message => `${message.partition}:${message.offset}`))
			},
			{ commitOffsets: false }
		)

		await until(() => outOfRange.length === 1 && delivered.length === 2, 'the out-of-range position')
		expect(outOfRange).toEqual([{ ...tp(0), offset: 1n }])
		expect(delivered).toEqual(['1:0', '1:1'])
		expect(consumer.metrics().partitions.find(partition => partition.partition === 0)?.paused).toBe(true)

		consumer.seek(TOPIC, 0, 3n)
		consumer.resume([tp(0)])
		await until(() => delivered.length === 4, 'the sought partition')
		expect(delivered.slice(2)).toEqual(['0:3', '0:4'])

		consumer.stop()
		await run
	})

	it("moves an out-of-range position under 'earliest' and reports the reset", async () => {
		const broker = fakeBroker()
		broker.produce(0, 5)
		broker.committed.set(0, { offset: 1n, metadata: null })
		broker.deleteBefore(0, 3n)
		const consumer = groupConsumer(broker, { autoOffsetReset: 'earliest' })
		const resets: OffsetResetEvent[] = []
		consumer.on('offsetReset', reset => resets.push(reset))
		const delivered: bigint[] = []
		const run = consumer.runBatch(
			TOPIC,
			async messages => {
				delivered.push(...messages.map(message => message.offset))
			},
			{ commitOffsets: false }
		)

		await until(() => delivered.length === 2, 'the records after the reset')
		expect(delivered).toEqual([3n, 4n])
		expect(resets).toContainEqual({ ...tp(0), from: 1n, to: 3n, reason: 'out-of-range' })

		consumer.stop()
		await run
	})
})
