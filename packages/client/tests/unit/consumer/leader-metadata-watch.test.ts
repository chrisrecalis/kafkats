import { describe, expect, it } from 'vitest'

import type { TopicPartition } from '@/consumer/types.js'
import { fakeBroker, groupConsumer, sleep, TOPIC, until } from './group-harness.js'

const partitionsOf = (partitions: TopicPartition[]) => partitions.map(tp => tp.partition).join(',')
const joins = (calls: string[]) => calls.filter(call => call === 'JoinGroup').length

// Kafka's ConsumerCoordinator: the leader keeps the metadata it assigned with and rejoins once the partitions of the
// subscribed topics differ from it (rejoinNeededOrPending, "cached metadata has changed").
describe('ConsumerGroup leader metadata watch', () => {
	it('rejoins once metadata shows a subscribed topic it did not show at the assignment', async () => {
		// A topic created moments before the consumer joined: the leader assigns nothing, then must not wait forever.
		const broker = fakeBroker({ topicVisible: false })
		const consumer = groupConsumer(broker)
		const assigned: string[] = []
		consumer.on('partitionsAssigned', partitions => assigned.push(partitionsOf(partitions)))
		consumer.on('running', () => broker.calls.push('running'))
		const run = consumer.runEach(TOPIC, async () => {}, { autoCommit: false })
		await until(() => broker.calls.includes('running'), 'the first (empty) assignment')
		expect(assigned).toEqual([])
		const refreshes = broker.metadataRefreshes
		await sleep(50)
		expect(broker.metadataRefreshes).toBeGreaterThan(refreshes) // looked up again at the heartbeats
		expect(joins(broker.calls)).toBe(1)

		broker.showTopic()
		await until(() => assigned.includes('0,1'), 'the assignment of the topic')
		expect(joins(broker.calls)).toBe(2)

		consumer.stop()
		await run
	})

	it('rejoins once a metadata refresh shows partitions added to a subscribed topic', async () => {
		const broker = fakeBroker()
		const consumer = groupConsumer(broker)
		const assigned: string[] = []
		consumer.on('partitionsAssigned', partitions => assigned.push(partitionsOf(partitions)))
		const run = consumer.runEach(TOPIC, async () => {}, { autoCommit: false })
		await until(() => assigned.length === 1, 'the first assignment')
		expect(assigned).toEqual(['0,1'])

		broker.addPartition()
		await sleep(50)
		// Unchanged metadata: no lookup and no rejoin until a refresh brings the change.
		expect(joins(broker.calls)).toBe(1)
		await broker.cluster.refreshMetadata()
		await until(() => assigned.includes('0,1,2'), 'the assignment of the added partition')
		expect(joins(broker.calls)).toBe(2)

		consumer.stop()
		await run
	})

	it('keeps heartbeating while a lookup of a missing topic hangs, and starts no second one meanwhile', async () => {
		const broker = fakeBroker({ topicVisible: false })
		const consumer = groupConsumer(broker)
		consumer.on('running', () => broker.calls.push('running'))
		const run = consumer.runEach(TOPIC, async () => {}, { autoCommit: false })
		await until(() => broker.calls.includes('running'), 'the first (empty) assignment')
		let lookups = 0
		const cluster = broker.cluster as unknown as { refreshMetadata: () => Promise<never> }
		cluster.refreshMetadata = () => {
			lookups++
			return new Promise<never>(() => {})
		}
		const heartbeats = broker.heartbeats

		await sleep(100) // about ten heartbeat intervals
		expect(broker.heartbeats - heartbeats).toBeGreaterThanOrEqual(5)
		expect(lookups).toBe(1)

		consumer.stop()
		await run
	})

	it('neither looks up metadata nor rejoins while the assigned topics are unchanged', async () => {
		const broker = fakeBroker()
		const consumer = groupConsumer(broker)
		const run = consumer.runEach(TOPIC, async () => {}, { autoCommit: false })
		await until(() => broker.calls.includes('SyncGroup'), 'the join')
		const refreshes = broker.metadataRefreshes

		await sleep(100) // about ten heartbeats
		expect(joins(broker.calls)).toBe(1)
		expect(broker.metadataRefreshes).toBe(refreshes)

		consumer.stop()
		await run
	})
})
