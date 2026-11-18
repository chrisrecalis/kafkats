import { describe, expect, it } from 'vitest'

import { KafkaClient } from '@/client/index.js'
import type { TopicPartition } from '@/consumer/types.js'

import { getLogLevel, requireKafkaBrokers } from '../helpers/kafka.js'
import { sleep, uniqueName } from '../helpers/testkit.js'

/** A client whose metadata refreshes every second, so that the leader's metadata watch sees changes soon. */
function watchingClient(clientId: string): KafkaClient {
	return new KafkaClient({
		brokers: requireKafkaBrokers(),
		clientId,
		logLevel: getLogLevel(),
		metadataRefreshIntervalMs: 1000,
	})
}

async function until(condition: () => boolean, message: string, timeoutMs = 30_000): Promise<void> {
	const deadline = Date.now() + timeoutMs
	while (!condition()) {
		if (Date.now() > deadline) throw new Error(`timed out waiting for ${message}`)
		await sleep(50)
	}
}

const topicsOf = (partitions: readonly TopicPartition[]) => new Set(partitions.map(tp => tp.topic))

// Kafka's ConsumerCoordinator: the leader watches the metadata of the whole group's subscription and rejoins once
// the partition counts it assigned with change.
describe.concurrent('Consumer (integration) - the leader metadata watch', () => {
	it.each(['range', 'cooperative-sticky'] as const)(
		'%s: deleting a subscribed topic whose consumed offsets are not committed yet does not end the consumer',
		async strategy => {
			const client = watchingClient(`it-watch-delete-${strategy}`)
			await client.connect()
			const kept = uniqueName('it-watch-kept')
			const deleted = uniqueName('it-watch-deleted')
			await client.createTopics([
				{ name: kept, numPartitions: 1, replicationFactor: 1 },
				{ name: deleted, numPartitions: 1, replicationFactor: 1 },
			])
			const producer = client.producer({ lingerMs: 0 })
			await producer.send(kept, [{ value: Buffer.from('1') }])
			await producer.send(deleted, [{ value: Buffer.from('1') }])

			const consumer = client.consumer({
				groupId: uniqueName('it-watch-delete'),
				autoOffsetReset: 'earliest',
				partitionAssignmentStrategy: strategy,
			})
			const consumed: string[] = []
			let assigned = new Set<string>()
			consumer.on('partitionsAssigned', partitions => {
				assigned = strategy === 'range' ? topicsOf(partitions) : new Set([...assigned, ...topicsOf(partitions)])
			})
			consumer.on('partitionsRevoked', partitions => {
				for (const topic of topicsOf(partitions)) assigned.delete(topic)
			})
			let failed: unknown
			const run = consumer
				.runEach(
					[kept, deleted],
					async message => {
						consumed.push(`${message.topic === kept ? 'kept' : 'deleted'}:${message.value?.toString()}`)
					},
					{ autoCommit: false }
				)
				.catch((error: unknown) => {
					failed = error
				})
			try {
				await until(() => consumed.length === 2, 'the first records')
				await client.admin().deleteTopics([deleted])
				// The leader sees the deletion at a metadata refresh and the group rebalances without the topic.
				await until(() => !assigned.has(deleted), 'the rebalance without the deleted topic')
				await producer.send(kept, [{ value: Buffer.from('2') }])
				await until(() => consumed.includes('kept:2'), 'a record after the rebalance')
				expect(failed).toBeUndefined()
			} finally {
				consumer.stop()
				await run
				await producer.disconnect()
				await client.disconnect()
			}
			expect(failed).toBeUndefined()
		},
		60_000
	)

	it('the leader assigns a topic only another member subscribes to', async () => {
		const leaderClient = watchingClient('it-watch-subscriptions-leader')
		const otherClient = watchingClient('it-watch-subscriptions-other')
		await Promise.all([leaderClient.connect(), otherClient.connect()])
		const shared = uniqueName('it-watch-shared')
		const own = uniqueName('it-watch-own')
		await leaderClient.createTopics([
			{ name: shared, numPartitions: 2, replicationFactor: 1 },
			{ name: own, numPartitions: 2, replicationFactor: 1 },
		])
		const groupId = uniqueName('it-watch-subscriptions')
		const leader = leaderClient.consumer({
			groupId,
			autoOffsetReset: 'earliest',
			partitionAssignmentStrategy: 'range',
		})
		const other = otherClient.consumer({
			groupId,
			autoOffsetReset: 'earliest',
			partitionAssignmentStrategy: 'range',
		})
		const ownConsumed: number[] = []
		let leaderRunning = false
		leader.once('running', () => {
			leaderRunning = true
		})
		const runs = [leader.runEach(shared, async () => {}, { autoCommit: false })]
		const producer = leaderClient.producer({ lingerMs: 0 })
		try {
			// The first member leads.
			await until(() => leaderRunning, 'the leader')
			runs.push(
				other.runEach(
					[shared, own],
					async message => {
						if (message.topic === own) ownConsumed.push(message.partition)
					},
					{ autoCommit: false }
				)
			)
			await producer.send(own, [
				{ value: Buffer.from('a'), partition: 0 },
				{ value: Buffer.from('b'), partition: 1 },
			])
			await until(() => ownConsumed.length === 2, 'the records of the topic only the other member reads')
			expect(ownConsumed.sort()).toEqual([0, 1])
		} finally {
			leader.stop()
			other.stop()
			await Promise.all(runs)
			await producer.disconnect()
			await Promise.all([leaderClient.disconnect(), otherClient.disconnect()])
		}
	}, 60_000)
})
