import { describe, expect, it, vi } from 'vitest'

import { CommitFailedError } from '@/client/errors.js'
import { OffsetManager } from '@/consumer/offset-manager.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('OffsetManager (integration)', () => {
	it('commits offsets with metadata and leader epoch, and reads them back', async () => {
		const client = createClient('it-om-commit')
		await client.connect()
		const topicName = uniqueName('it-om-commit')
		await client.createTopics([{ name: topicName, numPartitions: 2, replicationFactor: 1 }])
		const groupId = uniqueName('it-om-group')
		const partitions = [0, 1].map(partition => ({ topic: topicName, partition }))

		// Without a group: generation -1 and no member, as a consumer with a manual assignment commits.
		const manager = new OffsetManager(client.cluster, groupId)
		manager.updateGroupState('', -1, await client.cluster.getCoordinator('GROUP', groupId))
		manager.addAssignedPartitions(partitions)

		await manager.commitExplicit([
			{ topic: topicName, partition: 0, offset: 5n, metadata: 'AgAAAAAAAAAB', leaderEpoch: 0 },
			{ topic: topicName, partition: 1, offset: 3n },
		])

		expect(await manager.fetchCommittedOffsets(partitions)).toStrictEqual(
			new Map([
				[`${topicName}:0`, { offset: 5n, metadata: 'AgAAAAAAAAAB', leaderEpoch: 0 }],
				[`${topicName}:1`, { offset: 3n, metadata: null }],
			])
		)

		await client.disconnect()
	})

	it('reads back the metadata of offsets committed in a transaction', async () => {
		const client = createClient('it-om-txn-metadata')
		await client.connect()
		const topicName = uniqueName('it-om-txn-metadata')
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])
		const groupId = uniqueName('it-om-group')

		const producer = client.producer({ transactionalId: uniqueName('it-om-tx'), lingerMs: 0 })
		await producer.transaction(async tx => {
			await tx.sendOffsets({
				groupId,
				offsets: [{ topic: topicName, partition: 0, offset: 7n, metadata: 'txn' }],
			})
		})

		const manager = new OffsetManager(client.cluster, groupId)
		manager.updateGroupState('', -1, await client.cluster.getCoordinator('GROUP', groupId))
		const committed = await manager.fetchCommittedOffsets([{ topic: topicName, partition: 0 }])

		expect(committed.get(`${topicName}:0`)).toMatchObject({ offset: 7n, metadata: 'txn' })

		await producer.disconnect()
		await client.disconnect()
	})

	it('rejects a commit under a generation the group does not have with CommitFailedError', async () => {
		const client = createClient('it-om-stale-generation')
		await client.connect()
		const topicName = uniqueName('it-om-stale-generation')
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])
		const groupId = uniqueName('it-om-group')
		const partition = { topic: topicName, partition: 0 }

		const manager = new OffsetManager(client.cluster, groupId)
		manager.updateGroupState('gone-member', 5, await client.cluster.getCoordinator('GROUP', groupId))
		manager.addAssignedPartitions([partition])

		await expect(
			manager.commitExplicit([{ ...partition, offset: 1n }], { timeoutMs: 10_000 })
		).rejects.toBeInstanceOf(CommitFailedError)
		expect((await manager.fetchCommittedOffsets([partition])).size).toBe(0)

		await client.disconnect()
	})

	it("resolves 'latest' to the last stable offset under read_committed while a transaction is open", async () => {
		const client = createClient('it-om-lso')
		await client.connect()
		const topicName = uniqueName('it-om-lso')
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const records = (count: number) => Array.from({ length: count }, (_, i) => ({ value: Buffer.from(`v${i}`) }))
		const plain = client.producer({ lingerMs: 0 })
		await plain.send(topicName, records(3))

		const readCommitted = new OffsetManager(client.cluster, 'g', undefined, undefined, 60_000, 'read_committed')
		const readUncommitted = new OffsetManager(client.cluster, 'g', undefined, undefined, 60_000, 'read_uncommitted')

		const transactional = client.producer({ transactionalId: uniqueName('it-om-tx'), lingerMs: 0 })
		await transactional.transaction(async tx => {
			await tx.send(topicName, records(2))

			// Offsets 3 and 4 are written but not committed: the LSO stays at 3, the high watermark is 5.
			expect(await readCommitted.getLatestOffset(topicName, 0)).toBe(3n)
			expect(await readUncommitted.getLatestOffset(topicName, 0)).toBe(5n)
		})

		// The commit marker (offset 5, written shortly after EndTxn returns) ends the transaction: both levels agree.
		await vi.waitFor(async () => expect(await readCommitted.getLatestOffset(topicName, 0)).toBe(6n), {
			timeout: 10_000,
			interval: 100,
		})
		expect(await readUncommitted.getLatestOffset(topicName, 0)).toBe(6n)

		await transactional.disconnect()
		await plain.disconnect()
		await client.disconnect()
	})
})
