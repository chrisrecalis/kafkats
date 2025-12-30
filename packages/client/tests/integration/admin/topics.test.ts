import { describe, expect, it, vi } from 'vitest'

import { ErrorCode } from '@/protocol/messages/error-codes.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

describe.concurrent('Admin - Topics (integration)', () => {
	it('lists topics', async () => {
		const client = createClient('admin-list-topics')
		await client.connect()

		const admin = client.admin()
		const topicName = uniqueName('admin-list')

		// Create a topic first
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		// Wait for topic to be available
		await vi.waitFor(
			async () => {
				const topics = await admin.listTopics()
				expect(topics).toContain(topicName)
			},
			{ timeout: 30_000 }
		)

		await client.disconnect()
	})

	it('describes topics', async () => {
		const client = createClient('admin-describe-topics')
		await client.connect()

		const admin = client.admin()
		const topicName = uniqueName('admin-describe')

		// Create a topic with 3 partitions
		await client.createTopics([{ name: topicName, numPartitions: 3, replicationFactor: 1 }])

		// Wait for topic to be available and describe it
		await vi.waitFor(
			async () => {
				const descriptions = await admin.describeTopics([topicName])
				expect(descriptions).toHaveLength(1)

				const topic = descriptions[0]!
				expect(topic.name).toBe(topicName)
				expect(topic.partitions).toHaveLength(3)
				expect(topic.isInternal).toBe(false)

				// Verify partition info
				for (let i = 0; i < 3; i++) {
					const partition = topic.partitions.find(p => p.partitionIndex === i)
					expect(partition).toBeDefined()
					expect(partition!.leaderId).toBeGreaterThanOrEqual(0)
					expect(partition!.replicas).toHaveLength(1)
					expect(partition!.isr).toHaveLength(1)
				}
			},
			{ timeout: 30_000 }
		)

		await client.disconnect()
	})

	it('deletes topics', async () => {
		const client = createClient('admin-delete-topics')
		await client.connect()

		const admin = client.admin()
		const topicName = uniqueName('admin-delete')

		// Create a topic
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		// Wait for topic to be available
		await vi.waitFor(
			async () => {
				const topics = await admin.listTopics()
				expect(topics).toContain(topicName)
			},
			{ timeout: 30_000 }
		)

		// Delete the topic
		const results = await admin.deleteTopics([topicName])
		expect(results).toHaveLength(1)
		expect(results[0]!.name).toBe(topicName)
		expect(results[0]!.errorCode).toBe(ErrorCode.None)

		// Verify topic is deleted
		await vi.waitFor(
			async () => {
				const topics = await admin.listTopics()
				expect(topics).not.toContain(topicName)
			},
			{ timeout: 30_000 }
		)

		await client.disconnect()
	})

	it('handles deleting non-existent topics', async () => {
		const client = createClient('admin-delete-nonexistent')
		await client.connect()

		const admin = client.admin()
		const topicName = uniqueName('admin-nonexistent')

		// Try to delete a topic that doesn't exist
		const results = await admin.deleteTopics([topicName])
		expect(results).toHaveLength(1)
		expect(results[0]!.name).toBe(topicName)
		expect(results[0]!.errorCode).toBe(ErrorCode.UnknownTopicOrPartition)

		await client.disconnect()
	})
})
