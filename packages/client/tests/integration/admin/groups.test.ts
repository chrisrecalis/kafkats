import { describe, expect, it, vi } from 'vitest'

import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { withKafka } from '../helpers/kafka.js'
import { uniqueName, sleep } from '../helpers/testkit.js'

function waitUntilRunning(
	consumer: {
		once(event: 'running', cb: () => void): unknown
		once(event: 'error', cb: (err: Error) => void): unknown
	},
	run: Promise<void>
): Promise<void> {
	return Promise.race([
		new Promise<void>((resolve, reject) => {
			consumer.once('running', () => resolve())
			consumer.once('error', err => reject(err))
		}),
		run.then(
			() => {
				throw new Error('consumer stopped before reaching running state')
			},
			err => {
				throw err
			}
		),
	])
}

describe.concurrent('Admin - Consumer Groups (integration)', () => {
	it('lists consumer groups', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('admin-list-groups')
			await client.connect()

			const topicName = uniqueName('admin-groups-topic')
			const groupId = uniqueName('admin-groups-list')
			const testTopic = topic<string>(topicName, { value: string() })

			// Create a topic
			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			// Create a consumer and join a group
			const consumer = client.consumer({ groupId })

			// Start consuming in the background
			const consumePromise = consumer.runEach(testTopic, async () => {}, { autoCommit: false })

			// Wait for the consumer to join the group
			await waitUntilRunning(consumer, consumePromise)

			await vi.waitFor(
				async () => {
					const admin = client.admin()
					const groups = await admin.listGroups()
					const ourGroup = groups.find(g => g.groupId === groupId)
					expect(ourGroup).toBeDefined()
					expect(ourGroup!.protocolType).toBe('consumer')
				},
				{ timeout: 30_000 }
			)

			// Stop consumer
			consumer.stop()
			await consumePromise

			await client.disconnect()
		})
	})

	it('describes consumer groups', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('admin-describe-groups')
			await client.connect()

			const topicName = uniqueName('admin-describe-topic')
			const groupId = uniqueName('admin-describe-group')
			const testTopic = topic<string>(topicName, { value: string() })

			// Create a topic
			await client.createTopics([{ name: topicName, numPartitions: 2, replicationFactor: 1 }])

			// Create a consumer and join a group
			const consumer = client.consumer({ groupId })

			// Start consuming in the background
			const consumePromise = consumer.runEach(testTopic, async () => {}, { autoCommit: false })

			// Wait for the consumer to be stable and describe the group
			await waitUntilRunning(consumer, consumePromise)

			await vi.waitFor(
				async () => {
					const admin = client.admin()
					const descriptions = await admin.describeGroups([groupId])
					expect(descriptions).toHaveLength(1)

					const group = descriptions[0]!
					expect(group.groupId).toBe(groupId)
					expect(group.state).toBe('Stable')
					expect(group.protocolType).toBe('consumer')
					expect(group.members.length).toBeGreaterThanOrEqual(1)

					// Verify member info
					const member = group.members[0]!
					expect(member.memberId).toBeTruthy()
					expect(member.clientId).toBe('admin-describe-groups')
					expect(member.assignment.length).toBeGreaterThanOrEqual(1)
				},
				{ timeout: 30_000 }
			)

			// Stop consumer
			consumer.stop()
			await consumePromise

			await client.disconnect()
		})
	})

	it('deletes empty consumer groups', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('admin-delete-groups')
			await client.connect()

			const topicName = uniqueName('admin-delete-topic')
			const groupId = uniqueName('admin-delete-group')
			const testTopic = topic<string>(topicName, { value: string() })

			// Create a topic
			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			// Create a consumer and join a group
			const consumer = client.consumer({ groupId })

			// Start consuming briefly to create the group
			const consumePromise = consumer.runEach(testTopic, async () => {}, { autoCommit: false })

			// Wait for the consumer to join the group
			await waitUntilRunning(consumer, consumePromise)

			await vi.waitFor(
				async () => {
					const admin = client.admin()
					const groups = await admin.listGroups()
					expect(groups.some(g => g.groupId === groupId)).toBe(true)
				},
				{ timeout: 30_000 }
			)

			// Stop consumer to make the group empty
			consumer.stop()
			await consumePromise

			// Wait for the group to become empty
			await sleep(2000)

			// Delete the group
			const admin = client.admin()
			const results = await admin.deleteGroups([groupId])
			expect(results).toHaveLength(1)
			expect(results[0]!.groupId).toBe(groupId)
			expect(results[0]!.errorCode).toBe(ErrorCode.None)

			await client.disconnect()
		})
	})

	it('returns error when deleting non-empty groups', async () => {
		await withKafka(async ({ createClient }) => {
			const client = createClient('admin-delete-nonempty')
			await client.connect()

			const topicName = uniqueName('admin-nonempty-topic')
			const groupId = uniqueName('admin-nonempty-group')
			const testTopic = topic<string>(topicName, { value: string() })

			// Create a topic
			await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

			// Create a consumer and join a group
			const consumer = client.consumer({ groupId })

			// Start consuming in the background
			const consumePromise = consumer.runEach(testTopic, async () => {}, { autoCommit: false })

			// Wait for the consumer to join the group
			await waitUntilRunning(consumer, consumePromise)

			await vi.waitFor(
				async () => {
					const admin = client.admin()
					const groups = await admin.listGroups()
					expect(groups.some(g => g.groupId === groupId)).toBe(true)
				},
				{ timeout: 30_000 }
			)

			// Try to delete the non-empty group
			const admin = client.admin()
			const results = await admin.deleteGroups([groupId])
			expect(results).toHaveLength(1)
			expect(results[0]!.groupId).toBe(groupId)
			expect(results[0]!.errorCode).toBe(ErrorCode.NonEmptyGroup)

			// Stop consumer
			consumer.stop()
			await consumePromise

			await client.disconnect()
		})
	})
})
