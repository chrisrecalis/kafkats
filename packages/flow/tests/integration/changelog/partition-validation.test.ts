import { describe, expect, it, afterEach } from 'vitest'
import {
	codec,
	flow,
	ChangelogPartitionMismatchError,
	SourceTopicNotFoundError,
	type FlowApp,
	type FlowConfig,
} from '@kafkats/flow'
import { KafkaClient } from '@kafkats/client'

import { getBrokerAddress, getLogLevel, createClient } from '../helpers/kafka.js'
import { uniqueName, sleep } from '../helpers/testkit.js'

function createFlowAppWithConfig(config: Omit<FlowConfig, 'client'>): FlowApp {
	return flow({
		...config,
		client: {
			brokers: [getBrokerAddress()],
			clientId: `${config.applicationId}-client`,
			logLevel: getLogLevel(),
		},
	})
}

describe('Flow (integration) - changelog partition validation', () => {
	let client: KafkaClient
	let app: FlowApp

	afterEach(async () => {
		await app?.close()
		await client?.disconnect()
	})

	it('creates changelog with same partition count as source topic', async () => {
		const inputTopic = uniqueName('it-part-source')
		const appId = uniqueName('it-part-app')
		const expectedChangelogTopic = `${appId}-count-store-0-changelog`

		client = createClient('it-part-infer')
		await client.connect()

		// Create source topic with 4 partitions
		await client.createTopics([{ name: inputTopic, numPartitions: 4, replicationFactor: 1 }])

		type Click = { userId: string }

		app = createFlowAppWithConfig({ applicationId: appId })
		app.stream(inputTopic, { key: codec.string(), value: codec.json<Click>() }).groupByKey().count()

		// Start the app - this should create the changelog topic
		await app.start()

		// Wait for changelog topic to be created
		await sleep(2000)

		// Verify changelog topic has 4 partitions
		const metadata = await client.getMetadata([expectedChangelogTopic])
		const changelogMeta = metadata.topics.get(expectedChangelogTopic)

		expect(changelogMeta).toBeDefined()
		expect(changelogMeta!.partitions.size).toBe(4)
	})

	it('throws ChangelogPartitionMismatchError when existing changelog has wrong partition count', async () => {
		const inputTopic = uniqueName('it-mismatch-source')
		const appId = uniqueName('it-mismatch-app')
		const changelogTopic = `${appId}-count-store-0-changelog`

		client = createClient('it-mismatch')
		await client.connect()

		// Create source topic with 4 partitions
		await client.createTopics([{ name: inputTopic, numPartitions: 4, replicationFactor: 1 }])

		// Pre-create changelog topic with WRONG partition count (1 instead of 4)
		await client.createTopics([{ name: changelogTopic, numPartitions: 1, replicationFactor: 1 }])

		type Click = { userId: string }

		app = createFlowAppWithConfig({ applicationId: appId })
		app.stream(inputTopic, { key: codec.string(), value: codec.json<Click>() }).groupByKey().count()

		// Start should throw ChangelogPartitionMismatchError
		await expect(app.start()).rejects.toThrow(ChangelogPartitionMismatchError)

		try {
			await app.start()
		} catch (err) {
			expect(err).toBeInstanceOf(ChangelogPartitionMismatchError)
			const error = err as ChangelogPartitionMismatchError
			expect(error.changelogTopic).toBe(changelogTopic)
			expect(error.expectedPartitions).toBe(4)
			expect(error.actualPartitions).toBe(1)
			expect(error.sourceTopics).toContain(inputTopic)
		}
	})

	it('throws SourceTopicNotFoundError when source topic does not exist', async () => {
		const inputTopic = uniqueName('it-missing-source')
		const appId = uniqueName('it-missing-app')

		client = createClient('it-missing')
		await client.connect()

		// Do NOT create the input topic - it should not exist

		type Click = { userId: string }

		app = createFlowAppWithConfig({ applicationId: appId })
		app.stream(inputTopic, { key: codec.string(), value: codec.json<Click>() }).groupByKey().count()

		// Start should throw SourceTopicNotFoundError
		await expect(app.start()).rejects.toThrow(SourceTopicNotFoundError)

		try {
			await app.start()
		} catch (err) {
			expect(err).toBeInstanceOf(SourceTopicNotFoundError)
			const error = err as SourceTopicNotFoundError
			expect(error.topic).toBe(inputTopic)
		}
	})

	it('uses max partition count when merging streams from different topics', async () => {
		const topic2Partitions = uniqueName('it-merge-2p')
		const topic6Partitions = uniqueName('it-merge-6p')
		const appId = uniqueName('it-merge-app')
		const expectedChangelogTopic = `${appId}-count-store-0-changelog`

		client = createClient('it-merge-part')
		await client.connect()

		// Create source topics with different partition counts
		await client.createTopics([
			{ name: topic2Partitions, numPartitions: 2, replicationFactor: 1 },
			{ name: topic6Partitions, numPartitions: 6, replicationFactor: 1 },
		])

		type Event = { id: string }

		app = createFlowAppWithConfig({ applicationId: appId })

		const stream1 = app.stream(topic2Partitions, { key: codec.string(), value: codec.json<Event>() })
		const stream2 = app.stream(topic6Partitions, { key: codec.string(), value: codec.json<Event>() })

		// Merge streams and aggregate - need to provide key codec for count()
		stream1.merge(stream2).groupByKey({ key: codec.string() }).count()

		// Start the app
		await app.start()
		await sleep(2000)

		// Verify changelog topic has 6 partitions (max of 2 and 6)
		const metadata = await client.getMetadata([expectedChangelogTopic])
		const changelogMeta = metadata.topics.get(expectedChangelogTopic)

		expect(changelogMeta).toBeDefined()
		expect(changelogMeta!.partitions.size).toBe(6)
	})

	it('skips changelog creation when autoCreate is false', async () => {
		const inputTopic = uniqueName('it-no-create-source')
		const appId = uniqueName('it-no-create-app')
		const expectedChangelogTopic = `${appId}-count-store-0-changelog`

		client = createClient('it-no-create')
		await client.connect()

		// Create source topic
		await client.createTopics([{ name: inputTopic, numPartitions: 3, replicationFactor: 1 }])

		type Click = { userId: string }

		// Use autoCreate: false to skip changelog creation
		app = createFlowAppWithConfig({
			applicationId: appId,
			changelog: { autoCreate: false },
		})
		app.stream(inputTopic, { key: codec.string(), value: codec.json<Click>() }).groupByKey().count()

		await app.start()
		await sleep(2000)

		// Verify changelog topic was NOT created
		const metadata = await client.getMetadata([expectedChangelogTopic])
		const changelogMeta = metadata.topics.get(expectedChangelogTopic)

		// Topic should not exist (undefined in metadata)
		expect(changelogMeta).toBeUndefined()
	})

	it('validates existing changelog even when autoCreate is false', async () => {
		const inputTopic = uniqueName('it-validate-only-source')
		const appId = uniqueName('it-validate-only-app')
		const changelogTopic = `${appId}-count-store-0-changelog`

		client = createClient('it-validate-only')
		await client.connect()

		// Create source topic with 4 partitions
		await client.createTopics([{ name: inputTopic, numPartitions: 4, replicationFactor: 1 }])

		// Pre-create changelog with wrong partition count
		await client.createTopics([{ name: changelogTopic, numPartitions: 2, replicationFactor: 1 }])

		type Click = { userId: string }

		// Even with autoCreate: false, validation should still happen
		app = createFlowAppWithConfig({
			applicationId: appId,
			changelog: { autoCreate: false },
		})
		app.stream(inputTopic, { key: codec.string(), value: codec.json<Click>() }).groupByKey().count()

		// Should still throw mismatch error
		await expect(app.start()).rejects.toThrow(ChangelogPartitionMismatchError)
	})

	it('table() infers changelog partitions from source topic', async () => {
		const inputTopic = uniqueName('it-table-source')
		const appId = uniqueName('it-table-app')

		client = createClient('it-table-part')
		await client.connect()

		// Create source topic with 5 partitions
		await client.createTopics([{ name: inputTopic, numPartitions: 5, replicationFactor: 1 }])

		type User = { name: string }

		app = createFlowAppWithConfig({ applicationId: appId })
		app.table(inputTopic, { key: codec.string(), value: codec.json<User>() })

		await app.start()
		await sleep(2000)

		// Get metadata for all topics matching the pattern
		const metadata = await client.getMetadata()

		// Find the changelog topic for this table
		let changelogPartitions: number | undefined
		for (const [topicName, topicMeta] of metadata.topics) {
			if (topicName.startsWith(`${appId}-table-`) && topicName.endsWith('-changelog')) {
				changelogPartitions = topicMeta.partitions.size
				break
			}
		}

		expect(changelogPartitions).toBe(5)
	})

	it('uses global replicationFactor from FlowConfig', async () => {
		const inputTopic = uniqueName('it-rf-source')
		const appId = uniqueName('it-rf-app')
		const expectedChangelogTopic = `${appId}-count-store-0-changelog`

		client = createClient('it-rf')
		await client.connect()

		// Create source topic
		await client.createTopics([{ name: inputTopic, numPartitions: 2, replicationFactor: 1 }])

		type Click = { userId: string }

		// Set global replication factor (note: only 1 broker in test, so RF > 1 would fail)
		app = createFlowAppWithConfig({
			applicationId: appId,
			changelog: { replicationFactor: 1 },
		})
		app.stream(inputTopic, { key: codec.string(), value: codec.json<Click>() }).groupByKey().count()

		// Should start successfully with the configured RF
		await app.start()
		await sleep(2000)

		// Verify the changelog topic was created (RF=1 works with single broker)
		const metadata = await client.getMetadata([expectedChangelogTopic])
		const changelogMeta = metadata.topics.get(expectedChangelogTopic)
		expect(changelogMeta).toBeDefined()

		// Verify app is running
		expect(app.state()).toBe('RUNNING')
	})
})
