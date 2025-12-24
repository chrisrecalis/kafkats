import { describe, expect, it, afterEach } from 'vitest'
import { codec, type FlowApp } from '@kafkats/flow'
import { KafkaClient } from '@kafkats/client'

import { createClient, createFlowApp } from '../helpers/kafka.js'
import { uniqueName, sleep, consumeWithTimeout } from '../helpers/testkit.js'

describe('Flow (integration) - stream-table joins', () => {
	let client: KafkaClient
	let app: FlowApp

	afterEach(async () => {
		await app?.close()
		await client?.disconnect()
	})

	it('joins stream with table using inner join', async () => {
		const eventsTopic = uniqueName('it-stj-events')
		const usersTopic = uniqueName('it-stj-users')
		const outputTopic = uniqueName('it-stj-output')

		client = createClient('it-stream-table-join')
		await client.connect()
		await client.createTopics([
			{ name: eventsTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: usersTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Event = { action: string }
		type User = { name: string; email: string }
		type EnrichedEvent = { action: string; userName: string }

		app = createFlowApp({ applicationId: uniqueName('it-app-stj') })

		const usersTable = app.table(usersTopic, { key: codec.string(), value: codec.json<User>() })

		app.stream(eventsTopic, { key: codec.string(), value: codec.json<Event>() })
			.join(usersTable, (event, user) => ({
				action: event.action,
				userName: user.name,
			}))
			.to(outputTopic, { key: codec.string(), value: codec.json() })

		await app.start()
		await sleep(2000)

		const producer = client.producer({ lingerMs: 0 })

		// First populate the users table
		await producer.send(usersTopic, {
			key: Buffer.from('user1'),
			value: Buffer.from(JSON.stringify({ name: 'Alice', email: 'alice@example.com' })),
		})
		await producer.send(usersTopic, {
			key: Buffer.from('user2'),
			value: Buffer.from(JSON.stringify({ name: 'Bob', email: 'bob@example.com' })),
		})
		await producer.flush()

		await sleep(2000)

		// Send events that should join with the table
		await producer.send(eventsTopic, {
			key: Buffer.from('user1'),
			value: Buffer.from(JSON.stringify({ action: 'login' })),
		})
		await producer.send(eventsTopic, {
			key: Buffer.from('user2'),
			value: Buffer.from(JSON.stringify({ action: 'purchase' })),
		})
		await producer.flush()

		await sleep(2000)

		const consumer = client.consumer({ groupId: uniqueName('it-verify-stj'), autoOffsetReset: 'earliest' })
		const results = await consumeWithTimeout<EnrichedEvent>(consumer, outputTopic, { expectedCount: 2 })

		expect(results).toHaveLength(2)
		expect(results.find(r => r.userName === 'Alice')).toEqual({ action: 'login', userName: 'Alice' })
		expect(results.find(r => r.userName === 'Bob')).toEqual({ action: 'purchase', userName: 'Bob' })

		await producer.disconnect()
	})

	it('joins stream with table using left join (handles missing table entries)', async () => {
		const eventsTopic = uniqueName('it-stlj-events')
		const usersTopic = uniqueName('it-stlj-users')
		const outputTopic = uniqueName('it-stlj-output')

		client = createClient('it-stream-table-leftjoin')
		await client.connect()
		await client.createTopics([
			{ name: eventsTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: usersTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Event = { action: string }
		type User = { name: string }
		type EnrichedEvent = { action: string; userName: string | null }

		app = createFlowApp({ applicationId: uniqueName('it-app-stlj') })

		const usersTable = app.table(usersTopic, { key: codec.string(), value: codec.json<User>() })

		app.stream(eventsTopic, { key: codec.string(), value: codec.json<Event>() })
			.leftJoin(usersTable, (event, user) => ({
				action: event.action,
				userName: user?.name ?? null,
			}))
			.to(outputTopic, { key: codec.string(), value: codec.json() })

		await app.start()
		await sleep(2000)

		const producer = client.producer({ lingerMs: 0 })

		// Only populate one user
		await producer.send(usersTopic, {
			key: Buffer.from('user1'),
			value: Buffer.from(JSON.stringify({ name: 'Alice' })),
		})
		await producer.flush()

		await sleep(2000)

		// Send events - one with matching user, one without
		await producer.send(eventsTopic, {
			key: Buffer.from('user1'),
			value: Buffer.from(JSON.stringify({ action: 'click' })),
		})
		await producer.send(eventsTopic, {
			key: Buffer.from('user3'),
			value: Buffer.from(JSON.stringify({ action: 'view' })),
		})
		await producer.flush()

		await sleep(2000)

		const consumer = client.consumer({ groupId: uniqueName('it-verify-stlj'), autoOffsetReset: 'earliest' })
		const results = await consumeWithTimeout<EnrichedEvent>(consumer, outputTopic, { expectedCount: 2 })

		expect(results).toHaveLength(2)
		expect(results.find(r => r.action === 'click')).toEqual({ action: 'click', userName: 'Alice' })
		expect(results.find(r => r.action === 'view')).toEqual({ action: 'view', userName: null })

		await producer.disconnect()
	})

	it('reflects table updates in subsequent joins', async () => {
		const eventsTopic = uniqueName('it-upd-events')
		const usersTopic = uniqueName('it-upd-users')
		const outputTopic = uniqueName('it-upd-output')

		client = createClient('it-table-update-join')
		await client.connect()
		await client.createTopics([
			{ name: eventsTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: usersTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Event = { eventId: string }
		type User = { name: string }
		type Result = { eventId: string; userName: string }

		app = createFlowApp({ applicationId: uniqueName('it-app-upd') })

		const usersTable = app.table(usersTopic, { key: codec.string(), value: codec.json<User>() })

		app.stream(eventsTopic, { key: codec.string(), value: codec.json<Event>() })
			.join(usersTable, (event, user) => ({
				eventId: event.eventId,
				userName: user.name,
			}))
			.to(outputTopic, { key: codec.string(), value: codec.json() })

		await app.start()
		await sleep(2000)

		const producer = client.producer({ lingerMs: 0 })

		// Initial user
		await producer.send(usersTopic, {
			key: Buffer.from('user1'),
			value: Buffer.from(JSON.stringify({ name: 'OldName' })),
		})
		await producer.flush()
		await sleep(2000)

		// First event with old name
		await producer.send(eventsTopic, {
			key: Buffer.from('user1'),
			value: Buffer.from(JSON.stringify({ eventId: 'e1' })),
		})
		await producer.flush()
		await sleep(1000)

		// Update user name
		await producer.send(usersTopic, {
			key: Buffer.from('user1'),
			value: Buffer.from(JSON.stringify({ name: 'NewName' })),
		})
		await producer.flush()
		await sleep(2000)

		// Second event should use new name
		await producer.send(eventsTopic, {
			key: Buffer.from('user1'),
			value: Buffer.from(JSON.stringify({ eventId: 'e2' })),
		})
		await producer.flush()

		await sleep(2000)

		const consumer = client.consumer({ groupId: uniqueName('it-verify-upd'), autoOffsetReset: 'earliest' })
		const results = await consumeWithTimeout<Result>(consumer, outputTopic, { expectedCount: 2 })

		expect(results).toHaveLength(2)
		const e1Result = results.find(r => r.eventId === 'e1')
		const e2Result = results.find(r => r.eventId === 'e2')

		expect(e1Result?.userName).toBe('OldName')
		expect(e2Result?.userName).toBe('NewName')

		await producer.disconnect()
	})
})
