import { describe, expect, it, afterEach } from 'vitest'
import { codec, TimeWindows, SessionWindows, type FlowApp } from '@kafkats/flow'
import { KafkaClient } from '@kafkats/client'

import { createClient, createFlowApp } from '../helpers/kafka.js'
import { uniqueName, sleep } from '../helpers/testkit.js'

describe('Flow (integration) - windowing', () => {
	let client: KafkaClient
	let app: FlowApp

	afterEach(async () => {
		await app?.close()
		await client?.disconnect()
	})

	it('counts events in fixed time windows with TimeWindows.of()', async () => {
		const inputTopic = uniqueName('it-timewin-in')
		const outputTopic = uniqueName('it-timewin-out')

		client = createClient('it-timewindow')
		await client.connect()
		await client.createTopics([
			{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Event = { type: string }

		app = createFlowApp({ applicationId: uniqueName('it-app-timewin') })
		app.stream(inputTopic, { key: codec.string(), value: codec.json<Event>() })
			.groupByKey()
			.windowedBy(TimeWindows.of('10s'))
			.count()
			.toStream()
			.map(
				(windowedKey, count) =>
					[JSON.stringify({ key: windowedKey.key, windowStart: windowedKey.window.start }), count] as const
			)
			.to(outputTopic, { key: codec.string(), value: codec.json() })

		await app.start()
		await sleep(2000)

		const baseTime = Date.now()
		const producer = client.producer({ lingerMs: 0 })

		await producer.send(inputTopic, {
			key: Buffer.from('user1'),
			value: Buffer.from(JSON.stringify({ type: 'click' })),
			timestamp: new Date(baseTime),
		})
		await producer.send(inputTopic, {
			key: Buffer.from('user1'),
			value: Buffer.from(JSON.stringify({ type: 'scroll' })),
			timestamp: new Date(baseTime + 1000),
		})
		await producer.send(inputTopic, {
			key: Buffer.from('user1'),
			value: Buffer.from(JSON.stringify({ type: 'click' })),
			timestamp: new Date(baseTime + 2000),
		})
		await producer.flush()

		await sleep(3000) // Allow time for all messages to be processed

		const consumer = client.consumer({ groupId: uniqueName('it-verify-timewin'), autoOffsetReset: 'earliest' })
		const results: Array<{ key: string; windowStart: number; count: number }> = []

		const consumePromise = consumer.runEach(
			outputTopic,
			async (msg: { key: Buffer | null; value: Buffer }) => {
				const keyInfo = JSON.parse(msg.key?.toString() ?? '{}') as { key: string; windowStart: number }
				const count = JSON.parse(msg.value.toString()) as number
				results.push({ ...keyInfo, count })
				// Stop once we have at least 3 results
				if (results.length >= 3) {
					consumer.stop()
				}
			},
			{ autoCommit: false }
		)
		await Promise.race([consumePromise, sleep(10000).then(() => consumer.stop())])

		const latestCount = results[results.length - 1]
		expect(latestCount?.count).toBe(3)
		expect(latestCount?.key).toBe('user1')

		await producer.disconnect()
	})

	it('creates hopping windows with advanceBy()', async () => {
		const inputTopic = uniqueName('it-hopping-in')
		const outputTopic = uniqueName('it-hopping-out')

		client = createClient('it-hopping')
		await client.connect()
		await client.createTopics([
			{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Event = { value: number }

		app = createFlowApp({ applicationId: uniqueName('it-app-hopping') })
		app.stream(inputTopic, { key: codec.string(), value: codec.json<Event>() })
			.groupByKey()
			.windowedBy(TimeWindows.of('10s').advanceBy('5s'))
			.count()
			.toStream()
			.map(
				(windowedKey, count) =>
					[
						JSON.stringify({
							key: windowedKey.key,
							windowStart: windowedKey.window.start,
							windowEnd: windowedKey.window.end,
						}),
						count,
					] as const
			)
			.to(outputTopic, { key: codec.string(), value: codec.json() })

		await app.start()
		await sleep(2000)

		const baseTime = Date.now()
		const producer = client.producer({ lingerMs: 0 })

		await producer.send(inputTopic, {
			key: Buffer.from('sensor1'),
			value: Buffer.from(JSON.stringify({ value: 100 })),
			timestamp: new Date(baseTime),
		})
		await producer.send(inputTopic, {
			key: Buffer.from('sensor1'),
			value: Buffer.from(JSON.stringify({ value: 200 })),
			timestamp: new Date(baseTime + 6000),
		})
		await producer.flush()

		await sleep(2000)

		const consumer = client.consumer({ groupId: uniqueName('it-verify-hopping'), autoOffsetReset: 'earliest' })
		const results: Array<{ key: string; windowStart: number; windowEnd: number; count: number }> = []

		const consumePromise = consumer.runEach(
			outputTopic,
			async (msg: { key: Buffer | null; value: Buffer }) => {
				const keyInfo = JSON.parse(msg.key?.toString() ?? '{}') as {
					key: string
					windowStart: number
					windowEnd: number
				}
				const count = JSON.parse(msg.value.toString()) as number
				results.push({ ...keyInfo, count })
			},
			{ autoCommit: false }
		)
		await Promise.race([consumePromise, sleep(5000).then(() => consumer.stop())])

		expect(results.length).toBeGreaterThan(0)
		expect(results.every(r => r.key === 'sensor1')).toBe(true)

		await producer.disconnect()
	})

	it('creates session windows with SessionWindows.withInactivityGap()', async () => {
		const inputTopic = uniqueName('it-session-in')
		const outputTopic = uniqueName('it-session-out')

		client = createClient('it-session')
		await client.connect()
		await client.createTopics([
			{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
			{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
		])

		type Event = { action: string }

		app = createFlowApp({ applicationId: uniqueName('it-app-session') })
		app.stream(inputTopic, { key: codec.string(), value: codec.json<Event>() })
			.groupByKey()
			.windowedBy(SessionWindows.withInactivityGap('5s'))
			.count()
			.toStream()
			.map(
				(windowedKey, count) =>
					[JSON.stringify({ key: windowedKey.key, sessionStart: windowedKey.window.start }), count] as const
			)
			.to(outputTopic, { key: codec.string(), value: codec.json() })

		await app.start()
		await sleep(2000)

		const baseTime = Date.now()
		const producer = client.producer({ lingerMs: 0 })

		await producer.send(inputTopic, {
			key: Buffer.from('user1'),
			value: Buffer.from(JSON.stringify({ action: 'click' })),
			timestamp: new Date(baseTime),
		})
		await producer.send(inputTopic, {
			key: Buffer.from('user1'),
			value: Buffer.from(JSON.stringify({ action: 'scroll' })),
			timestamp: new Date(baseTime + 2000),
		})
		await producer.send(inputTopic, {
			key: Buffer.from('user1'),
			value: Buffer.from(JSON.stringify({ action: 'click' })),
			timestamp: new Date(baseTime + 4000),
		})
		await producer.flush()

		await sleep(2000)

		const consumer = client.consumer({ groupId: uniqueName('it-verify-session'), autoOffsetReset: 'earliest' })
		const results: Array<{ key: string; sessionStart: number; count: number }> = []

		const consumePromise = consumer.runEach(
			outputTopic,
			async (msg: { key: Buffer | null; value: Buffer }) => {
				const keyInfo = JSON.parse(msg.key?.toString() ?? '{}') as { key: string; sessionStart: number }
				const count = JSON.parse(msg.value.toString()) as number
				results.push({ ...keyInfo, count })
			},
			{ autoCommit: false }
		)
		await Promise.race([consumePromise, sleep(5000).then(() => consumer.stop())])

		const latestCount = results[results.length - 1]
		expect(latestCount?.count).toBe(3)
		expect(latestCount?.key).toBe('user1')

		await producer.disconnect()
	})
})
