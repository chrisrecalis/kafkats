import { describe, expect, it, afterEach } from 'vitest'
import { codec, TimeWindows, SessionWindows, type FlowApp } from '@kafkats/flow'
import { KafkaClient } from '@kafkats/client'

import { createClient, createFlowApp } from '../helpers/kafka.js'
import { uniqueName, waitForAppReady, waitFor } from '../helpers/testkit.js'

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
		await waitForAppReady(app)

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

		await waitFor(
			() => {
				if (results.length < 3) {
					throw new Error('Not enough results yet')
				}
			},
			{ timeout: 15000, interval: 100 }
		)
		consumer.stop()
		await consumePromise.catch(() => {})

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
		await waitForAppReady(app)

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

		await waitFor(
			() => {
				if (results.length === 0) {
					throw new Error('No results yet')
				}
			},
			{ timeout: 10000, interval: 100 }
		)
		consumer.stop()
		await consumePromise.catch(() => {})

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
		await waitForAppReady(app)

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

		await waitFor(
			() => {
				const latest = results[results.length - 1]
				if (latest?.count !== 3 || latest?.key !== 'user1') {
					throw new Error('Session count not complete yet')
				}
			},
			{ timeout: 10000, interval: 100 }
		)
		consumer.stop()
		await consumePromise.catch(() => {})

		const latestCount = results[results.length - 1]
		expect(latestCount?.count).toBe(3)
		expect(latestCount?.key).toBe('user1')

		await producer.disconnect()
	})
})
