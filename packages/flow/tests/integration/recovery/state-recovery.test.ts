import { describe, expect, it, afterEach } from 'vitest'
import { codec, flow, type FlowApp } from '@kafkats/flow'
import { KafkaClient } from '@kafkats/client'

import { createClient, getBrokerAddress, getLogLevel } from '../helpers/kafka.js'
import { uniqueName, sleep, waitForAppReady, consumeWithTimeout } from '../helpers/testkit.js'

describe('Flow (integration) - recovery', () => {
	let client: KafkaClient

	afterEach(async () => {
		await client?.disconnect()
	})

	describe('offset commit recovery', () => {
		it('does not reprocess committed messages after restart', async () => {
			const inputTopic = uniqueName('it-offset-recovery-in')
			const outputTopic = uniqueName('it-offset-recovery-out')
			const applicationId = uniqueName('it-app-offset-recovery')

			client = createClient('it-offset-recovery')
			await client.connect()
			await client.createTopics([
				{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
				{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
			])

			type Event = { id: string; seq: number }

			const createApp = () =>
				flow({
					applicationId,
					client: {
						brokers: [getBrokerAddress()],
						clientId: `${applicationId}-client`,
						logLevel: getLogLevel(),
					},
				})

			// --- Phase 1: Process initial messages ---
			let app: FlowApp = createApp()

			app.stream(inputTopic, { value: codec.json<Event>() })
				.mapValues(v => ({ ...v, processed: true }))
				.to(outputTopic, { value: codec.json() })

			await app.start()
			await waitForAppReady(app)
			await sleep(2000)

			const producer = client.producer({ lingerMs: 0 })
			await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ id: 'msg1', seq: 1 })) })
			await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ id: 'msg2', seq: 2 })) })
			await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ id: 'msg3', seq: 3 })) })
			await producer.flush()
			await sleep(2000)

			// Close cleanly - offsets should be committed
			await app.close()
			await sleep(500)

			await consumeWithTimeout<{ id: string; seq: number; processed: boolean }>(
				client.consumer({ groupId: uniqueName('it-verify-offset-recovery-p1'), autoOffsetReset: 'earliest' }),
				outputTopic,
				{ expectedCount: 3, timeoutMs: 20000 }
			)

			// --- Phase 2: Restart and send new messages ---
			app = createApp()

			app.stream(inputTopic, { value: codec.json<Event>() })
				.mapValues(v => ({ ...v, processed: true }))
				.to(outputTopic, { value: codec.json() })

			await app.start()
			await waitForAppReady(app)
			await sleep(2000)

			// Send new messages after restart
			await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ id: 'msg4', seq: 4 })) })
			await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ id: 'msg5', seq: 5 })) })
			await producer.flush()
			await sleep(3000)

			// Close app to flush all pending writes
			await app.close()
			await sleep(500)

			const results = await consumeWithTimeout<{ id: string; seq: number; processed: boolean }>(
				client.consumer({ groupId: uniqueName('it-verify-offset-recovery'), autoOffsetReset: 'earliest' }),
				outputTopic,
				{ expectedCount: 5, timeoutMs: 20000 }
			)

			const counts = new Map<string, number>()
			for (const result of results) {
				counts.set(result.id, (counts.get(result.id) ?? 0) + 1)
			}

			const expectedIds = ['msg1', 'msg2', 'msg3', 'msg4', 'msg5']
			expect(results).toHaveLength(5)
			expect([...counts.keys()].sort()).toEqual(expectedIds)
			expect([...counts.values()]).toEqual([1, 1, 1, 1, 1])

			await producer.disconnect()
		})

		it('resumes processing from correct offset after multiple restarts', async () => {
			const inputTopic = uniqueName('it-multi-offset-in')
			const outputTopic = uniqueName('it-multi-offset-out')
			const applicationId = uniqueName('it-app-multi-offset')

			client = createClient('it-multi-offset')
			await client.connect()
			await client.createTopics([
				{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
				{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
			])

			const producer = client.producer({ lingerMs: 0 })

			const createApp = () =>
				flow({
					applicationId,
					client: {
						brokers: [getBrokerAddress()],
						clientId: `${applicationId}-client`,
						logLevel: getLogLevel(),
					},
				})

			let finalResults: Array<{ id: string; processed: boolean }> = []

			// Perform 3 cycles of: start -> process -> stop
			for (let cycle = 1; cycle <= 3; cycle++) {
				const app = createApp()
				app.stream(inputTopic, { value: codec.json<{ id: string }>() })
					.mapValues(v => ({ ...v, processed: true }))
					.to(outputTopic, { value: codec.json() })

				await app.start()
				await waitForAppReady(app)
				await sleep(2000)

				// Send one message per cycle
				const msgId = `cycle${cycle}-msg`
				await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ id: msgId })) })
				await producer.flush()
				await sleep(2000)

				await app.close()
				await sleep(500)

				finalResults = await consumeWithTimeout<{ id: string; processed: boolean }>(
					client.consumer({
						groupId: uniqueName(`it-verify-multi-offset-cycle${cycle}`),
						autoOffsetReset: 'earliest',
					}),
					outputTopic,
					{ expectedCount: cycle, timeoutMs: 20000 }
				)
			}

			const results = finalResults

			const counts = new Map<string, number>()
			for (const result of results) {
				counts.set(result.id, (counts.get(result.id) ?? 0) + 1)
			}

			const expectedIds = ['cycle1-msg', 'cycle2-msg', 'cycle3-msg']
			expect(results).toHaveLength(3)
			expect([...counts.keys()].sort()).toEqual(expectedIds)
			expect([...counts.values()]).toEqual([1, 1, 1])

			await producer.disconnect()
		}, 120000)
	})

	describe('exactly-once recovery', () => {
		it('maintains EOS guarantees after clean restart', async () => {
			const inputTopic = uniqueName('it-eos-recovery-in')
			const outputTopic = uniqueName('it-eos-recovery-out')
			const applicationId = uniqueName('it-app-eos-recovery')

			client = createClient('it-eos-recovery')
			await client.connect()
			await client.createTopics([
				{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
				{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
			])

			type Event = { id: string; value: number }

			const createApp = () =>
				flow({
					applicationId,
					client: {
						brokers: [getBrokerAddress()],
						clientId: `${applicationId}-client`,
						logLevel: getLogLevel(),
					},
					processingGuarantee: 'exactly_once',
					commitIntervalMs: 100,
				})

			// --- Phase 1: Process with EOS, close cleanly ---
			let app: FlowApp = createApp()

			app.stream(inputTopic, { value: codec.json<Event>() })
				.mapValues(v => ({ ...v, processed: true }))
				.to(outputTopic, { value: codec.json() })

			await app.start()
			await waitForAppReady(app)
			await sleep(3000)

			const producer = client.producer({ lingerMs: 0 })
			await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ id: 'e1', value: 100 })) })
			await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ id: 'e2', value: 200 })) })
			await producer.flush()
			await sleep(3000)

			await app.close()
			await sleep(500)

			await consumeWithTimeout<{ id: string; value: number; processed: boolean }>(
				client.consumer({
					groupId: uniqueName('it-verify-eos-recovery-p1'),
					autoOffsetReset: 'earliest',
					isolationLevel: 'read_committed',
				}),
				outputTopic,
				{ expectedCount: 2, timeoutMs: 20000 }
			)

			// --- Phase 2: Restart and process more ---
			app = createApp()

			app.stream(inputTopic, { value: codec.json<Event>() })
				.mapValues(v => ({ ...v, processed: true }))
				.to(outputTopic, { value: codec.json() })

			await app.start()
			await waitForAppReady(app)
			await sleep(3000)

			await producer.send(inputTopic, { value: Buffer.from(JSON.stringify({ id: 'e3', value: 300 })) })
			await producer.flush()
			await sleep(4000)

			// Close app to flush pending transactions
			await app.close()
			await sleep(500)

			const results = await consumeWithTimeout<{ id: string; value: number; processed: boolean }>(
				client.consumer({
					groupId: uniqueName('it-verify-eos-recovery'),
					autoOffsetReset: 'earliest',
					isolationLevel: 'read_committed',
				}),
				outputTopic,
				{ expectedCount: 3, timeoutMs: 20000 }
			)

			// All 3 messages should appear exactly once
			expect(results).toHaveLength(3)
			expect(results.find(r => r.id === 'e1')).toEqual({ id: 'e1', value: 100, processed: true })
			expect(results.find(r => r.id === 'e2')).toEqual({ id: 'e2', value: 200, processed: true })
			expect(results.find(r => r.id === 'e3')).toEqual({ id: 'e3', value: 300, processed: true })

			// Verify no duplicates
			const ids = results.map(r => r.id)
			expect(new Set(ids).size).toBe(ids.length)

			await producer.disconnect()
		}, 120000)
	})

	describe('stateful recovery with in-memory state', () => {
		it('loses aggregation state with in-memory store (expected behavior)', async () => {
			// This test documents the expected behavior: in-memory state is lost on restart
			// Use LMDB or another persistent store for production stateful workloads

			const inputTopic = uniqueName('it-memory-state-in')
			const outputTopic = uniqueName('it-memory-state-out')
			const applicationId = uniqueName('it-app-memory-state')

			client = createClient('it-memory-state')
			await client.connect()
			await client.createTopics([
				{ name: inputTopic, numPartitions: 1, replicationFactor: 1 },
				{ name: outputTopic, numPartitions: 1, replicationFactor: 1 },
			])

			type Event = { userId: string }
			type Output = { phase: 'phase1' | 'phase2'; count: number }

			const createApp = (phase: Output['phase']) => {
				const app = flow({
					applicationId,
					client: {
						brokers: [getBrokerAddress()],
						clientId: `${applicationId}-client`,
						logLevel: getLogLevel(),
					},
					// Default in-memory state (not persistent)
				})

				app.stream(inputTopic, { key: codec.string(), value: codec.json<Event>() })
					.groupByKey()
					.count()
					.toStream()
					.mapValues(count => ({ phase, count }))
					.to(outputTopic, { key: codec.string(), value: codec.json<Output>() })

				return app
			}

			// --- Phase 1: Process and build up count ---
			let app: FlowApp = createApp('phase1')

			await app.start()
			await waitForAppReady(app)
			await sleep(2000)

			const producer = client.producer({ lingerMs: 0 })
			await producer.send(inputTopic, {
				key: Buffer.from('user1'),
				value: Buffer.from(JSON.stringify({ userId: 'user1' })),
			})
			await producer.send(inputTopic, {
				key: Buffer.from('user1'),
				value: Buffer.from(JSON.stringify({ userId: 'user1' })),
			})
			await producer.flush()
			await sleep(2000)

			await app.close()
			await sleep(500)

			await consumeWithTimeout<Output>(
				client.consumer({ groupId: uniqueName('it-verify-memory-state-p1'), autoOffsetReset: 'earliest' }),
				outputTopic,
				{ expectedCount: 2, timeoutMs: 20000 }
			)

			// --- Phase 2: Restart - state is lost, count restarts from 0 ---
			app = createApp('phase2')

			await app.start()
			await waitForAppReady(app)
			await sleep(2000)

			// Send one more message
			await producer.send(inputTopic, {
				key: Buffer.from('user1'),
				value: Buffer.from(JSON.stringify({ userId: 'user1' })),
			})
			await producer.flush()
			await sleep(2000)

			// Close app to flush all pending writes
			await app.close()
			await sleep(500)

			const results = await consumeWithTimeout<Output>(
				client.consumer({ groupId: uniqueName('it-verify-memory-state'), autoOffsetReset: 'earliest' }),
				outputTopic,
				{ expectedCount: 3, timeoutMs: 20000 }
			)
			const phase1Counts = results.filter(r => r.phase === 'phase1').map(r => r.count)
			const phase2Counts = results.filter(r => r.phase === 'phase2').map(r => r.count)

			expect(phase1Counts).toEqual([1, 2])
			expect(phase2Counts).toEqual([1])

			await producer.disconnect()
		}, 120000)
	})
})
