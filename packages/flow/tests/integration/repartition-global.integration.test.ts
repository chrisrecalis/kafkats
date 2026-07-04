import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { KafkaClient, type Producer } from '@kafkats/client'
import { buildRepartitionTopicName, codec, flow } from '../../src/index.js'
import { MultiTopicCollector, createTopics, requireKafkaBrokers, uniqueTopicName } from './test-helpers.js'

const stringCodec = codec.string()

const numberCodec = {
	encode: (v: number) => {
		const buf = Buffer.alloc(8)
		buf.writeDoubleLE(v, 0)
		return buf
	},
	decode: (b: Buffer) => b.readDoubleLE(0),
}

const restorationOptions = {
	idleTimeoutMs: 2_000,
	initialIdleTimeoutMs: 10_000,
	checkIntervalMs: 200,
	consumerMaxWaitMs: 200,
}

let client: KafkaClient
let producer: Producer

beforeAll(async () => {
	client = new KafkaClient({
		clientId: `flow-repart-integration-${Date.now()}`,
		brokers: requireKafkaBrokers(),
	})
	await client.connect()
	producer = client.producer()
})

afterAll(async () => {
	await producer.disconnect()
	await client.disconnect()
})

async function withTimeout<T>(label: string, promise: Promise<T>, timeoutMs: number): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined
	try {
		return await Promise.race([
			promise,
			new Promise<T>((_resolve, reject) => {
				timer = setTimeout(() => reject(new Error(`Timed out after ${timeoutMs}ms: ${label}`)), timeoutMs)
			}),
		])
	} finally {
		if (timer) clearTimeout(timer)
	}
}

describe('through() repartitioning', () => {
	// Pre-fix, through() forwarded records in-process (original partitioning) and never consumed
	// the topic back, so the downstream count store's sourceTopics ({rekeyed}) never intersected
	// the consumer assignment and restoration was silently skipped: after the restart below the
	// count restarts at 1 instead of resuming at 5, and the final waitFor(6) times out.
	it('consumes the through topic back and restores downstream state across restarts', async () => {
		const input = uniqueTopicName('flow-it-through-src')
		const rekeyed = uniqueTopicName('flow-it-through-rekeyed')
		const outputTopic = uniqueTopicName('flow-it-through-out')
		const appId = `flow-it-through-${Date.now()}`

		await createTopics(client, [
			{ name: input, partitions: 3 },
			{ name: rekeyed, partitions: 3 },
			{ name: outputTopic },
		])

		const buildApp = () => {
			const app = flow({
				applicationId: appId,
				client,
				numStreamThreads: 1,
				consumer: { autoOffsetReset: 'earliest', maxWaitMs: 100 },
				changelog: { restoration: restorationOptions },
			})
			app.stream(input, { key: stringCodec, value: numberCodec })
				.selectKey(() => 'all')
				.through(rekeyed, { key: stringCodec })
				.groupByKey()
				.count({ storeName: 'through-counts', key: stringCodec, value: numberCodec })
				.toStream()
				.to(outputTopic)
			return app
		}

		const out = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector-${Date.now()}`,
			topics: [{ topic: outputTopic, keyCodec: stringCodec, valueCodec: numberCodec }],
		})

		const app1 = buildApp()
		const app2 = buildApp()

		try {
			await withTimeout('collector start', out.start(), 15_000)
			await withTimeout('app1.start', app1.start(), 30_000)

			// Keys spread across all three input partitions; selectKey collapses them to one key.
			for (let i = 0; i < 5; i++) {
				await withTimeout(
					`produce input (${i})`,
					producer.send(input, { key: `k${i}`, value: numberCodec.encode(1), partition: i % 3 }),
					15_000
				)
			}
			await out.waitFor(outputTopic, m => m.key === 'all' && m.value === 5, 20_000)

			await withTimeout('app1.close', app1.close(), 20_000)
			await withTimeout('app2.start', app2.start(), 30_000)

			await withTimeout(
				'produce input (post-restart)',
				producer.send(input, { key: 'k-post', value: numberCodec.encode(1), partition: 0 }),
				15_000
			)
			try {
				await out.waitFor(outputTopic, m => m.key === 'all' && m.value === 6, 15_000)
			} catch (error) {
				const seen = out
					.getTopicMessages<string, number>(outputTopic)
					.filter(m => m.key === 'all')
					.map(m => m.value)
				throw new Error(
					`Expected restored count to reach 6 after restart, but saw values: [${seen.join(', ')}]`,
					{ cause: error instanceof Error ? error : new Error(String(error)) }
				)
			}
		} finally {
			await withTimeout('app2.close', app2.close(), 20_000).catch(() => {})
			await withTimeout('app1.close', app1.close(), 20_000).catch(() => {})
			await withTimeout('collector stop', out.stop(), 15_000).catch(() => {})
		}
	}, 120_000)
})

describe('groupBy() repartitioning', () => {
	type UserEvent = { user: string }
	const userEventCodec = codec.json<UserEvent>()

	// Pre-fix, groupBy() only inserted an in-process SelectKey node: with two instances splitting
	// the 3 input partitions, records for the same new key aggregated independently per instance
	// (alice never reaches 3 anywhere), and no repartition topic existed. Post-fix, all records
	// for a key land on one partition of the auto-created repartition topic, so exactly one
	// instance owns each key and the counts converge.
	it('converges counts across concurrent instances via an auto-created repartition topic', async () => {
		const input = uniqueTopicName('flow-it-groupby-src')
		const outputTopic = uniqueTopicName('flow-it-groupby-out')
		const appId = `flow-it-groupby-${Date.now()}`
		const repartitionTopic = buildRepartitionTopicName(appId, 'by-user')

		await createTopics(client, [{ name: input, partitions: 3 }, { name: outputTopic }])

		const buildApp = (clientId: string) => {
			const app = flow({
				applicationId: appId,
				client: { clientId, brokers: requireKafkaBrokers() },
				numStreamThreads: 1,
				consumer: { autoOffsetReset: 'earliest', maxWaitMs: 100 },
				changelog: { restoration: restorationOptions },
			})
			app.stream(input, { key: stringCodec, value: userEventCodec })
				.groupBy(
					(_key, value) => {
						if (value === null) {
							throw new Error('unexpected tombstone in groupBy() test')
						}
						return value.user
					},
					{ key: stringCodec, value: userEventCodec, name: 'by-user' }
				)
				.count({ storeName: 'user-counts', key: stringCodec, value: numberCodec })
				.toStream()
				.to(outputTopic)
			return app
		}

		const out = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector-${Date.now()}`,
			topics: [{ topic: outputTopic, keyCodec: stringCodec, valueCodec: numberCodec }],
		})

		const app1 = buildApp(`${appId}-a`)
		const app2 = buildApp(`${appId}-b`)

		try {
			await withTimeout('collector start', out.start(), 15_000)
			await withTimeout('app1.start', app1.start(), 30_000)
			await withTimeout('app2.start', app2.start(), 30_000)

			// The repartition topic is auto-created with the source's partition count.
			const meta = await client.getMetadata([repartitionTopic])
			expect(meta.topics.get(repartitionTopic)?.partitions.size).toBe(3)

			// Same user spread across ALL input partitions - pre-fix these aggregate independently
			// on whichever instance owns each original partition.
			const sends: Array<{ key: string; user: string; partition: number }> = [
				{ key: 'k0', user: 'alice', partition: 0 },
				{ key: 'k1', user: 'alice', partition: 1 },
				{ key: 'k2', user: 'alice', partition: 2 },
				{ key: 'k3', user: 'bob', partition: 0 },
				{ key: 'k4', user: 'bob', partition: 1 },
			]
			for (const send of sends) {
				await withTimeout(
					`produce ${send.key}`,
					producer.send(input, {
						key: send.key,
						value: userEventCodec.encode({ user: send.user }),
						partition: send.partition,
					}),
					15_000
				)
			}

			const assertCount = async (user: string, expected: number) => {
				try {
					await out.waitFor(outputTopic, m => m.key === user && m.value === expected, 25_000)
				} catch (error) {
					const seen = out
						.getTopicMessages<string, number>(outputTopic)
						.filter(m => m.key === user)
						.map(m => m.value)
					throw new Error(`Expected ${user} count to reach ${expected}, but saw: [${seen.join(', ')}]`, {
						cause: error instanceof Error ? error : new Error(String(error)),
					})
				}
			}
			await assertCount('alice', 3)
			await assertCount('bob', 2)
		} finally {
			await withTimeout('app2.close', app2.close(), 20_000).catch(() => {})
			await withTimeout('app1.close', app1.close(), 20_000).catch(() => {})
			await withTimeout('collector stop', out.stop(), 15_000).catch(() => {})
		}
	}, 120_000)
})

describe('globalTable()', () => {
	type User = { name: string }
	type Event = { action: string }
	type Joined = { action: string; name: string }

	const userCodec = codec.json<User>()
	const eventCodec = codec.json<Event>()
	const joinedCodec = codec.json<Joined>()

	// The users topic (3 partitions) and events topic (2 partitions) are deliberately NOT
	// co-partitioned. Pre-fix, globalTable() was consumed via the shared group consumer, so each
	// instance materialized only its ASSIGNED users partitions: with two instances, some events
	// were processed on an instance whose table lacked that user and the inner join silently
	// dropped them (the per-key waitFor times out). Post-fix, every instance tails ALL users
	// partitions with a dedicated group-less consumer, so every event joins on both instances.
	it('materializes all partitions on every instance and blocks start() until caught up', async () => {
		const usersTopic = uniqueTopicName('flow-it-global-users')
		const eventsTopic = uniqueTopicName('flow-it-global-events')
		const outputTopic = uniqueTopicName('flow-it-global-out')
		const appId = `flow-it-global-${Date.now()}`

		await createTopics(client, [
			{ name: usersTopic, partitions: 3 },
			{ name: eventsTopic, partitions: 2 },
			{ name: outputTopic },
		])

		// Seed users across ALL three partitions BEFORE the apps start: start() must block until
		// the global store is caught up, so every event below joins immediately.
		const userKeys = ['u0', 'u1', 'u2', 'u3', 'u4', 'u5']
		for (let i = 0; i < userKeys.length; i++) {
			await producer.send(usersTopic, {
				key: userKeys[i]!,
				value: userCodec.encode({ name: `name-${userKeys[i]}` }),
				partition: i % 3,
			})
		}

		const buildApp = (clientId: string) => {
			const app = flow({
				applicationId: appId,
				client: { clientId, brokers: requireKafkaBrokers() },
				numStreamThreads: 1,
				consumer: { autoOffsetReset: 'earliest', maxWaitMs: 100 },
				changelog: { restoration: restorationOptions },
			})
			const users = app.globalTable(usersTopic, {
				key: stringCodec,
				value: userCodec,
				materialized: { storeName: 'users-global' },
			})
			app.stream(eventsTopic, { key: stringCodec, value: eventCodec })
				.join(users, (event, user) => ({ action: event.action, name: user.name }))
				.to(outputTopic, { value: joinedCodec })
			return app
		}

		const out = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector-${Date.now()}`,
			topics: [{ topic: outputTopic, keyCodec: stringCodec, valueCodec: joinedCodec }],
		})

		const app1 = buildApp(`${appId}-a`)
		const app2 = buildApp(`${appId}-b`)

		try {
			await withTimeout('collector start', out.start(), 15_000)
			await withTimeout('app1.start', app1.start(), 30_000)
			await withTimeout('app2.start', app2.start(), 30_000)

			// One event per user, spread over both (non-co-partitioned) event partitions.
			for (let i = 0; i < userKeys.length; i++) {
				await withTimeout(
					`produce event ${userKeys[i]}`,
					producer.send(eventsTopic, {
						key: userKeys[i]!,
						value: eventCodec.encode({ action: 'click' }),
						partition: i % 2,
					}),
					15_000
				)
			}

			for (const key of userKeys) {
				try {
					await out.waitFor<string, Joined>(
						outputTopic,
						m => m.key === key && m.value.name === `name-${key}`,
						25_000
					)
				} catch (error) {
					const seen = out.getTopicMessages<string, Joined>(outputTopic).map(m => m.key)
					throw new Error(
						`Event for ${key} never joined its user (joined keys so far: [${seen.join(', ')}])`,
						{ cause: error instanceof Error ? error : new Error(String(error)) }
					)
				}
			}

			// The global store keeps tailing after catch-up: a user added post-start becomes
			// joinable. Retry-produce because the inner join drops events until the tail applies.
			await producer.send(usersTopic, { key: 'u-late', value: userCodec.encode({ name: 'name-late' }) })
			let lateJoined = false
			for (let attempt = 0; attempt < 20 && !lateJoined; attempt++) {
				await producer.send(eventsTopic, {
					key: 'u-late',
					value: eventCodec.encode({ action: 'late' }),
					partition: attempt % 2,
				})
				try {
					await out.waitFor<string, Joined>(
						outputTopic,
						m => m.key === 'u-late' && m.value.name === 'name-late',
						1_000
					)
					lateJoined = true
				} catch {
					// Tail not caught up yet - retry
				}
			}
			expect(lateJoined).toBe(true)
		} finally {
			await withTimeout('app2.close', app2.close(), 20_000).catch(() => {})
			await withTimeout('app1.close', app1.close(), 20_000).catch(() => {})
			await withTimeout('collector stop', out.stop(), 15_000).catch(() => {})
		}
	}, 180_000)
})
