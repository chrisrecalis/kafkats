import { afterAll, beforeAll, describe, it } from 'vitest'
import { KafkaClient, type Producer } from '@kafkats/client'
import { TimeWindows, codec, flow } from '../../src/index.js'
import { MultiTopicCollector, createTopics, requireKafkaBrokers, uniqueTopicName } from './test-helpers.js'

/**
 * G1 end-to-end: windowed-store changelog writes must land on the changelog partition matching the
 * task's source partition.
 *
 * Pre-fix, changelog writes carried no explicit partition, so the producer hashed the serialized
 * changelog KEY — which for windowed stores embeds the window boundaries. Different windows of ONE
 * task therefore scattered across ALL changelog partitions. Restoration, however, reads only the
 * task's assigned source-partition numbers: when a second instance joined the group and restored
 * its single assigned partition, most of that task's windowed counts were silently missing, and
 * the next record produced count 1 instead of continuing from the restored count.
 *
 * (G2 — bounded restore checkpoints on the idle escape — cannot be forced deterministically
 * against a real broker; it is covered by tests/unit/restore-idle-checkpoint.test.ts.)
 */

const stringCodec = codec.string()
const numberCodec = {
	encode: (v: number) => {
		const buf = Buffer.alloc(8)
		buf.writeDoubleLE(v, 0)
		return buf
	},
	decode: (b: Buffer) => b.readDoubleLE(0),
}

const WINDOW_SIZE_MS = 60_000
// Several windows per key: a single window's changelog key could coincidentally hash onto the
// right partition, but four windows all doing so is vanishingly unlikely.
const WINDOW_STARTS = [600_000, 660_000, 720_000, 780_000]

let client: KafkaClient
let producer: Producer

beforeAll(async () => {
	client = new KafkaClient({
		clientId: `flow-it-windowed-changelog-${Date.now()}`,
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

describe('windowed changelog partition alignment', () => {
	it('windowed counts survive a restore on an instance that owns a partition subset', async () => {
		const input = uniqueTopicName('flow-it-windowed-changelog-src')
		const outputTopic = uniqueTopicName('flow-it-windowed-changelog-out')
		const appId = `flow-it-windowed-changelog-${Date.now()}`
		const storeName = 'wcounts'

		await createTopics(client, [{ name: input, partitions: 2 }, { name: outputTopic }])
		const keysByPartition = new Map<number, string>([
			[0, 'p0'],
			[1, 'p1'],
		])

		const seedClientId = `${appId}-seed-${Date.now()}`
		const restoreClientId = `${appId}-restore-${Date.now()}`

		const buildApp = (clientId: string, skipRestoration: boolean) => {
			const app = flow({
				applicationId: appId,
				client: {
					clientId,
					brokers: requireKafkaBrokers(),
				},
				numStreamThreads: 1,
				consumer: { autoOffsetReset: 'earliest', maxWaitMs: 100 },
				changelog: {
					restoration: {
						idleTimeoutMs: 2_000,
						initialIdleTimeoutMs: 10_000,
						checkIntervalMs: 200,
						consumerMaxWaitMs: 200,
					},
				},
			})
			app.stream(input, { key: stringCodec, value: numberCodec })
				.groupByKey()
				.windowedBy(TimeWindows.of(WINDOW_SIZE_MS))
				.count({ storeName, changelog: { skipRestoration } })
				.toStream()
				.map<string, number>((windowedKey, count) => [
					`${windowedKey!.key}@${windowedKey!.window.start}`,
					count!,
				])
				.to(outputTopic, { key: stringCodec, value: numberCodec })
			return app
		}

		const waitForMemberAssignment = async (params: {
			groupId: string
			memberClientId: string
			topic: string
			timeoutMs?: number
		}): Promise<number> => {
			const timeoutMs = params.timeoutMs ?? 20_000
			const admin = client.admin()
			const startedAt = Date.now()

			while (Date.now() - startedAt < timeoutMs) {
				const [desc] = await admin.describeGroups([params.groupId])
				if (desc?.state !== 'Stable') {
					await new Promise(resolve => setTimeout(resolve, 200))
					continue
				}

				const member = desc.members.find(m => m.clientId === params.memberClientId)
				const partition = member?.assignment.find(tp => tp.topic === params.topic)?.partition
				if (partition !== undefined) {
					return partition
				}

				await new Promise(resolve => setTimeout(resolve, 200))
			}

			throw new Error(`Timed out waiting for group assignment for ${params.memberClientId} in ${params.groupId}`)
		}

		const out = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector-${Date.now()}`,
			topics: [{ topic: outputTopic, keyCodec: stringCodec, valueCodec: numberCodec }],
		})

		const app1 = buildApp(seedClientId, true)
		const app2 = buildApp(restoreClientId, false)

		try {
			await withTimeout('output collector start', out.start(), 15_000)
			await withTimeout('app1.start', app1.start(), 30_000)

			// Seed one count into each window of each partition's key.
			for (const [partition, key] of keysByPartition) {
				for (const windowStart of WINDOW_STARTS) {
					await withTimeout(
						`produce seed (${key}@${windowStart})`,
						producer.send(input, {
							key,
							value: numberCodec.encode(1),
							partition,
							timestamp: new Date(windowStart + 1_000),
						}),
						15_000
					)
				}
			}
			for (const key of keysByPartition.values()) {
				for (const windowStart of WINDOW_STARTS) {
					await out.waitFor(outputTopic, m => m.key === `${key}@${windowStart}` && m.value === 1, 15_000)
				}
			}

			// Second instance joins: it gets one source partition and restores ONLY that partition
			// of the windowed changelog. Pre-fix the seed writes were key-hash scattered across both
			// changelog partitions, so most of this task's windows were missing after the restore.
			await withTimeout('app2.start', app2.start(), 30_000)

			const assignedPartition = await withTimeout(
				'wait for app2 assignment',
				waitForMemberAssignment({ groupId: appId, memberClientId: restoreClientId, topic: input }),
				25_000
			)
			const key = keysByPartition.get(assignedPartition)
			if (!key) {
				throw new Error(`Missing test key for assigned partition ${assignedPartition}`)
			}

			for (const windowStart of WINDOW_STARTS) {
				await withTimeout(
					`produce post-restore (${key}@${windowStart})`,
					producer.send(input, {
						key,
						value: numberCodec.encode(1),
						partition: assignedPartition,
						timestamp: new Date(windowStart + 2_000),
					}),
					15_000
				)
			}

			// Every window must continue from its restored count (2), not restart at 1.
			for (const windowStart of WINDOW_STARTS) {
				try {
					await out.waitFor(outputTopic, m => m.key === `${key}@${windowStart}` && m.value === 2, 15_000)
				} catch (error) {
					const seen = out
						.getTopicMessages<string, number>(outputTopic)
						.filter(m => m.key === `${key}@${windowStart}`)
						.map(m => m.value)
					throw new Error(
						`Window ${key}@${windowStart} did not reach count 2 after restore; saw [${seen.join(', ')}] ` +
							'(windowed changelog writes likely landed on the wrong partition)',
						{ cause: error instanceof Error ? error : new Error(String(error)) }
					)
				}
			}
		} finally {
			await withTimeout('app2.close', app2.close(), 20_000).catch(() => {})
			await withTimeout('app1.close', app1.close(), 20_000).catch(() => {})
			await withTimeout('output collector stop', out.stop(), 15_000).catch(() => {})
		}
	}, 120_000)
})
