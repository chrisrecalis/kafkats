/**
 * Round 3 / attack 9: lifecycle storms. start()/close() races, close() during rebalance / restoration / worker
 * backoff must neither hang (> 20s) nor leak sockets nor leave state() inconsistent nor leak unhandled rejections.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow, type FlowApp } from '../../src/index.js'
import { createTopics, requireKafkaBrokers, uniqueTopicName } from './test-helpers.js'
import { injectCommitFailures, newClient, sleep } from './eos-test-util.js'

const str = codec.string()
const num = codec.json<number>()

function withTimeout<T>(label: string, p: Promise<T>, ms: number): Promise<T> {
	let t: ReturnType<typeof setTimeout>
	return Promise.race([
		p,
		new Promise<T>((_r, rej) => {
			t = setTimeout(() => rej(new Error(`HANG > ${ms}ms: ${label}`)), ms)
		}),
	]).finally(() => clearTimeout(t))
}

// an app that creates its client from a config object owns (and must disconnect) it
const ownedClient = () => ({
	clientId: `r3-lc-${Math.random().toString(36).slice(2, 8)}`,
	brokers: requireKafkaBrokers(),
})
const sockets = () =>
	(process as unknown as { _getActiveHandles(): Array<{ constructor: { name: string } }> })
		._getActiveHandles()
		.filter(h => h.constructor.name === 'Socket').length

describe('round3: lifecycle storms', () => {
	let admin: KafkaClient
	let producer: Producer
	const rejections: unknown[] = []
	const onRej = (r: unknown) => rejections.push(r)
	beforeAll(async () => {
		process.on('unhandledRejection', onRej)
		admin = newClient('r3-lc-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})
	afterAll(async () => {
		process.off('unhandledRejection', onRej)
		await producer.disconnect()
		await admin.disconnect()
	})

	const build = (
		applicationId: string,
		input: string,
		output: string,
		guarantee: 'at_least_once' | 'exactly_once',
		client: KafkaClient | { clientId: string; brokers: string[] } = ownedClient()
	): FlowApp => {
		const app = flow({
			applicationId,
			client,
			processingGuarantee: guarantee,
			commitIntervalMs: 100,
			consumer: {
				autoOffsetReset: 'earliest',
				maxWaitMs: 50,
				sessionTimeoutMs: 6_000,
				rebalanceTimeoutMs: 10_000,
			},
			producer: { lingerMs: 0 },
			changelog: { restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 3_000, checkIntervalMs: 50 } },
		})
		app.stream(input, { key: str, value: str })
			.groupByKey()
			.count({ storeName: 'counts', key: str, value: num })
			.toStream()
			.to(output, { key: str, value: num })
		return app
	}

	for (const guarantee of ['exactly_once', 'at_least_once'] as const) {
		it(`${guarantee}: 10 concurrent start()/close() races, with a peer instance forcing rebalances, never hang or leak`, async () => {
			const input = uniqueTopicName('r3-lc-in')
			const output = uniqueTopicName('r3-lc-out')
			const applicationId = uniqueTopicName('r3-lc-app')
			await createTopics(admin, [{ name: input, partitions: 4 }, { name: output }])
			await producer.send(
				input,
				Array.from({ length: 40 }, (_, i) => ({ key: `k${i % 8}`, value: Buffer.from('x') }))
			)
			const peer = build(applicationId, input, output, guarantee)
			await peer.start()
			await sleep(1_000)
			const before = sockets()
			try {
				for (let i = 0; i < 10; i++) {
					const app = build(applicationId, input, output, guarantee)
					const started = app.start().then(
						() => 'started',
						() => 'start-failed'
					)
					await sleep(Math.floor(Math.random() * 1_800)) // lands during connect / join / rebalance / restore
					await withTimeout(`close #${i}`, app.close(), 20_000)
					await withTimeout(`start settle #${i}`, started, 20_000)
					expect(app.state(), `iteration ${i}`).toBe('STOPPED')
				}
				expect(peer.state()).not.toBe('ERROR')
				await sleep(2_000)
				expect(sockets() - before, 'sockets leaked by 10 start()/close() cycles').toBeLessThanOrEqual(5)
				expect(rejections).toEqual([])
			} finally {
				await peer.close().catch(() => {})
			}
		}, 88_000)
	}

	it('close() while the worker is in restart backoff returns promptly and leaves STOPPED', async () => {
		const input = uniqueTopicName('r3-lc-bo-in')
		const output = uniqueTopicName('r3-lc-bo-out')
		const applicationId = uniqueTopicName('r3-lc-bo-app')
		await createTopics(admin, [{ name: input, partitions: 2 }, { name: output }])
		const client = newClient('r3-lc-bo')
		injectCommitFailures(client, () => true)
		const app = build(applicationId, input, output, 'exactly_once', client)
		try {
			await app.start()
			await producer.send(
				input,
				Array.from({ length: 10 }, (_, i) => ({ key: `k${i}`, value: Buffer.from('x') }))
			)
			// every commit fails: the worker keeps failing and backing off (0.5s .. 30s)
			const deadline = Date.now() + 15_000
			while (Date.now() < deadline && app.state() !== 'REBALANCING') await sleep(100)
			await sleep(2_500)
			const t0 = Date.now()
			await withTimeout('close during backoff', app.close(), 20_000)
			expect(Date.now() - t0).toBeLessThan(10_000)
			expect(app.state()).toBe('STOPPED')
			expect(rejections).toEqual([])
		} finally {
			await app.close().catch(() => {})
			await client.disconnect().catch(() => {})
		}
	}, 60_000)
})
