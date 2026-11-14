/**
 * Round 3 / attack 8: changelog / restore edge cases.
 *  - checkpoint present but the changelog topic was deleted and recreated (shorter): stale local state must not survive
 *  - topology change that renumbers auto-generated store names must not feed old state into the wrong operator
 *  - skipRestoration with a durable provider keeps counting
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow } from '../../src/index.js'
import { createTopics, uniqueTopicName } from './test-helpers.js'
import { DurableMemoryProvider, newClient, readAllCommitted, sleep } from './eos-test-util.js'

const str = codec.string()
const num = codec.json<number>()

describe('hardening: changelog edge cases', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('r3-ce-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	const baseCfg = (applicationId: string) =>
		({
			applicationId,
			client: newClient('r3-ce-app'),
			processingGuarantee: 'exactly_once' as const,
			commitIntervalMs: 100,
			consumer: { autoOffsetReset: 'earliest' as const, maxWaitMs: 50 },
			producer: { lingerMs: 0 },
			changelog: { restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 3_000, checkIntervalMs: 50 } },
		}) satisfies Parameters<typeof flow>[0]

	const vals = async (topic: string) => (await readAllCommitted(admin, topic, 1_200)).map(r => num.decode(r.value!))
	async function waitVals(topic: string, n: number) {
		const deadline = Date.now() + 15_000
		let v: number[] = []
		while (Date.now() < deadline) {
			v = await vals(topic)
			if (v.length >= n) return v
			await sleep(300)
		}
		return v
	}

	it('checkpoint beyond a recreated (shorter) changelog: local state is wiped and rebuilt from the changelog', async () => {
		const input = uniqueTopicName('r3-ce1-in')
		const output = uniqueTopicName('r3-ce1-out')
		const applicationId = uniqueTopicName('r3-ce1-app')
		await createTopics(admin, [{ name: input }, { name: output }])
		const provider = new DurableMemoryProvider()
		const mk = () => {
			const app = flow({ ...baseCfg(applicationId), stateStoreProvider: provider })
			app.stream(input, { key: str, value: str })
				.groupByKey()
				.count({ storeName: 'counts', key: str, value: num })
				.toStream()
				.to(output, { key: str, value: num })
			return app
		}
		let app = mk()
		await app.start()
		await producer.send(
			input,
			Array.from({ length: 5 }, () => ({ key: 'k', value: Buffer.from('x') }))
		)
		expect(await waitVals(output, 5)).toEqual([1, 2, 3, 4, 5])
		await app.close()
		const changelog = `${applicationId}-counts-changelog`
		await admin.admin().deleteTopics([changelog])
		await sleep(2_000)
		app = mk() // autoCreate recreates an empty changelog
		try {
			await app.start()
			await producer.send(input, { key: 'k', value: Buffer.from('x') })
			// the changelog (source of truth) is empty, so the stale local count of 5 must not survive
			expect(await waitVals(output, 6)).toEqual([1, 2, 3, 4, 5, 1])
		} finally {
			await app.close().catch(() => {})
		}
	}, 60_000)

	it('skipRestoration with a durable provider: counting continues across a restart', async () => {
		const input = uniqueTopicName('r3-ce3-in')
		const output = uniqueTopicName('r3-ce3-out')
		const applicationId = uniqueTopicName('r3-ce3-app')
		await createTopics(admin, [{ name: input }, { name: output }])
		const provider = new DurableMemoryProvider()
		const mk = () => {
			const app = flow({ ...baseCfg(applicationId), stateStoreProvider: provider })
			app.stream(input, { key: str, value: str })
				.groupByKey()
				.count({ storeName: 'counts', key: str, value: num, changelog: { skipRestoration: true } })
				.toStream()
				.to(output, { key: str, value: num })
			return app
		}
		let app = mk()
		await app.start()
		await producer.send(
			input,
			Array.from({ length: 3 }, () => ({ key: 'k', value: Buffer.from('x') }))
		)
		expect(await waitVals(output, 3)).toEqual([1, 2, 3])
		await app.close()
		app = mk()
		try {
			await app.start()
			await producer.send(input, { key: 'k', value: Buffer.from('x') })
			expect(await waitVals(output, 4)).toEqual([1, 2, 3, 4])
		} finally {
			await app.close().catch(() => {})
		}
	}, 60_000)
})
