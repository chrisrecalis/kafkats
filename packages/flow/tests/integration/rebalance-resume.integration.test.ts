/**
 * Rebalances on the task runtime (D9, 4.3, 4.7.3, BC17): a task that stays with an instance across a rebalance is
 * suspended and resumed, and catches up from its checkpoint without a restore consumer (Kafka Streams resumes such
 * tasks); counts stay exact. A task an instance gets anew is restored, and `state()` is 'REBALANCING' until it runs.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { codec, type KafkaClient, type Producer } from '@kafkats/client'
import type { FlowApp, FlowConfig } from '../../src/api/app.js'
import { flow } from '../../src/app/flow.js'
import type { KeyValueStore, KeyValueStoreOptions } from '../../src/state.js'
import { InMemoryStateStoreProvider } from '../../src/state/memory.js'
import {
	collectUnhandledRejections,
	createTopics,
	delayPuts,
	newClient,
	readAllCommitted,
	sleep,
	uniqueTopicName,
	waitUntil,
} from './test-helpers.js'

const str = codec.string()
const num = codec.json<number>()
const KEYS = 8

let admin: KafkaClient
let producer: Producer

/** A client whose restore consumers (groupId `<applicationId>-restore-<taskId>-<n>`, DL14) are counted. */
function countingRestores(applicationId: string): { client: KafkaClient; restores: () => number } {
	const client = newClient('resume-app')
	let restores = 0
	const createConsumer = client.consumer.bind(client)
	client.consumer = config => {
		if (config.groupId.startsWith(`${applicationId}-restore-`)) restores += 1
		return createConsumer(config)
	}
	return { client, restores: () => restores }
}

/** In-memory stores whose next put, once armed, blocks until released. */
class BlockingProvider extends InMemoryStateStoreProvider {
	private armed = false
	private blocked = false
	private release: () => void = () => {}

	arm(): void {
		this.armed = true
	}

	get blocking(): boolean {
		return this.blocked
	}

	unblock(): void {
		this.release()
	}

	override createKeyValueStore<K, V>(name: string, options: KeyValueStoreOptions<K, V>): KeyValueStore<K, V> {
		return delayPuts(super.createKeyValueStore(name, options), async () => {
			if (!this.armed) return
			this.armed = false
			this.blocked = true
			await new Promise<void>(resolve => {
				this.release = resolve
			})
			this.blocked = false
		})
	}
}

/** Counts per key of `input` (two partitions) into `out`. */
function counting(applicationId: string, input: string, out: string, config: Partial<FlowConfig>): FlowApp {
	const app = flow({
		applicationId,
		client: newClient('resume-app'),
		consumer: { maxWaitMs: 100 },
		producer: { lingerMs: 0 },
		changelog: { restoration: { idleTimeoutMs: 5_000, consumerMaxWaitMs: 100 } },
		...config,
	})
	app.stream(input, { key: str, value: str })
		.groupByKey()
		.count({ storeName: 'counts', key: str, value: num })
		.toStream()
		.to(out, { key: str, value: num })
	return app
}

/** One record per key; key `k<i>` lives on partition i % 2. */
async function round(input: string): Promise<void> {
	await producer.send(
		input,
		Array.from({ length: KEYS }, (_, i) => ({ key: `k${i}`, value: Buffer.from('x'), partition: i % 2 }))
	)
}

/** The committed counts per key, in output order. */
async function counts(out: string): Promise<Map<string, number[]>> {
	const byKey = new Map<string, number[]>()
	for (const m of await readAllCommitted(admin, out, 800)) {
		const key = m.key!.toString()
		byKey.set(key, [...(byKey.get(key) ?? []), num.decode(m.value!)])
	}
	return byKey
}

async function countsReach(out: string, n: number): Promise<Map<string, number[]>> {
	let last = new Map<string, number[]>()
	for (const deadline = Date.now() + 30_000; Date.now() < deadline; await sleep(200)) {
		last = await counts(out)
		if (last.size === KEYS && [...last.values()].every(seq => seq.at(-1) === n)) break
	}
	return last
}

const exactly = (n: number) =>
	new Map(Array.from({ length: KEYS }, (_, i) => [`k${i}`, Array.from({ length: n }, (_, c) => c + 1)]))

describe('rebalance and resume', () => {
	const unhandled = collectUnhandledRejections()

	beforeAll(async () => {
		admin = newClient('resume-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})

	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
		expect(unhandled).toEqual([])
	})

	it('exactly_once: a task kept across a rebalance resumes without a restore consumer; counts are exact', async () => {
		const input = uniqueTopicName('resume-in')
		const out = uniqueTopicName('resume-out')
		const applicationId = uniqueTopicName('resume-app')
		await createTopics(admin, [{ name: input, partitions: 2 }, { name: out }])
		const { client: firstClient, restores } = countingRestores(applicationId)
		const config = { processingGuarantee: 'exactly_once' as const, commitIntervalMs: 100 }
		const first = counting(applicationId, input, out, { ...config, client: firstClient })
		const second = counting(applicationId, input, out, config)
		try {
			await first.start()
			await round(input)
			expect(await countsReach(out, 1)).toEqual(exactly(1))

			// The second instance joins: the first one suspends both tasks, keeps one and resumes it from its
			// checkpoint, which the revocation commit wrote: nothing to restore, no restore consumer.
			await second.start()
			await waitUntil('the first instance runs again', () => first.state() === 'RUNNING', 20_000)
			expect(restores()).toBe(0)
			await round(input)
			expect(await countsReach(out, 2)).toEqual(exactly(2))

			// The second instance leaves: its task is new to the first one since the second one counted on it, so it
			// is restored (one consumer); the kept task resumes without one again.
			await second.close()
			await waitUntil('the first instance runs both tasks', () => first.state() === 'RUNNING', 20_000)
			await round(input)
			expect(await countsReach(out, 3)).toEqual(exactly(3))
			expect(restores()).toBe(1)
		} finally {
			await first.close()
			await second.close()
			await firstClient.disconnect()
		}
	}, 90_000)

	it("state() is 'REBALANCING' while a task assigned anew restores, then 'RUNNING'", async () => {
		const input = uniqueTopicName('resume-state-in')
		const out = uniqueTopicName('resume-state-out')
		const applicationId = uniqueTopicName('resume-state-app')
		await createTopics(admin, [{ name: input, partitions: 2 }, { name: out }])
		const provider = new BlockingProvider()
		const first = counting(applicationId, input, out, { stateStoreProvider: provider, commitIntervalMs: 100 })
		const second = counting(applicationId, input, out, { commitIntervalMs: 100 })
		try {
			await first.start()
			await second.start()
			await waitUntil('the first instance runs again', () => first.state() === 'RUNNING', 20_000)
			await round(input)
			expect(await countsReach(out, 1)).toEqual(exactly(1))

			// The first instance's next store write is the restore of the task it gets when the second one leaves.
			provider.arm()
			await second.close()
			await waitUntil('the restore writes', () => provider.blocking, 20_000)
			expect(first.state()).toBe('REBALANCING')
			await sleep(1000)
			expect(first.state()).toBe('REBALANCING')
			provider.unblock()
			await waitUntil('the first instance runs both tasks', () => first.state() === 'RUNNING', 20_000)
			await round(input)
			expect(await countsReach(out, 2)).toEqual(exactly(2))
		} finally {
			provider.unblock()
			await first.close()
			await second.close()
		}
	}, 90_000)
})
