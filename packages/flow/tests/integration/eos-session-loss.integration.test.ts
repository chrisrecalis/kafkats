/**
 * Exactly-once and the loss of the group session (CR1, runtime§6.10): an instance whose commit is stuck in its flush
 * loses its session (no heartbeat for longer than `sessionTimeoutMs`); the group goes on without it, and the other
 * instance processes the same input. When the flush ends, the stale transaction is aborted (its tasks were lost, or
 * the broker fences its offsets by the generation): nothing of it becomes visible, and every input is output once.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { codec, type KafkaClient, type Producer } from '@kafkats/client'
import type { FlowApp } from '../../src/api/app.js'
import { flow } from '../../src/app/flow.js'
import {
	collectUnhandledRejections,
	createTopics,
	fetchCommitted,
	newClient,
	readAllCommitted,
	sleep,
	uniqueTopicName,
	waitUntil,
} from './test-helpers.js'

const str = codec.string()
const SESSION_TIMEOUT_MS = 6000

let admin: KafkaClient
let producer: Producer

/** While closed, `passed` is pending; open from the start. */
class Gate {
	passed: Promise<void> = Promise.resolve()
	closed = false
	private release = (): void => {}

	close(): void {
		this.closed = true
		this.passed = new Promise(resolve => {
			this.release = resolve
		})
	}

	open(): void {
		this.closed = false
		this.release()
	}
}

/**
 * A client whose group member sends no heartbeat while `gate` is closed, and whose transactional sends are not
 * acknowledged before it opens (so a commit waits in its flush). Counts the held sends.
 */
function stalledClient(gate: Gate): { client: KafkaClient; held: () => number } {
	const client = newClient('session-loss-a')
	const { cluster } = client
	const dedicated = cluster.getDedicatedCoordinator.bind(cluster)
	cluster.getDedicatedCoordinator = async (type, key) => {
		const broker = await dedicated(type, key)
		const heartbeat = broker.heartbeat.bind(broker)
		broker.heartbeat = async request => {
			await gate.passed
			return heartbeat(request)
		}
		return broker
	}
	let held = 0
	const createProducer = client.producer.bind(client)
	client.producer = config => {
		const producer = createProducer(config)
		const begin = producer.beginTransaction.bind(producer)
		producer.beginTransaction = async options => {
			const transaction = await begin(options)
			const send = transaction.send.bind(transaction)
			transaction.send = ((topic: string, message: never) => {
				const sent = send(topic, message)
				if (!gate.closed) {
					return sent
				}
				held += 1
				// Its failure is reported once the gate opens.
				sent.catch(() => {})
				return gate.passed.then(() => sent)
			}) as typeof transaction.send
			return transaction
		}
		return producer
	}
	return { client, held: () => held }
}

/** The number of members of the group once it is stable, else -1. */
async function members(groupId: string): Promise<number> {
	const [group] = await admin.admin().describeGroups([groupId])
	return group?.state === 'Stable' ? group.members.length : -1
}

/** Values `from`..`to - 1`; value i on partition i % 2. */
async function produce(input: string, from: number, to: number): Promise<void> {
	await producer.send(
		input,
		Array.from({ length: to - from }, (_, i) => ({
			key: `k${from + i}`,
			value: Buffer.from(String(from + i)),
			partition: (from + i) % 2,
		}))
	)
}

describe('exactly-once session loss', () => {
	const unhandled = collectUnhandledRejections()

	beforeAll(async () => {
		admin = newClient('session-loss-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})

	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
		expect(unhandled).toEqual([])
	})

	it('a transaction stuck in its flush while the session expires is aborted: each input is output once', async () => {
		const input = uniqueTopicName('session-loss-in')
		const out = uniqueTopicName('session-loss-out')
		const applicationId = uniqueTopicName('session-loss-app')
		await createTopics(admin, [{ name: input, partitions: 2 }, { name: out }])
		const gate = new Gate()
		const stalled = stalledClient(gate)
		/** Outputs `<value>@<tag>`: which instance processed the input. */
		const build = (client: KafkaClient, tag: string): FlowApp => {
			const app = flow({
				applicationId,
				client,
				processingGuarantee: 'exactly_once',
				consumer: { maxWaitMs: 100, sessionTimeoutMs: SESSION_TIMEOUT_MS, heartbeatIntervalMs: 500 },
				// The broker must not abort the stuck transaction by itself first.
				producer: { lingerMs: 0, transactionTimeoutMs: 60_000 },
			})
			app.stream(input, { key: str, value: str })
				.mapValues(value => `${value}@${tag}`)
				.to(out, { key: str, value: str })
			return app
		}
		const outputs = async () => (await readAllCommitted(admin, out, 800)).map(m => m.value!.toString())
		const stuck = build(stalled.client, 'A')
		const healthy = build(newClient('session-loss-b'), 'B')
		try {
			await stuck.start()
			await healthy.start()
			await produce(input, 0, 10)
			for (const deadline = Date.now() + 20_000; (await outputs()).length < 10 && Date.now() < deadline;) {
				await sleep(200)
			}
			expect(await outputs()).toHaveLength(10)

			// From now on the first instance's sends are not acknowledged and it sends no heartbeat.
			gate.close()
			await produce(input, 10, 30)
			await waitUntil('a send held in the transaction', () => stalled.held() > 0, 10_000)
			for (const deadline = Date.now() + 30_000; (await members(applicationId)) !== 1 && Date.now() < deadline;) {
				await sleep(250)
			}
			expect(await members(applicationId)).toBe(1)
			// The second instance took over and committed every input, the first instance's partition included.
			for (const deadline = Date.now() + 20_000; Date.now() < deadline; await sleep(250)) {
				const committed = await Promise.all([0, 1].map(p => fetchCommitted(admin, applicationId, input, p)))
				if (committed.every(({ offset }) => offset === 15n)) break
			}
			for (const p of [0, 1]) expect((await fetchCommitted(admin, applicationId, input, p)).offset).toBe(15n)

			// The flush ends: its transaction is aborted, and the first instance joins again.
			gate.open()
			for (const deadline = Date.now() + 30_000; (await members(applicationId)) !== 2 && Date.now() < deadline;) {
				await sleep(250)
			}
			await waitUntil('the first instance runs again', () => stuck.state() === 'RUNNING', 20_000)
			await produce(input, 30, 40)
			for (const deadline = Date.now() + 20_000; (await outputs()).length < 40 && Date.now() < deadline;) {
				await sleep(250)
			}
			const values = await outputs()
			const inputs = values.map(value => Number(value.split('@')[0]))
			expect(inputs.sort((a, b) => a - b)).toEqual(Array.from({ length: 40 }, (_, i) => i))
			// Every input the stuck transaction held was output by the second instance only.
			const phase2 = values.filter(value => {
				const i = Number(value.split('@')[0])
				return i >= 10 && i < 30
			})
			expect(phase2.every(value => value.endsWith('@B'))).toBe(true)
			expect(stuck.state()).toBe('RUNNING')
		} finally {
			gate.open()
			await stuck.close()
			await healthy.close()
			await stalled.client.disconnect()
		}
	}, 120_000)
})
