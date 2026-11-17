/**
 * At-least-once commit failures (D10, 4.6.1, 4.8.1; KF5, DL14): a failed interval commit migrates (the run is
 * abandoned and joins again from the committed offsets), so every input is processed at least once. A rebalance that
 * prevents the revocation commit is only a warning: the offsets stay uncommitted and are processed again. Commit
 * timeouts (a coordinator that does not answer) are retried at the next interval: the tasks stay and nothing
 * rebalances, unless they last `taskTimeoutMs`.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import {
	codec,
	ErrorCode,
	KafkaError,
	RebalanceInProgressError,
	TimeoutError,
	type KafkaClient,
	type Producer,
} from '@kafkats/client'
import type { FlowApp, FlowConfig, StreamState } from '../../src/api/app.js'
import { TaskMigratedError } from '../../src/api/errors.js'
import { flow } from '../../src/app/flow.js'
import {
	collectUnhandledRejections,
	createTopics,
	fetchCommitted,
	injectConsumerCommitFailures,
	MultiTopicCollector,
	newClient,
	sleep,
	uniqueTopicName,
	waitUntil,
} from './test-helpers.js'

const str = codec.string()

let admin: KafkaClient
let producer: Producer

/** `input` copied to `out`, on a client of its own whose consumer commits fail while `failing()` holds. */
function copying(
	applicationId: string,
	input: string,
	out: string,
	failing: { fail: () => boolean; error: () => Error } | null,
	config: Partial<FlowConfig> = {}
): { app: FlowApp; client: KafkaClient } {
	const client = newClient('alos-failure-app')
	if (failing) injectConsumerCommitFailures(client, failing.fail, failing.error)
	const app = flow({ applicationId, client, consumer: { maxWaitMs: 100 }, producer: { lingerMs: 0 }, ...config })
	app.stream(input, { key: str, value: str }).to(out, { key: str, value: str })
	return { app, client }
}

/** Values `from`..`to - 1`; key `k<i>` on partition i % partitions. */
async function produce(input: string, from: number, to: number, partitions = 1): Promise<void> {
	await producer.send(
		input,
		Array.from({ length: to - from }, (_, i) => ({
			key: `k${from + i}`,
			value: Buffer.from(String(from + i)),
			partition: (from + i) % partitions,
		}))
	)
}

async function topics(prefix: string, partitions = 1) {
	const input = uniqueTopicName(`${prefix}-in`)
	const out = uniqueTopicName(`${prefix}-out`)
	await createTopics(admin, [{ name: input, partitions }, { name: out }])
	const output = new MultiTopicCollector({
		client: admin,
		groupId: uniqueTopicName(`${prefix}-reader`),
		topics: [{ topic: out, valueCodec: str }],
	})
	await output.start()
	const values = () => output.getTopicMessages<string, string>(out).map(m => Number(m.value))
	return { input, out, output, values }
}

/** Every value of 0..n-1 is there at least `times` times. */
function covers(values: readonly number[], n: number, times = 1): boolean {
	const counts = new Map<number, number>()
	for (const value of values) counts.set(value, (counts.get(value) ?? 0) + 1)
	return Array.from({ length: n }, (_, i) => i).every(i => (counts.get(i) ?? 0) >= times)
}

/** The member ids of the group: unchanged as long as no member left and joined again. */
async function members(groupId: string): Promise<string[]> {
	const [group] = await admin.admin().describeGroups([groupId])
	return (group?.members ?? []).map(member => member.memberId).sort()
}

/** Records every state() the app is in while `during` runs. */
async function statesDuring(app: FlowApp, during: () => Promise<void>): Promise<Set<StreamState>> {
	const states = new Set<StreamState>([app.state()])
	const poll = setInterval(() => states.add(app.state()), 20)
	try {
		await during()
	} finally {
		clearInterval(poll)
	}
	states.add(app.state())
	return states
}

describe('at-least-once commit failures', () => {
	const unhandled = collectUnhandledRejections()

	beforeAll(async () => {
		admin = newClient('alos-failure-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})

	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
		expect(unhandled).toEqual([])
	})

	it('a failed interval commit recovers by joining again: every input is processed at least once', async () => {
		const { input, out, output, values } = await topics('alos-failed-commit')
		const applicationId = uniqueTopicName('alos-failed-commit')
		const injected = new KafkaError('injected commit failure', ErrorCode.UnknownServerError)
		let armed = false
		let failures = 0
		const fail = () => {
			if (!armed) return false
			armed = false
			failures += 1
			return true
		}
		const { app, client } = copying(
			applicationId,
			input,
			out,
			{ fail, error: () => injected },
			{ commitIntervalMs: 200 }
		)
		try {
			await app.start()
			await produce(input, 0, 10)
			await waitUntil('10 copied', () => covers(values(), 10), 15_000)
			armed = true
			await produce(input, 10, 20)
			await waitUntil('the commit failed', () => failures === 1, 15_000)
			await waitUntil('recovered', () => app.state() === 'RUNNING', 20_000)
			// A recoverable failure: getError() reports it, state() does not.
			expect(app.getError()).toBe(injected)
			await produce(input, 20, 30)
			await waitUntil('30 copied', () => covers(values(), 30), 20_000)
			await sleep(500)
			expect(app.state()).toBe('RUNNING')
		} finally {
			await app.close()
			await client.disconnect()
			await output.stop()
		}
	}, 60_000)

	it('a RebalanceInProgressError on the revocation commit is a warning: the offsets are processed again', async () => {
		const { input, out, output, values } = await topics('alos-revocation', 2)
		const applicationId = uniqueTopicName('alos-revocation')
		let armed = false
		let failures = 0
		const fail = () => {
			if (!armed) return false
			armed = false
			failures += 1
			return true
		}
		const error = () => new RebalanceInProgressError(applicationId)
		// Only revocation and close commit: the interval is longer than the test.
		const config = { commitIntervalMs: 120_000 }
		const first = copying(applicationId, input, out, { fail, error }, config)
		const second = copying(applicationId, input, out, null, config)
		try {
			await first.app.start()
			await produce(input, 0, 10, 2)
			await waitUntil('10 copied', () => covers(values(), 10), 15_000)
			armed = true
			await second.app.start()
			expect(failures).toBe(1)
			await waitUntil('both run', () => first.app.state() === 'RUNNING', 20_000)
			// Nothing of the first instance's work was committed: every record is processed again.
			await waitUntil('10 copied twice', () => covers(values(), 10, 2), 20_000)
			expect(first.app.getError()).toBeNull()
			await produce(input, 10, 20, 2)
			await waitUntil('20 copied', () => covers(values(), 20), 20_000)
			expect(first.app.state()).toBe('RUNNING')
		} finally {
			await first.app.close()
			await second.app.close()
			await first.client.disconnect()
			await second.client.disconnect()
			await output.stop()
		}
	}, 60_000)

	it('commit timeouts are retried at the next interval: the tasks stay, nothing rebalances, then commits catch up', async () => {
		const { input, out, output, values } = await topics('alos-timeouts')
		const applicationId = uniqueTopicName('alos-timeouts')
		let timingOut = false
		let timeouts = 0
		const fail = () => {
			if (timingOut) timeouts += 1
			return timingOut
		}
		const error = () => new TimeoutError('injected commit timeout', 10_000)
		const { app, client } = copying(applicationId, input, out, { fail, error }, { commitIntervalMs: 200 })
		try {
			await app.start()
			await produce(input, 0, 10)
			await waitUntil('10 copied', () => covers(values(), 10), 15_000)
			const before = await members(applicationId)
			const states = await statesDuring(app, async () => {
				timingOut = true
				await produce(input, 10, 20)
				await sleep(3000)
				timingOut = false
			})
			// Processing went on, every commit timed out, and the app never left RUNNING (no migration, no rebalance).
			expect(covers(values(), 20)).toBe(true)
			expect(timeouts).toBeGreaterThanOrEqual(3)
			expect([...states]).toEqual(['RUNNING'])
			expect(app.getError()).toBeNull()
			expect(await members(applicationId)).toEqual(before)
			// The next interval commits everything.
			for (const deadline = Date.now() + 5000; Date.now() < deadline; await sleep(100)) {
				if ((await fetchCommitted(admin, applicationId, input)).offset === 20n) break
			}
			expect((await fetchCommitted(admin, applicationId, input)).offset).toBe(20n)
		} finally {
			await app.close()
			await client.disconnect()
			await output.stop()
		}
	}, 60_000)

	it('commit timeouts lasting taskTimeoutMs migrate: the app joins again and goes on', async () => {
		const { input, out, output, values } = await topics('alos-task-timeout')
		const applicationId = uniqueTopicName('alos-task-timeout')
		let timingOut = false
		const error = () => new TimeoutError('injected commit timeout', 10_000)
		const { app, client } = copying(
			applicationId,
			input,
			out,
			{ fail: () => timingOut, error },
			{ commitIntervalMs: 200, taskTimeoutMs: 1000 }
		)
		try {
			await app.start()
			await produce(input, 0, 10)
			await waitUntil('10 copied', () => covers(values(), 10), 15_000)
			timingOut = true
			await produce(input, 10, 20)
			await waitUntil('migrated', () => app.getError() instanceof TaskMigratedError, 15_000)
			timingOut = false
			await waitUntil('recovered', () => app.state() === 'RUNNING', 20_000)
			await produce(input, 20, 30)
			await waitUntil('30 copied', () => covers(values(), 30), 20_000)
		} finally {
			await app.close()
			await client.disconnect()
			await output.stop()
		}
	}, 60_000)
})
