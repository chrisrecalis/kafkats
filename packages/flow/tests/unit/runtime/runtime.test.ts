/**
 * FlowRuntime over fake IO (4.6.2, 4.10, 4.11): recovery (task migration with backoff), halt, revive in place, the
 * prune of unowned state, close bounded by construction (CR2) and close during start (CR14); then the runtime blocks
 * of the legacy flow.test.ts with the task runtime's expectations: one consumer and one producer per instance, the
 * transactional id `<applicationId>-<processId>`, one transaction for every task, a record delivered while stopping is
 * not processed under either guarantee, a halt stops the whole runtime, also a recovery waiting out its backoff.
 */
import { describe, expect, it, vi } from 'vitest'
import { KafkaClient, string, TimeoutError } from '@kafkats/client'
import type { FlowApp, FlowConfig } from '@/api/app.js'
import { FailedProcessingError, NoOffsetResetError } from '@/api/errors.js'
import { flow } from '@/app/flow.js'
import { codec } from '@/codec.js'
import { COUNT_CODEC } from '@/dsl/ops/aggregate.js'
import { producerConfig, type KafkaIOOptions } from '@/runtime/kafka-io.js'
import { valueAndTimestampCodec } from '@/state/timestamped.js'
import type { FakeIO } from '../support/fake-io.js'
import { RuntimeHarness as Harness } from '../support/runtime-harness.js'

/** flow() apps of the last describe block run on fake IO; the options their start passed are kept. */
const kafkaIO = vi.hoisted(() => ({ created: [] as { options: KafkaIOOptions; io: FakeIO }[] }))
vi.mock('@/runtime/kafka-io.js', async original => {
	const { FakeIO } = await import('../support/fake-io.js')
	return {
		...(await original<typeof import('@/runtime/kafka-io.js')>()),
		createKafkaIO: (options: KafkaIOOptions) => {
			const io = new FakeIO({ groupId: options.settings.applicationId, time: Date.now() })
			for (const topic of ['events', 'out']) io.cluster.createTopic(topic, 1)
			kafkaIO.created.push({ options, io })
			return io
		},
	}
})

const strings = { key: string(), value: string() }

/** `in` -> mapValues (throws for 'boom') -> `out`. */
function stateless(app: FlowApp): void {
	app.stream('in', strings)
		.mapValues(value => {
			if (value === 'boom') throw new Error('boom')
			return value?.toUpperCase() ?? null
		})
		.to('out', strings)
}

function counting(app: FlowApp): void {
	app.stream('in', strings).groupByKey().count().toStream().to('out', { key: string() })
}

/** `in` joined with the global table `users` (JSON values) -> `out`. */
function withGlobalTable(app: FlowApp): void {
	const users = app.globalTable('users', { key: string(), value: codec.json<string>() })
	app.stream('in', strings)
		.join(users, (value, user) => `${value}:${user}`)
		.to('out', strings)
}

function changelogOf(h: Harness): string {
	return [...h.topology.internalTopics.values()].find(spec => spec.kind === 'changelog')!.name
}

/** Listing offsets of `topic` partition 1 never answers (the restore of task 0_1 stays in flight). */
function hangRestoreOfPartition1(h: Harness, topic: string): void {
	h.io.admin.steps.listOffsets.always(() => (h.io.admin.calls.at(-1)!.includes(`${topic}:1`) ? 'hang' : 'ok'))
}

/** Lets the runtime's promise chains and timers set from them run. */
const settle = () => new Promise(resolve => setTimeout(resolve, 10))

const connectionReset = () => new Error('connection reset')

describe('recovery (task migration, 4.10.2)', () => {
	/** Waits until the failed run released its producer, then moves the clock to just before and to the backoff's end. */
	async function backoff(h: Harness, ms: number, runs: number): Promise<void> {
		await vi.waitFor(() => expect(h.io.created.producers[runs - 2]!.closed).toBe(true))
		await settle()
		h.io.clock.advance(ms - 1)
		await settle()
		expect(h.io.created.consumers).toHaveLength(runs - 1)
		h.io.clock.advance(1)
		await h.joined(runs)
	}

	it('abandons the failed run, then a new run joins after the backoff, which doubles until a run committed', async () => {
		const h = new Harness(stateless, { commitIntervalMs: 1000 })
		await h.started()
		const cause = connectionReset()
		h.consumer.fail(cause)
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('REBALANCING'))
		expect(h.lifecycle.error).toBe(cause)
		await backoff(h, 500, 2)
		expect(h.io.created.consumers[0]!.calls.at(-1)).toBe('stop')

		h.consumer.fail(connectionReset())
		await backoff(h, 1000, 3)
		h.consumer.fail(connectionReset())
		await backoff(h, 2000, 4)

		// This run commits: the backoff starts over.
		await h.consumer.rebalance('in:0')
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('RUNNING'))
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.output()).toEqual(['A']))
		h.io.clock.advance(1000)
		await vi.waitFor(() => expect(h.committed()).toBe(1n))
		h.consumer.fail(connectionReset())
		await backoff(h, 500, 5)
		expect(h.io.created.producers).toHaveLength(5)
		await h.stop()
		expect(h.lifecycle.state).toBe('STOPPED')
	})

	it("exactly-once: aborts the failed run's transaction; the new run redoes the work on a new producer", async () => {
		const h = new Harness(stateless, { processingGuarantee: 'exactly_once' })
		await h.started()
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.producer.sent).toHaveLength(1))
		const failed = h.producer
		h.consumer.fail(connectionReset())
		await backoff(h, 500, 2)
		expect(failed.transactions.map(t => t.state)).toEqual(['aborted'])
		expect(h.producer).not.toBe(failed)

		await h.consumer.rebalance('in:0')
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.producer.sent).toHaveLength(1))
		await h.stop()
		expect(h.output()).toEqual(['A'])
		expect(h.committed()).toBe(1n)
	})

	it('at-least-once: commits that time out are retried at the next interval, and migrate after taskTimeoutMs', async () => {
		const h = new Harness(stateless, { commitIntervalMs: 1000, taskTimeoutMs: 2500 })
		await h.started()
		h.consumer.steps.commit.always(new TimeoutError('commit', 10_000))
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.output()).toEqual(['A']))
		const commits = () => h.consumer.calls.filter(call => call.startsWith('commit'))
		for (const attempts of [1, 2]) {
			h.io.clock.advance(1000)
			await vi.waitFor(() => expect(commits()).toHaveLength(attempts))
			await settle()
			expect(h.lifecycle.state).toBe('RUNNING')
		}
		h.io.clock.advance(1000)
		await vi.waitFor(() =>
			expect(h.lifecycle.error?.message).toBe('offset commits failed for more than taskTimeoutMs')
		)
		expect(h.lifecycle.state).toBe('REBALANCING')
		await backoff(h, 500, 2)
		await h.stop()
		expect(h.committed()).toBeUndefined()
	})

	it('a close during the backoff ends the recovery: no new run', async () => {
		const h = new Harness(stateless)
		await h.started()
		h.consumer.fail(connectionReset())
		await vi.waitFor(() => expect(h.producer.closed).toBe(true))
		await h.stop()
		h.io.clock.advance(60_000)
		await settle()
		expect(h.io.created.consumers).toHaveLength(1)
		expect(h.lifecycle.state).toBe('STOPPED')
	})

	it('a fatal failure during the backoff ends it: no new run, ERROR, the global consumers stopped (CR5)', async () => {
		const h = new Harness(withGlobalTable, {}, { partitions: { in: 1, out: 1, users: 1 } })
		await h.started()
		h.consumer.fail(connectionReset())
		await vi.waitFor(() => expect(h.producer.closed).toBe(true))
		// A row of the global table that cannot be read stops the app.
		h.io.cluster.append('users', 0, { key: 'k', value: '{not json' })
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('ERROR'))
		expect(h.lifecycle.error).toBeInstanceOf(FailedProcessingError)
		h.io.clock.advance(60_000)
		await settle()
		expect(h.io.created.consumers).toHaveLength(1)
		await vi.waitFor(() => expect(h.io.created.globalConsumers.map(c => c.calls.at(-1))).toEqual(['stop']))
		await h.stop()
		expect(h.lifecycle.state).toBe('STOPPED')
	})
})

describe('halt (4.10.3)', () => {
	it('the hooks of an abandoned run return at once and acknowledge nothing (CR11)', async () => {
		const h = new Harness(stateless, { processingGuarantee: 'exactly_once' })
		await h.started()
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.producer.sent).toHaveLength(1))
		// The abandon's abort hangs: its exclusive op does not end.
		h.producer.steps.abort.always('hang')
		h.consumer.emit('offsetOutOfRange', { topic: 'in', partition: 0, offset: 7n })
		await vi.waitFor(() => expect(h.producer.steps.abort.hung).toHaveLength(1))
		expect(h.lifecycle.state).toBe('ERROR')
		expect(h.lifecycle.error).toBeInstanceOf(NoOffsetResetError)

		h.consumer.calls.length = 0
		await h.consumer.rebalance('in:0')
		expect(h.consumer.calls).toEqual([])
		expect(h.producer.transactions.map(t => t.state)).toEqual(['open'])

		h.producer.steps.abort.answerAll()
		await vi.waitFor(() => expect(h.producer.closed).toBe(true))
		expect(h.producer.transactions.map(t => t.state)).toEqual(['aborted'])
		await h.stop()
		expect(h.output()).toEqual([])
		expect(h.committed()).toBeUndefined()
	})
})

describe('revive in place (4.7.5)', () => {
	it('at-least-once: a send that failed after the retries revives its task from the last commit, without a rebalance', async () => {
		const h = new Harness(stateless, { commitIntervalMs: 1000 })
		await h.started()
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.output()).toEqual(['A']))
		h.io.clock.advance(1000)
		await vi.waitFor(() => expect(h.committed()).toBe(1n))

		h.producer.steps.send.next(new TimeoutError('produce', 30_000))
		h.consumer.deliver('in:0', [
			{ key: 'k', value: 'b' },
			{ key: 'k', value: 'c' },
		])
		await vi.waitFor(() => expect(h.producer.sent).toHaveLength(3))
		h.consumer.calls.length = 0
		h.io.clock.advance(1000)
		// Paused and rewound to its last commit; the processed records come again.
		await vi.waitFor(() => expect(h.consumer.calls).toEqual(['pause in:0', 'seek in:0 1', 'resume in:0']))
		expect(h.committed()).toBe(1n)
		h.consumer.deliver('in:0', [
			{ key: 'k', value: 'b' },
			{ key: 'k', value: 'c' },
		])
		await vi.waitFor(() => expect(h.output()).toEqual(['A', 'C', 'B', 'C']))
		await h.stop()
		expect(h.committed()).toBe(3n)
		expect(h.io.created.consumers).toHaveLength(1)
	})

	it('a second corruption of a task within 60 s migrates', async () => {
		const h = new Harness(stateless, { commitIntervalMs: 1000 })
		await h.started()
		/** Delivers a record at offset 0 whose send fails after the retries, then lets the next commit find it. */
		const corrupt = async (value: string): Promise<void> => {
			h.producer.steps.send.next(new TimeoutError('produce', 30_000))
			h.consumer.deliver('in:0', [{ key: 'k', value, offset: 0n }])
			await vi.waitFor(() => expect(h.producer.sent.at(-1)?.message.value?.toString()).toBe(value.toUpperCase()))
			h.io.clock.advance(1000)
		}
		await corrupt('a')
		await vi.waitFor(() => expect(h.consumer.calls.slice(-2)).toEqual(['seek in:0 0', 'resume in:0']))
		await corrupt('b')
		await vi.waitFor(() => expect(h.lifecycle.error?.message).toBe('task 0_0 was corrupted twice within 60 s'))
		await vi.waitFor(() => expect(h.producer.closed).toBe(true))
		expect(h.io.created.consumers[0]!.calls.at(-1)).toBe('stop')
		await settle()
		h.io.clock.advance(500)
		await h.joined(2)
		await h.stop()
	})

	it('a restore that finds its records deleted revives the task with its state wiped; the start goes on', async () => {
		const h = new Harness(counting)
		const changelog = changelogOf(h)
		const stored = valueAndTimestampCodec(COUNT_CODEC)
		h.io.cluster.createTopic(changelog, 1)
		for (const count of [5, 7]) {
			const value = stored.encode({ value: count, timestamp: 0 })
			h.io.cluster.log(changelog, 0).append({ key: Buffer.from('k'), value, timestamp: 0n, headers: {} })
		}
		// Retention deletes the first record between the offsets the restore lists and its first fetch.
		h.io.admin.steps.listOffsets.next('ok', () => {
			h.io.cluster.log(changelog, 0).deleteBefore(1n)
			return 'ok'
		})
		await h.started()
		expect(h.io.created.restoreConsumers.map(c => c.calls[0])).toEqual([
			`assign ${changelog}:0@0`,
			`assign ${changelog}:0@1`,
		])
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.output(COUNT_CODEC.decode)).toEqual([8]))
		await h.stop()
	})
})

describe('prune of unowned in-memory state (4.10.4)', () => {
	it('drops state no task owns 30 s after every task runs; a rebalance meanwhile postpones it', async () => {
		const h = new Harness(counting, {}, { partitions: { in: 2, out: 1 } })
		const changelog = changelogOf(h)
		await h.started('in:0', 'in:1')
		h.consumer.deliver('in:1', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.output(COUNT_CODEC.decode)).toEqual([1]))
		await h.consumer.rebalance('in:0')
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('RUNNING'))
		expect(h.stateDirectory.unleased()).toEqual([{ store: expect.any(String), partition: 1 }])
		expect(await h.checkpoints.get(changelog, 1)).toBe(1n)

		h.io.clock.advance(29_000)
		h.consumer.requestRebalance()
		h.io.clock.advance(30_000)
		await settle()
		expect(h.stateDirectory.unleased()).toHaveLength(1)

		await h.consumer.rebalance('in:0')
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('RUNNING'))
		h.io.clock.advance(29_999)
		await settle()
		expect(h.stateDirectory.unleased()).toHaveLength(1)
		h.io.clock.advance(1)
		await vi.waitFor(() => expect(h.stateDirectory.unleased()).toEqual([]))
		expect(await h.checkpoints.get(changelog, 1)).toBe(-1n)
		await h.stop()
	})
})

describe('close (4.11)', () => {
	it('returns at its deadline whatever hangs: a commit, a revocation hook and a restore in flight (CR2)', async () => {
		const h = new Harness(counting, { processingGuarantee: 'exactly_once' }, { partitions: { in: 2, out: 1 } })
		await h.started('in:0')
		hangRestoreOfPartition1(h, changelogOf(h))
		await h.consumer.rebalance('in:0', 'in:1')
		await vi.waitFor(() => expect(h.consumer.partitions.get('in:0')?.paused).toBe(false))
		expect(h.io.admin.steps.listOffsets.hung).toHaveLength(2)

		// From here on nothing the run sends, commits, aborts or stops is answered.
		h.producer.steps.send.always('hang')
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.producer.sent).toHaveLength(2))
		for (const step of ['commit', 'abort', 'close'] as const) h.producer.steps[step].always('hang')
		h.consumer.steps.stop.always('hang')
		h.io.clock.advance(100)
		// The interval commit waits for the sends; the revocation hook waits for the commit.
		const rebalance = h.consumer.rebalance('in:0', 'in:1')
		await settle()

		let closed = false
		const closing = h.stop(5000).then(() => {
			closed = true
		})
		await settle()
		h.io.clock.advance(4000)
		await settle()
		expect(closed).toBe(false)
		h.io.clock.advance(1000)
		await vi.waitFor(() => expect(closed).toBe(true), { timeout: 100 })
		await closing
		await rebalance
		expect(h.lifecycle.state).toBe('STOPPED')
		expect(h.output()).toEqual([])
		expect(h.committed()).toBeUndefined()
	})

	it('during the start: commits the tasks that run and releases everything once (CR14)', async () => {
		const h = new Harness(counting, {}, { partitions: { in: 2, out: 1 } })
		hangRestoreOfPartition1(h, changelogOf(h))
		const starting = h.start()
		await h.joined()
		await h.consumer.rebalance('in:0', 'in:1')
		await vi.waitFor(() => expect(h.consumer.partitions.get('in:0')?.paused).toBe(false))
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.output(COUNT_CODEC.decode)).toEqual([1]))

		const closing = h.stop()
		await expect(starting).rejects.toThrow('the app is closing')
		await closing
		expect(h.committed('in:0')).toBe(1n)
		expect(h.committed('in:1')).toBeUndefined()
		expect(h.consumer.calls.filter(call => call === 'stop')).toHaveLength(1)
		expect(h.connection.disconnected).toBe(1)
		expect(h.lifecycle.state).toBe('STOPPED')
	})
})

describe('runtime blocks of the legacy flow.test.ts', () => {
	it('exactly-once: the records and their offsets commit in one transaction at close', async () => {
		const h = new Harness(stateless, { processingGuarantee: 'exactly_once' })
		await h.started()
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.producer.sent).toHaveLength(1))
		await h.stop()
		const [transaction] = h.producer.transactions
		expect(h.producer.transactions.map(t => t.state)).toEqual(['committed'])
		expect(transaction!.offsets.map(o => [o.topic, o.partition, o.offset])).toEqual([['in', 0, 1n]])
	})

	it('exactly-once: one transaction covers the records of every task', async () => {
		const h = new Harness(
			app => {
				app.stream('first').to('out')
				app.stream('second').to('out')
			},
			{ processingGuarantee: 'exactly_once' },
			{ partitions: { first: 1, second: 1, out: 1 } }
		)
		await h.started('first:0', 'second:0')
		h.consumer.deliver('first:0', [{ value: 'one' }])
		h.consumer.deliver('second:0', [{ value: 'two' }])
		await vi.waitFor(() => expect(h.producer.sent).toHaveLength(2))
		await h.stop()
		expect(h.producer.transactions).toHaveLength(1)
		expect(h.producer.transactions[0]!.offsets.map(o => `${o.topic}:${o.partition}@${o.offset}`).sort()).toEqual([
			'first:0@1',
			'second:0@1',
		])
		expect(h.output()).toEqual(['one', 'two'])
	})

	for (const guarantee of ['exactly_once', 'at_least_once'] as const) {
		it(`${guarantee}: a record delivered while stopping is not processed; the next owner starts at it`, async () => {
			const h = new Harness(stateless, { processingGuarantee: guarantee })
			await h.started()
			h.consumer.deliver('in:0', [{ key: 'k', value: 'before-close' }])
			await vi.waitFor(() => expect(h.producer.sent).toHaveLength(1))
			const closing = h.stop()
			h.consumer.deliver('in:0', [{ key: 'k', value: 'during-close' }])
			await closing
			expect(h.producer.sent.map(r => r.message.value?.toString())).toEqual(['BEFORE-CLOSE'])
			expect(h.producer.transactions).toHaveLength(guarantee === 'exactly_once' ? 1 : 0)
			expect(h.committed()).toBe(1n)
		})
	}

	it('no run starts after a halt during the recovery backoff', async () => {
		const h = new Harness(stateless)
		await h.started()
		h.consumer.fail(connectionReset())
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('REBALANCING'))
		// Something outside the run fails for good meanwhile (a global table, say).
		h.runtime.fail(new FailedProcessingError('global table failed'), 'global')
		h.io.clock.advance(800)
		await settle()
		expect(h.io.created.consumers).toHaveLength(1)
		expect(h.lifecycle.state).toBe('ERROR')
		expect(h.lifecycle.error?.message).toBe('global table failed')
		await h.stop()
	})

	it('a failure no handler continues halts the whole runtime: the run and the global tables stop', async () => {
		const h = new Harness(
			app => {
				const users = app.globalTable('users', strings)
				app.stream('in', strings)
					.join(users, (): string => {
						throw new Error('bug in a joiner')
					})
					.to('out', strings)
			},
			{},
			{ partitions: { in: 1, out: 1, users: 1 } }
		)
		h.io.cluster.append('users', 0, { key: 'k', value: 'alice' })
		await h.started()
		h.consumer.deliver('in:0', [{ key: 'k', value: 'x' }])
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('ERROR'))
		expect(h.lifecycle.error).toBeInstanceOf(FailedProcessingError)
		expect(h.lifecycle.error!.cause).toEqual(new Error('bug in a joiner'))
		await vi.waitFor(() => expect(h.producer.closed).toBe(true))
		expect(h.consumer.calls.filter(call => call === 'stop')).toHaveLength(1)
		await vi.waitFor(() => expect(h.io.created.globalConsumers.map(c => c.calls.at(-1))).toEqual(['stop']))
		await h.stop()
	})

	describe('instances started by flow()', () => {
		async function started(
			config: Partial<FlowConfig> = {}
		): Promise<{ app: FlowApp; options: KafkaIOOptions; io: FakeIO }> {
			const client = new KafkaClient({ clientId: 'test-app', brokers: ['localhost:9092'] })
			vi.spyOn(client, 'connect').mockResolvedValue()
			const app = flow({ applicationId: 'test-app', client, processingGuarantee: 'exactly_once', ...config })
			app.stream('events').to('out')
			const count = kafkaIO.created.length
			const starting = app.start()
			await vi.waitFor(() => expect(kafkaIO.created[count]?.io.created.consumers).toHaveLength(1))
			const { options, io } = kafkaIO.created[count]!
			await io.created.consumers[0]!.rebalance('events:0')
			await starting
			return { app, options, io }
		}

		it('run one consumer and one producer each', async () => {
			const { app, io } = await started()
			expect(io.created.consumers).toHaveLength(1)
			expect(io.created.producers).toHaveLength(1)
			await app.close()
		})

		it('have unique transactional ids <applicationId>-<processId>, a new one per start', async () => {
			const first = await started()
			const second = await started()
			const ids = [first, second].map(({ options }) => producerConfig(options).transactionalId)
			expect(ids[0]).toMatch(/^test-app-[0-9a-f-]{36}$/)
			expect(ids[1]).toMatch(/^test-app-[0-9a-f-]{36}$/)
			expect(ids[0]).not.toBe(ids[1])
			await first.app.close()
			await second.app.close()
			const restarted = await started()
			expect(producerConfig(restarted.options).transactionalId).not.toBe(ids[0])
			await restarted.app.close()
		})

		it('keep an explicit transactional id as is', async () => {
			const { app, options } = await started({ producer: { transactionalId: 'orders-pod-0' } })
			expect(producerConfig(options).transactionalId).toBe('orders-pod-0')
			await app.close()
		})
	})
})
