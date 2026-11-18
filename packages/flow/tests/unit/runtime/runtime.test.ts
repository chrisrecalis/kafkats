/**
 * FlowRuntime over fake IO (4.6.2, 4.10, 4.11): recovery (task migration with backoff), halt, revive in place, the
 * prune of unowned state, close bounded by construction (CR2) and close during start (CR14); then the runtime blocks
 * of the legacy flow.test.ts with the task runtime's expectations: one consumer and one producer per instance, the
 * transactional id `<applicationId>-<processId>`, one transaction for every task, a record delivered while stopping is
 * not processed under either guarantee, a halt stops the whole runtime, also a recovery waiting out its backoff.
 */
import { describe, expect, it, vi } from 'vitest'
import {
	ErrorCode,
	KafkaClient,
	KafkaProtocolError,
	NoOffsetForPartitionError,
	string,
	TimeoutError,
} from '@kafkats/client'
import type { FlowApp, FlowConfig } from '@/api/app.js'
import { FailedProcessingError, NoOffsetResetError, TaskCorruptedError, TaskMigratedError } from '@/api/errors.js'
import { flow } from '@/app/flow.js'
import { codec } from '@/codec.js'
import { COUNT_CODEC } from '@/dsl/ops/aggregate.js'
import { producerConfig, type KafkaIOOptions } from '@/runtime/kafka-io.js'
import { InMemoryStateStoreProvider } from '@/state/memory.js'
import { valueAndTimestampCodec } from '@/state/timestamped.js'
import { deferred, type FakeIO } from '../support/fake-io.js'
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

	it('the next run waits for the stores a task of the failed run still releases; an op that outlived the run creates nothing (F3)', async () => {
		const h = new Harness(counting, { commitIntervalMs: 1000 }, { partitions: { in: 2, out: 1 } })
		await h.started('in:0', 'in:1')
		const released = deferred()
		const release = h.stateDirectory.release.bind(h.stateDirectory)
		vi.spyOn(h.stateDirectory, 'release').mockImplementation(async (owner, store, partition, options) => {
			if (partition === 1) await released.promise
			return release(owner, store, partition, options)
		})
		h.producer.steps.send.next(new TimeoutError('produce', 30_000))
		h.consumer.deliver('in:1', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.producer.sent).toHaveLength(2))
		// The commit finds task 0_1 corrupted; its revival holds the scheduler while the old incarnation's store is
		// released (a slow wipe, say). The run fails meanwhile: its end stops waiting for the revival after 5 s.
		h.io.clock.advance(1000)
		await vi.waitFor(() => expect(h.logger.warnings).toContain('tasks are revived from their last commit'))
		h.consumer.fail(connectionReset())
		await vi.waitFor(() => expect(h.producer.closed).toBe(true), { timeout: 8000 })
		await settle()
		h.io.clock.advance(60_000)
		await settle()
		expect(h.io.created.consumers).toHaveLength(1)

		released.resolve()
		await settle()
		h.io.clock.advance(500)
		await h.joined(2)
		await h.consumer.rebalance('in:0', 'in:1')
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('RUNNING'))
		h.consumer.deliver('in:1', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.output(COUNT_CODEC.decode)).toEqual([1]))
		await h.stop()
		expect(h.lifecycle.state).toBe('STOPPED')
	}, 15_000)

	it('a failed run reports nothing more: what its held records throw meanwhile neither halts nor replaces the cause', async () => {
		const held = deferred()
		const entered: string[] = []
		const uncaught = vi.fn(() => 'shutdown' as const)
		const h = new Harness(
			app => {
				app.stream('in', strings).process(() => ({
					process: async record => {
						entered.push(record.value!)
						await held.promise
						// A fatal and a corrupted failure, side effects of the run's end.
						throw record.value === 'corrupt' ? new TaskCorruptedError('corrupted', []) : new Error('boom')
					},
				}))
			},
			{ uncaughtExceptionHandler: uncaught },
			{ partitions: { in: 2, out: 1 } }
		)
		await h.started('in:0', 'in:1')
		h.consumer.deliver('in:0', [{ key: 'k', value: 'fatal' }])
		h.consumer.deliver('in:1', [{ key: 'k', value: 'corrupt' }])
		await vi.waitFor(() => expect(entered).toHaveLength(2))
		const cause = connectionReset()
		h.consumer.fail(cause)
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('REBALANCING'))
		held.resolve()
		await vi.waitFor(() => expect(h.producer.closed).toBe(true))
		await settle()
		expect(uncaught).not.toHaveBeenCalled()
		expect(h.lifecycle.state).toBe('REBALANCING')
		expect(h.lifecycle.error).toBe(cause)
		h.io.clock.advance(500)
		await h.joined(2)
		await h.stop()
		expect(h.lifecycle.state).toBe('STOPPED')
	})

	it('exactly-once: a loss whose dirty close outlives the end of its failed run creates no producer afterwards', async () => {
		const h = new Harness(counting, { processingGuarantee: 'exactly_once' })
		await h.started()
		const released = deferred()
		const release = h.stateDirectory.release.bind(h.stateDirectory)
		vi.spyOn(h.stateDirectory, 'release').mockImplementation(async (...args) => {
			await released.promise
			return release(...args)
		})
		// The loss hook closes the task dirty (its store release hangs), then would continue on a new producer.
		const lost = h.consumer.lose()
		await settle()
		h.consumer.fail(connectionReset())
		// The run's end stops waiting for the hook after 5 s and closes the producer.
		await vi.waitFor(() => expect(h.producer.closed).toBe(true), { timeout: 8000 })
		released.resolve()
		await lost
		await settle()
		expect(h.io.created.producers.map(producer => producer.closed)).toEqual([true])
		h.io.clock.advance(500)
		await h.joined(2)
		await h.stop()
		expect(h.io.created.producers.map(producer => producer.closed)).toEqual([true, true])
	}, 15_000)

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

describe('start (4.2)', () => {
	/**
	 * `in` (2 partitions) -> a processor that throws `failure()` for the value 'boom'; its init() for partition 1 waits
	 * for `gate`, so that task 0_0 runs while the completion op of task 0_1 holds the scheduler.
	 */
	function gatedStart(failure: () => Error) {
		const gate = deferred()
		const processed: string[] = []
		const h = new Harness(
			app => {
				app.stream('in', strings).process(() => ({
					init: async context => {
						if (context.taskId.partition === 1) await gate.promise
					},
					process: record => {
						if (record.value === 'boom') throw failure()
						processed.push(record.value!)
					},
				}))
			},
			{},
			{ partitions: { in: 2, out: 1 } }
		)
		return { h, gate, processed }
	}

	/**
	 * A record of the RUNNING task 0_0 waits while 0_1 completes; the first slice after the last completion runs it
	 * before start() returns (start() resolves a few microtasks after the tasks are all RUNNING).
	 */
	async function failingAsTheStartEnds(h: Harness, gate: { resolve(): void }): Promise<'resolved' | unknown> {
		const starting = h.start()
		await h.joined()
		void h.consumer.rebalance('in:0', 'in:1')
		await vi.waitFor(() => expect(h.consumer.partitions.get('in:0')?.paused).toBe(false))
		h.consumer.deliver('in:0', [{ key: 'k', value: 'boom' }])
		await settle()
		gate.resolve()
		return starting.then(
			() => 'resolved',
			(error: unknown) => error
		)
	}

	it('a fatal failure right after the first assignment is RUNNING goes to the uncaught handler: ERROR, not lost', async () => {
		const { h, gate, processed } = gatedStart(() => new Error('boom'))
		expect(await failingAsTheStartEnds(h, gate)).toBe('resolved')
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('ERROR'))
		expect(h.lifecycle.error).toBeInstanceOf(FailedProcessingError)
		expect(h.logger.errors).toContain('the app stopped after a failure: close() releases it')
		await vi.waitFor(() => expect(h.consumer.calls.at(-1)).toBe('stop'))
		expect(processed).toEqual([])
		await h.stop()
	})

	it('a migration right after the first assignment is RUNNING joins the group again', async () => {
		const { h, gate } = gatedStart(() => new TaskMigratedError('fenced'))
		expect(await failingAsTheStartEnds(h, gate)).toBe('resolved')
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('REBALANCING'))
		expect(h.lifecycle.error).toBeInstanceOf(TaskMigratedError)
		await vi.waitFor(() => expect(h.producer.closed).toBe(true))
		expect(h.consumer.calls.at(-1)).toBe('stop')
		await settle()
		h.io.clock.advance(500)
		await h.joined(2)
		await h.stop()
	})
})

describe('halt (4.10.3)', () => {
	it("reset policy 'none' without a committed offset is a NoOffsetResetError: start() rejects with it", async () => {
		const h = new Harness(stateless)
		const starting = h.start()
		await h.joined()
		h.consumer.fail(new NoOffsetForPartitionError('in', 0))
		const failure = await starting.catch((error: unknown) => error)
		expect(failure).toBeInstanceOf(NoOffsetResetError)
		expect((failure as Error).message).toBe(
			"'in' partition 0 has no committed offset and its reset policy is 'none'"
		)
		expect((failure as Error).cause).toBeInstanceOf(NoOffsetForPartitionError)
		expect(h.lifecycle.error).toBe(failure)
		await h.stop()
	})

	it("reset policy 'none' without a committed offset after the start: the uncaught handler gets a NoOffsetResetError", async () => {
		const seen: Error[] = []
		const h = new Harness(stateless, {
			uncaughtExceptionHandler: error => {
				seen.push(error)
				return 'shutdown'
			},
		})
		await h.started()
		// A rebalance assigned a partition without a committed offset: the client's run fails.
		h.consumer.fail(new NoOffsetForPartitionError('in', 1))
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('ERROR'))
		expect(seen).toEqual([expect.any(NoOffsetResetError)])
		expect(h.lifecycle.error).toBe(seen[0])
		expect(seen[0]!.cause).toBeInstanceOf(NoOffsetForPartitionError)
		await h.stop()
	})

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
		// Paused and rewound to its last commit; the processed records come again. getError() reports it.
		await vi.waitFor(() => expect(h.consumer.calls).toEqual(['pause in:0', 'seek in:0 1', 'resume in:0']))
		expect(h.committed()).toBe(1n)
		expect(h.lifecycle.error).toBeInstanceOf(TaskCorruptedError)
		expect(h.lifecycle.state).toBe('RUNNING')
		h.consumer.deliver('in:0', [
			{ key: 'k', value: 'b' },
			{ key: 'k', value: 'c' },
		])
		await vi.waitFor(() => expect(h.output()).toEqual(['A', 'C', 'B', 'C']))
		await h.stop()
		expect(h.committed()).toBe(3n)
		expect(h.io.created.consumers).toHaveLength(1)
	})

	it('at-least-once: a revocation queued before the revive commits nothing of the corrupted task (F1)', async () => {
		const h = new Harness(stateless, { commitIntervalMs: 1000 })
		await h.started()
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.output()).toEqual(['A']))
		h.io.clock.advance(1000)
		await vi.waitFor(() => expect(h.committed()).toBe(1n))

		h.producer.steps.send.next('hang')
		h.consumer.deliver('in:0', [{ key: 'k', value: 'b' }])
		await vi.waitFor(() => expect(h.producer.steps.send.hung).toHaveLength(1))
		// The interval commit waits for the send; meanwhile a rebalance queues the revocation op.
		h.io.clock.advance(1000)
		await settle()
		const rebalance = h.consumer.rebalance('in:0')
		await settle()
		h.producer.steps.send.hung[0]!.answer(new TimeoutError('produce', 30_000))
		await rebalance
		expect(h.consumer.calls.filter(call => call.startsWith('commit'))).toEqual(['commit in:0@1'])
		expect(h.committed()).toBe(1n)

		// The next incarnation starts at the record whose output was lost.
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('RUNNING'))
		h.consumer.deliver('in:0', [{ key: 'k', value: 'b' }])
		await vi.waitFor(() => expect(h.output()).toEqual(['A', 'B']))
		await h.stop()
		expect(h.committed()).toBe(2n)
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
		expect(h.stateDirectory.unleased()).toEqual([
			{ store: expect.any(String), partition: 1, prunableAt: expect.any(Number) },
		])
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

describe('cleanup of unowned durable state (Kafka Streams: state.cleanup.delay.ms)', () => {
	/** A durable checkpoint store (as LMDB's and RocksDB's): StateDirectory decides on durable checkpoints alone. */
	function durableCheckpoints() {
		const values = new Map<string, bigint>()
		return {
			values,
			get: (topic: string, partition: number) => Promise.resolve(values.get(`${topic}:${partition}`)),
			set: (topic: string, partition: number, offset: bigint) => {
				values.set(`${topic}:${partition}`, offset)
				return Promise.resolve()
			},
		}
	}

	it('drops durable state no task owned for stateCleanupDelayMs: a task coming back later rebuilds it', async () => {
		const store = durableCheckpoints()
		const h = new Harness(
			counting,
			{ stateCleanupDelayMs: 120_000 },
			{ partitions: { in: 2, out: 1 }, checkpointStore: store }
		)
		const changelog = changelogOf(h)
		await h.started('in:0', 'in:1')
		h.consumer.deliver('in:1', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.output(COUNT_CODEC.decode)).toEqual([1]))
		await h.consumer.rebalance('in:0')
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('RUNNING'))
		await vi.waitFor(() => expect(store.values.get(`${changelog}:1`)).toBe(1n))

		// Kept past the 30 s of in-memory state: a task that comes back soon catches up from its checkpoint.
		h.io.clock.advance(119_000)
		await settle()
		expect(h.stateDirectory.unleased()).toHaveLength(1)
		expect(store.values.get(`${changelog}:1`)).toBe(1n)
		h.io.clock.advance(1000)
		await vi.waitFor(() => expect(h.stateDirectory.unleased()).toEqual([]))
		expect(store.values.get(`${changelog}:1`)).toBe(-1n)
		await h.stop()
	})

	it('invalidates the checkpoints an earlier start left of partitions no task of this start took', async () => {
		const store = durableCheckpoints()
		const h = new Harness(
			counting,
			{ stateCleanupDelayMs: 120_000 },
			{ partitions: { in: 2, out: 1 }, checkpointStore: store }
		)
		const changelog = changelogOf(h)
		store.values.set(`${changelog}:1`, 7n)
		await h.started('in:0')
		h.io.clock.advance(119_000)
		await settle()
		expect(store.values.get(`${changelog}:1`)).toBe(7n)
		h.io.clock.advance(1000)
		await vi.waitFor(() => expect(store.values.get(`${changelog}:1`)).toBe(-1n))
		await h.stop()
	})
})

describe('exactly-once: the producer is initialized before any restore (KS RecordCollector.initialize)', () => {
	it('a restore waits for the InitProducerId that fences the transaction an earlier producer left open', async () => {
		const h = new Harness(counting, { processingGuarantee: 'exactly_once' })
		h.io.scriptProducer = producer => producer.steps.initTransactions.next('hang')
		const starting = h.start()
		await h.joined()
		// It began as the run started, before the group assigned anything: while the consumer joins, its epoch bump
		// aborts a transaction whose pending offsets would hold the assignment's offset fetch (requireStable).
		const init = h.producer.steps.initTransactions
		expect(init.hung).toHaveLength(1)
		await h.consumer.rebalance('in:0')
		const restoreReads = () => h.io.admin.calls.filter(call => call.startsWith('listOffsets'))
		await settle()
		// The restore plans nothing (no end offsets listed) and no transaction begins until the producer is initialized.
		expect(restoreReads()).toEqual([])
		expect(h.producer.transactions).toHaveLength(0)

		init.hung[0]!.answer()
		await starting
		expect(h.producer.initialized).toBe(1)
		expect(restoreReads()).not.toEqual([])
		await h.stop()
	})
})

describe('exactly-once: a producer that cannot be initialized', () => {
	it('a transactional id the principal may not use is fatal, not a migration (KS: a StreamsException)', async () => {
		const seen: Error[] = []
		const h = new Harness(stateless, {
			processingGuarantee: 'exactly_once',
			uncaughtExceptionHandler: error => {
				seen.push(error)
				return 'shutdown'
			},
		})
		await h.started()
		// The ACL is gone and a recovery needs a new producer.
		const denied = new KafkaProtocolError(ErrorCode.TransactionalIdAuthorizationFailed, 'InitProducerId failed')
		h.io.scriptProducer = producer => producer.steps.initTransactions.always(denied)
		h.consumer.fail(connectionReset())
		await vi.waitFor(() => expect(h.io.created.producers[0]!.closed).toBe(true))
		await settle()
		h.io.clock.advance(500)
		await h.joined(2)
		await h.consumer.rebalance('in:0')
		await vi.waitFor(() => expect(h.lifecycle.state).toBe('ERROR'))
		expect(seen).toEqual([denied])
		expect(h.io.created.consumers).toHaveLength(2)
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

	it('exactly-once: a commit that outlives the 5 s wait of the close commits nothing beside its dirty close (F2)', async () => {
		const h = new Harness(counting, { processingGuarantee: 'exactly_once' })
		await h.started()
		h.producer.steps.send.always('hang')
		h.consumer.deliver('in:0', [{ key: 'k', value: 'a' }])
		await vi.waitFor(() => expect(h.producer.steps.send.hung).toHaveLength(2))
		// The revocation commit waits for the sends (a slow broker); the close waits 5 s for it, then aborts and
		// closes the tasks dirty beside it. The sends are answered after that, which ends the commit's flush.
		const rebalance = h.consumer.rebalance('in:0')
		await settle()
		const closing = h.stop()
		await new Promise(resolve => setTimeout(resolve, 5500))
		h.producer.steps.send.answerAll()
		await closing
		await rebalance
		expect(h.producer.transactions.map(t => t.state)).toEqual(['aborted'])
		expect(h.committed()).toBeUndefined()
		expect(h.output(COUNT_CODEC.decode)).toEqual([])
		expect(h.lifecycle.state).toBe('STOPPED')
	}, 15_000)

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
			config: Partial<FlowConfig> = {},
			build: (app: FlowApp) => void = app => app.stream('events').to('out')
		): Promise<{ app: FlowApp; options: KafkaIOOptions; io: FakeIO }> {
			const client = new KafkaClient({ clientId: 'test-app', brokers: ['localhost:9092'] })
			vi.spyOn(client, 'connect').mockResolvedValue()
			vi.spyOn(client, 'disconnect').mockResolvedValue()
			const app = flow({ applicationId: 'test-app', client, processingGuarantee: 'exactly_once', ...config })
			build(app)
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

		it('close({ timeoutMs }) returns within its timeout whatever hangs (D13)', async () => {
			const { app, io } = await started()
			const [consumer] = io.created.consumers
			const [producer] = io.created.producers
			producer!.steps.send.always('hang')
			consumer!.deliver('events:0', [{ value: 'a' }])
			await vi.waitFor(() => expect(producer!.steps.send.hung).toHaveLength(1))
			for (const step of ['commit', 'abort', 'close'] as const) producer!.steps[step].always('hang')
			consumer!.steps.stop.always('hang')
			let closed = false
			// close() reads its deadline from the wall clock (Date.now() + timeoutMs), the runtime waits for it on the IO
			// clock: in production both are the same clock, here the wall clock is pinned to the fake one for the call.
			const now = vi.spyOn(Date, 'now').mockReturnValue(io.clock.now())
			const closing = app.close({ timeoutMs: 3000 }).then(() => (closed = true))
			now.mockRestore()
			await settle()
			io.clock.advance(2500)
			await settle()
			expect(closed).toBe(false)
			io.clock.advance(500)
			await vi.waitFor(() => expect(closed).toBe(true), { timeout: 1000 })
			await closing
			expect(app.state()).toBe('STOPPED')
		})

		it('a changelog checkpoint that fails to persist is reported by getError(), and the app keeps running', async () => {
			const failure = new Error('disk full')
			const checkpoints = new Map<string, bigint>()
			class FailingCheckpoints extends InMemoryStateStoreProvider {
				getChangelogCheckpointStore() {
					return {
						get: async (topic: string, partition: number) => checkpoints.get(`${topic}:${partition}`),
						set: async (topic: string, partition: number, offset: bigint) => {
							// The invalidation of a fresh store's checkpoint must persist (it is thrown otherwise).
							if (offset !== -1n) throw failure
							checkpoints.set(`${topic}:${partition}`, offset)
						},
					}
				}
			}
			const { app, io } = await started(
				{
					processingGuarantee: 'at_least_once',
					commitIntervalMs: 100,
					stateStoreProvider: new FailingCheckpoints(),
				},
				counted =>
					counted.stream('events', strings).groupByKey().count().toStream().to('out', { key: string() })
			)
			io.created.consumers[0]!.deliver('events:0', [{ key: 'k', value: 'a' }])
			await vi.waitFor(() => expect(io.created.producers[0]!.sent.length).toBeGreaterThan(0))
			io.clock.advance(100)
			await vi.waitFor(() => expect(app.getError()).toBe(failure))
			expect(app.state()).toBe('RUNNING')
			await app.close()
		})
	})
})
