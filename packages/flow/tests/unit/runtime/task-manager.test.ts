import { describe, expect, it, vi } from 'vitest'
import { CommitFailedError, ErrorCode, KafkaProtocolError, type TopicPartition } from '@kafkats/client'
import { AssignmentError, FailedProcessingError, TaskCorruptedError, TaskMigratedError } from '@/api/errors.js'
import { ChangelogPartitionMismatchError } from '@/changelog/errors.js'
import type { ProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import { TimestampExtractors } from '@/processor/timestamps.js'
import { CommitCoordinator, type CommitOutcome, type CommitReason } from '@/runtime/commit.js'
import type { ResolvedFlowConfig } from '@/runtime/config.js'
import { classify, type FailureSite } from '@/runtime/errors.js'
import type { AssignedPartition, CommittedOffset } from '@/runtime/io.js'
import { RecordCollector } from '@/runtime/record-collector.js'
import { TaskIdRegistry } from '@/runtime/task-id.js'
import { TaskManager } from '@/runtime/task-manager.js'
import type { StreamTask } from '@/runtime/task.js'
import type { KeyValueStore } from '@/state.js'
import { Checkpoints } from '@/state/checkpoints.js'
import { inMemory } from '@/state/memory.js'
import { StateDirectory } from '@/state/state-directory.js'
import { InternalTopologyBuilder } from '@/topology/builder.js'
import {
	deferred,
	FakeGroupConsumer,
	FakeProducer,
	FakeRestorer,
	FakeStartPositions,
	message,
	SerialExclusive,
	type TestRecord,
} from '../support/fake-io.js'
import { ManualClock } from '../support/manual-clock.js'
import { APP, kvStore, passThrough, pipe, RecordingLogger } from '../support/task-harness.js'

/** Puts every record into store `s`, forwards it, then fails if its value is 'boom'. */
const writer: ProcessorSupplier<string, string, string, string> = () => {
	let context: ProcessorContext<string, string>
	let store: KeyValueStore<string, string>
	return {
		init: ctx => {
			context = ctx
			store = ctx.getStateStore('s')
		},
		process: async record => {
			await store.put(record.key ?? '', record.value ?? '')
			await context.forward(record)
			if (record.value === 'boom') throw new Error('boom')
		},
	}
}

const stateful =
	(topics = ['input'], processor = writer) =>
	(b: InternalTopologyBuilder) =>
		pipe(b, { topics, processor, stores: [kvStore('s')] })

const stateless = (topics: string[]) => (b: InternalTopologyBuilder) => pipe(b, { topics, processor: passThrough })

function tp(key: string): TopicPartition {
	const [topic, partition] = key.split(':')
	return { topic: topic!, partition: Number(partition) }
}

/** An assigned partition (`topic:partition`), from its commit if it has one, else from offset 0. */
function assigned(key: string, committed: CommittedOffset | null = null): AssignedPartition {
	return { ...tp(key), offset: committed?.offset ?? 0n, committed }
}

/** A TaskManager of a Run over fake ports, with the real tasks, collector, commits and state layer. */
class Harness {
	readonly clock = new ManualClock(1_000)
	readonly logger = new RecordingLogger()
	readonly consumer = new FakeGroupConsumer({ groupId: APP })
	readonly producer = new FakeProducer()
	readonly restorer = new FakeRestorer()
	readonly startPositions = new FakeStartPositions()
	readonly exclusive = new SerialExclusive()
	readonly checkpoints = new Checkpoints(undefined, error => this.logger.error(error.message))
	readonly stateDirectory = new StateDirectory(APP, inMemory(), this.checkpoints, this.logger)
	readonly hardStop = new AbortController()
	readonly shutdown = new AbortController()
	readonly failures: { error: unknown; site: FailureSite }[] = []
	/** onAllRunningChanged calls. */
	readonly allRunning: boolean[] = []
	/** onWork calls (task keys) and output resets ('resetOutput'). */
	readonly events: string[] = []
	/** The run's `failed` flag: set by every failure but a corruption (FlowRuntime.fail). */
	failed = false
	collector: RecordCollector
	readonly config: ResolvedFlowConfig
	readonly commits: CommitCoordinator
	readonly manager: TaskManager

	constructor(
		options: {
			build?: (b: InternalTopologyBuilder) => void
			counts?: Record<string, number>
			eos?: boolean
			bufferedRecordsPerPartition?: number
			/** The TestDriver's exclusive, which runs the op at once (4.14), instead of the scheduler's FIFO. */
			direct?: boolean
		} = {}
	) {
		const builder = new InternalTopologyBuilder(APP)
		const build = options.build ?? stateful()
		build(builder)
		const topology = builder.build()
		const eos = options.eos ?? false
		this.consumer.group = { groupId: APP, generationId: 1, memberId: 'member' }
		this.config = {
			applicationId: APP,
			processId: 'process',
			eos,
			commitIntervalMs: 100,
			taskTimeoutMs: 300_000,
			startTimeoutMs: 120_000,
			maxTaskIdleMs: 0,
			bufferedRecordsPerPartition: options.bufferedRecordsPerPartition ?? 1000,
			emitIntervalMs: 0,
			repartitionPurgeIntervalMs: 0,
			transactionTimeoutMs: 10_000,
			rebalanceTimeoutMs: 60_000,
			defaultTimestampExtractor: TimestampExtractors.failOnInvalid,
			defaultResetPolicy: 'earliest',
			handlers: {
				deserialization: () => 'fail',
				processing: () => 'fail',
				production: () => 'fail',
				uncaught: () => 'shutdown',
			},
			restoration: {
				idleTimeoutMs: 30_000,
				initialIdleTimeoutMs: 30_000,
				checkIntervalMs: 1000,
				maxConcurrentTasks: 4,
			},
		}
		this.collector = this.newCollector()
		this.commits = new CommitCoordinator({
			consumer: this.consumer,
			collector: () => this.collector,
			tasks: () => this.manager.tasks(),
			assignmentMetadata: () => this.manager.assignmentMetadata,
			eos,
			checkpoints: this.checkpoints,
			onCommitted: { noteCommitted: () => {} },
			failed: () => this.failed,
			hardStop: this.hardStop.signal,
			config: this.config,
			clock: this.clock,
			logger: this.logger,
		})
		this.manager = new TaskManager({
			topology,
			counts: new Map(Object.entries(options.counts ?? { input: 1 })),
			ids: new TaskIdRegistry(),
			config: this.config,
			stateDirectory: this.stateDirectory,
			checkpoints: this.checkpoints,
			globalStore: name => {
				throw new Error(`no global store '${name}'`)
			},
			consumer: this.consumer,
			outputs: {
				forTask: task => this.collector.forTask(task),
				ensureTransaction: signal => this.collector.ensureTransaction(signal),
			},
			restorer: this.restorer,
			startPositions: this.startPositions,
			exclusive: options.direct ? fn => fn() : this.exclusive.run,
			commit: (reason: CommitReason, signal: AbortSignal): Promise<CommitOutcome> =>
				this.commits.commit(reason, signal),
			resetOutput: () => this.resetOutput(),
			onWork: task => void this.events.push(task.id.key),
			requestCommit: () => {},
			onAllRunningChanged: running => void this.allRunning.push(running),
			failed: () => this.failed,
			onFailure: (error, site) => {
				this.failures.push({ error, site })
				if (classify(error, site) !== 'corrupted') this.failed = true
			},
			hardStop: this.hardStop.signal,
			signal: AbortSignal.any([this.hardStop.signal, this.shutdown.signal]),
			clock: this.clock,
			logger: this.logger,
		})
	}

	newCollector(): RecordCollector {
		return new RecordCollector(this.producer, {
			eos: this.config.eos,
			handlers: this.config.handlers,
			counts: { get: () => 1, load: () => Promise.resolve(1) },
			logger: this.logger,
		})
	}

	/** Run.resetOutput (3.11): abort or discard, close every task dirty, a new collector. */
	async resetOutput(): Promise<void> {
		this.events.push('resetOutput')
		if (this.config.eos) await this.collector.abortTransaction(this.hardStop.signal)
		else this.collector.discard()
		await this.manager.closeAll('dirty', this.hardStop.signal)
		this.collector.close()
		this.collector = this.newCollector()
	}

	// The rebalance hooks, wrapped as the Run wraps them (4.3).

	assign(...partitions: (string | AssignedPartition)[]): Promise<void> {
		const payload = partitions.map(p => (typeof p === 'string' ? assigned(p) : p))
		return this.exclusive.run(() => this.manager.handleAssignment(payload))
	}

	revoke(...keys: string[]): Promise<void> {
		return this.exclusive.run(() => this.manager.handleRevocation(keys.map(tp)))
	}

	lose(...keys: string[]): Promise<void> {
		this.manager.markLost(keys.map(tp))
		return this.exclusive.run(() => this.manager.handleLost(keys.map(tp)))
	}

	revive(tasks: readonly StreamTask[], rebuild = false): Promise<void> {
		return this.exclusive.run(() => this.manager.revive(tasks, { rebuild }))
	}

	closeAll(mode: 'clean' | 'dirty', signal = this.hardStop.signal): Promise<void> {
		return this.exclusive.run(() => this.manager.closeAll(mode, signal))
	}

	/** Lets every pipeline and queued exclusive op run. */
	async settle(): Promise<void> {
		for (let i = 0; i < 3; i++) {
			await new Promise(resolve => setImmediate(resolve))
			await this.exclusive.idle()
		}
	}

	tasks(): StreamTask[] {
		return [...this.manager.tasks()]
	}

	states(): [string, string][] {
		return this.manager.tasks().map(task => [task.id.key, task.state])
	}

	/** Delivers records (dense offsets) through the consumer's handler and moves its position. */
	enqueue(key: string, ...records: TestRecord[]): void {
		const { topic, partition } = tp(key)
		const position = this.consumer.partition(key)
		const messages = records.map(record => message(topic, partition, position.position++, record))
		this.manager.enqueue({ topic, partition }, messages)
	}

	/** Processes what the task can process now (in a transaction under exactly-once, as the scheduler does). */
	async process(task: StreamTask): Promise<void> {
		if (this.config.eos) await this.collector.ensureTransaction(this.hardStop.signal)
		while (task.isProcessable(this.clock.now(), tp => this.consumer.currentLag(tp) ?? 0n)) {
			if (!(await task.processOne(this.clock.now()))) break
		}
	}

	commit(): Promise<CommitOutcome> {
		return this.commits.commit('interval', this.hardStop.signal)
	}
}

async function running(options: ConstructorParameters<typeof Harness>[0] = {}, ...partitions: string[]) {
	const h = new Harness(options)
	await h.assign(...(partitions.length > 0 ? partitions : ['input:0']))
	await h.settle()
	return h
}

const tick = () => new Promise(resolve => setImmediate(resolve))

describe('TaskManager: eager rebalances (4.3)', () => {
	it('revocation commits and suspends; the next assignment resumes the tasks it keeps through the pipeline (KF3) and closes the others', async () => {
		const h = await running({ counts: { input: 2 } }, 'input:0', 'input:1')
		const [t0, t1] = h.tasks()
		expect(h.states()).toEqual([
			['0_0', 'RUNNING'],
			['0_1', 'RUNNING'],
		])
		expect(h.consumer.calls).toEqual(['pause input:0', 'pause input:1', 'resume input:0', 'resume input:1'])
		h.enqueue('input:0', { key: 'k', value: 'v' })
		await h.process(t0!)

		await h.revoke('input:0', 'input:1')
		expect(h.consumer.commits).toEqual([[expect.objectContaining({ topic: 'input', partition: 0, offset: 1n })]])
		expect([t0!.state, t1!.state]).toEqual(['SUSPENDED', 'SUSPENDED'])

		h.consumer.calls.length = 0
		await h.assign(assigned('input:0', h.consumer.commits[0]![0]!))
		await h.settle()
		expect(h.tasks()).toHaveLength(1)
		expect(h.tasks()[0]).toBe(t0)
		expect([t0!.state, t1!.state]).toEqual(['RUNNING', 'CLOSED'])
		expect(h.restorer.restored).toEqual([t0, t1, t0])
		expect(h.consumer.calls).toEqual(['pause input:0', 'resume input:0'])
		expect(h.manager.taskFor(tp('input:1'))).toBeUndefined()
		expect(h.failures).toEqual([])
	})

	it("the TestDriver's cumulative assignments keep RUNNING tasks as they are", async () => {
		const h = await running({ counts: { input: 2 }, direct: true })
		const [t0] = h.tasks()
		h.consumer.calls.length = 0
		await h.assign('input:0', 'input:1')
		await h.manager.whenAllRunning(h.shutdown.signal)
		const [k0, t1] = h.tasks()
		expect(k0).toBe(t0)
		expect([t0!.state, t1!.state]).toEqual(['RUNNING', 'RUNNING'])
		expect(h.restorer.restored).toEqual([t0, t1])
		expect(h.consumer.calls).toEqual(['pause input:1', 'resume input:1'])
		expect(h.allRunning).toEqual([false, true, false, true])
	})

	it('a stateful task needs all its partitions: a partial assignment migrates and creates nothing (D9)', async () => {
		const h = new Harness({ build: stateful(['a', 'b']), counts: { a: 2, b: 2 } })
		await h.assign('a:0', 'b:0', 'a:1')
		expect(h.failures).toEqual([{ error: expect.any(TaskMigratedError), site: 'assignment' }])
		expect(classify(h.failures[0]!.error, 'assignment')).toBe('migrate')
		expect(h.tasks()).toEqual([])
		expect(h.consumer.calls).toEqual([])
		await expect(h.manager.whenAllRunning(h.shutdown.signal)).rejects.toBeInstanceOf(TaskMigratedError)
	})

	it('a stateless task accepts a partial assignment (R9)', async () => {
		const h = await running({ build: stateless(['a', 'b']), counts: { a: 2, b: 1 } }, 'a:1', 'b:0')
		expect(h.tasks().map(task => [task.id.key, task.state, task.inputPartitions])).toEqual([
			['0_0', 'RUNNING', [tp('b:0')]],
			['0_1', 'RUNNING', [tp('a:1')]],
		])
	})

	it('a stateless task whose next assignment is a subset of its partitions is replaced, not resumed (R9)', async () => {
		const h = await running({ build: stateless(['a', 'b']), counts: { a: 1, b: 1 } }, 'a:0', 'b:0')
		const [before] = h.tasks()
		await h.revoke('a:0', 'b:0')
		await h.assign('a:0')
		await h.settle()
		expect(before!.state).toBe('CLOSED')
		expect(h.tasks().map(task => [task.id.key, task.state, task.inputPartitions])).toEqual([
			['0_0', 'RUNNING', [tp('a:0')]],
		])
		expect(h.manager.taskFor(tp('b:0'))).toBeUndefined()
	})

	it('CR16: a source topic of a stateful task that grew since start is fatal, and nothing is created', async () => {
		const h = new Harness({ counts: { input: 2 } })
		await h.assign('input:0', 'input:2')
		const [{ error, site }] = h.failures as [{ error: ChangelogPartitionMismatchError; site: FailureSite }]
		expect(error).toBeInstanceOf(ChangelogPartitionMismatchError)
		expect([error.expectedPartitions, error.actualPartitions, error.sourceTopics]).toEqual([3, 2, ['input']])
		expect(classify(error, site)).toBe('fatal')
		expect(h.tasks()).toEqual([])
	})

	it('an assigned topic the topology does not read is fatal (instances run different topologies)', async () => {
		const h = new Harness()
		await h.assign('input:0', 'other:0')
		expect(h.failures).toEqual([{ error: expect.any(AssignmentError), site: 'assignment' }])
		expect(classify(h.failures[0]!.error, 'assignment')).toBe('fatal')
		expect(h.tasks()).toEqual([])
	})

	it('loss closes every task dirty through one output reset; repeated loss and a later revocation find nothing', async () => {
		const h = await running({ counts: { input: 2 }, eos: true }, 'input:0', 'input:1')
		const tasks = h.tasks()
		h.enqueue('input:0', { key: 'k', value: 'v' })
		await h.process(tasks[0]!)
		const transaction = h.producer.transactions[0]!

		await h.lose('input:0', 'input:1')
		expect(tasks.map(task => [task.lost, task.state])).toEqual([
			[true, 'CLOSED'],
			[true, 'CLOSED'],
		])
		expect(h.events.filter(e => e === 'resetOutput')).toHaveLength(1)
		expect(transaction.state).toBe('aborted')

		await h.lose('input:0', 'input:1')
		await h.revoke('input:0', 'input:1')
		expect(h.events.filter(e => e === 'resetOutput')).toHaveLength(1)
		expect(h.producer.transactions.filter(t => t.state === 'committed')).toEqual([])
		expect(h.failures).toEqual([])
	})

	it('runtime§6.1: a failed run acknowledges nothing; a revocation after fail() commits no partial output (EOS)', async () => {
		const h = await running({ eos: true })
		const [task] = h.tasks()
		h.enqueue('input:0', { key: 'k', value: 'boom' })
		await expect(h.process(task!)).rejects.toBeInstanceOf(FailedProcessingError)
		h.failed = true // FlowRuntime.fail, synchronously
		const transaction = h.producer.transactions[0]!
		expect(transaction.calls).toEqual(['send'])

		await h.revoke('input:0')
		await h.lose('input:0')
		await h.assign('input:0')
		await h.settle()
		expect(transaction.calls).toEqual(['send'])
		expect(transaction.state).toBe('open')
		expect(h.consumer.commits).toEqual([])
		expect(task!.state).toBe('RUNNING')
		expect(h.events).not.toContain('resetOutput')
	})

	it.each([
		['migrate', new CommitFailedError(APP, new KafkaProtocolError(ErrorCode.IllegalGeneration)), []],
		['fatal', new KafkaProtocolError(ErrorCode.GroupAuthorizationFailed), ['commit']],
	])(
		'a revocation commit that fails (%s) resets the output and lets the rebalance proceed',
		async (_class, error, sites) => {
			const h = await running({ counts: { input: 2 } }, 'input:0', 'input:1')
			const tasks = h.tasks()
			h.enqueue('input:0', { key: 'k', value: 'v' })
			await h.process(tasks[0]!)
			h.consumer.steps.commit.always(error)
			await h.revoke('input:0', 'input:1')
			expect(h.events.filter(e => e === 'resetOutput')).toHaveLength(1)
			expect(tasks.map(task => task.state)).toEqual(['CLOSED', 'CLOSED'])
			expect(h.failures.map(f => f.site)).toEqual(sites)
		}
	)

	it('a close in progress (shutdown aborted) does not cut the revocation commit short', async () => {
		const h = await running()
		const [task] = h.tasks()
		h.enqueue('input:0', { key: 'k', value: 'v' })
		await h.process(task!)
		h.shutdown.abort()
		await h.revoke('input:0')
		expect(h.consumer.commits).toHaveLength(1)
		expect(task!.state).toBe('SUSPENDED')
	})
})

describe('TaskManager: restore pipelines (4.7.1)', () => {
	it.each(['revocation', 'closeAll'] as const)(
		'runtime§6.31: a %s waits until the stopped restore finished its in-flight store write',
		async op => {
			const h = new Harness()
			h.restorer.manual = true
			await h.assign('input:0')
			const [task] = h.tasks()
			const job = h.restorer.job(task!)
			const write = deferred()
			job.write = write.promise
			const done = op === 'revocation' ? h.revoke('input:0') : h.closeAll('dirty')
			await tick()
			expect(job.signal.aborted).toBe(true)
			expect(task!.state).toBe('RESTORING')
			write.resolve()
			await done
			expect(task!.state).toBe(op === 'revocation' ? 'SUSPENDED' : 'CLOSED')
			expect(h.failures).toEqual([])
		}
	)

	it('runtime§6.30: a completion queued behind an op that suspended the task does nothing', async () => {
		const h = new Harness()
		h.restorer.manual = true
		await h.assign('input:0')
		const [task] = h.tasks()
		const gate = deferred()
		void h.exclusive.run(() => gate.promise)
		const revoked = h.revoke('input:0')
		h.restorer.job(task!).finish()
		await tick()
		gate.resolve()
		await revoked
		await h.settle()
		expect(task!.state).toBe('SUSPENDED')
		expect(h.consumer.calls).toEqual(['pause input:0'])
	})

	it('runtime§6.30: the completion of a stopped pipeline does nothing, even once the task restores again', async () => {
		const h = new Harness()
		h.restorer.manual = true
		await h.assign('input:0')
		const [task] = h.tasks()
		const first = h.restorer.job(task!)
		const gate = deferred()
		void h.exclusive.run(() => gate.promise)
		const revoked = h.revoke('input:0')
		const reassigned = h.assign('input:0')
		first.finish()
		await tick()
		gate.resolve()
		await Promise.all([revoked, reassigned])
		await h.settle()
		const second = h.restorer.job(task!)
		expect(second).not.toBe(first)
		expect(task!.state).toBe('RESTORING')
		expect(h.consumer.calls).toEqual(['pause input:0', 'pause input:0'])
		second.finish()
		await h.settle()
		expect(task!.state).toBe('RUNNING')
	})

	it('runtime§6.30: a restore completion after fail() neither reaches RUNNING nor resumes', async () => {
		const h = new Harness()
		h.restorer.manual = true
		await h.assign('input:0')
		const [task] = h.tasks()
		h.failed = true
		h.restorer.job(task!).finish()
		await h.settle()
		expect(task!.state).toBe('RESTORING')
		expect(h.consumer.calls).toEqual(['pause input:0'])
		expect(h.allRunning).toEqual([false])
	})

	it('runtime§6.30: a run that fails while an exactly-once completion opens its transaction never runs init()', async () => {
		let inits = 0
		const counting: ProcessorSupplier<string, string, string, string> = () => ({
			init: () => void inits++,
			process: () => {},
		})
		const h = new Harness({ eos: true, build: stateful(['input'], counting) })
		h.restorer.manual = true
		await h.assign('input:0')
		const [task] = h.tasks()
		const gate = deferred()
		const ensureTransaction = h.collector.ensureTransaction.bind(h.collector)
		let opening = false
		h.collector.ensureTransaction = async signal => {
			opening = true
			await gate.promise
			return ensureTransaction(signal)
		}
		h.restorer.job(task!).finish()
		while (!opening) await tick()
		h.failed = true
		gate.resolve()
		await h.settle()
		expect(inits).toBe(0)
		expect(task!.state).toBe('RESTORING')
		expect(h.consumer.calls).toEqual(['pause input:0'])
		expect(h.allRunning).toEqual([false])
	})

	it("runtime§6.30: a completion queued before the lost hook's op neither runs init(), resumes nor counts the lost task", async () => {
		for (const eos of [false, true]) {
			let inits = 0
			const counting: ProcessorSupplier<string, string, string, string> = () => ({
				init: () => void inits++,
				process: () => {},
			})
			const h = new Harness({ eos, build: stateful(['input'], counting), counts: { input: 2 } })
			h.restorer.manual = true
			await h.assign('input:0', 'input:1')
			const [restoring, other] = h.tasks()
			h.restorer.job(other!).finish()
			await h.settle()
			inits = 0
			const gate = deferred()
			void h.exclusive.run(() => gate.promise)
			h.restorer.job(restoring!).finish()
			await tick()
			const lost = h.lose('input:0', 'input:1')
			gate.resolve()
			await lost
			await h.settle()
			expect(inits).toBe(0)
			expect(h.consumer.calls).not.toContain('resume input:0')
			expect(h.allRunning).not.toContain(true)
			expect(restoring!.state).toBe('CLOSED')
		}
	})

	it('a restore failure is reported at the restore site and fails the start', async () => {
		const h = new Harness()
		h.restorer.manual = true
		await h.assign('input:0')
		const started = h.manager.whenAllRunning(h.shutdown.signal)
		const error = new Error('restore failed')
		h.restorer.job(h.tasks()[0]!).fail(error)
		await expect(started).rejects.toBe(error)
		expect(h.failures).toEqual([{ error, site: 'restore' }])
	})

	it('a restore-time corruption does not fail the start: the task is revived in place and rebuilds its stores', async () => {
		const h = new Harness()
		h.restorer.manual = true
		await h.assign('input:0')
		const started = h.manager.whenAllRunning(h.shutdown.signal)
		const [task] = h.tasks()
		h.restorer.job(task!).fail(new TaskCorruptedError('out of range', [task!.id]))
		await h.settle()
		expect(h.failures.map(f => [f.error, f.site])).toEqual([[expect.any(TaskCorruptedError), 'restore']])
		await h.revive([task!], true)
		const [revived] = h.tasks()
		expect(revived).not.toBe(task)
		h.restorer.job(revived!).finish()
		await started
		expect(revived!.state).toBe('RUNNING')
	})
})

describe('TaskManager: allRunning (4.3.5, CR14)', () => {
	it('onAllRunningChanged: false at every assignment, true once when the last task is RUNNING', async () => {
		const h = new Harness({ counts: { input: 2 } })
		h.restorer.manual = true
		const started = h.manager.whenAllRunning(h.shutdown.signal)
		await h.assign('input:0', 'input:1')
		const [t0, t1] = h.tasks()
		h.restorer.job(t1!).finish()
		await h.settle()
		expect([h.allRunning, h.manager.allRunning]).toEqual([[false], false])
		h.restorer.job(t0!).finish()
		await started
		expect([h.allRunning, h.manager.allRunning]).toEqual([[false, true], true])

		await h.revoke('input:0', 'input:1')
		await h.assign('input:0', 'input:1')
		expect(h.allRunning).toEqual([false, true, false, false])
	})

	it('a completion that runs while an assignment is under way does not count (an exclusive that runs at once)', async () => {
		const h = new Harness({ counts: { input: 3 }, direct: true })
		h.restorer.manual = true
		await h.assign('input:0', 'input:1')
		const [t0, t1] = h.tasks()
		const write = deferred()
		h.restorer.job(t1!).write = write.promise
		const reassigned = h.assign('input:0', 'input:2')
		h.restorer.job(t0!).finish()
		await tick()
		expect(t0!.state).toBe('RUNNING')
		expect(h.allRunning).toEqual([false, false])
		write.resolve()
		await reassigned
		h.restorer.job(h.tasks()[1]!).finish()
		await h.manager.whenAllRunning(h.shutdown.signal)
		expect(h.allRunning).toEqual([false, false, true])
	})

	it('CR14: an empty assignment completes (all RUNNING), a failed one never counts', async () => {
		const empty = new Harness()
		await empty.assign()
		await empty.manager.whenAllRunning(empty.shutdown.signal)
		expect(empty.allRunning).toEqual([false, true])

		const failed = new Harness({ build: stateful(['a', 'b']), counts: { a: 1, b: 1 } })
		await failed.assign('a:0')
		expect(failed.allRunning).toEqual([false])
		await expect(failed.manager.whenAllRunning(failed.shutdown.signal)).rejects.toBeInstanceOf(TaskMigratedError)
	})

	it('whenAllRunning rejects with the reason of its signal', async () => {
		const h = new Harness()
		const abort = new AbortController()
		const started = h.manager.whenAllRunning(abort.signal)
		const reason = new Error('did not join the group within startTimeoutMs')
		abort.abort(reason)
		await expect(started).rejects.toBe(reason)
	})
})

describe('TaskManager: revive in place (4.7.5)', () => {
	it('D5: rewinds to the last commit, seeds the clocks from it and re-commits no start position', async () => {
		const h = await running()
		expect(h.startPositions.commits).toEqual(['input:0@0'])
		const [task] = h.tasks()
		h.enqueue('input:0', { key: 'a', value: '1', timestamp: 10 }, { key: 'b', value: '2', timestamp: 20 })
		await h.process(task!)
		expect(await h.commit()).toBe('committed')
		const [commit] = h.consumer.commits[0]!
		h.enqueue('input:0', { key: 'c', value: '3', timestamp: 30 })
		await h.process(task!)
		expect(task!.streamTime).toBe(30)
		h.consumer.calls.length = 0

		await h.revive([task!])
		await h.settle()
		const [revived] = h.tasks()
		expect(revived).not.toBe(task)
		expect([task!.state, revived!.state]).toEqual(['CLOSED', 'RUNNING'])
		expect(h.consumer.calls).toEqual(['pause input:0', 'seek input:0 2', 'resume input:0'])
		expect(revived!.starts.get('input:0')).toEqual({
			committed: { offset: 2n, metadata: commit!.metadata },
			resolved: 2n,
		})
		expect(h.startPositions.commits).toEqual(['input:0@0'])
		expect(revived!.streamTime).toBe(20)
		expect(h.restorer.restored).toEqual([task, revived])
	})

	it.each([true, false])(
		'rebuild %s: the stores are released with their checkpoints invalidated (or not), never cleared here',
		async rebuild => {
			const h = await running()
			const [task] = h.tasks()
			h.enqueue('input:0', { key: 'k', value: 'v' })
			await h.process(task!)
			const release = vi.spyOn(h.stateDirectory, 'release')
			await h.revive([task!], rebuild)
			await h.settle()
			expect(release.mock.calls.map(call => call[3])).toEqual([{ invalidate: rebuild }])
			// Clearing a logged store is the restore's, outside exclusive; an unlogged one keeps its state either way.
			const store = h.tasks()[0]!.store('s') as KeyValueStore<string, string>
			expect(await store.get('k')).toBe('v')
		}
	)

	it.each([
		[59_999, true],
		[60_000, false],
	])('a second corruption %d ms after the first escalates to a migration: %s', async (after, escalates) => {
		const h = await running()
		await h.revive(h.tasks())
		await h.settle()
		const [first] = h.tasks()
		h.clock.advance(after)
		await h.revive([first!])
		await h.settle()
		expect(h.failures.map(f => f.error)).toEqual(escalates ? [expect.any(TaskMigratedError)] : [])
		expect(h.tasks()[0] === first).toBe(escalates)
		expect(h.tasks()[0]!.state).toBe('RUNNING')
	})

	it('only closes a task suspended meanwhile: the next assignment re-creates it from its commit (F1)', async () => {
		const h = await running()
		const [task] = h.tasks()
		await h.revoke('input:0')
		await h.revive([task!], true)
		expect(h.tasks()).toEqual([])
		expect(task!.state).toBe('CLOSED')
		expect(h.restorer.restored).toEqual([task])
		await h.assign('input:0')
		await h.settle()
		const [created] = h.tasks()
		expect(created).not.toBe(task)
		expect(created!.state).toBe('RUNNING')
	})

	it('skips tasks that were replaced meanwhile', async () => {
		const h = await running()
		const [task] = h.tasks()
		await h.revive([task!])
		const [revived] = h.tasks()
		await h.settle()
		h.clock.advance(60_000)
		await h.revive([task!])
		expect(h.tasks()).toEqual([revived])
		expect(h.failures).toEqual([])
	})
})

describe('TaskManager: an op that outlives the end of its run (Run.end closes the tasks beside it, F3)', () => {
	/** What Run.end does once an op held exclusive too long: the run failed, every task closed dirty beside the op. */
	async function endBeside(h: Harness): Promise<void> {
		h.failed = true
		await h.manager.closeAll('dirty', h.hardStop.signal)
	}

	it('a revival whose close outlives the end re-creates and leases nothing', async () => {
		const h = await running({ counts: { input: 2 } }, 'input:0', 'input:1')
		const [, task] = h.tasks()
		const wiped = deferred()
		const release = h.stateDirectory.release.bind(h.stateDirectory)
		vi.spyOn(h.stateDirectory, 'release').mockImplementation(async (owner, store, partition, options) => {
			if (options?.invalidate) await wiped.promise
			return release(owner, store, partition, options)
		})
		const reviving = h.revive([task!], true)
		await tick()
		await endBeside(h)
		let released = false
		const allReleased = h.stateDirectory.released().then(() => (released = true))
		await tick()
		expect(released).toBe(false)
		wiped.resolve()
		await reviving
		await allReleased
		expect(h.tasks()).toEqual([])
		expect(h.failures).toEqual([])
	})

	it('an assignment whose task initializes past the end closes that task again', async () => {
		const h = new Harness()
		const initialized = deferred()
		const lease = h.stateDirectory.lease.bind(h.stateDirectory)
		vi.spyOn(h.stateDirectory, 'lease').mockImplementation(async (...args) => {
			await initialized.promise
			return lease(...args)
		})
		const assigning = h.assign('input:0')
		await tick()
		await endBeside(h)
		initialized.resolve()
		await assigning
		await h.stateDirectory.released()
		expect(h.tasks()).toEqual([])
		expect(h.restorer.restored).toEqual([])
		expect(h.failures).toEqual([])
	})

	it('a revocation whose commit outlives the end suspends nothing, resets nothing and reports nothing', async () => {
		const h = await running({ counts: { input: 2 } }, 'input:0', 'input:1')
		const tasks = h.tasks()
		h.enqueue('input:0', { key: 'k', value: 'v' })
		await h.process(tasks[0]!)
		h.consumer.steps.commit.always('hang')
		const revoking = h.revoke('input:0', 'input:1')
		await tick()
		await endBeside(h)
		h.consumer.steps.commit.answerAll(new Error('the group consumer stopped'))
		await revoking
		expect(tasks.map(task => task.state)).toEqual(['CLOSED', 'CLOSED'])
		expect(h.events).not.toContain('resetOutput')
		expect(h.failures).toEqual([])
	})
})

describe('TaskManager: closeAll', () => {
	it("'clean' suspends and closes every task clean; a lost one closes dirty", async () => {
		const h = await running({ counts: { input: 2 } }, 'input:0', 'input:1')
		const tasks = h.tasks()
		const closes = tasks.map(task => vi.spyOn(task, 'close'))
		h.manager.markLost([tp('input:1')])
		await h.closeAll('clean')
		expect(closes.map(close => close.mock.calls[0]?.[0])).toEqual(['clean', 'dirty'])
		expect(tasks.map(task => task.state)).toEqual(['CLOSED', 'CLOSED'])
		expect(h.tasks()).toEqual([])
		expect(h.allRunning.at(-1)).toBe(false)
	})

	it('a task whose processors fail to close closes dirty; the failure is thrown once every task is closed', async () => {
		const failing: ProcessorSupplier<string, string, string, string> = () => ({
			process: () => {},
			close: () => {
				throw new Error('close failed')
			},
		})
		const h = await running({ build: stateful(['input'], failing), counts: { input: 2 } }, 'input:0', 'input:1')
		const tasks = h.tasks()
		await expect(h.closeAll('clean')).rejects.toThrow("failed to close processor 'processor'")
		expect(tasks.map(task => task.state)).toEqual(['CLOSED', 'CLOSED'])
	})

	it('forgets every task before closing one: a closeAll beside it (Run.end beside an op) closes none of them again', async () => {
		const h = await running({ counts: { input: 2 } }, 'input:0', 'input:1')
		const tasks = h.tasks()
		const closes = tasks.map(task => vi.spyOn(task, 'close'))
		const released = deferred()
		const release = h.stateDirectory.release.bind(h.stateDirectory)
		vi.spyOn(h.stateDirectory, 'release').mockImplementation(async (...args) => {
			await released.promise
			return release(...args)
		})
		// The op's closeAll waits in the close of 0_0 while the end's runs beside it.
		const op = h.manager.closeAll('dirty', h.hardStop.signal)
		await tick()
		const end = h.manager.closeAll('dirty', h.hardStop.signal)
		await tick()
		released.resolve()
		await Promise.all([op, end])
		expect(closes.map(close => close.mock.calls.length)).toEqual([1, 1])
		expect(tasks.map(task => task.state)).toEqual(['CLOSED', 'CLOSED'])
		expect(h.tasks()).toEqual([])
	})

	it('waits for the closes until its signal aborts; the rest close in the background', async () => {
		const h = new Harness()
		h.restorer.manual = true
		await h.assign('input:0')
		const job = h.restorer.job(h.tasks()[0]!)
		job.write = new Promise(() => {})
		await h.closeAll('dirty', AbortSignal.timeout(20))
		expect(h.tasks()).toEqual([])
		expect(h.logger.warnings).toContain(
			'closing the tasks did not finish in time: the rest close in the background'
		)
	})
})

describe('TaskManager: enqueue (4.5.1)', () => {
	it('routes records to their task and pauses a partition once its queue grew past bufferedRecordsPerPartition', async () => {
		const h = await running({ bufferedRecordsPerPartition: 2 })
		const [task] = h.tasks()
		h.consumer.calls.length = 0
		h.events.length = 0
		h.enqueue('input:0', { value: '1' }, { value: '2' })
		expect(h.consumer.calls).toEqual([])
		h.enqueue('input:0', { value: '3' })
		h.enqueue('input:0', { value: '4' })
		expect(h.consumer.calls).toEqual(['pause input:0'])
		expect(h.events).toEqual(['0_0', '0_0', '0_0'])
		expect(h.manager.taskFor(tp('input:0'))).toBe(task)
	})

	it('drops records of a partition without a task', async () => {
		const h = await running()
		h.events.length = 0
		h.enqueue('input:3', { value: 'v' })
		expect(h.events).toEqual([])
	})
})
