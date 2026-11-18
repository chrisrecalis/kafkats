/**
 * The TestDriver's runtime (4.14, KS `TopologyTestDriver`): the production TaskManager, CommitCoordinator,
 * RecordCollector, StateDirectory and Checkpoints over the test ports; only the Scheduler is replaced, by this
 * deterministic loop. A delivery resolves once the record and everything it led to were processed: records written to
 * topics the topology reads are fed back in. Tasks are created on demand by cumulative assignments (nothing is ever
 * revoked), commits follow every record unless configured otherwise, and the first failure stops the loop: the driver
 * has no log to replay from, so it neither revives tasks nor restarts.
 */
import type { TopicPartition } from '@kafkats/client'
import type { AppInternals } from '@/app/internal.js'
import type { MaybePromise } from '@/processor/api.js'
import { CommitCoordinator } from '@/runtime/commit.js'
import type { ResolvedFlowConfig } from '@/runtime/config.js'
import { classify, errorMessage, type FailureSite } from '@/runtime/errors.js'
import type { AssignedPartition } from '@/runtime/io.js'
import { RecordCollector } from '@/runtime/record-collector.js'
import { TaskIdRegistry, tpKey } from '@/runtime/task-id.js'
import { TaskManager } from '@/runtime/task-manager.js'
import type { StreamTask } from '@/runtime/task.js'
import { flowDiagnostics } from '@/runtime/tracing.js'
import type { StateStore } from '@/state.js'
import { Checkpoints } from '@/state/checkpoints.js'
import { StateDirectory } from '@/state/state-directory.js'
import { GlobalStores } from '@/testing/globals.js'
import { MockConsumer, MockProducer, type SendOptions, type TestRecord } from '@/testing/mocks.js'
import { writesInTransaction } from '@/testing/store-writes.js'
import { TestClock, TestConsumer, TestPartitions, type InputRecord } from '@/testing/test-io.js'
import type { BuiltTopology } from '@/topology/built-topology.js'

export interface TestLoopOptions {
	/** After every record (default), only at close, or whenever the wall clock moved this many ms since the last commit. */
	readonly commitEvery: 'record' | 'stop' | number
	/** Partition counts of topics (TestPartitions). */
	readonly partitions: Readonly<Record<string, number>>
	readonly initialWallClockTimeMs: number
	readonly emitIntervalMs: number
}

/** Every partition is caught up: tasks never wait for data that is not delivered yet (unless maxTaskIdleMs says so). */
const NO_LAG = (): bigint => 0n

export class TestLoop {
	readonly producer: MockProducer
	readonly consumer: MockConsumer
	private readonly settings: ResolvedFlowConfig
	private readonly clock: TestClock
	private readonly ports: TestConsumer
	private readonly partitions: TestPartitions
	private readonly stateDirectory: StateDirectory
	private readonly commits: CommitCoordinator
	private readonly tasks: TaskManager
	/** Opened at start, before any task exists. */
	private globals: GlobalStores | undefined
	private collector: RecordCollector
	/** Never aborted: the driver waits for everything it starts. */
	private readonly signal = new AbortController().signal
	/** The cumulative assignment. */
	private readonly assigned = new Map<string, TopicPartition>()
	/** Records written to topics the topology reads and their partitions, not fed back yet (in production order). */
	private readonly produced: { readonly record: TestRecord<Buffer, Buffer>; readonly partition: number }[] = []
	private commitRequested = false
	private failure: { readonly error: unknown } | undefined

	private constructor(
		private readonly app: AppInternals,
		private readonly topology: BuiltTopology,
		private readonly options: TestLoopOptions
	) {
		const { logger, stateStoreProvider } = app
		this.settings = { ...app.settings, processId: 'test-driver', emitIntervalMs: options.emitIntervalMs }
		this.clock = new TestClock(options.initialWallClockTimeMs)
		this.ports = new TestConsumer(this.settings.applicationId)
		this.partitions = new TestPartitions(topology, options.partitions)
		const checkpoints = new Checkpoints(stateStoreProvider.getChangelogCheckpointStore?.(), error =>
			logger.error('writing a changelog checkpoint failed', { error: error.message })
		)
		this.stateDirectory = new StateDirectory(this.settings.applicationId, stateStoreProvider, checkpoints, logger)
		this.producer = new MockProducer(
			topic => this.partitions.reported(topic),
			(record, partition) => {
				if (this.reads(record.topic)) this.produced.push({ record, partition })
			},
			offsets => this.ports.record(offsets)
		)
		this.consumer = new MockConsumer(
			(topic, value, sent) => this.deliver(topic, this.input(value, sent)),
			this.ports.committed
		)
		this.collector = this.newCollector()
		this.commits = new CommitCoordinator({
			consumer: this.ports,
			collector: () => this.collector,
			tasks: () => this.tasks.tasks(),
			assignmentMetadata: () => this.tasks.assignmentMetadata,
			eos: this.settings.eos,
			checkpoints,
			onCommitted: { noteCommitted: () => {} },
			failed: () => this.failure !== undefined,
			hardStop: this.signal,
			config: this.settings,
			clock: this.clock,
			logger,
		})
		this.tasks = new TaskManager({
			topology,
			counts: this.partitions.counts,
			ids: new TaskIdRegistry(),
			config: this.settings,
			stateDirectory: this.stateDirectory,
			checkpoints,
			globalStore: name => this.globals!.view(name),
			consumer: this.ports,
			outputs: {
				forTask: task => this.collector.forTask(task),
				ensureTransaction: signal => this.collector.ensureTransaction(signal),
			},
			restorer: { restore: () => Promise.resolve({ storeTimes: new Map() }) },
			startPositions: { settle: task => Promise.resolve(uncommittedPositions(task)) },
			exclusive: fn => fn(),
			commit: (reason, signal) => this.commits.commit(reason, signal),
			resetOutput: () => this.resetOutput(),
			onWork: () => {},
			requestCommit: () => {
				this.commitRequested = true
			},
			onAllRunningChanged: () => {},
			failed: () => this.failure !== undefined,
			onFailure: (error, site) => this.fail(error, site),
			hardStop: this.signal,
			signal: this.signal,
			clock: this.clock,
			logger,
		})
	}

	/** Opens the global stores and creates the tasks of partition 0 of every subtopology (KS creates its tasks at start). */
	static async start(app: AppInternals, options: TestLoopOptions): Promise<TestLoop> {
		const topology = app.build()
		const loop = new TestLoop(app, topology, options)
		const { settings, stateDirectory, clock } = loop
		const counts = loop.partitions.counts
		try {
			loop.globals = await loop.guard('global', () =>
				GlobalStores.create(topology.globalStores, { settings, stateDirectory, clock, logger: app.logger })
			)
			await loop.guard('assignment', () =>
				loop.assign(topology.subtopologies.flatMap(s => topology.inputPartitions(s, 0, counts)))
			)
			await loop.processAll()
		} catch (error) {
			await loop.close()
			throw error
		}
		return loop
	}

	/** Delivers one record of `topic` and processes everything it leads to. */
	async deliver(topic: string, record: InputRecord): Promise<void> {
		this.throwIfFailed()
		await this.enqueue(topic, record)
		await this.processAll()
	}

	/** Moves the wall clock: wall-clock punctuations due, a commit if one is due, then everything processable. */
	async advanceWallClockTime(ms: number): Promise<void> {
		if (!Number.isSafeInteger(ms) || ms < 0) {
			throw new RangeError(`advanceWallClockTime() takes a non-negative whole number of ms, got ${ms}`)
		}
		this.throwIfFailed()
		this.clock.advance(ms)
		const now = this.clock.now()
		for (const task of this.tasks.tasks()) {
			if (task.wallClockPunctuationDue(now)) {
				await this.ensureTransaction()
				await this.guard('punctuation', () => task.maybePunctuateSystemTime(now))
			}
		}
		await this.commitIfDue()
		await this.processAll()
	}

	/**
	 * The store `name` of the task of `partition` (a handle on the top of its stack), or the global store of that
	 * name. Under exactly-once a write opens the driver's transaction first, so it is logged and committed like a
	 * processor's.
	 */
	store<S extends StateStore>(name: string, partition = 0): S {
		const global = this.globals?.store(name)
		if (global !== undefined) {
			return global as S
		}
		const store = this.tasks
			.tasks()
			.find(task => task.partition === partition && task.subtopology.stores.some(s => s.name === name))
			?.store(name)
		if (store === undefined) {
			throw new Error(`No task of partition ${partition} has a state store named '${name}'`)
		}
		return (this.settings.eos ? writesInTransaction(store, () => this.ensureTransaction()) : store) as S
	}

	/** Commits and closes every task clean; after a failure, closes them dirty. Closes the stores. */
	async close(): Promise<void> {
		try {
			if (this.failure === undefined) {
				await this.guard('commit', () => this.commits.commit('close', this.signal))
				await this.guard('processing', () => this.tasks.closeAll('clean', this.signal))
			}
		} finally {
			if (this.failure !== undefined) {
				await this.resetOutput()
			}
			this.collector.close()
			await this.globals?.close()
			await this.stateDirectory.close()
		}
	}

	// ---------------------------------------------------------------------------------------------------------

	/**
	 * Processes until no task can (KS `completeAllProcessableWork`): one record of the first processable task in
	 * TaskId order, the stream-time punctuations due, a commit if one is due; records written to topics the topology
	 * reads are fed back before each step.
	 */
	private async processAll(): Promise<void> {
		for (;;) {
			await this.feedBack()
			const now = this.clock.now()
			const task = this.tasks.tasks().find(t => t.isProcessable(now, NO_LAG))
			if (task === undefined) {
				return
			}
			await this.ensureTransaction()
			await this.guard('processing', () => task.processOne(now, flowDiagnostics.process.hasSubscribers))
			for (const each of this.tasks.tasks()) {
				if (each.streamTimePunctuationDue()) {
					await this.guard('punctuation', () => each.maybePunctuateStreamTime())
				}
			}
			await this.commitIfDue()
		}
	}

	private async feedBack(): Promise<void> {
		for (let next = this.produced.shift(); next !== undefined; next = this.produced.shift()) {
			const { record, partition } = next
			await this.enqueue(record.topic, {
				partition,
				key: record.key,
				value: record.value,
				timestamp: record.timestamp === undefined ? this.clock.now() : Number(record.timestamp),
				headers: record.headers ?? {},
			})
		}
	}

	/** Hands the record to its task (created first if needed) or applies it to its global store. */
	private async enqueue(topic: string, record: InputRecord): Promise<void> {
		const { globals } = this
		if (globals?.has(topic)) {
			const message = this.ports.deliver(topic, record)
			await this.guard('global', () => globals.apply(message))
			return
		}
		const subtopology = this.topology.subtopologyOf(topic)
		if (subtopology === undefined) {
			return
		}
		const tp = { topic, partition: record.partition }
		if (this.tasks.taskFor(tp) === undefined) {
			await this.guard('assignment', async () => {
				this.partitions.ensure(topic, record.partition)
				const counts = this.partitions.counts
				await this.assign(this.topology.inputPartitions(subtopology, record.partition, counts))
			})
		}
		this.tasks.enqueue(tp, [this.ports.deliver(topic, record)])
	}

	/** Adds the partitions to the cumulative assignment and waits until every task is RUNNING. */
	private async assign(partitions: readonly TopicPartition[]): Promise<void> {
		for (const tp of partitions) {
			this.assigned.set(tpKey(tp), tp)
		}
		const assignment = [...this.assigned.values()].map(tp => this.assignedPartition(tp))
		await this.tasks.handleAssignment(assignment)
		await this.tasks.whenAllRunning(this.signal)
	}

	/** From the committed offset if there is one, else from the next offset delivered. */
	private assignedPartition(tp: TopicPartition): AssignedPartition {
		const committed = this.ports.committed.get(tpKey(tp)) ?? null
		return { ...tp, offset: committed?.offset ?? this.ports.nextOffset(tp), committed }
	}

	private async commitIfDue(): Promise<void> {
		const every = this.options.commitEvery
		const due =
			every === 'record' || (typeof every === 'number' && this.clock.now() - this.commits.lastCommitAt >= every)
		if (due || this.commitRequested) {
			this.commitRequested = false
			await this.guard('commit', () => this.commits.commit(due ? 'interval' : 'user', this.signal))
		}
	}

	/** Exactly-once: processing and punctuation send inside a transaction. */
	private async ensureTransaction(): Promise<void> {
		if (this.settings.eos) {
			await this.guard('commit', () => this.collector.ensureTransaction(this.signal))
		}
	}

	/** Run.resetOutput: abort or discard, every task closed dirty, a new collector. */
	private async resetOutput(): Promise<void> {
		if (this.settings.eos) {
			await this.collector.abortTransaction(this.signal)
		} else {
			this.collector.discard()
		}
		await this.tasks.closeAll('dirty', this.signal)
		this.collector.close()
		this.collector = this.newCollector()
	}

	private newCollector(): RecordCollector {
		return new RecordCollector(this.producer, {
			eos: this.settings.eos,
			handlers: this.settings.handlers,
			counts: {
				get: topic => this.partitions.reported(topic),
				load: topic => Promise.resolve(this.partitions.reported(topic)),
			},
			logger: this.app.logger,
		})
	}

	/** Runs `work`; a failure stops the loop and is rethrown. */
	private async guard<T>(site: FailureSite, work: () => MaybePromise<T>): Promise<T> {
		try {
			return await work()
		} catch (error) {
			this.fail(error, site)
			throw error
		}
	}

	/** The first failure stops the loop; a fatal one goes to the uncaught exception handler, whose answer changes nothing. */
	private fail(error: unknown, site: FailureSite): void {
		if (this.failure !== undefined) {
			return
		}
		this.failure = { error }
		if (classify(error, site) === 'fatal') {
			try {
				this.settings.handlers.uncaught(error instanceof Error ? error : new Error(String(error)))
			} catch (handlerError) {
				this.app.logger.warn('the uncaught exception handler threw', { error: errorMessage(handlerError) })
			}
		}
	}

	private throwIfFailed(): void {
		if (this.failure !== undefined) {
			throw this.failure.error
		}
	}

	/** A source topic or a global topic. */
	private reads(topic: string): boolean {
		return this.topology.subtopologyOf(topic) !== undefined || this.globals?.has(topic) === true
	}

	private input(value: Buffer | null, options: SendOptions): InputRecord {
		return {
			partition: options.partition ?? 0,
			key: options.key ?? null,
			value,
			timestamp: options.timestamp === undefined ? this.clock.now() : Number(options.timestamp),
			headers: options.headers ?? {},
		}
	}
}

/** What StartPositions.settle returns without pins: the start of every partition without a commit. */
function uncommittedPositions(task: StreamTask): ReadonlyMap<string, bigint> {
	const positions = new Map<string, bigint>()
	for (const [key, start] of task.starts) {
		if (start.committed === null) positions.set(key, start.resolved)
	}
	return positions
}
