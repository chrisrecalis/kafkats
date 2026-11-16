/**
 * Task harness: tasks of a topology built with the real InternalTopologyBuilder, over test ports: a fake consumer
 * (commits, next offsets, group metadata), a fake producer with transactions, the real RecordCollector and
 * CommitCoordinator, a manual clock, and the real state layer over in-memory stores (StateDirectory, Checkpoints, the
 * tasks' TaskStateManagers, probed for what the tasks ask of them). Free of vitest: the micro-benchmark uses it too.
 */
import type { ConsumerGroupMetadata, Logger, Message, SendResult, TopicPartition } from '@kafkats/client'
import { string } from '@/codec.js'
import type { HandlerResponse } from '@/api/handlers.js'
import type { ProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import { TimestampExtractors, type TimestampExtractor } from '@/processor/timestamps.js'
import { CommitCoordinator, type CommitOutcome, type CommitReason } from '@/runtime/commit.js'
import type { ResolvedHandlers } from '@/runtime/config.js'
import type { Clock, OffsetAndMetadata, ProducedMessage, ProducerPort, TransactionPort } from '@/runtime/io.js'
import { RecordCollector } from '@/runtime/record-collector.js'
import { TaskIdRegistry, tpKey } from '@/runtime/task-id.js'
import { StreamTask, type PartitionStart } from '@/runtime/task.js'
import type { ChangelogCheckpointStore, StateStore } from '@/state.js'
import { Checkpoints } from '@/state/checkpoints.js'
import { inMemory } from '@/state/memory.js'
import { StateDirectory } from '@/state/state-directory.js'
import type { TaskStateManager } from '@/state/state-manager.js'
import { keyValueStoreBuilder } from '@/state/store-builders.js'
import { InternalTopologyBuilder } from '@/topology/builder.js'
import type { BuiltTopology } from '@/topology/built-topology.js'
import type { StoreBuilder } from '@/topology/nodes.js'

export const APP = 'app'

// ----------------------------------------------------------------------------------------------------------
// Clock and logger
// ----------------------------------------------------------------------------------------------------------

export class ManualClock implements Clock {
	constructor(public time = 0) {}

	now(): number {
		return this.time
	}

	setTimer(): () => void {
		return () => {}
	}

	advance(ms: number): void {
		this.time += ms
	}
}

/** Keeps every message logged at warn level and above. */
export class RecordingLogger implements Logger {
	readonly warnings: string[] = []
	readonly errors: string[] = []

	error(message: string): void {
		this.errors.push(message)
	}

	warn(message: string): void {
		this.warnings.push(message)
	}

	info(): void {}

	debug(): void {}

	child(): Logger {
		return this
	}
}

// ----------------------------------------------------------------------------------------------------------
// Producer and consumer
// ----------------------------------------------------------------------------------------------------------

export interface SentRecord {
	readonly topic: string
	readonly message: ProducedMessage
	readonly transaction?: FakeTransaction
}

export class FakeProducer implements ProducerPort {
	readonly sent: SentRecord[] = []
	readonly transactions: FakeTransaction[] = []
	/** Answers every send; by default acks at the next offset of the partition. Replace to fail or delay. */
	answer: (topic: string, message: ProducedMessage) => Promise<SendResult> = (topic, message) =>
		Promise.resolve(this.ack(topic, message))
	private readonly offsets = new Map<string, bigint>()

	ack(topic: string, message: ProducedMessage): SendResult {
		const partition = message.partition ?? 0
		const key = `${topic}:${partition}`
		const offset = this.offsets.get(key) ?? 0n
		this.offsets.set(key, offset + 1n)
		return { topic, partition, offset, timestamp: new Date(0) }
	}

	send(topic: string, message: ProducedMessage): Promise<SendResult> {
		this.sent.push({ topic, message })
		return this.answer(topic, message)
	}

	beginTransaction(): Promise<TransactionPort> {
		const transaction = new FakeTransaction(this)
		this.transactions.push(transaction)
		return Promise.resolve(transaction)
	}

	partitionCount(): Promise<number> {
		return Promise.resolve(1)
	}

	close(): Promise<void> {
		return Promise.resolve()
	}

	/** Records sent to committed transactions (exactly-once) or acked (at-least-once). */
	committedRecords(topic: string): SentRecord[] {
		return this.sent.filter(
			r => r.topic === topic && (r.transaction === undefined || r.transaction.state === 'committed')
		)
	}
}

export class FakeTransaction implements TransactionPort {
	state: 'open' | 'committed' | 'aborted' = 'open'
	readonly sent: SentRecord[] = []
	offsets: OffsetAndMetadata[] = []
	group: ConsumerGroupMetadata | undefined
	/** Every call in order: 'send', 'sendOffsets', 'commit', 'abort'. */
	readonly calls: string[] = []
	failSendOffsets: Error | undefined
	failCommit: Error | undefined

	constructor(private readonly producer: FakeProducer) {}

	send(topic: string, message: ProducedMessage): Promise<SendResult> {
		this.calls.push('send')
		const record = { topic, message, transaction: this }
		this.sent.push(record)
		this.producer.sent.push(record)
		return this.producer.answer(topic, message)
	}

	async sendOffsets(params: {
		consumerGroupMetadata: ConsumerGroupMetadata
		offsets: readonly OffsetAndMetadata[]
	}): Promise<void> {
		this.calls.push('sendOffsets')
		if (this.failSendOffsets) {
			throw this.failSendOffsets
		}
		this.group = params.consumerGroupMetadata
		this.offsets = [...params.offsets]
	}

	async commit(): Promise<void> {
		this.calls.push('commit')
		if (this.failCommit) {
			throw this.failCommit
		}
		this.state = 'committed'
	}

	async abort(): Promise<void> {
		this.calls.push('abort')
		this.state = 'aborted'
	}
}

/** The parts of the group consumer that tasks and commits use. */
export class FakeConsumer {
	rebalancePending = false
	group: ConsumerGroupMetadata | undefined = { groupId: APP, generationId: 1, memberId: 'member' }
	readonly commits: OffsetAndMetadata[][] = []
	/** Next delivery offset per tp. */
	readonly positions = new Map<string, bigint>()
	/** Lag per tp (readiness); default 0. */
	readonly lags = new Map<string, bigint | undefined>()
	/** Replace to fail or delay commits. */
	answerCommit: (offsets: readonly OffsetAndMetadata[], signal: AbortSignal) => Promise<void> = offsets => {
		this.commits.push([...offsets])
		return Promise.resolve()
	}

	commit(offsets: readonly OffsetAndMetadata[], options: { signal: AbortSignal }): Promise<void> {
		return this.answerCommit(offsets, options.signal)
	}

	nextOffset(tp: TopicPartition): bigint | undefined {
		return this.positions.get(tpKey(tp))
	}

	groupMetadata(): ConsumerGroupMetadata | undefined {
		return this.group
	}

	readonly lag = (tp: TopicPartition): bigint | undefined => {
		const key = tpKey(tp)
		return this.lags.has(key) ? this.lags.get(key) : 0n
	}
}

// ----------------------------------------------------------------------------------------------------------
// State
// ----------------------------------------------------------------------------------------------------------

/** What a task asked of its TaskStateManager, recorded by wrapping the instance's methods. */
export class StateProbe {
	registered = false
	stackClosed = false
	released: { wipe?: boolean } | undefined
	bufferApplies = 0
	bufferDiscards = 0
	readonly checkpoints: { positions: ReadonlyMap<string, bigint>; eos: boolean }[] = []
	/** Replaces the release when set (reject to make it fail). */
	release: ((options?: { wipe?: boolean }) => Promise<void>) | undefined

	constructor(
		readonly manager: TaskStateManager,
		label: string,
		events: string[]
	) {
		const original = {
			register: manager.register.bind(manager),
			commitBuffers: manager.commitBuffers.bind(manager),
			discardBuffers: manager.discardBuffers.bind(manager),
			checkpoint: manager.checkpoint.bind(manager),
			closeStack: manager.closeStack.bind(manager),
			release: manager.release.bind(manager),
		}
		manager.register = async () => {
			await original.register()
			this.registered = true
		}
		manager.commitBuffers = () => {
			this.bufferApplies++
			events.push(`apply ${label}`)
			return original.commitBuffers()
		}
		manager.discardBuffers = () => {
			this.bufferDiscards++
			original.discardBuffers()
		}
		manager.checkpoint = (positions, eos) => {
			this.checkpoints.push({ positions, eos })
			events.push(`checkpoint ${label}`)
			return original.checkpoint(positions, eos)
		}
		manager.closeStack = () => {
			this.stackClosed = true
			original.closeStack()
		}
		manager.release = options => {
			this.released = options ?? {}
			return (this.release ?? original.release)(options)
		}
	}
}

/** An unlogged in-memory key-value store of strings. */
export function kvStore(name: string, extra: Partial<StoreBuilder> = {}): StoreBuilder {
	const builder = keyValueStoreBuilder(
		{ applicationId: APP },
		{
			name,
			keyCodec: string(),
			valueCodec: string(),
			logging: false,
		}
	)
	return { ...builder, ...extra } as StoreBuilder
}

/** A durable checkpoint store over a Map; its flushes are logged as 'checkpoints.flush'. */
function checkpointStore(events: string[]): ChangelogCheckpointStore & { readonly values: Map<string, bigint> } {
	const values = new Map<string, bigint>()
	return {
		values,
		get: (topic, partition) => Promise.resolve(values.get(`${topic}:${partition}`)),
		set: (topic, partition, offset) => {
			values.set(`${topic}:${partition}`, offset)
			return Promise.resolve()
		},
		flush: () => {
			events.push('checkpoints.flush')
			return Promise.resolve()
		},
	}
}

// ----------------------------------------------------------------------------------------------------------
// Harness
// ----------------------------------------------------------------------------------------------------------

export interface TaskHarnessOptions {
	/** Registers the topology. */
	readonly build: (builder: InternalTopologyBuilder) => void
	readonly eos?: boolean
	/** Partition count of every source topic; default 1. */
	readonly partitions?: number
	readonly maxTaskIdleMs?: number
	readonly bufferedRecordsPerPartition?: number
	readonly handlers?: Partial<ResolvedHandlers>
	readonly extractor?: TimestampExtractor
	readonly taskTimeoutMs?: number
}

export interface TestRecord {
	readonly key?: string | null
	readonly value?: string | null
	readonly timestamp?: number
	readonly headers?: Record<string, Buffer>
	/** Default: the next offset of the partition. */
	readonly offset?: bigint
}

const fail = (): HandlerResponse => 'fail'

export class TaskHarness {
	readonly topology: BuiltTopology
	readonly ids = new TaskIdRegistry()
	readonly clock = new ManualClock(1_000)
	readonly logger = new RecordingLogger()
	readonly producer = new FakeProducer()
	readonly consumer = new FakeConsumer()
	readonly tasks: StreamTask[] = []
	readonly probes = new Map<StreamTask, StateProbe>()
	/** The read-only views of the global stores, by name (what `TaskDeps.globalStore` answers). */
	readonly globalStores = new Map<string, StateStore>()
	readonly handlers: ResolvedHandlers
	readonly hardStop = new AbortController()
	readonly commits: CommitCoordinator
	collector: RecordCollector
	/** The run's failed flag. */
	failed = false
	/** Buffer applies, checkpoints, checkpoint flushes and commit notes, in order (tests append their own). */
	readonly events: string[] = []
	readonly checkpointStore = checkpointStore(this.events)
	readonly checkpoints = new Checkpoints(this.checkpointStore, error => this.logger.error(error.message))
	readonly stateDirectory = new StateDirectory(APP, inMemory(), this.checkpoints, this.logger)
	assignment: ConsumerGroupMetadata | undefined = { groupId: APP, generationId: 1, memberId: 'member' }
	commitRequests = 0
	private readonly counts: ReadonlyMap<string, number>

	constructor(readonly options: TaskHarnessOptions) {
		const builder = new InternalTopologyBuilder(APP)
		options.build(builder)
		this.topology = builder.build()
		const partitions = options.partitions ?? 1
		this.counts = new Map(
			this.topology.subtopologies.flatMap(s => [...s.sourceTopics].map(topic => [topic, partitions] as const))
		)
		this.handlers = {
			deserialization: fail,
			processing: fail,
			production: fail,
			uncaught: () => 'shutdown',
			...options.handlers,
		}
		this.collector = this.newCollector()
		this.commits = new CommitCoordinator({
			consumer: this.consumer,
			collector: () => this.collector,
			tasks: () => this.tasks,
			assignmentMetadata: () => this.assignment,
			eos: this.eos,
			checkpoints: this.checkpoints,
			onCommitted: { noteCommitted: () => void this.events.push('noteCommitted') },
			failed: () => this.failed,
			hardStop: this.hardStop.signal,
			config: { taskTimeoutMs: options.taskTimeoutMs ?? 300_000 },
			clock: this.clock,
			logger: this.logger,
		})
	}

	get eos(): boolean {
		return this.options.eos ?? false
	}

	newCollector(): RecordCollector {
		return new RecordCollector(this.producer, {
			eos: this.eos,
			handlers: this.handlers,
			counts: { get: () => 1, load: () => Promise.resolve(1) },
			logger: this.logger,
		})
	}

	/** A new task object for `<subtopology>_<partition>` (CREATED). */
	task(subtopology = 0, partition = 0): StreamTask {
		const sub = this.topology.subtopologies[subtopology]!
		const id = this.ids.get(subtopology, partition)
		const task = new StreamTask(
			{
				subtopology: sub,
				config: {
					applicationId: APP,
					eos: this.eos,
					extractor: () => this.options.extractor ?? TimestampExtractors.failOnInvalid,
					maxTaskIdleMs: this.options.maxTaskIdleMs ?? 0,
					bufferedRecordsPerPartition: this.options.bufferedRecordsPerPartition ?? 1000,
					emitIntervalMs: 0,
					handlers: this.handlers,
				},
				stateDirectory: this.stateDirectory,
				checkpoints: this.checkpoints,
				globalStore: name => {
					const store = this.globalStores.get(name)
					if (!store) {
						throw new Error(`the harness has no global store '${name}'`)
					}
					return store
				},
				outputFor: owner => this.collector.forTask(owner),
				clock: this.clock,
				logger: this.logger,
				onCommitRequested: () => void this.commitRequests++,
			},
			id,
			this.topology.inputPartitions(sub, partition, this.counts)
		)
		const manager = (task as unknown as { stateManager: TaskStateManager }).stateManager
		this.probes.set(task, new StateProbe(manager, id.key, this.events))
		this.tasks.push(task)
		return task
	}

	state(task: StreamTask): StateProbe {
		return this.probes.get(task)!
	}

	/** CREATED -> RESTORING -> RUNNING with `starts` (under exactly-once a transaction is opened first). */
	async start(task: StreamTask, starts: ReadonlyMap<string, PartitionStart> = new Map()): Promise<StreamTask> {
		await task.initialize()
		task.setStarts(starts)
		await this.collector.ensureTransaction(this.hardStop.signal)
		await task.completeRestoration(new Map())
		return task
	}

	/** A new task started from the commits recorded so far (the assignment payload after a restart). */
	async restart(subtopology = 0, partition = 0): Promise<StreamTask> {
		const task = this.task(subtopology, partition)
		const starts = new Map<string, PartitionStart>()
		for (const offset of this.committedOffsets()) {
			starts.set(tpKey(offset), {
				committed: { offset: offset.offset, metadata: offset.metadata },
				resolved: offset.offset,
			})
		}
		return this.start(task, starts)
	}

	/** The last committed offset per tp, in commit order: consumer commits, or offsets of committed transactions. */
	committedOffsets(): OffsetAndMetadata[] {
		const batches = this.eos
			? this.producer.transactions.filter(t => t.state === 'committed').map(t => t.offsets)
			: this.consumer.commits
		const last = new Map<string, OffsetAndMetadata>()
		for (const offset of batches.flat()) {
			last.set(tpKey(offset), offset)
		}
		return [...last.values()]
	}

	/** Delivers records to `task` (dense offsets per partition by default) and moves the consumer's position. */
	enqueue(task: StreamTask, topic: string, records: readonly TestRecord[], partition = task.partition): number {
		const tp = { topic, partition }
		const key = tpKey(tp)
		let next = this.consumer.positions.get(key) ?? 0n
		const messages = records.map(record => {
			const offset = record.offset ?? next
			next = offset + 1n
			return message(topic, partition, offset, record)
		})
		this.consumer.positions.set(key, next)
		return task.addRecords(tp, messages)
	}

	/** Processes every record the task can process now (in a transaction under exactly-once, as the scheduler does). */
	async process(task: StreamTask): Promise<number> {
		await this.collector.ensureTransaction(this.hardStop.signal)
		let processed = 0
		while (task.isProcessable(this.clock.now(), this.consumer.lag)) {
			if (!(await task.processOne(this.clock.now()))) {
				break
			}
			processed++
		}
		return processed
	}

	commit(reason: CommitReason = 'interval'): Promise<CommitOutcome> {
		return this.commits.commit(reason, this.hardStop.signal)
	}

	/** Records sent to `topic`, decoded as strings. */
	output(topic: string): { key: string | null; value: string | null; timestamp: number | undefined }[] {
		return this.producer.sent
			.filter(r => r.topic === topic)
			.map(r => ({
				key: r.message.key?.toString() ?? null,
				value: r.message.value?.toString() ?? null,
				timestamp: r.message.timestamp?.getTime(),
			}))
	}
}

export function message(
	topic: string,
	partition: number,
	offset: bigint,
	record: TestRecord = {}
): Message<Buffer, Buffer> {
	return {
		topic,
		partition,
		offset,
		timestamp: BigInt(record.timestamp ?? 0),
		key: record.key === undefined || record.key === null ? null : Buffer.from(record.key),
		value: (record.value === undefined || record.value === null ? null : Buffer.from(record.value)) as Buffer,
		headers: record.headers ?? {},
	}
}

// ----------------------------------------------------------------------------------------------------------
// Topologies
// ----------------------------------------------------------------------------------------------------------

/** source(topics, string codecs) -> processor -> sink(`output`). */
export function pipe(
	builder: InternalTopologyBuilder,
	options: {
		topics?: readonly string[]
		processor: ProcessorSupplier<string, string, string, string>
		stores?: readonly StoreBuilder[]
		output?: string
	}
): void {
	builder.addSource({
		name: 'source',
		topics: options.topics ?? ['input'],
		keyCodec: string(),
		valueCodec: string(),
		internal: false,
	})
	for (const store of options.stores ?? []) {
		builder.addStateStore(store)
	}
	builder.addProcessor({
		name: 'processor',
		parents: ['source'],
		stores: (options.stores ?? []).map(s => s.name),
		supplier: options.processor as ProcessorSupplier<unknown, unknown, unknown, unknown>,
	})
	builder.addSink({
		name: 'sink',
		parents: ['processor'],
		topic: options.output ?? 'output',
		keyCodec: string(),
		valueCodec: string(),
		internal: false,
	})
}

/** Forwards every record unchanged. */
export const passThrough: ProcessorSupplier<string, string, string, string> = () => {
	let context: ProcessorContext<string, string>
	return {
		init: ctx => {
			context = ctx
		},
		process: record => context.forward(record),
	}
}
