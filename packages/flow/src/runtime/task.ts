/**
 * A stream task (KS `StreamTask`): subtopology x partition. It owns its input queues, its processor graph while
 * RUNNING, its stores, stream time, consumed and committed offsets and its processor metadata, and follows Kafka
 * Streams' lifecycle CREATED -> RESTORING -> RUNNING -> SUSPENDED -> CLOSED (4.4).
 */
import type { Logger, Message, TopicPartition } from '@kafkats/client'
import { IllegalStateError } from '@/api/errors.js'
import type { DroppedReason, MaybePromise, TaskId } from '@/processor/api.js'
import { decodeCommitMetadata, encodeCommitMetadata, ProcessorMetadataImpl } from '@/processor/metadata.js'
import type { TaskOutput } from '@/processor/output.js'
import type { TimestampExtractor } from '@/processor/timestamps.js'
import type { ResolvedHandlers } from '@/runtime/config.js'
import { errorMessage } from '@/runtime/errors.js'
import type { Clock, CommittedOffset, OffsetAndMetadata } from '@/runtime/io.js'
import { PartitionGroup } from '@/runtime/partition-group.js'
import { isSkipMarker, RecordQueue } from '@/runtime/record-queue.js'
import { tpKey } from '@/runtime/task-id.js'
import { TaskTopology } from '@/runtime/task-topology.js'
import type { StateStore } from '@/state.js'
import type { Checkpoints } from '@/state/checkpoints.js'
import type { StateDirectory } from '@/state/state-directory.js'
import { TaskStateManager, type RegisteredStore } from '@/state/state-manager.js'
import type { Subtopology } from '@/topology/built-topology.js'

export type TaskState = 'CREATED' | 'RESTORING' | 'RUNNING' | 'SUSPENDED' | 'CLOSED'

/** What the assignment says about one input partition. */
export interface PartitionStart {
	readonly committed: CommittedOffset | null
	readonly resolved: bigint
}

export interface TaskConfig {
	readonly applicationId: string
	readonly eos: boolean
	readonly extractor: (topic: string) => TimestampExtractor
	readonly maxTaskIdleMs: number
	readonly bufferedRecordsPerPartition: number
	readonly emitIntervalMs: number
	readonly handlers: ResolvedHandlers
}

export interface TaskDeps {
	readonly subtopology: Subtopology
	readonly config: TaskConfig
	/** The task builds its own TaskStateManager from these. */
	readonly stateDirectory: StateDirectory
	readonly checkpoints: Checkpoints
	/** The read-only view of a global store (writes throw), which every processor may read without being connected. */
	readonly globalStore: (name: string) => StateStore
	/** Called once by the constructor: the TaskOutput bound to this task incarnation. */
	readonly outputFor: (task: StreamTask) => TaskOutput
	readonly clock: Clock
	readonly logger: Logger
	/** The scheduler's commit request. */
	readonly onCommitRequested: () => void
}

type Lag = (tp: TopicPartition) => bigint | undefined

export class StreamTask {
	readonly partition: number
	readonly subtopology: Subtopology
	readonly inputPartitions: readonly TopicPartition[]
	private taskState: TaskState = 'CREATED'
	private closedAs: 'clean' | 'dirty' | undefined
	private isLost = false
	private isInitializing = false
	private readonly config: TaskConfig
	private readonly logger: Logger
	private readonly queues: readonly RecordQueue[]
	private readonly inputKeys: ReadonlySet<string>
	private readonly group: PartitionGroup
	private readonly metadata = new ProcessorMetadataImpl()
	private readonly output: TaskOutput
	private readonly stateManager: TaskStateManager
	private topology: TaskTopology | undefined
	/** Records processed or punctuations run since the last commit. */
	private commitNeeded = false
	private userCommitRequested = false
	private drained = false
	/** Offset of the last record processed per input partition since the last commit. */
	private readonly consumedOffsets = new Map<RecordQueue, bigint>()
	private startsByKey: ReadonlyMap<string, PartitionStart> = new Map()
	private readonly lastCommittedOffsets = new Map<string, bigint>()
	/** The last successful commit per input partition (offset and metadata), for a revive in place. */
	private readonly lastCommits = new Map<string, OffsetAndMetadata>()
	/** Partitions the TaskManager paused because their queue grew beyond `bufferedRecordsPerPartition`. */
	private readonly pausedForSize = new Set<RecordQueue>()
	private toResume: TopicPartition[] = []
	private metadataWarned = false

	constructor(
		private readonly deps: TaskDeps,
		readonly id: TaskId,
		inputPartitions: readonly TopicPartition[]
	) {
		this.partition = id.partition
		this.subtopology = deps.subtopology
		this.inputPartitions = inputPartitions
		this.config = deps.config
		this.logger = deps.logger.child({ task: id.key })
		const { config } = this
		this.queues = inputPartitions.map(
			tp =>
				new RecordQueue(
					tp,
					this.subtopology.sourcesByTopic.get(tp.topic) ?? [],
					config.extractor(tp.topic),
					config.handlers.deserialization,
					(reason, source, detail) => this.recordDropped(reason, source, detail),
					id
				)
		)
		this.inputKeys = new Set(inputPartitions.map(tpKey))
		this.group = new PartitionGroup(this.queues, config.maxTaskIdleMs)
		this.output = deps.outputFor(this)
		this.stateManager = new TaskStateManager(id, id.partition, this.subtopology.stores, {
			stateDirectory: deps.stateDirectory,
			output: this.output,
			eos: config.eos,
			checkpoints: deps.checkpoints,
			metadata: this.metadata,
		})
	}

	get state(): TaskState {
		return this.taskState
	}

	get lost(): boolean {
		return this.isLost
	}

	/** Its processors' init() is running and no close took the task meanwhile: the task's output accepts sends. */
	get initializing(): boolean {
		return this.isInitializing && this.taskState === 'RESTORING'
	}

	/** Max timestamp polled and seeded partition times, -1 unknown. */
	get streamTime(): number {
		return this.group.streamTime
	}

	/**
	 * The input record being processed (what a send failure is reported with); null in init(), punctuators and between
	 * records.
	 */
	get input(): Message<Buffer | null, Buffer> | null {
		return this.topology?.context.input ?? null
	}

	// ---------------------------------------------------------------------------------------------------------
	// Lifecycle
	// ---------------------------------------------------------------------------------------------------------

	/** CREATED -> RESTORING: leases the stores and builds their stacks (queues exist since construction). */
	async initialize(): Promise<void> {
		this.expectState('initialize', 'CREATED')
		await this.stateManager.register()
		this.taskState = 'RESTORING'
	}

	/** The assignment payload (created or resumed task): the committed offsets become the last committed ones. */
	setStarts(starts: ReadonlyMap<string, PartitionStart>): void {
		this.startsByKey = starts
		this.lastCommittedOffsets.clear()
		this.lastCommits.clear()
		for (const [key, start] of starts) {
			if (start.committed) {
				this.lastCommittedOffsets.set(key, start.committed.offset)
			}
		}
	}

	get starts(): ReadonlyMap<string, PartitionStart> {
		return this.startsByKey
	}

	/**
	 * Starts of the next incarnation after a dirty close in place: per input partition the last successful commit of
	 * this task if any (else the assignment's), resolved at the last committed offset (else the assignment's).
	 */
	restartStarts(): ReadonlyMap<string, PartitionStart> {
		const starts = new Map<string, PartitionStart>()
		for (const queue of this.queues) {
			const key = tpKey(queue.tp)
			const start = this.startsByKey.get(key)
			const resolved = this.lastCommittedOffsets.get(key) ?? start?.resolved
			if (resolved === undefined) {
				continue
			}
			const commit = this.lastCommits.get(key)
			const committed = commit ? { offset: commit.offset, metadata: commit.metadata } : (start?.committed ?? null)
			starts.set(key, { committed, resolved })
		}
		return starts
	}

	get stores(): readonly RegisteredStore[] {
		return this.stateManager.stores
	}

	/** Top of the store's stack. */
	store(name: string): StateStore {
		return this.stateManager.store(name)
	}

	commitBuffers(): Promise<void> {
		return this.stateManager.commitBuffers()
	}

	discardBuffers(): void {
		this.stateManager.discardBuffers()
	}

	/** Settled start positions of partitions without a commit become their last committed offsets. */
	setSettledPositions(positions: ReadonlyMap<string, bigint>): void {
		for (const [key, offset] of positions) {
			if (!this.lastCommittedOffsets.has(key)) {
				this.lastCommittedOffsets.set(key, offset)
			}
		}
	}

	/**
	 * RESTORING -> RUNNING: seeds the clocks and the processor metadata from the commits, then creates and
	 * initializes the processor graph (whose sends and store writes may need a commit). A close that did not wait for
	 * init() (a close past its deadline) wins: the new processors are closed, and their failure counts for nothing, like
	 * that of a slice that outlived a close.
	 */
	async completeRestoration(restoredStoreTimes: ReadonlyMap<string, number>): Promise<void> {
		this.expectState('completeRestoration', 'RESTORING')
		this.seed(restoredStoreTimes)
		this.commitNeeded = false
		const topology = TaskTopology.create(this, {
			stores: this.stateManager,
			globalStore: this.deps.globalStore,
			output: this.output,
			metadata: this.metadata,
			clock: this.deps.clock,
			handlers: this.config.handlers,
			onCommitRequested: () => this.requestCommit(),
			config: this.config,
			logger: this.logger,
		})
		this.isInitializing = true
		try {
			await topology.init()
		} catch (error) {
			await this.discardProcessors(topology)
			if (this.taskState === 'RESTORING') {
				throw error
			}
			this.logger.warn('ignoring a failure of processors closed meanwhile', { error: errorMessage(error) })
			return
		} finally {
			this.isInitializing = false
		}
		if (this.taskState !== 'RESTORING') {
			await this.discardProcessors(topology)
			return
		}
		if (this.stateManager.dirty) {
			this.commitNeeded = true
		}
		this.topology = topology
		this.taskState = 'RUNNING'
	}

	/**
	 * CREATED | RESTORING | RUNNING -> SUSPENDED. A RUNNING task closes its processor graph (late forwards and writes
	 * of this incarnation throw) and drops its queued records, which are fetched again from the committed offsets;
	 * the store stacks stay open for a resume.
	 */
	async suspend(): Promise<void> {
		this.expectState('suspend', 'CREATED', 'RESTORING', 'RUNNING')
		const { topology } = this
		this.topology = undefined
		this.taskState = 'SUSPENDED'
		this.group.clear()
		this.consumedOffsets.clear()
		this.pausedForSize.clear()
		this.toResume = []
		await topology?.close()
	}

	/** SUSPENDED -> RESTORING; the caller sets the new starts. */
	resume(): void {
		this.expectState('resume', 'SUSPENDED')
		this.taskState = 'RESTORING'
	}

	/**
	 * Clean (from SUSPENDED): closes the store stacks and releases the stores; uncommitted work or a failure closes
	 * dirty instead (KS). Dirty (any state): suspends if needed, drops buffered writes and releases the stores without
	 * a checkpoint (`wipe`: invalidated and cleared); never throws. Returns the mode used.
	 */
	async close(mode: 'clean' | 'dirty', options: { wipe?: boolean } = {}): Promise<'clean' | 'dirty'> {
		if (this.closedAs !== undefined) {
			return this.closedAs
		}
		if (mode === 'dirty') {
			return this.closeDirty(options)
		}
		this.expectState('close clean', 'SUSPENDED')
		if (this.commitNeeded) {
			this.logger.warn('closing dirty instead of clean: the task has uncommitted work')
			return this.closeDirty(options)
		}
		try {
			this.stateManager.closeStack()
			await this.stateManager.release(options)
		} catch (error) {
			this.logger.warn('closing clean failed, closing dirty', { error: errorMessage(error) })
			return this.closeDirty(options)
		}
		return this.closed('clean')
	}

	/** Synchronous, from the lost hook: no further record, punctuation or send of this task. */
	markLost(): void {
		this.isLost = true
	}

	// ---------------------------------------------------------------------------------------------------------
	// Intake and processing
	// ---------------------------------------------------------------------------------------------------------

	/** Appends without decoding; returns the partition's queue size. Records of a suspended or closed task are dropped. */
	addRecords(tp: TopicPartition, messages: readonly Message<Buffer, Buffer>[]): number {
		if (this.taskState === 'SUSPENDED' || this.taskState === 'CLOSED') {
			this.logger.debug('dropped records delivered to a task that is not active', { state: this.taskState })
			return 0
		}
		const queue = this.group.queue(tp)
		const size = queue.addRawRecords(messages)
		if (size > this.config.bufferedRecordsPerPartition) {
			this.pausedForSize.add(queue)
		}
		return size
	}

	/** RUNNING, not lost, and ready (KIP-695); evaluated once per slice. */
	isProcessable(now: number, lag: Lag): boolean {
		return this.active && this.group.readyToProcess(now, lag)
	}

	/** RUNNING, not lost, and records are ready or a punctuation is due: the scheduler's work-set test. */
	hasWork(now: number, lag: Lag): boolean {
		return (
			this.active &&
			(this.group.readyToProcess(now, lag) ||
				this.streamTimePunctuationDue() ||
				this.wallClockPunctuationDue(now))
		)
	}

	/**
	 * Processes one record. false: none ready, or the task left RUNNING while the record's subtree ran (a slice that
	 * outlived a close: its record is not counted, and neither is its failure). A promise only when the record's
	 * subtree went async.
	 */
	processOne(now: number): MaybePromise<boolean> {
		this.drained = false
		const { topology } = this
		if (!this.active || topology === undefined) {
			return false
		}
		const record = this.group.nextRecord()
		if (record === null) {
			return false
		}
		if (isSkipMarker(record)) {
			return this.processed(record.queue, record.offset)
		}
		const result = topology.process(record, now)
		if (result === undefined) {
			return this.processed(record.queue, record.offset)
		}
		return result.then(
			() => this.topology === topology && this.processed(record.queue, record.offset),
			(error: unknown) => this.orphaned(topology, error)
		)
	}

	/** A queue became empty during the last processOne(): readiness must be evaluated again. */
	get queueDrained(): boolean {
		return this.drained
	}

	streamTimePunctuationDue(): boolean {
		const time = this.group.streamTime
		return this.active && time >= 0 && this.topology!.streamPunctuation.canPunctuate(time)
	}

	wallClockPunctuationDue(now: number): boolean {
		return this.active && this.topology!.wallClockPunctuation.canPunctuate(now)
	}

	/** Runs the stream-time punctuations due (also for a task without records); whether any ran. */
	async maybePunctuateStreamTime(): Promise<boolean> {
		if (!this.streamTimePunctuationDue()) {
			return false
		}
		const topology = this.topology!
		return this.punctuated(topology, topology.punctuateStreamTime(this.group.streamTime))
	}

	/** Runs the wall-clock punctuations due at `now`; whether any ran. */
	async maybePunctuateSystemTime(now: number): Promise<boolean> {
		if (!this.wallClockPunctuationDue(now)) {
			return false
		}
		const topology = this.topology!
		return this.punctuated(topology, topology.punctuateWallClock(now))
	}

	nextWallClockDue(): number | undefined {
		return this.active ? this.topology!.wallClockPunctuation.nextDue() : undefined
	}

	/** When an idling partition stops being waited for (KIP-695), if the task is waiting. */
	nextIdleDeadline(): number | undefined {
		return this.active ? this.group.nextIdleDeadline() : undefined
	}

	/** Partitions paused for size whose queue dropped to the limit (KS backpressure); taken once. */
	partitionsToResume(): TopicPartition[] {
		const partitions = this.toResume
		this.toResume = []
		return this.taskState === 'RUNNING' ? partitions : []
	}

	// ---------------------------------------------------------------------------------------------------------
	// Commit
	// ---------------------------------------------------------------------------------------------------------

	/** `context.commit()`: a commit of all tasks at the next barrier. */
	requestCommit(): void {
		this.userCommitRequested = true
		this.deps.onCommitRequested()
	}

	get commitRequested(): boolean {
		return this.userCommitRequested
	}

	/** The task's output accepted a send. */
	markCommitNeeded(): void {
		this.commitNeeded = true
	}

	/** Work to commit, or an empty partition whose fetch position moved past the last commit (markers). */
	needsCommit(next: Lag): boolean {
		if (this.commitNeeded || this.metadata.needsCommit) {
			return true
		}
		return this.queues.some(queue => this.movedPastCommit(queue, next))
	}

	/**
	 * Offsets to commit (KS `committableOffsetsAndMetadata`, KIP-1094): every input partition when the processor
	 * metadata changed, else the consumed ones and those whose fetch position moved past the last commit. The offset
	 * is the head of the queue, else the next offset the consumer delivers; never past a queued record.
	 */
	committableOffsets(next: Lag): OffsetAndMetadata[] {
		if (this.taskState !== 'RUNNING') {
			return []
		}
		const all = this.metadata.needsCommit
		const offsets: OffsetAndMetadata[] = []
		for (const queue of this.queues) {
			if (!all && !this.consumedOffsets.has(queue) && !this.movedPastCommit(queue, next)) {
				continue
			}
			const offset = queue.headOffset ?? next(queue.tp)
			if (offset === undefined) {
				continue
			}
			const { topic, partition } = queue.tp
			offsets.push({
				topic,
				partition,
				offset,
				metadata: encodeCommitMetadata(queue.partitionTime, this.metadata),
			})
		}
		return offsets
	}

	/**
	 * After a successful commit: clears the commit flags, records the committed offsets of this task's partitions,
	 * then checkpoints the changelog positions written since the last commit.
	 */
	async postCommit(positions: ReadonlyMap<string, bigint>, committed: readonly OffsetAndMetadata[]): Promise<void> {
		this.commitNeeded = false
		this.userCommitRequested = false
		this.consumedOffsets.clear()
		this.metadata.clearNeedsCommit()
		for (const offset of committed) {
			const key = tpKey(offset)
			if (this.inputKeys.has(key)) {
				this.lastCommittedOffsets.set(key, offset.offset)
				this.lastCommits.set(key, offset)
			}
		}
		await this.stateManager.checkpoint(positions, this.config.eos)
	}

	/** The offset a partition restarts from after a dirty close in place. */
	lastCommitted(tp: TopicPartition): bigint | undefined {
		return this.lastCommittedOffsets.get(tpKey(tp))
	}

	// ---------------------------------------------------------------------------------------------------------

	private get active(): boolean {
		return this.taskState === 'RUNNING' && !this.isLost
	}

	/** Kafka Streams' transitions; any other is a bug. A CLOSED task is never revived: a new object takes its place. */
	private expectState(operation: string, ...allowed: TaskState[]): void {
		if (!allowed.includes(this.taskState)) {
			throw new IllegalStateError(`task ${this.id.key} cannot ${operation} while ${this.taskState}`, {
				taskId: this.id,
			})
		}
	}

	/** Replaces the processor metadata by the max-merge of the committed one; seeds partition times and store clocks. */
	private seed(restoredStoreTimes: ReadonlyMap<string, number>): void {
		const committed: ReadonlyMap<string, number>[] = []
		for (const queue of this.queues) {
			const metadata = this.startsByKey.get(tpKey(queue.tp))?.committed?.metadata
			if (metadata === undefined || metadata === null) {
				continue
			}
			const decoded = decodeCommitMetadata(metadata)
			if (decoded.invalid !== undefined && !this.metadataWarned) {
				this.metadataWarned = true
				this.logger.warn('ignoring commit metadata that cannot be decoded', {
					topic: queue.tp.topic,
					partition: queue.tp.partition,
					reason: decoded.invalid,
				})
			}
			committed.push(decoded.metadata)
			if (decoded.partitionTime >= 0) {
				this.group.setPartitionTime(queue.tp, decoded.partitionTime)
			}
		}
		this.metadata.replaceAll(committed, this.subtopology.metadataKeys)
		for (const { builder, clock } of this.stateManager.stores) {
			if (clock) {
				const observed = this.metadata.get(`${builder.name}.observed`) ?? -1
				clock.seed(Math.max(observed, restoredStoreTimes.get(builder.name) ?? -1))
			}
		}
	}

	private movedPastCommit(queue: RecordQueue, next: Lag): boolean {
		if (queue.size > 0) {
			return false
		}
		const last = this.lastCommittedOffsets.get(tpKey(queue.tp))
		const position = last === undefined ? undefined : next(queue.tp)
		return position !== undefined && position > last!
	}

	/** The record (or the skipped ones) settled: its offset is consumed and its record context cleared. */
	private processed(queue: RecordQueue, offset: bigint): true {
		this.topology!.context.input = null
		this.consumedOffsets.set(queue, offset)
		this.commitNeeded = true
		if (queue.size === 0) {
			this.drained = true
		}
		if (this.pausedForSize.has(queue) && queue.size <= this.config.bufferedRecordsPerPartition) {
			this.pausedForSize.delete(queue)
			this.toResume.push(queue.tp)
		}
		return true
	}

	/** Punctuations of an incarnation closed meanwhile (a slice that outlived a close) need no commit. */
	private async punctuated(topology: TaskTopology, punctuation: Promise<boolean>): Promise<boolean> {
		let ran: boolean
		try {
			ran = await punctuation
		} catch (error) {
			return this.orphaned(topology, error)
		}
		if (ran && this.topology === topology) {
			this.commitNeeded = true
		}
		return ran
	}

	/**
	 * A failure of the running incarnation is thrown. One of an incarnation closed meanwhile is void with its work
	 * (typically a write or forward the close forbade): its record is processed again after the last commit, where a
	 * real failure shows again.
	 */
	private orphaned(topology: TaskTopology, error: unknown): false {
		if (this.topology === topology) {
			throw error
		}
		this.logger.warn('ignoring a failure of processors closed meanwhile', { error: errorMessage(error) })
		return false
	}

	/** Closes processors that never ran: their init() failed, or a close took the task meanwhile. */
	private async discardProcessors(topology: TaskTopology): Promise<void> {
		await topology.close().catch((error: unknown) => {
			this.logger.warn('closing processors that never ran failed', { error: errorMessage(error) })
		})
	}

	private recordDropped(reason: DroppedReason, source: string, detail: Readonly<Record<string, unknown>>): void {
		if (this.topology) {
			this.topology.context.recordDropped(reason, source, detail)
		} else {
			this.logger.warn('dropped a record', { reason, source, ...detail })
		}
	}

	private async closeDirty(options: { wipe?: boolean }): Promise<'dirty'> {
		const swallow = async (step: string, run: () => unknown) => {
			try {
				await run()
			} catch (error) {
				this.logger.warn(`closing dirty: ${step} failed`, { error: errorMessage(error) })
			}
		}
		if (this.taskState !== 'SUSPENDED') {
			await swallow('closing the processors', () => this.suspend())
		}
		await swallow('closing the store stacks', () => this.stateManager.closeStack())
		await swallow('dropping buffered writes', () => this.stateManager.discardBuffers())
		await swallow('releasing the stores', () => this.stateManager.release(options))
		return this.closed('dirty')
	}

	private closed<M extends 'clean' | 'dirty'>(mode: M): M {
		this.taskState = 'CLOSED'
		this.closedAs = mode
		return mode
	}
}
