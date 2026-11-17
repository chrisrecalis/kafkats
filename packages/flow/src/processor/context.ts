/**
 * Processor contexts. `TaskContext` is what every processor context of one task incarnation shares (input record,
 * slice time, punctuation queues, closed state); `ProcessorContextImpl` is the context of one processor node, with
 * the forward guards that keep every forward of a node inside its process() or punctuator, awaited.
 */
import type { Logger, Message } from '@kafkats/client'
import { IllegalStateError, StreamsError, TopologyError } from '@/api/errors.js'
import type { StoreDefinition } from '@/api/stores.js'
import type {
	Cancellable,
	DroppedReason,
	ForwardResult,
	InternalProcessorContext,
	ProcessorMetadata,
	PunctuationType,
	Punctuator,
	RecordContext,
	RecordMetadata,
	TaskId,
} from '@/processor/api.js'
import type { ProcessorNode } from '@/processor/node.js'
import type { TaskOutput } from '@/processor/output.js'
import { PunctuationQueue } from '@/processor/punctuation.js'
import type { RecordHeaders, StreamRecord } from '@/processor/record.js'
import type { Clock } from '@/runtime/io.js'
import type { StateStore } from '@/state.js'

/** A dropped record is logged at most once per this interval, per reason and task. */
const DROP_LOG_INTERVAL_MS = 1000

/**
 * Misuse of `forward()` (outside process() or a punctuator, while a previous forward is pending, or a process() that
 * settled with a pending forward). Runtime control: no exception handler sees it.
 */
export class ForwardGuardError extends StreamsError {}

export interface TaskContextOptions {
	readonly applicationId: string
	readonly taskId: TaskId
	readonly config: Readonly<{ emitIntervalMs: number; eos: boolean }>
	readonly metadata: ProcessorMetadata
	readonly output: TaskOutput
	readonly clock: Clock
	readonly logger: Logger
	/** Task stream time, -1 while unknown. */
	readonly streamTime: () => number
	/** A store of the task by name (connection checks are done by the contexts). */
	readonly store: (name: string) => StateStore
	/** `context.commit()`: a commit of all tasks at the next commit barrier. */
	readonly requestCommit: () => void
}

export class TaskContext {
	readonly streamPunctuation = new PunctuationQueue()
	readonly wallClockPunctuation = new PunctuationQueue()
	/** The task's input record while it is processed (null in init() and punctuators); set by the task. */
	input: Message<Buffer | null, Buffer> | null = null
	/** Wall clock captured at the start of the current slice; set by the task. */
	systemTime: number
	private isClosed = false
	private readonly drops = new Map<DroppedReason, { total: number; unlogged: number; loggedAt: number }>()

	constructor(readonly options: TaskContextOptions) {
		this.systemTime = options.clock.now()
	}

	get closed(): boolean {
		return this.isClosed
	}

	/** The task left RUNNING: every context of this incarnation is closed and its punctuations are dropped. */
	close(): void {
		this.isClosed = true
		this.streamPunctuation.clear()
		this.wallClockPunctuation.clear()
	}

	/** Counts a dropped record and logs it at most once per second per reason. */
	recordDropped(reason: DroppedReason, processor: string, detail?: Readonly<Record<string, unknown>>): void {
		const now = this.options.clock.now()
		let drops = this.drops.get(reason)
		if (!drops) {
			drops = { total: 0, unlogged: 0, loggedAt: -Infinity }
			this.drops.set(reason, drops)
		}
		drops.total++
		drops.unlogged++
		if (now - drops.loggedAt < DROP_LOG_INTERVAL_MS) {
			return
		}
		this.options.logger.warn('dropped records', {
			task: this.options.taskId.key,
			processor,
			reason,
			dropped: drops.unlogged,
			...detail,
		})
		drops.loggedAt = now
		drops.unlogged = 0
	}

	/** Records dropped for `reason` by this task incarnation. */
	droppedCount(reason: DroppedReason): number {
		return this.drops.get(reason)?.total ?? 0
	}
}

export class ProcessorContextImpl<KOut = never, VOut = never> implements InternalProcessorContext<KOut, VOut> {
	readonly applicationId: string
	readonly taskId: TaskId
	readonly metadata: ProcessorMetadata
	readonly config: Readonly<{ emitIntervalMs: number; eos: boolean }>
	private readonly stores: ReadonlySet<string>
	private node: ProcessorNode<unknown, unknown, KOut, VOut> | undefined
	/** True during this node's process() and punctuators. */
	private active = false
	/** Record context of the record at this node. */
	private timestamp = -1
	private headers: RecordHeaders = {}
	/** The forward of this node that has not settled yet. */
	private pending: Promise<void> | undefined
	private readonly forwardSettled = () => {
		this.pending = undefined
	}
	private readonly forwardFailed = (error: unknown) => {
		this.pending = undefined
		throw error
	}

	constructor(
		private readonly task: TaskContext,
		readonly processorName: string,
		stores: readonly string[] = []
	) {
		this.applicationId = task.options.applicationId
		this.taskId = task.options.taskId
		this.metadata = task.options.metadata
		this.config = task.options.config
		this.stores = new Set(stores)
	}

	/** Called by the node constructor. */
	attach(node: ProcessorNode<unknown, unknown, KOut, VOut>): void {
		this.node = node
	}

	/** The node starts processing a record or running a punctuator (timestamp = punctuation time, no headers). */
	enter(timestamp: number, headers: RecordHeaders): void {
		this.active = true
		this.timestamp = timestamp
		this.headers = headers
	}

	/** process() or the punctuator settled: a forward still pending is a ForwardGuardError. */
	leave(): void {
		const pending = this.pending !== undefined
		this.exit()
		if (pending) {
			throw new ForwardGuardError(
				`process() or a punctuator of '${this.processorName}' settled with a pending forward: await or return the result of forward()`,
				{ taskId: this.taskId }
			)
		}
	}

	/** Leaves without checks (failure paths); the error of an abandoned forward is not reported twice. */
	exit(): void {
		this.pending?.catch(() => {})
		this.pending = undefined
		this.active = false
	}

	recordMetadata(): RecordMetadata | null {
		return this.active ? this.task.input : null
	}

	recordContext(): RecordContext | null {
		if (!this.active) {
			return null
		}
		const input = this.task.input
		return {
			topic: input?.topic ?? null,
			partition: input?.partition ?? -1,
			offset: input?.offset ?? -1n,
			timestamp: this.timestamp,
			headers: this.headers,
			rawKey: input?.key ?? null,
			rawValue: input?.value ?? null,
		}
	}

	currentStreamTimeMs(): number {
		return this.task.options.streamTime()
	}

	currentSystemTimeMs(): number {
		return this.task.systemTime
	}

	getStateStore<S extends StateStore>(store: StoreDefinition<S>): S
	getStateStore<S extends StateStore = StateStore>(name: string): S
	getStateStore(store: string | StoreDefinition<StateStore>): StateStore {
		this.checkOpen('getStateStore')
		const name = typeof store === 'string' ? store : store.name
		if (!this.stores.has(name)) {
			throw new TopologyError(`processor '${this.processorName}' is not connected to state store '${name}'`)
		}
		return this.task.options.store(name)
	}

	forward<K extends KOut, V extends VOut>(record: StreamRecord<K, V>, childName?: string): ForwardResult {
		this.checkOpen('forward')
		if (!this.active) {
			throw new ForwardGuardError(
				`forward() of '${this.processorName}' called outside its process() or punctuators`,
				{ taskId: this.taskId }
			)
		}
		if (this.pending !== undefined) {
			this.pending.catch(() => {})
			throw new ForwardGuardError(
				`forward() of '${this.processorName}' called while its previous forward is pending: await or return it`,
				{ taskId: this.taskId }
			)
		}
		const node = this.node!
		let result: ForwardResult
		if (childName === undefined) {
			const children = node.children
			result = children.length === 1 ? children[0]!.process(record) : forwardAll(children, record, 0)
		} else {
			const child = node.childByName.get(childName)
			if (!child) {
				throw new StreamsError(`'${this.processorName}' has no child named '${childName}'`, {
					taskId: this.taskId,
				})
			}
			result = child.process(record)
		}
		if (result === undefined) {
			return
		}
		this.pending = result.then(this.forwardSettled, this.forwardFailed)
		return this.pending
	}

	schedule(intervalMs: number, type: PunctuationType, punctuator: Punctuator): Cancellable {
		this.checkOpen('schedule')
		if (!Number.isSafeInteger(intervalMs) || intervalMs < 1) {
			throw new TypeError(`punctuation interval must be an integer >= 1 ms, got ${intervalMs}`)
		}
		const node = this.node!
		const punctuate = (timestamp: number) => node.punctuate(timestamp, punctuator)
		const { task } = this
		if (type === 'stream-time') {
			return task.streamPunctuation.schedule(0, intervalMs, punctuate)
		}
		if (type === 'wall-clock') {
			return task.wallClockPunctuation.schedule(task.options.clock.now() + intervalMs, intervalMs, punctuate)
		}
		throw new TypeError(`unknown punctuation type '${String(type)}'`)
	}

	commit(): void {
		this.checkOpen('commit')
		this.task.options.requestCommit()
	}

	recordDropped(reason: DroppedReason, detail?: Readonly<Record<string, unknown>>): void {
		this.task.recordDropped(reason, this.processorName, detail)
	}

	output(): TaskOutput {
		return this.task.options.output
	}

	private checkOpen(operation: string): void {
		if (this.task.closed) {
			throw new IllegalStateError(
				`${operation}() of '${this.processorName}' after its task ${this.taskId.key} was closed`,
				{ taskId: this.taskId }
			)
		}
	}
}

/** Depth-first to every child in connection order; asynchronous from the first child that is. */
function forwardAll<K, V>(
	children: readonly ProcessorNode<K, V, unknown, unknown>[],
	record: StreamRecord<K, V>,
	from: number
): ForwardResult {
	for (let i = from; i < children.length; i++) {
		const result = children[i]!.process(record)
		if (result !== undefined) {
			return result.then(() => forwardAll(children, record, i + 1))
		}
	}
}
