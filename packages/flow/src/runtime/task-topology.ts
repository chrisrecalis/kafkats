/**
 * The processor graph of one RUNNING task incarnation (KS `ProcessorTopology` instance): nodes created from the
 * subtopology's suppliers, their contexts, punctuation queues and store handles. It exists iff the task is RUNNING.
 */
import type { Logger } from '@kafkats/client'
import type { ForwardResult, ProcessorMetadata, TaskId } from '@/processor/api.js'
import { ProcessorContextImpl, TaskContext } from '@/processor/context.js'
import { ProcessorNode, processingErrorRoute } from '@/processor/node.js'
import type { TaskOutput } from '@/processor/output.js'
import type { PunctuationQueue } from '@/processor/punctuation.js'
import { StreamRecord } from '@/processor/record.js'
import { SinkNode, serializationErrorRoute } from '@/processor/sink.js'
import { SourceNode } from '@/processor/source.js'
import type { ResolvedHandlers } from '@/runtime/config.js'
import type { Clock } from '@/runtime/io.js'
import type { StampedRecord } from '@/runtime/record-queue.js'
import type { StateStore } from '@/state.js'
import { storeHandle } from '@/state/handles.js'
import type { TaskStateManager } from '@/state/state-manager.js'
import type { Subtopology } from '@/topology/built-topology.js'

type AnyNode = ProcessorNode<unknown, unknown, unknown, unknown>

/** What the topology needs of its task. */
export interface TopologyOwner {
	readonly id: TaskId
	readonly subtopology: Subtopology
	/** Task stream time, -1 unknown. */
	readonly streamTime: number
}

export interface TaskTopologyDeps {
	/** The task's store stacks. */
	readonly stores: Pick<TaskStateManager, 'stores' | 'stackClosed'>
	/** The read-only view of a global store, which every processor may read without being connected (KS). */
	readonly globalStore: (name: string) => StateStore
	readonly output: TaskOutput
	readonly metadata: ProcessorMetadata
	readonly clock: Clock
	readonly handlers: ResolvedHandlers
	readonly onCommitRequested: () => void
	readonly config: Readonly<{ applicationId: string; eos: boolean; emitIntervalMs: number }>
	readonly logger: Logger
}

export class TaskTopology {
	private closed = false
	private readonly handles = new Map<string, StateStore>()

	private constructor(
		/** Shared by every context of this incarnation: input record, slice time, punctuation, drops. */
		readonly context: TaskContext,
		/** Topological (insertion) order. */
		private readonly nodes: readonly AnyNode[],
		/** Source nodes of each topic in registration order. */
		private readonly sources: ReadonlyMap<string, readonly AnyNode[]>,
		private readonly deps: Pick<TaskTopologyDeps, 'stores' | 'globalStore'>,
		private readonly globals: ReadonlySet<string>
	) {}

	static create(task: TopologyOwner, deps: TaskTopologyDeps): TaskTopology {
		let topology: TaskTopology | undefined
		const context = new TaskContext({
			applicationId: deps.config.applicationId,
			taskId: task.id,
			config: { emitIntervalMs: deps.config.emitIntervalMs, eos: deps.config.eos },
			metadata: deps.metadata,
			output: deps.output,
			clock: deps.clock,
			logger: deps.logger,
			streamTime: () => task.streamTime,
			store: name => topology!.store(name),
			requestCommit: deps.onCommitRequested,
		})
		const { processing, production } = deps.handlers
		const { globalStores } = task.subtopology
		const nodes = new Map<string, AnyNode>()
		for (const def of task.subtopology.nodes) {
			let node: AnyNode
			if (def.kind === 'source') {
				const ctx = new ProcessorContextImpl<unknown, unknown>(context, def.name)
				node = new SourceNode<unknown, unknown>(def, ctx, processingErrorRoute(processing, ctx))
			} else if (def.kind === 'processor') {
				const stores = globalStores.length === 0 ? def.stores : [...def.stores, ...globalStores]
				const ctx = new ProcessorContextImpl<unknown, unknown>(context, def.name, stores)
				node = new ProcessorNode(def.name, def.supplier(), ctx, processingErrorRoute(processing, ctx))
			} else {
				const ctx = new ProcessorContextImpl(context, def.name)
				const route = processingErrorRoute(processing, ctx)
				node = new SinkNode(def, ctx, route, serializationErrorRoute(production, ctx)) as AnyNode
			}
			if (def.kind !== 'source') {
				for (const parent of def.parents) {
					nodes.get(parent)!.addChild(node)
				}
			}
			nodes.set(def.name, node)
		}
		// The order the record queues decode in.
		const sources = new Map<string, AnyNode[]>()
		for (const [topic, defs] of task.subtopology.sourcesByTopic) {
			sources.set(
				topic,
				defs.map(def => nodes.get(def.name)!)
			)
		}
		topology = new TaskTopology(context, [...nodes.values()], sources, deps, new Set(globalStores))
		return topology
	}

	/** init() of every node in topological order, without a record context. */
	async init(): Promise<void> {
		for (const node of this.nodes) {
			await node.init()
		}
	}

	/**
	 * Passes the record to the source nodes of its topic in registration order, each after the previous one's
	 * subtree settled. Synchronous when every subtree is.
	 */
	process(record: StampedRecord, now: number): ForwardResult {
		const { message } = record
		this.context.input = message
		this.context.systemTime = now
		const sources = this.sources.get(message.topic)!
		const first = new StreamRecord(record.key, record.value, record.timestamp, message.headers)
		if (record.more === undefined) {
			return sources[0]!.process(first)
		}
		const records = [
			first,
			...record.more.map(r => new StreamRecord(r.key, r.value, record.timestamp, message.headers)),
		]
		return processFrom(sources, records, 0)
	}

	get streamPunctuation(): PunctuationQueue {
		return this.context.streamPunctuation
	}

	get wallClockPunctuation(): PunctuationQueue {
		return this.context.wallClockPunctuation
	}

	/** Runs the stream-time punctuations due at `streamTime`; whether any ran. */
	punctuateStreamTime(streamTime: number): Promise<boolean> {
		this.context.input = null
		return this.context.streamPunctuation.maybePunctuate(streamTime)
	}

	/** Runs the wall-clock punctuations due at `now`; whether any ran. */
	punctuateWallClock(now: number): Promise<boolean> {
		this.context.input = null
		this.context.systemTime = now
		return this.context.wallClockPunctuation.maybePunctuate(now)
	}

	/**
	 * A handle on the top of a task store's stack whose writes reject once this topology or the stack closed (one per
	 * store; the stack closes first when a close takes the task while its processors' init() runs); the read-only view
	 * of a global store, which belongs to no task.
	 */
	store(name: string): StateStore {
		if (this.globals.has(name)) {
			return this.deps.globalStore(name)
		}
		let handle = this.handles.get(name)
		if (!handle) {
			const { stores } = this.deps
			const { builder, top } = stores.stores.find(store => store.builder.name === name)!
			const owner = `processors of task ${this.context.options.taskId.key}`
			handle = storeHandle(top, builder.kind, () => this.closed || stores.stackClosed, owner)
			this.handles.set(name, handle)
		}
		return handle
	}

	/**
	 * Closes every context and store handle first, so a late forward or write of this incarnation throws
	 * IllegalStateError; then every node. The first error is rethrown after all closed.
	 */
	async close(): Promise<void> {
		this.closed = true
		this.context.close()
		const errors: unknown[] = []
		for (const node of this.nodes) {
			await node.close().catch((error: unknown) => errors.push(error))
		}
		if (errors.length > 0) {
			throw errors[0]
		}
	}
}

function processFrom(
	sources: readonly AnyNode[],
	records: readonly StreamRecord<unknown, unknown>[],
	from: number
): ForwardResult {
	for (let i = from; i < sources.length; i++) {
		const result = sources[i]!.process(records[i]!)
		if (result !== undefined) {
			return result.then(() => processFrom(sources, records, i + 1))
		}
	}
}
