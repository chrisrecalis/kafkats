/**
 * Processor harness: one processor graph (sources, processors, sinks) of one task with real processor contexts and
 * in-memory stores, but no task, queues or runtime. Records go straight to the source nodes of a topic; stream time
 * is the max timestamp processed (as the task's partition group keeps it); punctuation and the wall clock are driven
 * explicitly; sends are recorded.
 */
import { noopLogger, type Logger } from '@kafkats/client'
import type { ProcessingExceptionHandler, ProductionExceptionHandler } from '@/api/handlers.js'
import type { ProcessorSupplier, TaskId } from '@/processor/api.js'
import { ProcessorContextImpl, TaskContext } from '@/processor/context.js'
import { ProcessorMetadataImpl } from '@/processor/metadata.js'
import { ProcessorNode, processingErrorRoute } from '@/processor/node.js'
import type { CollectedRecord, SendKind, TaskOutput } from '@/processor/output.js'
import { StreamRecord, type RecordHeaders } from '@/processor/record.js'
import { SinkNode, serializationErrorRoute } from '@/processor/sink.js'
import { SourceNode } from '@/processor/source.js'
import type { StateStore } from '@/state.js'
import type { NodeDef, ProcessorDef, SinkDef, SourceDef } from '@/topology/nodes.js'

type AnyNode = ProcessorNode<unknown, unknown, unknown, unknown>

export interface HarnessOptions {
	readonly applicationId?: string
	/** Default `0_0`. */
	readonly taskId?: TaskId
	readonly eos?: boolean
	readonly emitIntervalMs?: number
	/** Stores of the task, initialized by start() and closed by close(). */
	readonly stores?: readonly StateStore[]
	/** Default 'fail'. */
	readonly processing?: ProcessingExceptionHandler
	/** Default 'fail'. */
	readonly production?: ProductionExceptionHandler
	/** Overrides of the recording output (by default every topic has 1 partition, cached). */
	readonly output?: Partial<TaskOutput>
	/** Processor metadata the task was seeded with. */
	readonly metadata?: Readonly<Record<string, number>>
	/** Initial wall clock; default 0. */
	readonly wallClock?: number
	readonly logger?: Logger
}

export interface HarnessRecord<K, V> {
	readonly key: K
	readonly value: V
	readonly timestamp: number
	readonly headers?: RecordHeaders
	/** Default: the next offset of the topic, from 0. */
	readonly offset?: bigint
	/** Input bytes seen by error handlers; default: key/value when they are Buffers. */
	readonly rawKey?: Buffer | null
	readonly rawValue?: Buffer | null
}

export interface Sent {
	readonly kind: SendKind
	readonly record: CollectedRecord
}

export function taskId(subtopology = 0, partition = 0): TaskId {
	return Object.freeze({ subtopology, partition, key: `${subtopology}_${partition}` })
}

export function sourceDef(name: string, topics: readonly string[], extra: Partial<SourceDef> = {}): SourceDef {
	return { kind: 'source', name, topics, internal: false, ...extra }
}

export function processorDef(
	name: string,
	parents: readonly string[],
	supplier: ProcessorSupplier<never, never, unknown, unknown>,
	stores: readonly string[] = []
): ProcessorDef {
	return {
		kind: 'processor',
		name,
		parents,
		stores,
		supplier: supplier as ProcessorSupplier<unknown, unknown, unknown, unknown>,
	}
}

export function sinkDef(
	name: string,
	parents: readonly string[],
	topic: SinkDef['topic'],
	extra: Partial<SinkDef> = {}
): SinkDef {
	return { kind: 'sink', name, parents, topic, internal: false, ...extra }
}

/** A terminal processor that records what reaches it. */
export function capture<K = unknown, V = unknown>(
	name: string,
	parents: readonly string[]
): { def: ProcessorDef; records: StreamRecord<K, V>[] } {
	const records: StreamRecord<K, V>[] = []
	return { def: processorDef(name, parents, () => ({ process: record => void records.push(record) })), records }
}

export class ProcessorHarness {
	readonly metadata = new ProcessorMetadataImpl()
	readonly sent: Sent[] = []
	readonly task: TaskContext
	commitRequests = 0
	scheduled = 0
	/** Max timestamp processed, -1 unknown (settable to drive stream-time punctuation). */
	streamTime = -1
	wallClock: number
	private readonly nodes = new Map<string, AnyNode>()
	private readonly sources = new Map<string, SourceNode<unknown, unknown>[]>()
	private readonly offsets = new Map<string, bigint>()
	private readonly stores: Map<string, StateStore>

	private constructor(
		private readonly defs: readonly NodeDef[],
		private readonly options: HarnessOptions
	) {
		this.wallClock = options.wallClock ?? 0
		this.stores = new Map((options.stores ?? []).map(store => [store.name, store]))
		const output: TaskOutput = {
			send: (kind, record) => void this.sent.push({ kind, record }),
			partitionCount: () => 1,
			loadPartitionCount: () => Promise.resolve(1),
			...options.output,
		}
		this.task = new TaskContext({
			applicationId: options.applicationId ?? 'test-app',
			taskId: options.taskId ?? taskId(),
			config: { emitIntervalMs: options.emitIntervalMs ?? 0, eos: options.eos ?? false },
			metadata: this.metadata,
			output,
			clock: { now: () => this.wallClock, setTimer: () => () => {} },
			logger: options.logger ?? noopLogger,
			streamTime: () => this.streamTime,
			store: name => this.store(name),
			requestCommit: () => void this.commitRequests++,
			onScheduled: () => void this.scheduled++,
		})
		const seed = new Map(Object.entries(options.metadata ?? {}))
		this.metadata.replaceAll([seed], new Set(seed.keys()))
	}

	/** Builds the graph (`defs` in topological order), initializes the stores, then every node in order. */
	static async start(defs: readonly NodeDef[], options: HarnessOptions = {}): Promise<ProcessorHarness> {
		const harness = new ProcessorHarness(defs, options)
		harness.build()
		for (const store of harness.stores.values()) {
			await store.init()
		}
		for (const def of defs) {
			await harness.node(def.name).init()
		}
		return harness
	}

	node(name: string): AnyNode {
		const node = this.nodes.get(name)
		if (!node) {
			throw new Error(`no node '${name}'`)
		}
		return node
	}

	context(name: string): ProcessorContextImpl<unknown, unknown> {
		return this.node(name).context
	}

	store<S extends StateStore = StateStore>(name: string): S {
		const store = this.stores.get(name)
		if (!store) {
			throw new Error(`no store '${name}'`)
		}
		return store as S
	}

	/** Records sent to `topic`. */
	output(topic: string): CollectedRecord[] {
		return this.sent.filter(sent => sent.record.topic === topic).map(sent => sent.record)
	}

	/** Processes one input record of `topic` like a task: stream time first, then each source node of the topic. */
	async process<K, V>(topic: string, record: HarnessRecord<K, V>): Promise<void> {
		const sources = this.sources.get(topic)
		if (!sources) {
			throw new Error(`no source node reads '${topic}'`)
		}
		const offset = record.offset ?? this.offsets.get(topic) ?? 0n
		this.offsets.set(topic, offset + 1n)
		const headers = record.headers ?? {}
		this.streamTime = Math.max(this.streamTime, record.timestamp)
		this.task.systemTime = this.wallClock
		this.task.input = {
			topic,
			partition: this.task.options.taskId.partition,
			offset,
			timestamp: BigInt(record.timestamp),
			key: record.rawKey !== undefined ? record.rawKey : Buffer.isBuffer(record.key) ? record.key : null,
			value:
				record.rawValue !== undefined ? record.rawValue : Buffer.isBuffer(record.value) ? record.value : null,
			headers,
		}
		try {
			for (const source of sources) {
				await source.process(
					new StreamRecord<unknown, unknown>(record.key, record.value, record.timestamp, headers)
				)
			}
		} finally {
			this.task.input = null
		}
	}

	/** Stream-time punctuation at the current stream time (nothing while it is unknown). */
	async punctuateStreamTime(): Promise<boolean> {
		return this.streamTime >= 0 && (await this.task.streamPunctuation.maybePunctuate(this.streamTime))
	}

	/** Advances the wall clock, then runs the wall-clock punctuations due. */
	async advanceWallClock(ms: number): Promise<boolean> {
		this.wallClock += ms
		this.task.systemTime = this.wallClock
		return this.task.wallClockPunctuation.maybePunctuate(this.wallClock)
	}

	/** Closes the contexts first, then every node (first error rethrown after all closed), then the stores. */
	async close(): Promise<void> {
		this.task.close()
		const errors: unknown[] = []
		for (const def of this.defs) {
			await this.node(def.name)
				.close()
				.catch((error: unknown) => errors.push(error))
		}
		for (const store of this.stores.values()) {
			await store.close()
		}
		if (errors.length > 0) {
			throw errors[0]
		}
	}

	private build(): void {
		const processing = this.options.processing ?? (() => 'fail')
		const production = this.options.production ?? (() => 'fail')
		for (const def of this.defs) {
			let node: AnyNode
			if (def.kind === 'source') {
				const context = new ProcessorContextImpl<unknown, unknown>(this.task, def.name)
				const source = new SourceNode<unknown, unknown>(def, context, processingErrorRoute(processing, context))
				for (const topic of def.topics) {
					this.sources.set(topic, [...(this.sources.get(topic) ?? []), source])
				}
				node = source
			} else if (def.kind === 'processor') {
				const context = new ProcessorContextImpl<unknown, unknown>(this.task, def.name, def.stores)
				node = new ProcessorNode(def.name, def.supplier(), context, processingErrorRoute(processing, context))
			} else {
				const context = new ProcessorContextImpl(this.task, def.name)
				const route = processingErrorRoute(processing, context)
				node = new SinkNode(def, context, route, serializationErrorRoute(production, context)) as AnyNode
			}
			if (def.kind !== 'source') {
				for (const parent of def.parents) {
					this.node(parent).addChild(node)
				}
			}
			this.nodes.set(def.name, node)
		}
	}
}
