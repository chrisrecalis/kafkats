/**
 * The TestDriver's global stores (4.14, 6.19): a global topic's records are applied through the global store's
 * processor (its KTableSource) into `<applicationId>.<store>`; tasks read the store through a read-only view.
 */
import type { Logger, Message } from '@kafkats/client'
import { IllegalStateError } from '@/api/errors.js'
import type { RecordContext } from '@/processor/api.js'
import { ProcessorContextImpl, TaskContext } from '@/processor/context.js'
import { ProcessorMetadataImpl } from '@/processor/metadata.js'
import { applyHandler, errorHandlerContext, ProcessorNode, processingErrorRoute } from '@/processor/node.js'
import type { TaskOutput } from '@/processor/output.js'
import { StreamRecord } from '@/processor/record.js'
import type { ResolvedFlowConfig } from '@/runtime/config.js'
import type { Clock } from '@/runtime/io.js'
import { TaskIdRegistry } from '@/runtime/task-id.js'
import type { KeyValueStore, StateStore } from '@/state.js'
import type { StateDirectory } from '@/state/state-directory.js'
import type { GlobalStoreDef } from '@/topology/nodes.js'

/** Global processors belong to no task: their context names this one. */
const GLOBAL_TASK = new TaskIdRegistry().get(-1, 0)

/** Global processors write their store directly; they never send. */
const NO_OUTPUT: TaskOutput = {
	send: () => {
		throw new IllegalStateError('a global store processor cannot send records')
	},
	partitionCount: () => undefined,
	loadPartitionCount: () => Promise.reject(new IllegalStateError('a global store processor cannot send records')),
}

interface GlobalStore {
	readonly def: GlobalStoreDef
	readonly store: KeyValueStore<unknown, unknown>
	/** Its processor's record context: the record being applied. */
	readonly task: TaskContext
	readonly node: ProcessorNode<unknown, unknown, never, never>
}

export interface GlobalStoresDeps {
	readonly settings: ResolvedFlowConfig
	readonly stateDirectory: StateDirectory
	readonly clock: Clock
	readonly logger: Logger
}

export class GlobalStores {
	private constructor(
		private readonly byTopic: ReadonlyMap<string, GlobalStore>,
		private readonly byName: ReadonlyMap<string, GlobalStore>,
		private readonly views: ReadonlyMap<string, StateStore>,
		private readonly deps: GlobalStoresDeps
	) {}

	/** Opens every global store and initializes its processor. */
	static async create(defs: readonly GlobalStoreDef[], deps: GlobalStoresDeps): Promise<GlobalStores> {
		const stores = await Promise.all(defs.map(def => open(def, deps)))
		return new GlobalStores(
			new Map(stores.map(store => [store.def.topic, store])),
			new Map(stores.map(store => [store.def.store.name, store])),
			new Map(stores.map(store => [store.def.store.name, readOnly(store.store)])),
			deps
		)
	}

	has(topic: string): boolean {
		return this.byTopic.has(topic)
	}

	/** The store of a global table by name (what tests read). */
	store(name: string): StateStore | undefined {
		return this.byName.get(name)?.store
	}

	/** What processors of tasks get: reads only. */
	view(name: string): StateStore {
		const view = this.views.get(name)
		if (view === undefined) {
			throw new IllegalStateError(`no global store named '${name}'`)
		}
		return view
	}

	/**
	 * Applies one record of a global topic: decoded with the source's codecs (a failure goes to the deserialization
	 * handler: 'continue' skips the record), timestamped by its extractor (a negative timestamp skips it, as in a
	 * task's record queue), then processed by the store's processor.
	 */
	async apply(message: Message<Buffer, Buffer>): Promise<void> {
		const { def, task, node } = this.byTopic.get(message.topic)!
		const { handlers, defaultTimestampExtractor } = this.deps.settings
		let key: unknown
		let value: unknown
		try {
			key = message.key === null ? null : (def.keyCodec?.decode(message.key) ?? message.key)
			value = message.value === null ? null : (def.valueCodec?.decode(message.value) ?? message.value)
		} catch (error) {
			const ctx = errorHandlerContext(
				{ processorName: def.sourceName, taskId: GLOBAL_TASK },
				recordContext(message)
			)
			const failure = `deserialization of a record of global topic '${message.topic}' failed`
			applyHandler(() => handlers.deserialization(ctx, message, error), error, failure, GLOBAL_TASK)
			return
		}
		const extractor = def.timestampExtractor ?? defaultTimestampExtractor
		const timestamp = extractor({ ...message, key, value, timestamp: Number(message.timestamp) }, -1)
		if (timestamp < 0) {
			this.deps.logger.warn('dropped a record of a global topic with a negative timestamp', {
				topic: message.topic,
				partition: message.partition,
				offset: message.offset,
				timestamp,
			})
			return
		}
		task.input = message
		try {
			await node.process(new StreamRecord(key, value, timestamp, message.headers))
		} finally {
			task.input = null
		}
	}

	async close(): Promise<void> {
		for (const { node } of this.byName.values()) {
			await node.close()
		}
	}
}

async function open(def: GlobalStoreDef, deps: GlobalStoresDeps): Promise<GlobalStore> {
	const { settings } = deps
	const store = (await deps.stateDirectory.globalStore(def.store)) as KeyValueStore<unknown, unknown>
	const task = new TaskContext({
		applicationId: settings.applicationId,
		taskId: GLOBAL_TASK,
		config: { emitIntervalMs: settings.emitIntervalMs, eos: settings.eos },
		metadata: new ProcessorMetadataImpl(),
		output: NO_OUTPUT,
		clock: deps.clock,
		logger: deps.logger,
		streamTime: () => -1,
		store: () => store,
		requestCommit: () => {},
	})
	const context = new ProcessorContextImpl<never, never>(task, def.processorName, [def.store.name])
	const node = new ProcessorNode(
		def.processorName,
		def.supplier(),
		context,
		processingErrorRoute(settings.handlers.processing, context)
	)
	await node.init()
	return { def, store, task, node }
}

/** Reads pass through; writes and lifecycle calls reject. */
function readOnly(store: KeyValueStore<unknown, unknown>): KeyValueStore<unknown, unknown> {
	const reject = (method: string) => () =>
		Promise.reject(
			new IllegalStateError(`${method}() of global store '${store.name}': processors may only read it`)
		)
	return {
		name: store.name,
		get: key => store.get(key),
		all: () => store.all(),
		range: (from, to) => store.range(from, to),
		approximateNumEntries: () => store.approximateNumEntries(),
		put: reject('put'),
		delete: reject('delete'),
		flush: reject('flush'),
		init: reject('init'),
		close: reject('close'),
	}
}

function recordContext(message: Message<Buffer, Buffer>): RecordContext {
	const { topic, partition, offset, headers, key, value } = message
	return { topic, partition, offset, timestamp: Number(message.timestamp), headers, rawKey: key, rawValue: value }
}
