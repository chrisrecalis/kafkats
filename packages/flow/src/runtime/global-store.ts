/**
 * One global store and its processor (KS `GlobalStateUpdateTask`, 6.19): records of the global topic are decoded and
 * timestamped as in a task's record queue and applied through the store's processor (its KTableSource) into
 * `<applicationId>.<store>`; processors of tasks read the store through a read-only view. Shared by the
 * GlobalStateManager (4.12) and the TestDriver (4.14).
 */
import type { Logger, Message } from '@kafkats/client'
import { IllegalStateError } from '@/api/errors.js'
import { ProcessorContextImpl, TaskContext } from '@/processor/context.js'
import { ProcessorMetadataImpl } from '@/processor/metadata.js'
import { ProcessorNode, processingErrorRoute } from '@/processor/node.js'
import type { TaskOutput } from '@/processor/output.js'
import { StreamRecord } from '@/processor/record.js'
import type { ResolvedFlowConfig } from '@/runtime/config.js'
import type { Clock } from '@/runtime/io.js'
import { RecordDecoder, type DecodedRecord } from '@/runtime/record-decoder.js'
import { yieldIfDue } from '@/runtime/signals.js'
import { TaskIdRegistry } from '@/runtime/task-id.js'
import type { KeyValueStore } from '@/state.js'
import { clearStore } from '@/state/bulk.js'
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
	close: () => {},
}

export interface GlobalStoreDeps {
	readonly config: Pick<
		ResolvedFlowConfig,
		'applicationId' | 'eos' | 'emitIntervalMs' | 'defaultTimestampExtractor' | 'handlers'
	>
	readonly stateDirectory: StateDirectory
	readonly clock: Clock
	readonly logger: Logger
}

/** A decoded record with the message it came from (its record context). */
interface Decoded {
	readonly message: Message<Buffer, Buffer>
	readonly record: DecodedRecord
}

export class GlobalStore {
	/** What processors of tasks get: reads only. */
	readonly view: KeyValueStore<unknown, unknown>
	/** Max timestamp applied per partition (what the extractor sees as partition time), -1 unknown. */
	private readonly partitionTimes = new Map<number, number>()

	private constructor(
		readonly def: GlobalStoreDef,
		/** The provider store, written only through the processor. */
		readonly store: KeyValueStore<unknown, unknown>,
		private readonly task: TaskContext,
		private readonly node: ProcessorNode<unknown, unknown, never, never>,
		private readonly decoder: RecordDecoder
	) {
		this.view = readOnly(store)
	}

	/** Opens the store (`<applicationId>.<store>`, initialized once per start) and initializes its processor. */
	static async open(def: GlobalStoreDef, deps: GlobalStoreDeps): Promise<GlobalStore> {
		const { config } = deps
		const store = (await deps.stateDirectory.globalStore(def.store)) as KeyValueStore<unknown, unknown>
		const task = new TaskContext({
			applicationId: config.applicationId,
			taskId: GLOBAL_TASK,
			config: { emitIntervalMs: config.emitIntervalMs, eos: config.eos },
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
			processingErrorRoute(config.handlers.processing, context)
		)
		await node.init()
		const decoder = new RecordDecoder(
			[{ name: def.sourceName, keyCodec: def.keyCodec, valueCodec: def.valueCodec }],
			def.timestampExtractor ?? config.defaultTimestampExtractor,
			config.handlers.deserialization,
			(reason, source, detail) => task.recordDropped(reason, source, detail),
			GLOBAL_TASK
		)
		return new GlobalStore(def, store, task, node, decoder)
	}

	/**
	 * Applies records of one partition, in offset order: each decoded and timestamped as in a task's record queue (a
	 * record the deserialization handler lets go, or one with a negative timestamp, is skipped), then, of the records
	 * of one key, only the last one is processed by the store's processor (only that one decides what the table
	 * holds). Rejects with the first failure: a row failure.
	 */
	async apply(messages: readonly Message<Buffer, Buffer>[]): Promise<void> {
		const decoded = this.decode(messages)
		const last = new Map<string, Decoded>()
		for (const entry of decoded) {
			if (entry.message.key !== null) last.set(entry.message.key.toString('latin1'), entry)
		}
		const { task, node } = this
		for (const entry of decoded) {
			const { message, record } = entry
			if (message.key !== null && last.get(message.key.toString('latin1')) !== entry) continue
			task.input = message
			try {
				await node.process(new StreamRecord(record.key, record.value, record.timestamp, message.headers))
			} finally {
				task.input = null
			}
			await yieldIfDue()
		}
	}

	/** Removes every row (the topic is the source of truth) and forgets the partition times. */
	async wipe(): Promise<void> {
		this.partitionTimes.clear()
		await clearStore(this.store)
	}

	close(): Promise<void> {
		return this.node.close()
	}

	private decode(messages: readonly Message<Buffer, Buffer>[]): Decoded[] {
		const decoded: Decoded[] = []
		for (const message of messages) {
			const partitionTime = this.partitionTimes.get(message.partition) ?? -1
			const record = this.decoder.decode(message, partitionTime)
			if (record === undefined) continue
			this.partitionTimes.set(message.partition, Math.max(partitionTime, record.timestamp))
			decoded.push({ message, record })
		}
		return decoded
	}
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
