import type { ConsumeContext, Consumer, Producer, SendResult } from '@kafkats/client'
import type { Codec } from '@/codec.js'
import type { PendingCheckpoints } from '@/runtime/checkpoints.js'
import type { PendingSends } from '@/runtime/pending-sends.js'
import type { TransactionalBuffer } from '@/runtime/context.js'
import type { SourceNode } from '@/processors/source-sink.js'

// Internal types (not exported from public API)

export type StreamRecord<K, V> = {
	key: K | null
	value: V | null
	/**
	 * The key's prior value, set by a table state node (null if the key was absent before this
	 * update); undefined for records not produced by a table state node. Table-table join nodes read
	 * it to retract a result only when one previously existed. Typed `unknown` so it rides through
	 * value-retyping spreads (`{ ...record, value }`) without a type error; the table nodes that read
	 * it know its concrete type.
	 */
	oldValue?: unknown
	topic: string
	partition: number
	offset: bigint
	timestamp: bigint
	headers: Record<string, Buffer>
}

export type StreamFormat<K, V> = {
	keyCodec?: Codec<K>
	valueCodec?: Codec<V>
}

export type ActiveTransaction = {
	readonly signal: AbortSignal
	send(
		topic: string,
		messages: {
			key?: Buffer | null
			value: Buffer | null
			headers?: Record<string, Buffer>
			partition?: number
			timestamp?: Date
		}
	): Promise<SendResult>
	sendOffsets(
		context: ConsumeContext,
		offsets: Array<{ topic: string; partition: number; offset: bigint }>
	): Promise<void>
}

export type StreamClock = { streamTimeMs: number; lastCleanupMs: number }

export type TopicPartitionKey = `${string}:${number}`

export type WorkerContext = {
	id: number
	producer: Producer
	consumer: Consumer
	activeTransaction: ActiveTransaction | null
	sourcesByTopic: Map<string, SourceNode<unknown, unknown>[]>
	assignedPartitions: Map<TopicPartitionKey, { topic: string; partition: number }>
	/** Changelog positions of the open transaction, checkpointed once it commits. */
	pendingCheckpoints: PendingCheckpoints
	/** Produce requests awaited at the next commit point instead of per record. */
	sends: PendingSends
	/**
	 * Stream-time clocks of this worker, keyed `<trackerId>|<topic>:<partition>`. Reset whenever the
	 * worker's uncommitted progress is discarded or a partition is (re)assigned.
	 */
	streamClocks: Map<string, StreamClock>
	/** Stores holding writes buffered by the open transaction. */
	dirtyStores: Set<TransactionalBuffer>
	/** Set once the worker has failed; it processes nothing more until restarted. */
	failure: { error: Error; recoverable: boolean } | null
	/** Whether the worker made progress since it last (re)started; resets the restart backoff. */
	processedSinceRestart: boolean
}

/** Holder of a state store, shared by every worker clone of the node that uses it. */
export type StoreRef<S> = { store: S | null }

export function requireStore<S>(ref: StoreRef<S>, name: string): S {
	if (!ref.store) {
		throw new Error(`State store '${name}' not initialized`)
	}
	return ref.store
}

export type AnyProcessor = Processor<unknown, unknown, unknown, unknown>
export type OutputProcessor<K, V> = Processor<unknown, unknown, K, V>

/**
 * A processor consumes records of type `<IK, IV>` and may emit records of type `<OK, OV>`.
 *
 * The type parameters are intentionally split to avoid the need for `unknown` casts when chaining nodes:
 * the stream "head" is represented by the output types of the last node in the chain.
 */
export abstract class Processor<IK, IV, OK = IK, OV = IV> {
	protected downstream: Array<Processor<OK, OV, unknown, unknown>> = []

	connect<NK, NV>(node: Processor<OK, OV, NK, NV>): void {
		this.downstream.push(node)
	}

	cloneGraph(worker: WorkerContext, map: Map<AnyProcessor, AnyProcessor>): Processor<IK, IV, OK, OV> {
		const existing = map.get(this as AnyProcessor)
		if (existing) {
			return existing as Processor<IK, IV, OK, OV>
		}
		const cloned = this.clone(worker)
		map.set(this as AnyProcessor, cloned as AnyProcessor)
		this.cloneEdges(cloned, worker, map)
		return cloned
	}

	protected async forward(record: StreamRecord<OK, OV>): Promise<void> {
		for (const node of this.downstream) {
			await node.process(record)
		}
	}

	protected cloneEdges(
		cloned: Processor<IK, IV, OK, OV>,
		worker: WorkerContext,
		map: Map<AnyProcessor, AnyProcessor>
	): void {
		for (const node of this.downstream) {
			const childClone = node.cloneGraph(worker, map)
			cloned.connect(childClone)
		}
	}

	abstract clone(worker: WorkerContext): Processor<IK, IV, OK, OV>
	abstract process(record: StreamRecord<IK, IV>): Promise<void>
}

export class PassThroughNode<K, V> extends Processor<K, V, K, V> {
	clone(): Processor<K, V, K, V> {
		return new PassThroughNode<K, V>()
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		await this.forward(record)
	}
}
