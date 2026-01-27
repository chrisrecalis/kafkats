import type { Consumer, Producer, SendResult } from '@kafkats/client'
import type { Codec } from '@/codec.js'

// Internal types (not exported from public API)

export type StreamRecord<K, V> = {
	key: K | null
	value: V | null
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
	send(
		topic: string,
		messages: { key?: Buffer | null; value: Buffer | null; headers?: Record<string, Buffer>; partition?: number }
	): Promise<SendResult>
	sendOffsets(params: {
		groupId?: string
		consumerGroupMetadata?: {
			groupId: string
			generationId: number
			memberId: string
			groupInstanceId?: string | null
		}
		offsets: Array<{ topic: string; partition: number; offset: bigint }>
	}): Promise<void>
}

export type TopicPartitionKey = `${string}:${number}`

export type WorkerContext = {
	id: number
	producer: Producer
	consumer: Consumer
	activeTransaction: ActiveTransaction | null
	activeTransactionPromise: Promise<void> | null
	groupInstanceId?: string | null
	sourcesByTopic: Map<string, SourceNode<unknown, unknown>[]>
	assignedPartitions: Map<TopicPartitionKey, { topic: string; partition: number }>
	// Transaction batching state for EOS
	pendingOffsets: Map<TopicPartitionKey, { topic: string; partition: number; offset: bigint }>
	pendingChangelogOffsets: Map<TopicPartitionKey, { topic: string; partition: number; offset: bigint }>
	eosQueue: Promise<void>
	transactionActive: boolean
	commitTimer: ReturnType<typeof setTimeout> | null
	lastCommitTime: number
}

// Forward declaration for WorkerContext type
import type { SourceNode } from '@/processors/source-sink.js'

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
	clone(worker: WorkerContext): Processor<K, V, K, V> {
		void worker
		return new PassThroughNode<K, V>()
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		await this.forward(record)
	}
}
