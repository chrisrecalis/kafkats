import { StreamsError } from '@/api/errors.js'
import type { ProductionExceptionHandler } from '@/api/handlers.js'
import type { Codec } from '@/codec.js'
import type { MaybePromise, Processor, RecordContext } from '@/processor/api.js'
import type { ProcessorContextImpl } from '@/processor/context.js'
import { applyHandler, errorHandlerContext, type HandlerScope, ProcessorNode } from '@/processor/node.js'
import type { CollectedRecord, SendKind, TaskOutput } from '@/processor/output.js'
import type { StreamRecord } from '@/processor/record.js'
import type { ProcessingErrorRoute } from '@/runtime/config.js'
import type { SinkDef } from '@/topology/nodes.js'

/** A record whose key or value could not be encoded: returns to drop it ('continue'), throws to fail. */
export type SerializationErrorRoute = (error: unknown, record: CollectedRecord, ctx: RecordContext | null) => void

/**
 * Writes records to a topic through the task's output (KS `SinkNode`): encodes, picks the partition and sends
 * `record.timestamp` and `record.headers` (never producer time). Repartition sinks drop null keys silently unless a
 * relaxed join is downstream (`nullKeys: 'keep'`).
 */
export class SinkNode<K, V> extends ProcessorNode<K, V, never, never> {
	constructor(
		readonly def: SinkDef,
		context: ProcessorContextImpl<never, never>,
		onError: ProcessingErrorRoute,
		onSerializationError: SerializationErrorRoute
	) {
		super(def.name, new SinkProcessor<K, V>(def, context, onSerializationError), context, onError)
	}
}

class SinkProcessor<K, V> implements Processor<K, V> {
	private readonly kind: SendKind
	private readonly dropNullKeys: boolean

	constructor(
		private readonly def: SinkDef,
		private readonly context: ProcessorContextImpl<never, never>,
		private readonly onSerializationError: SerializationErrorRoute
	) {
		this.kind = def.internal === 'repartition' ? 'repartition' : 'sink'
		this.dropNullKeys = def.internal === 'repartition' && def.nullKeys !== 'keep'
	}

	process(record: StreamRecord<K, V>): MaybePromise<void> {
		const { def, context } = this
		if (this.dropNullKeys && record.key == null) {
			return
		}
		const topic =
			typeof def.topic === 'string' ? def.topic : def.topic(record.key, record.value, context.recordMetadata())
		let key: Buffer | null = null
		let value: Buffer | null = null
		try {
			key = encode(record.key, def.keyCodec, 'key')
			value = encode(record.value, def.valueCodec, 'value')
		} catch (error) {
			const failed = { topic, key, value, timestamp: record.timestamp, headers: record.headers }
			this.onSerializationError(error, failed, context.recordContext())
			return
		}
		const output = context.output()
		const encoded = { topic, key, value, timestamp: record.timestamp, headers: record.headers }
		if (!def.partitioner) {
			output.send(this.kind, encoded)
			return
		}
		const count = output.partitionCount(topic)
		if (count !== undefined) {
			this.sendTo(output, encoded, record, count)
			return
		}
		return output.loadPartitionCount(topic).then(loaded => this.sendTo(output, encoded, record, loaded))
	}

	/** The partitioner's answer must be undefined (producer default on the encoded key) or in `[0, count)`. */
	private sendTo(output: TaskOutput, encoded: CollectedRecord, record: StreamRecord<K, V>, count: number): void {
		const partition = this.def.partitioner!(encoded.topic, record.key, record.value, count)
		if (partition !== undefined && !(Number.isInteger(partition) && partition >= 0 && partition < count)) {
			throw new StreamsError(
				`partitioner of sink '${this.def.name}' returned partition ${partition} for topic '${encoded.topic}' with ${count} partitions`,
				{ taskId: this.context.taskId }
			)
		}
		output.send(this.kind, partition === undefined ? encoded : { ...encoded, partition })
	}
}

/** null and undefined are encoded as null (a tombstone value); without a codec only Buffers pass. */
function encode<T>(data: T, codec: Codec<unknown> | undefined, part: 'key' | 'value'): Buffer | null {
	if (data === null || data === undefined) {
		return null
	}
	if (codec) {
		return codec.encode(data)
	}
	if (Buffer.isBuffer(data)) {
		return data
	}
	throw new TypeError(`no ${part} codec for a ${part} that is not a Buffer`)
}

/** The serialization error route of one sink (4.6.5): the production handler with origin 'serialization'. */
export function serializationErrorRoute(
	handler: ProductionExceptionHandler,
	context: HandlerScope
): SerializationErrorRoute {
	return (error, record, recordContext) => {
		const ctx = errorHandlerContext(context, recordContext)
		applyHandler(
			() => handler(ctx, record, error, 'serialization'),
			error,
			`serialization for topic '${record.topic}' failed in '${context.processorName}'`,
			context.taskId
		)
		context.recordDropped('production', { topic: record.topic, origin: 'serialization' })
	}
}
