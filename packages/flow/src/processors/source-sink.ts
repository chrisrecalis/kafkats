import type { Message, ConsumeContext } from '@kafkats/client'
import type { Codec } from '@/codec.js'
import { Processor, type StreamRecord, type StreamFormat, type WorkerContext } from '@/processors/base.js'
import type { Produced } from '@/types.js'

/** The part of the app a ProduceNode writes through (an interface to avoid a circular import). */
export interface RecordSender {
	sendToTopic<K, V>(
		worker: WorkerContext,
		topic: string,
		record: StreamRecord<K, V>,
		options: Produced<K, V> | undefined,
		format: StreamFormat<K, V>
	): Promise<void>
}

// Without a codec the raw buffer is passed through.
function decode<T>(raw: Buffer | null, codec: Codec<T> | undefined): T | null {
	if (raw === null) return null
	return codec ? codec.decode(raw) : (raw as unknown as T)
}

export class SourceNode<K, V> extends Processor<K, V> {
	constructor(
		readonly topic: string,
		private readonly format: StreamFormat<K, V>
	) {
		super()
	}

	clone(): Processor<K, V, K, V> {
		return new SourceNode(this.topic, this.format)
	}

	async handleMessage(message: Message<Buffer>, _ctx: ConsumeContext): Promise<void> {
		await this.forward({
			key: decode(message.key, this.format.keyCodec),
			value: decode(message.value, this.format.valueCodec),
			topic: message.topic,
			partition: message.partition,
			offset: message.offset,
			timestamp: message.timestamp,
			headers: message.headers,
		})
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		await this.forward(record)
	}
}

export class ProduceNode<K, V> extends Processor<K, V> {
	constructor(
		private readonly app: RecordSender,
		private readonly topic: string,
		private readonly produced: Produced<K, V> | undefined,
		private readonly format: StreamFormat<K, V>,
		private readonly worker?: WorkerContext
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, K, V> {
		return new ProduceNode(this.app, this.topic, this.produced, this.format, worker)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		if (!this.worker) {
			throw new Error('ProduceNode is not bound to a worker')
		}
		await this.app.sendToTopic(this.worker, this.topic, record, this.produced, this.format)
	}
}
