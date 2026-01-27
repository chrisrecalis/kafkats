import type { Message, ConsumeContext } from '@kafkats/client'
import { Processor, type StreamRecord, type StreamFormat, type WorkerContext } from '@/processors/base.js'
import type { Produced } from '@/types.js'

// FlowAppImpl type for the app reference - uses interface to avoid circular import
export interface FlowAppInterface {
	sendToTopic<K, V>(
		worker: WorkerContext,
		topic: string,
		record: StreamRecord<K, V>,
		options: Produced<K, V> | undefined,
		format: StreamFormat<K, V>
	): Promise<void>
}

export class SourceNode<K, V> extends Processor<K, V> {
	constructor(
		readonly topic: string,
		private readonly format: StreamFormat<K, V>
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, K, V> {
		void worker
		return new SourceNode(this.topic, this.format)
	}

	async handleMessage(message: Message<Buffer>, ctx: ConsumeContext): Promise<void> {
		void ctx
		const key = this.decodeKey(message.key)
		const value = this.decodeValue(message.value)

		const record: StreamRecord<K, V> = {
			key,
			value,
			topic: message.topic,
			partition: message.partition,
			offset: message.offset,
			timestamp: message.timestamp,
			headers: message.headers,
		}

		await this.forward(record)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		await this.forward(record)
	}

	private decodeKey(raw: Buffer | null): K | null {
		if (raw === null) return null
		const codec = this.format.keyCodec
		return codec ? codec.decode(raw) : (raw as unknown as K)
	}

	private decodeValue(raw: Buffer | null): V | null {
		if (raw === null) return null
		const codec = this.format.valueCodec
		return codec ? codec.decode(raw) : (raw as unknown as V)
	}
}

export class ProduceNode<K, V> extends Processor<K, V> {
	constructor(
		private readonly app: FlowAppInterface,
		private readonly topic: string,
		private readonly produced: Produced<K, V> | undefined,
		private readonly format: StreamFormat<K, V>,
		private readonly forwardAfter: boolean,
		private readonly worker?: WorkerContext
	) {
		super()
	}

	clone(worker: WorkerContext): Processor<K, V, K, V> {
		return new ProduceNode(this.app, this.topic, this.produced, this.format, this.forwardAfter, worker)
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		if (!this.worker) {
			throw new Error('ProduceNode is not bound to a worker')
		}
		await this.app.sendToTopic(this.worker, this.topic, record, this.produced, this.format)
		if (this.forwardAfter) {
			await this.forward(record)
		}
	}
}
