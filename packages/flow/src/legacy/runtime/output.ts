import type { KafkaClient } from '@kafkats/client'
import type { Codec } from '@/codec.js'
import type { RecordSender, StreamFormat, StreamRecord, WorkerContext } from '@/legacy/processors/index.js'
import type { Produced } from '@/legacy/types.js'

/** Writes records produced by the topology (`to()`, repartition and through topics). */
export class RecordOutput implements RecordSender {
	private readonly partitionCounts = new Map<string, number>()

	constructor(
		private readonly client: KafkaClient,
		private readonly eosEnabled: boolean
	) {}

	async send<K, V>(
		worker: WorkerContext,
		topic: string,
		record: StreamRecord<K, V>,
		options: Produced<K, V> | undefined,
		format: StreamFormat<K, V>
	): Promise<void> {
		const producer = worker.producer

		const keyCodec = options?.key ?? format.keyCodec
		const valueCodec = options?.value ?? format.valueCodec

		const key = encodeKey(record.key, keyCodec)
		const value = record.value === null ? null : encodeValue(record.value, valueCodec)

		const partition = options?.partitioner
			? await this.resolvePartition(topic, record.key, record.value, options.partitioner)
			: undefined

		// Preserve event time: downstream windowing/joins on the output topic must see the
		// record's timestamp, not the wall-clock time at which it happened to be produced.
		const timestamp = record.timestamp > 0n ? new Date(Number(record.timestamp)) : undefined

		if (this.eosEnabled && !worker.activeTransaction) {
			throw new Error('exactly_once requires a transactional context')
		}

		// The acknowledgement is awaited at the worker's next commit point (see PendingSends).
		const target = worker.activeTransaction ?? producer
		worker.sends.track(
			target.send(topic, {
				key: key ?? undefined,
				value,
				headers: record.headers,
				partition,
				timestamp,
			})
		)
	}

	private async resolvePartition<K, V>(
		topic: string,
		key: K | null,
		value: V | null,
		partitioner: (key: K | null, value: V | null, partitionCount: number) => number
	): Promise<number> {
		const count = await this.getPartitionCount(topic)
		const partition = partitioner(key, value, count)
		if (!Number.isInteger(partition) || partition < 0 || partition >= count) {
			throw new Error(`partitioner returned invalid partition ${partition} for topic ${topic}`)
		}
		return partition
	}

	private async getPartitionCount(topic: string): Promise<number> {
		const cached = this.partitionCounts.get(topic)
		if (cached !== undefined) {
			return cached
		}

		const metadata = await this.client.getMetadata([topic])
		const meta = metadata.topics.get(topic)
		if (!meta) {
			throw new Error(`Topic ${topic} not found in metadata`)
		}

		const count = meta.partitions.size
		this.partitionCounts.set(topic, count)
		return count
	}
}

function encodeKey<K>(key: K | null, codec?: Codec<K>): Buffer | null {
	if (key === null || key === undefined) {
		return null
	}
	if (codec) {
		return codec.encode(key)
	}
	if (Buffer.isBuffer(key)) {
		return key
	}
	throw new Error('No key codec provided for non-Buffer key')
}

function encodeValue<V>(value: V, codec?: Codec<V>): Buffer {
	if (codec) {
		return codec.encode(value)
	}
	if (Buffer.isBuffer(value)) {
		return value
	}
	throw new Error('No value codec provided for non-Buffer value')
}
