/**
 * What a test sees of the driver's Kafka: MockProducer records every send (sink, repartition and changelog records)
 * and the committed transactions; MockConsumer feeds records to the topology and shows the committed offsets.
 */
import type { SendResult } from '@kafkats/client'
import type { RecordHeaders } from '@/processor/record.js'
import type {
	CommittedOffset,
	OffsetAndMetadata,
	ProducedMessage,
	ProducerPort,
	TransactionPort,
} from '@/runtime/io.js'
import { tpKey } from '@/runtime/task-id.js'

export interface TestRecord<K = unknown, V = unknown> {
	topic: string
	key: K | null
	value: V | null
	/** Set when a partitioner picked one; otherwise the record went to partition 0. */
	partition?: number
	timestamp?: bigint
	headers?: RecordHeaders
}

export interface CommittedTransaction {
	readonly messages: TestRecord<Buffer, Buffer>[]
	readonly offsets: OffsetAndMetadata[]
}

/** The driver's producer: records every send, answers it at once with the next offset of its partition. */
export class MockProducer implements ProducerPort {
	readonly messages: TestRecord<Buffer, Buffer>[] = []
	/** Committed transactions, in commit order (exactly-once). */
	readonly transactions: CommittedTransaction[] = []
	private readonly offsets = new Map<string, bigint>()
	private open: { messages: TestRecord<Buffer, Buffer>[]; offsets: OffsetAndMetadata[] } | undefined

	constructor(
		/** Partition count per topic. */
		private readonly partitions: (topic: string) => number,
		/** Told about every send (the driver feeds records of topics the topology reads back to it). */
		private readonly onSend: (record: TestRecord<Buffer, Buffer>) => void,
		/** Told about the offsets of every committed transaction. */
		private readonly onCommit: (offsets: readonly OffsetAndMetadata[]) => void
	) {}

	/** Overridable, e.g. to watch or fail sends; transactional sends go through it too. */
	send(topic: string, message: ProducedMessage): Promise<SendResult> {
		const record: TestRecord<Buffer, Buffer> = {
			topic,
			key: message.key,
			value: message.value,
			partition: message.partition,
			timestamp: message.timestamp === undefined ? undefined : BigInt(message.timestamp.getTime()),
			headers: message.headers,
		}
		this.messages.push(record)
		this.open?.messages.push(record)
		this.onSend(record)
		const partition = message.partition ?? 0
		const key = tpKey({ topic, partition })
		const offset = this.offsets.get(key) ?? 0n
		this.offsets.set(key, offset + 1n)
		return Promise.resolve({ topic, partition, offset, timestamp: message.timestamp ?? new Date(0) })
	}

	/** One transaction at a time, like a transactional producer: its sends are recorded with it once it commits. */
	beginTransaction(): Promise<TransactionPort> {
		const open = { messages: [] as TestRecord<Buffer, Buffer>[], offsets: [] as OffsetAndMetadata[] }
		this.open = open
		const end = () => {
			if (this.open === open) this.open = undefined
		}
		return Promise.resolve({
			send: (topic, message) => this.send(topic, message),
			sendOffsets: async ({ offsets }) => {
				open.offsets.push(...offsets)
			},
			commit: async () => {
				end()
				this.transactions.push(open)
				this.onCommit(open.offsets)
			},
			abort: async () => end(),
		})
	}

	partitionCount(topic: string): Promise<number> {
		return Promise.resolve(this.partitions(topic))
	}

	close(): Promise<void> {
		return Promise.resolve()
	}

	/** Messages produced to a topic. */
	messagesFor(topic: string): TestRecord<Buffer, Buffer>[] {
		return this.messages.filter(m => m.topic === topic)
	}

	/** Forgets every recorded message and transaction. */
	clear(): void {
		this.messages.length = 0
		this.transactions.length = 0
	}
}

/** A delivered record's key, partition (default 0), timestamp (default: the mock wall clock) and headers. */
export interface SendOptions {
	key?: Buffer | null
	partition?: number
	timestamp?: bigint | number
	headers?: RecordHeaders
}

/** Sends records to the topology like a consumer would deliver them. */
export class MockConsumer {
	constructor(
		private readonly deliver: (topic: string, value: Buffer | null, options: SendOptions) => Promise<void>,
		/** The group's committed offsets by `topic:partition`. */
		readonly committed: ReadonlyMap<string, CommittedOffset>
	) {}

	/** Delivers one record and processes everything it leads to. */
	sendMessage(topic: string, value: Buffer | null, options: SendOptions = {}): Promise<void> {
		return this.deliver(topic, value, options)
	}
}
