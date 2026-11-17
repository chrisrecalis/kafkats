/**
 * The records of one input partition of a task (KS `RecordQueue`): a FIFO of the delivered batches, decoded lazily
 * at the head by a RecordDecoder, which extracts the timestamp and skips records that cannot be processed.
 */
import type { Message, TopicPartition } from '@kafkats/client'
import { IllegalStateError } from '@/api/errors.js'
import type { TaskId } from '@/processor/api.js'
import type { TimestampExtractor } from '@/processor/timestamps.js'
import type { ResolvedDeserializationHandler } from '@/runtime/config.js'
import { RecordDecoder, type DropListener } from '@/runtime/record-decoder.js'
import type { SourceDef } from '@/topology/nodes.js'

/** A decoded record at the head of its queue, with its extracted timestamp. */
export interface StampedRecord {
	readonly queue: RecordQueue
	/** Raw, as delivered. */
	readonly message: Message<Buffer, Buffer>
	readonly offset: bigint
	/** Extracted. */
	readonly timestamp: number
	/** Decoded for the first source node of the topic. */
	readonly key: unknown
	readonly value: unknown
	/** Decoded for the other source nodes of the topic, in registration order (rare). */
	readonly more?: readonly { key: unknown; value: unknown }[]
}

/**
 * The head of a queue whose remaining records were all skipped (KS `CorruptedRecord`, KAFKA-6502): processing it
 * only advances the consumed offset to the last skipped record.
 */
export interface SkipMarker {
	readonly skipped: true
	readonly queue: RecordQueue
	readonly offset: bigint
}

export function isSkipMarker(record: StampedRecord | SkipMarker): record is SkipMarker {
	return (record as SkipMarker).skipped === true
}

/** Delivered batches are kept as they are; the array of batches is compacted once this many were read. */
const COMPACT_AFTER_BATCHES = 1024

export class RecordQueue {
	/** Max timestamp polled from this partition, -1 unknown. */
	partitionTime = -1
	private batches: (readonly Message<Buffer, Buffer>[])[] = []
	/** The batch being read and the read index in it. */
	private batch = 0
	private index = 0
	/** Delivered messages not yet taken to the head. */
	private pending = 0
	private headRecord: StampedRecord | SkipMarker | null = null
	private readonly decoder: RecordDecoder

	constructor(
		readonly tp: TopicPartition,
		sources: readonly SourceDef[],
		extractor: TimestampExtractor,
		handler: ResolvedDeserializationHandler,
		onDrop: DropListener,
		taskId: TaskId
	) {
		if (sources.length === 0) {
			throw new IllegalStateError(`no source node reads '${tp.topic}'`)
		}
		this.decoder = new RecordDecoder(sources, extractor, handler, onDrop, taskId)
	}

	/** O(1): the batch is kept, not copied. Returns the queue size after the add. */
	addRawRecords(messages: readonly Message<Buffer, Buffer>[]): number {
		if (messages.length > 0) {
			this.batches.push(messages)
			this.pending += messages.length
		}
		return this.size
	}

	/** Decodes up to the next processable record; a SkipMarker if every remaining record was skipped. */
	head(): StampedRecord | SkipMarker | null {
		if (this.headRecord === null && this.pending > 0) {
			this.updateHead()
		}
		return this.headRecord
	}

	/** Removes the head; a record (not a SkipMarker) raises the partition time. */
	poll(): StampedRecord | SkipMarker {
		const head = this.head()
		if (head === null) {
			throw new IllegalStateError(`poll() of the empty record queue of ${this.tp.topic}-${this.tp.partition}`)
		}
		this.headRecord = null
		if (!isSkipMarker(head) && head.timestamp > this.partitionTime) {
			this.partitionTime = head.timestamp
		}
		return head
	}

	get size(): number {
		return this.pending + (this.headRecord === null ? 0 : 1)
	}

	/** -1 for a SkipMarker, so it is polled first. */
	get headTimestamp(): number | undefined {
		const head = this.head()
		return head === null ? undefined : isSkipMarker(head) ? -1 : head.timestamp
	}

	/** Offset of the first record not processed yet (without decoding). */
	get headOffset(): bigint | undefined {
		if (this.headRecord !== null) {
			return this.headRecord.offset
		}
		return this.pending > 0 ? this.batches[this.batch]![this.index]!.offset : undefined
	}

	clear(): void {
		this.batches = []
		this.batch = 0
		this.index = 0
		this.pending = 0
		this.headRecord = null
		this.partitionTime = -1
	}

	private take(): Message<Buffer, Buffer> {
		const batch = this.batches[this.batch]!
		const message = batch[this.index++]!
		this.pending--
		if (this.index === batch.length) {
			this.index = 0
			this.batch++
			if (this.pending === 0) {
				this.batches = []
				this.batch = 0
			} else if (this.batch >= COMPACT_AFTER_BATCHES) {
				this.batches.splice(0, this.batch)
				this.batch = 0
			}
		}
		return message
	}

	private updateHead(): void {
		let lastSkipped: Message<Buffer, Buffer> | undefined
		while (this.pending > 0) {
			const message = this.take()
			const decoded = this.decoder.decode(message, this.partitionTime)
			if (decoded === undefined) {
				lastSkipped = message
				continue
			}
			const { key, value, timestamp, more } = decoded
			this.headRecord = { queue: this, message, offset: message.offset, timestamp, key, value, more }
			return
		}
		if (lastSkipped !== undefined) {
			this.headRecord = { skipped: true, queue: this, offset: lastSkipped.offset }
		}
	}
}
