/**
 * The records of one input partition of a task (KS `RecordQueue`): a FIFO of the delivered batches, decoded lazily
 * at the head, where the timestamp is extracted and records that cannot be processed are skipped.
 */
import type { Message, TopicPartition } from '@kafkats/client'
import { IllegalStateError, InvalidTimestampError, StreamsError } from '@/api/errors.js'
import type { Codec } from '@/codec.js'
import type { DroppedReason, TaskId } from '@/processor/api.js'
import { applyHandler } from '@/processor/node.js'
import { builtInRule, type TimestampExtractor, type TimestampRule } from '@/processor/timestamps.js'
import type { ResolvedDeserializationHandler } from '@/runtime/config.js'
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

/** Reports a dropped record: reason, source node, details for the log. */
export type DropListener = (reason: DroppedReason, source: string, detail: Readonly<Record<string, unknown>>) => void

/** Delivered batches are kept as they are; the array of batches is compacted once this many were read. */
const COMPACT_AFTER_BATCHES = 1024

type Decoded = Pick<StampedRecord, 'key' | 'value' | 'more'>

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
	private readonly rule: TimestampRule | undefined
	/** Every source of the topic decodes with the same codecs: decode once. */
	private readonly sameCodecs: boolean

	constructor(
		readonly tp: TopicPartition,
		private readonly sources: readonly SourceDef[],
		private readonly extractor: TimestampExtractor,
		private readonly handler: ResolvedDeserializationHandler,
		private readonly onDrop: DropListener,
		private readonly taskId: TaskId
	) {
		if (sources.length === 0) {
			throw new IllegalStateError(`no source node reads '${tp.topic}'`)
		}
		this.rule = builtInRule(extractor)
		const [first] = sources
		this.sameCodecs = sources.every(s => s.keyCodec === first!.keyCodec && s.valueCodec === first!.valueCodec)
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
			const decoded = this.decode(message)
			if (decoded === undefined) {
				lastSkipped = message
				continue
			}
			const timestamp = this.extract(message, decoded)
			if (timestamp < 0) {
				this.onDrop('negative-timestamp', this.sources[0]!.name, { ...position(message), timestamp })
				lastSkipped = message
				continue
			}
			this.headRecord = {
				queue: this,
				message,
				offset: message.offset,
				timestamp,
				key: decoded.key,
				value: decoded.value,
				more: decoded.more,
			}
			return
		}
		if (lastSkipped !== undefined) {
			this.headRecord = { skipped: true, queue: this, offset: lastSkipped.offset }
		}
	}

	/** Undefined: the deserialization handler answered 'continue' (the record is skipped). */
	private decode(message: Message<Buffer, Buffer>): Decoded | undefined {
		const { sources } = this
		let source = sources[0]!
		try {
			const key = decode(message.key, source.keyCodec)
			const value = decode(message.value, source.valueCodec)
			if (this.sameCodecs) {
				return {
					key,
					value,
					more: sources.length > 1 ? sources.slice(1).map(() => ({ key, value })) : undefined,
				}
			}
			const more: { key: unknown; value: unknown }[] = []
			for (let i = 1; i < sources.length; i++) {
				source = sources[i]!
				more.push({
					key: decode(message.key, source.keyCodec),
					value: decode(message.value, source.valueCodec),
				})
			}
			return { key, value, more }
		} catch (error) {
			const ctx = {
				...position(message),
				headers: message.headers,
				timestamp: Number(message.timestamp),
				processorNodeId: source.name,
				taskId: this.taskId,
				sourceRawKey: message.key,
				sourceRawValue: message.value,
			}
			applyHandler(
				() => this.handler(ctx, message, error),
				error,
				`deserialization of ${describe(message)} failed in '${source.name}'`,
				this.taskId
			)
			this.onDrop('deserialization', source.name, {
				...position(message),
				error: error instanceof Error ? error.message : String(error),
			})
			return undefined
		}
	}

	private extract(message: Message<Buffer, Buffer>, decoded: Decoded): number {
		const recordTimestamp = Number(message.timestamp)
		let timestamp: number
		try {
			if (this.rule) {
				return this.rule(recordTimestamp, this.partitionTime, message)
			}
			const record = {
				...position(message),
				key: decoded.key,
				value: decoded.value,
				timestamp: recordTimestamp,
				headers: message.headers,
			}
			timestamp = this.extractor(record, this.partitionTime)
		} catch (error) {
			if (error instanceof StreamsError) {
				throw error
			}
			throw new StreamsError(`the timestamp extractor failed for ${describe(message)}`, {
				cause: error,
				taskId: this.taskId,
			})
		}
		if (!Number.isSafeInteger(timestamp)) {
			throw new InvalidTimestampError(
				`the timestamp extractor returned ${timestamp} for ${describe(message)}, not an integer`,
				{ taskId: this.taskId }
			)
		}
		return timestamp
	}
}

/** No codec: the raw Buffer; null stays null. */
function decode(data: Buffer | null, codec: Codec<unknown> | undefined): unknown {
	return data === null || codec === undefined ? data : codec.decode(data)
}

function position(message: Message<Buffer, Buffer>): { topic: string; partition: number; offset: bigint } {
	return { topic: message.topic, partition: message.partition, offset: message.offset }
}

function describe(message: Message<Buffer, Buffer>): string {
	return `${message.topic}-${message.partition}@${message.offset}`
}
