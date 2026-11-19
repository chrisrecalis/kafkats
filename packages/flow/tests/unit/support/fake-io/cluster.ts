/**
 * The broker side of the fake IO: topic logs (with transactions and their markers) and the committed offsets of
 * consumer groups. The producer writes into it, the admin and the restore consumers read from it.
 */
import { ErrorCode, KafkaProtocolError, type Message } from '@kafkats/client'
import type { CommittedOffset } from '@/runtime/io.js'
import { deferred } from './steps.js'
import { message, type TestRecord } from './records.js'

/** A transaction as the log sees it: its records are stable once it is no longer open. */
export interface LoggedTransaction {
	readonly state: 'open' | 'committed' | 'aborted'
}

export type Isolation = 'read_committed' | 'read_uncommitted'

/** What a log stores of a record: a message without its position. */
export type LogRecord = Omit<Message<Buffer, Buffer>, 'topic' | 'partition' | 'offset'>

interface Entry {
	readonly offset: bigint
	/** Undefined for a transaction marker (a control record). */
	readonly message: Message<Buffer, Buffer> | undefined
	readonly transaction: LoggedTransaction | undefined
}

/** One partition's log: offsets from `start` (the log start) to `end` (the high watermark); markers take an offset. */
export class PartitionLog {
	/** Contiguous entries from `start`. */
	private entries: Entry[] = []
	private first = 0n

	constructor(
		readonly topic: string,
		readonly partition: number,
		private readonly changed: () => void
	) {}

	get start(): bigint {
		return this.first
	}

	get end(): bigint {
		return this.first + BigInt(this.entries.length)
	}

	/** The last stable offset: the first offset of a transaction still open, else the end. */
	get lastStable(): bigint {
		return this.entries.find(entry => entry.transaction?.state === 'open')?.offset ?? this.end
	}

	/** Appends a record (part of `transaction` if given) and returns its offset. */
	append(record: LogRecord, transaction?: LoggedTransaction): bigint {
		const offset = this.end
		this.entries.push({
			offset,
			message: { ...record, topic: this.topic, partition: this.partition, offset },
			transaction,
		})
		this.changed()
		return offset
	}

	/** Ends `transaction` in this partition: its commit or abort marker. */
	appendMarker(transaction: LoggedTransaction): void {
		this.entries.push({ offset: this.end, message: undefined, transaction })
		this.changed()
	}

	/**
	 * The records from `from` a consumer with `isolation` reads, at most `max`: up to the end (read_uncommitted, aborted
	 * records included) or the last stable offset (read_committed, aborted records skipped); never markers. `next` is
	 * the position after the entries read, skipped ones included. `from` is within the log (`start` to `end`).
	 */
	read(from: bigint, isolation: Isolation, max = Infinity): { messages: Message<Buffer, Buffer>[]; next: bigint } {
		const limit = isolation === 'read_committed' ? this.lastStable : this.end
		const messages: Message<Buffer, Buffer>[] = []
		let next = from
		while (next < limit && messages.length < max) {
			const entry = this.entries[Number(next - this.first)]!
			next++
			if (entry.message && !(isolation === 'read_committed' && entry.transaction?.state === 'aborted')) {
				messages.push(entry.message)
			}
		}
		return { messages, next }
	}

	/** Deletes the records before `offset` (DeleteRecords, retention): the log start moves there. */
	deleteBefore(offset: bigint): void {
		if (offset > this.end) {
			throw new KafkaProtocolError(ErrorCode.OffsetOutOfRange)
		}
		if (offset <= this.first) return
		this.entries = this.entries.slice(Number(offset - this.first))
		this.first = offset
		this.changed()
	}
}

export class FakeTopic {
	readonly partitions: PartitionLog[] = []

	constructor(
		readonly name: string,
		partitions: number,
		readonly configs: Readonly<Record<string, string>>,
		/** Created explicitly: exactly its partitions exist. Otherwise it grows to the partitions used. */
		readonly fixed: boolean,
		private readonly changed: () => void
	) {
		this.grow(partitions)
	}

	grow(partitions: number): void {
		while (this.partitions.length < partitions) {
			this.partitions.push(new PartitionLog(this.name, this.partitions.length, this.changed))
		}
	}
}

export class FakeCluster {
	readonly topics = new Map<string, FakeTopic>()
	private readonly groups = new Map<string, Map<string, CommittedOffset>>()
	private change = deferred()

	/** A topic with exactly `partitions` partitions; an existing topic stays as it is. */
	createTopic(name: string, partitions: number, configs: Readonly<Record<string, string>> = {}): FakeTopic {
		let topic = this.topics.get(name)
		if (!topic) {
			topic = new FakeTopic(name, partitions, configs, true, this.notify)
			this.topics.set(name, topic)
		}
		return topic
	}

	/** A topic nobody created springs into existence on first use with one partition (a lenient broker). */
	topic(name: string): FakeTopic {
		let topic = this.topics.get(name)
		if (!topic) {
			topic = new FakeTopic(name, 1, {}, false, this.notify)
			this.topics.set(name, topic)
		}
		return topic
	}

	/**
	 * The log of a partition. A topic nobody created grows to the partitions used; one that was created has exactly
	 * its partitions: another is UnknownTopicOrPartition.
	 */
	log(name: string, partition: number): PartitionLog {
		const topic = this.topic(name)
		if (!topic.fixed) topic.grow(partition + 1)
		const log = topic.partitions[partition]
		if (!log) {
			throw new KafkaProtocolError(ErrorCode.UnknownTopicOrPartition)
		}
		return log
	}

	/** Appends records as a producer without a transaction does; returns their offsets. */
	append(topic: string, partition: number, ...records: TestRecord[]): bigint[] {
		const log = this.log(topic, partition)
		return records.map(record => {
			const { key, value, timestamp, headers } = message(topic, partition, 0n, record)
			return log.append({ key, value, timestamp, headers })
		})
	}

	/** The committed offsets of a consumer group by `topic:partition`: offset commits and committed transactions. */
	offsets(groupId: string): Map<string, CommittedOffset> {
		let offsets = this.groups.get(groupId)
		if (!offsets) {
			offsets = new Map()
			this.groups.set(groupId, offsets)
		}
		return offsets
	}

	/** Resolves at the next change of a log (a record, a marker, a deletion). */
	changed(): Promise<void> {
		return this.change.promise
	}

	private readonly notify = (): void => {
		const change = this.change
		this.change = deferred()
		change.resolve()
	}
}
