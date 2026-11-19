/**
 * The TestDriver's deterministic ports (4.14): a mock wall clock, the group consumer as the task manager and the
 * commit coordinator use it, and the partition counts of the driver's topics. Pause, resume and seek do nothing
 * (records reach tasks only through the driver); the next offset of a partition is a dense counter of the records
 * delivered to it; the lag is always 0.
 */
import type { ConsumerGroupMetadata, Message, TopicPartition } from '@kafkats/client'
import type { RecordHeaders } from '@/processor/record.js'
import type { Clock, CommittedOffset, GroupConsumerPort, OffsetAndMetadata } from '@/runtime/io.js'
import { tpKey } from '@/runtime/task-id.js'
import type { BuiltTopology } from '@/topology/built-topology.js'
import type { PartitionCounts } from '@/topology/nodes.js'
import { resolvePartitionCounts } from '@/topology/partition-counts.js'

export class TestClock implements Clock {
	private timers: { at: number; fn: () => void }[] = []

	constructor(private time: number) {}

	now(): number {
		return this.time
	}

	setTimer(at: number, fn: () => void): () => void {
		const timer = { at, fn }
		this.timers.push(timer)
		return () => {
			this.timers = this.timers.filter(t => t !== timer)
		}
	}

	/** Moves the clock forward and fires the timers now due, in time order. */
	advance(ms: number): void {
		this.time += ms
		const due = this.timers.filter(timer => timer.at <= this.time).sort((a, b) => a.at - b.at)
		this.timers = this.timers.filter(timer => timer.at > this.time)
		for (const timer of due) timer.fn()
	}
}

/** A record as the driver hands it to the consumer. */
export interface InputRecord {
	readonly partition: number
	readonly key: Buffer | null
	readonly value: Buffer | null
	readonly timestamp: number
	readonly headers: RecordHeaders
}

export type TestConsumerPort = Pick<
	GroupConsumerPort,
	'pause' | 'resume' | 'seek' | 'nextOffset' | 'currentLag' | 'commit' | 'groupMetadata' | 'rebalancePending'
>

export class TestConsumer implements TestConsumerPort {
	readonly rebalancePending = false
	/** The group's committed offsets by `topic:partition`: offset commits and committed transactions. */
	readonly committed = new Map<string, CommittedOffset>()
	private readonly group: ConsumerGroupMetadata
	private readonly positions = new Map<string, bigint>()

	constructor(groupId: string) {
		this.group = { groupId, generationId: 1, memberId: 'test-driver' }
	}

	pause(): void {}

	resume(): void {}

	seek(): void {}

	nextOffset(tp: TopicPartition): bigint {
		return this.positions.get(tpKey(tp)) ?? 0n
	}

	currentLag(): bigint {
		return 0n
	}

	commit(offsets: readonly OffsetAndMetadata[]): Promise<void> {
		this.record(offsets)
		return Promise.resolve()
	}

	groupMetadata(): ConsumerGroupMetadata {
		return this.group
	}

	/** Offsets committed by a transaction or an offset commit. */
	record(offsets: readonly OffsetAndMetadata[]): void {
		for (const { topic, partition, offset, metadata } of offsets) {
			this.committed.set(tpKey({ topic, partition }), { offset, metadata })
		}
	}

	/** The message of the next offset of the record's partition. */
	deliver(topic: string, record: InputRecord): Message<Buffer, Buffer> {
		const tp = { topic, partition: record.partition }
		const offset = this.nextOffset(tp)
		this.positions.set(tpKey(tp), offset + 1n)
		return {
			topic,
			partition: record.partition,
			offset,
			timestamp: BigInt(record.timestamp),
			key: record.key,
			value: record.value as Buffer,
			headers: record.headers,
		}
	}
}

/**
 * Partition counts of the driver's topics. A topic listed in `TestDriverConfig.partitions` has that many partitions;
 * every other topic the topology reads has as many as the records sent to such topics need (the same number for all
 * of them, so they stay co-partitioned). Internal topics follow from these (4.1.3), as at a real start.
 */
export class TestPartitions {
	private readonly current = new Map<string, number>()
	/** Partition count of the topics without a configured one. */
	private width = 1

	constructor(
		private readonly topology: BuiltTopology,
		private readonly configured: Readonly<Record<string, number>>
	) {
		this.resolve()
	}

	/** The counts the tasks are created with; grown in place by `ensure()`. */
	get counts(): PartitionCounts {
		return this.current
	}

	/** The partition count a sink's partitioner sees. */
	reported(topic: string): number {
		return this.configured[topic] ?? this.current.get(topic) ?? 1
	}

	/** Makes room for a record of partition `partition` of a topic the topology reads. */
	ensure(topic: string, partition: number): void {
		const count = this.current.get(topic)
		if (count === undefined || partition < count) {
			return
		}
		if (topic in this.configured || !this.isExternal(topic)) {
			throw new RangeError(
				`Topic '${topic}' has ${count} partitions in this test, so it has no partition ${partition}`
			)
		}
		this.width = partition + 1
		this.resolve()
	}

	private resolve(): void {
		const external = new Map<string, number>()
		for (const topic of this.externalTopics()) {
			external.set(topic, this.configured[topic] ?? this.width)
		}
		const counts = resolvePartitionCounts(this.topology, external)
		this.current.clear()
		for (const [topic, count] of counts) this.current.set(topic, count)
	}

	/** Source topics that are not repartition topics, and global topics. */
	private externalTopics(): string[] {
		const sources = this.topology.subtopologies.flatMap(s => [...s.sourceTopics])
		const globals = this.topology.globalStores.map(g => g.topic)
		return [...sources, ...globals].filter(topic => this.isExternal(topic))
	}

	private isExternal(topic: string): boolean {
		return this.topology.internalTopics.get(topic)?.kind !== 'repartition'
	}
}
