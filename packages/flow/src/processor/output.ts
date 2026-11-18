import type { RecordHeaders } from '@/processor/record.js'

/** Who sends a record: a sink of the topology, a repartition sink or a changelog layer. */
export type SendKind = 'sink' | 'repartition' | 'changelog'

/** An encoded record on its way to the producer. */
export interface CollectedRecord {
	readonly topic: string
	readonly partition?: number
	readonly key: Buffer | null
	readonly value: Buffer | null
	/** Undefined for changelog records: the producer stamps them with its wall-clock time. */
	readonly timestamp?: number
	readonly headers?: RecordHeaders
}

/**
 * The output of one task incarnation, used by sinks and changelog layers. Implemented by the runtime's
 * record collector.
 */
export interface TaskOutput {
	/**
	 * Not awaited. Accepted while the task is RUNNING or initializing (its processors' `init()`). Dropped with
	 * a warning (never sent, never begins a transaction) otherwise or once the collector closed.
	 */
	send(kind: SendKind, record: CollectedRecord): void
	/** Synchronous; known for static sink topics from the start, refreshed in the background. */
	partitionCount(topic: string): number | undefined
	/** Loads the partition count of a topic named by a TopicNameExtractor on a cache miss. */
	loadPartitionCount(topic: string): Promise<number>
	/**
	 * The task closed (Kafka Streams closes a task's RecordCollector with it): what the output kept for it, such as the
	 * changelog offsets acknowledged since its last commit, is dropped, and acknowledgements arriving later are ignored.
	 */
	close(): void
}
