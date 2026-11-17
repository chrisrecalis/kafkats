/**
 * The input offsets of one task (KS `StreamTask`'s consumed and committed offsets): where each input partition starts,
 * which partitions consumed records since the last commit, what was committed last, and the offsets the next commit
 * takes (4.8.3, KIP-1094).
 */
import type { TopicPartition } from '@kafkats/client'
import type { CommittedOffset, OffsetAndMetadata } from '@/runtime/io.js'
import type { RecordQueue } from '@/runtime/record-queue.js'
import { tpKey } from '@/runtime/task-id.js'

/** What the assignment says about one input partition. */
export interface PartitionStart {
	readonly committed: CommittedOffset | null
	readonly resolved: bigint
}

type Lag = (tp: TopicPartition) => bigint | undefined

export class TaskOffsets {
	private startsByKey: ReadonlyMap<string, PartitionStart> = new Map()
	/** Input partitions with a record processed (or skipped) since the last commit. */
	private readonly consumed = new Set<RecordQueue>()
	private readonly lastCommittedOffsets = new Map<string, bigint>()
	/** The last successful commit per input partition (offset and metadata), for a revive in place. */
	private readonly lastCommits = new Map<string, OffsetAndMetadata>()
	private readonly inputKeys: ReadonlySet<string>

	constructor(private readonly queues: readonly RecordQueue[]) {
		this.inputKeys = new Set(queues.map(queue => tpKey(queue.tp)))
	}

	get starts(): ReadonlyMap<string, PartitionStart> {
		return this.startsByKey
	}

	/** The assignment payload (created or resumed task): the committed offsets become the last committed ones. */
	setStarts(starts: ReadonlyMap<string, PartitionStart>): void {
		this.startsByKey = starts
		this.lastCommittedOffsets.clear()
		this.lastCommits.clear()
		for (const [key, start] of starts) {
			if (start.committed) {
				this.lastCommittedOffsets.set(key, start.committed.offset)
			}
		}
	}

	/**
	 * Starts of the next incarnation after a dirty close in place: per input partition the last successful commit of
	 * this task if any (else the assignment's), resolved at the last committed offset (else the assignment's).
	 */
	restartStarts(): ReadonlyMap<string, PartitionStart> {
		const starts = new Map<string, PartitionStart>()
		for (const queue of this.queues) {
			const key = tpKey(queue.tp)
			const start = this.startsByKey.get(key)
			const resolved = this.lastCommittedOffsets.get(key) ?? start?.resolved
			if (resolved === undefined) {
				continue
			}
			const commit = this.lastCommits.get(key)
			const committed = commit ? { offset: commit.offset, metadata: commit.metadata } : (start?.committed ?? null)
			starts.set(key, { committed, resolved })
		}
		return starts
	}

	/** Settled start positions of partitions without a commit become their last committed offsets. */
	setSettledPositions(positions: ReadonlyMap<string, bigint>): void {
		for (const [key, offset] of positions) {
			if (!this.lastCommittedOffsets.has(key)) {
				this.lastCommittedOffsets.set(key, offset)
			}
		}
	}

	/** A record of the queue was processed, or its remaining records skipped. */
	markConsumed(queue: RecordQueue): void {
		this.consumed.add(queue)
	}

	/** The queued records were dropped (suspend): they are fetched again from the committed offsets. */
	clearConsumed(): void {
		this.consumed.clear()
	}

	/** An empty input partition whose fetch position moved past its last commit (transaction markers). */
	anyMovedPastCommit(next: Lag): boolean {
		return this.queues.some(queue => this.movedPastCommit(queue, next))
	}

	/**
	 * Every input partition (`all`: the processor metadata changed), else the consumed ones and those whose fetch
	 * position moved past the last commit. The offset is the head of the queue, else the next offset the consumer
	 * delivers; never past a queued record.
	 */
	committable(all: boolean, next: Lag, metadata: (queue: RecordQueue) => string): OffsetAndMetadata[] {
		const offsets: OffsetAndMetadata[] = []
		for (const queue of this.queues) {
			if (!all && !this.consumed.has(queue) && !this.movedPastCommit(queue, next)) {
				continue
			}
			const offset = queue.headOffset ?? next(queue.tp)
			if (offset !== undefined) {
				const { topic, partition } = queue.tp
				offsets.push({ topic, partition, offset, metadata: metadata(queue) })
			}
		}
		return offsets
	}

	/** After a successful commit: nothing is consumed since; the committed offsets of the task's partitions are recorded. */
	committed(offsets: readonly OffsetAndMetadata[]): void {
		this.consumed.clear()
		for (const offset of offsets) {
			const key = tpKey(offset)
			if (this.inputKeys.has(key)) {
				this.lastCommittedOffsets.set(key, offset.offset)
				this.lastCommits.set(key, offset)
			}
		}
	}

	/** The offset a partition restarts from after a dirty close in place. */
	lastCommitted(tp: TopicPartition): bigint | undefined {
		return this.lastCommittedOffsets.get(tpKey(tp))
	}

	private movedPastCommit(queue: RecordQueue, next: Lag): boolean {
		if (queue.size > 0) {
			return false
		}
		const last = this.lastCommittedOffsets.get(tpKey(queue.tp))
		const position = last === undefined ? undefined : next(queue.tp)
		return position !== undefined && position > last!
	}
}
