/**
 * The record queues of one task (KS `PartitionGroup`): timestamp-ordered selection across the input partitions,
 * KIP-695 readiness with `maxTaskIdleMs`, partition times and the task's stream time.
 */
import type { Message, TopicPartition } from '@kafkats/client'
import { IllegalStateError } from '@/api/errors.js'
import { isSkipMarker, type RecordQueue, type SkipMarker, type StampedRecord } from '@/runtime/record-queue.js'
import { tpKey } from '@/runtime/task-id.js'

/** Kafka Streams' MAX_TASK_IDLE_MS_DISABLED: process whatever is buffered, never wait for other partitions. */
const IDLING_DISABLED = -1

export class PartitionGroup {
	private time = -1
	private readonly byKey = new Map<string, RecordQueue>()
	/** When each empty partition with zero lag stops being waited for. */
	private readonly deadlines = new Map<RecordQueue, number>()
	/** The deadline the last readiness check waited for. */
	private idlingUntil: number | undefined

	constructor(
		private readonly queues: readonly RecordQueue[],
		private readonly maxTaskIdleMs: number
	) {
		for (const queue of queues) {
			this.byKey.set(tpKey(queue.tp), queue)
		}
	}

	addRawRecords(tp: TopicPartition, messages: readonly Message<Buffer, Buffer>[]): number {
		return this.queue(tp).addRawRecords(messages)
	}

	/**
	 * KIP-695 (KS `PartitionGroup.readyToProcess`): a task waits for a partition without buffered records while its
	 * lag is unknown or positive, and for up to `maxTaskIdleMs` once it is 0; then it processes what it has
	 * (enforced processing, possibly out of timestamp order). Disabled idling or a single partition: ready iff
	 * something is buffered.
	 */
	readyToProcess(now: number, lag: (tp: TopicPartition) => bigint | undefined): boolean {
		this.idlingUntil = undefined
		if (this.maxTaskIdleMs === IDLING_DISABLED || this.queues.length === 1) {
			return this.queues.some(queue => queue.size > 0)
		}
		let queued = false
		let enforced = false
		for (const queue of this.queues) {
			if (queue.size > 0) {
				this.deadlines.delete(queue)
				queued = true
				continue
			}
			const fetchedLag = lag(queue.tp)
			if (fetchedLag === undefined || fetchedLag > 0n) {
				// Waiting to fetch the partition, or for records known to be on the broker.
				this.deadlines.delete(queue)
				return false
			}
			let deadline = this.deadlines.get(queue)
			if (deadline === undefined) {
				deadline = now + this.maxTaskIdleMs
				this.deadlines.set(queue, deadline)
			}
			if (now < deadline) {
				this.idlingUntil = deadline
				return false
			}
			enforced = true
		}
		return !enforced || queued
	}

	/**
	 * The head with the smallest timestamp (ties: input partition order); advances the stream time. The partition
	 * polled had records, so it is not idling: its deadline goes, even if no readiness check saw it non-empty (records
	 * that arrived and drained within one slice), and its next idle period starts afresh (KS checks before every record).
	 */
	nextRecord(): StampedRecord | SkipMarker | null {
		let next: RecordQueue | undefined
		let nextTimestamp = Infinity
		for (const queue of this.queues) {
			if (queue.size === 0) {
				continue
			}
			const timestamp = queue.headTimestamp!
			if (timestamp < nextTimestamp) {
				next = queue
				nextTimestamp = timestamp
			}
		}
		if (next === undefined) {
			return null
		}
		this.deadlines.delete(next)
		const record = next.poll()
		if (!isSkipMarker(record) && record.timestamp > this.time) {
			this.time = record.timestamp
		}
		return record
	}

	/** Max timestamp polled and seeded partition times, -1 unknown. */
	get streamTime(): number {
		return this.time
	}

	/** Seeds a partition time (from commit metadata); raises the stream time too. */
	setPartitionTime(tp: TopicPartition, time: number): void {
		this.queue(tp).partitionTime = time
		if (time > this.time) {
			this.time = time
		}
	}

	partitionTime(tp: TopicPartition): number {
		return this.queue(tp).partitionTime
	}

	queue(tp: TopicPartition): RecordQueue {
		const queue = this.byKey.get(tpKey(tp))
		if (!queue) {
			throw new IllegalStateError(`${tp.topic}-${tp.partition} is not an input partition of this task`)
		}
		return queue
	}

	get numBuffered(): number {
		let buffered = 0
		for (const queue of this.queues) {
			buffered += queue.size
		}
		return buffered
	}

	/** When the last readiness check may change by time alone (an idling deadline); undefined if not idling. */
	nextIdleDeadline(): number | undefined {
		return this.idlingUntil
	}

	clear(): void {
		for (const queue of this.queues) {
			queue.clear()
		}
		this.time = -1
		this.deadlines.clear()
		this.idlingUntil = undefined
	}
}
