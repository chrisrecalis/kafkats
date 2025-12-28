/**
 * RecordAccumulator - batches messages by topic-partition
 *
 * Groups messages destined for the same partition and flushes them
 * when either the linger timer expires or the batch size is exceeded.
 */

import { EventEmitter } from 'node:events'
import type { QueuedMessage, PartitionBatch, ResolvedProducerConfig } from './types.js'
import type { Logger } from '@/logger.js'

interface AccumulatorEvents {
	batchReady: [batch: PartitionBatch]
	drain: []
}

/**
 * RecordAccumulator groups messages by topic-partition and manages batching
 *
 * Batches are flushed when:
 * 1. lingerMs timer expires (time-based flush)
 * 2. Batch size exceeds maxBatchBytes (size-based flush)
 * 3. flush() is called explicitly
 */
export class RecordAccumulator extends EventEmitter<AccumulatorEvents> {
	private readonly batches = new Map<string, PartitionBatch>() // key: "topic:partition"
	private readonly timers = new Map<string, ReturnType<typeof setTimeout>>()
	private readonly config: ResolvedProducerConfig
	private readonly logger: Logger
	private draining = false
	private pendingBatches = 0
	private fenced = false
	private suppressFlush = false // When true, don't auto-flush on size limit

	constructor(config: ResolvedProducerConfig, logger: Logger) {
		super()
		this.config = config
		this.logger = logger
	}

	/**
	 * Begin a batch append transaction
	 *
	 * While in transaction mode, batches won't auto-flush when exceeding
	 * maxBatchBytes. Call endAppendTransaction() to flush all ready batches.
	 * This allows a single send([array]) to collect all messages before flushing.
	 */
	beginAppendTransaction(): void {
		this.suppressFlush = true
	}

	/**
	 * End batch append transaction and flush all batches
	 */
	endAppendTransaction(): void {
		this.suppressFlush = false
		this.flush()
	}

	/**
	 * Set fenced state
	 *
	 * When fenced, new messages are rejected immediately.
	 */
	setFenced(fenced: boolean): void {
		this.fenced = fenced
	}

	/**
	 * Append a message to the appropriate batch
	 */
	append(message: QueuedMessage): void {
		if (this.fenced) {
			message.reject(new Error('Producer is fenced, re-initialization in progress'))
			return
		}

		const key = `${message.topic}:${message.partition}`

		let batch = this.batches.get(key)
		if (!batch) {
			batch = {
				topic: message.topic,
				partition: message.partition,
				messages: [],
				sizeBytes: 0,
				createdAt: Date.now(),
			}
			this.batches.set(key, batch)

			if (this.config.lingerMs > 0) {
				const timer = setTimeout(() => {
					this.flushBatch(key)
				}, this.config.lingerMs)
				timer.unref() // Don't prevent process exit
				this.timers.set(key, timer)
			}
		}

		const messageSize = this.estimateMessageSize(message)
		batch.messages.push(message)
		batch.sizeBytes += messageSize

		if (!this.suppressFlush && batch.sizeBytes >= this.config.maxBatchBytes) {
			this.flushBatch(key)
			return
		}

		if (!this.suppressFlush && this.config.lingerMs === 0) {
			this.flushBatch(key)
		}
	}

	/**
	 * Flush a specific batch by key
	 */
	private flushBatch(key: string): void {
		const batch = this.batches.get(key)
		if (!batch || batch.messages.length === 0) {
			return
		}

		const timer = this.timers.get(key)
		if (timer) {
			clearTimeout(timer)
			this.timers.delete(key)
		}

		this.batches.delete(key)
		this.pendingBatches++

		this.logger.debug('flushing batch', {
			topic: batch.topic,
			partition: batch.partition,
			messageCount: batch.messages.length,
			sizeBytes: batch.sizeBytes,
		})

		this.emit('batchReady', batch)
	}

	/**
	 * Flush all batches
	 */
	flush(): void {
		for (const key of this.batches.keys()) {
			this.flushBatch(key)
		}
	}

	/**
	 * Drain all batches and wait for completion
	 *
	 * Call this before disconnecting to ensure all messages are sent.
	 */
	async drain(): Promise<void> {
		this.draining = true
		this.flush()

		// If there are pending batches, wait for them
		if (this.pendingBatches > 0) {
			await new Promise<void>(resolve => {
				this.once('drain', resolve)
			})
		}

		this.draining = false
	}

	/**
	 * Signal that a batch has been processed
	 *
	 * Call this after sending a batch (success or failure).
	 */
	batchCompleted(): void {
		this.pendingBatches--

		if (this.draining && this.pendingBatches === 0 && this.batches.size === 0) {
			this.emit('drain')
		}
	}

	/**
	 * Estimate the size of a message in bytes
	 */
	private estimateMessageSize(message: QueuedMessage): number {
		let size = 0
		if (message.key) size += message.key.length
		if (message.value) size += message.value.length
		for (const value of Object.values(message.headers)) {
			size += value.length
		}
		// Add overhead for record encoding (attributes, offsets, lengths, varints)
		size += 50
		return size
	}

	/**
	 * Clear all batches and timers without sending
	 *
	 * Use this on disconnect to clean up. Pending messages will be rejected.
	 */
	clear(): void {
		for (const timer of this.timers.values()) {
			clearTimeout(timer)
		}
		this.timers.clear()
		this.batches.clear()
	}

	/**
	 * Clear all batches and reject their messages with an error
	 *
	 * Used during fencing recovery to fail all accumulated batches.
	 */
	clearWithRejection(error: Error): void {
		for (const timer of this.timers.values()) {
			clearTimeout(timer)
		}
		this.timers.clear()

		for (const batch of this.batches.values()) {
			for (const msg of batch.messages) {
				msg.reject(error)
			}
		}
		this.batches.clear()
	}

	/**
	 * Get number of batches awaiting flush
	 */
	get batchCount(): number {
		return this.batches.size
	}

	/**
	 * Get number of batches currently being sent
	 */
	get pendingCount(): number {
		return this.pendingBatches
	}
}
