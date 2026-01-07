/**
 * Record processor abstraction for unified fetch loop handling
 *
 * Provides a strategy pattern for processing records in different modes (each/batch).
 */

import type { DecodedRecord } from '@/protocol/records/index.js'
import { buildDecoderMaps, decodeRecord } from './message-decoder.js'
import type {
	Message,
	ConsumeContext,
	BatchConsumeContext,
	MessageHandler,
	BatchHandler,
	TopicSubscription,
} from './types.js'
import type { OffsetManager } from './offset-manager.js'

/**
 * Callback type for processing fetched records
 */
export type FetchCallback = (topic: string, partition: number, records: DecodedRecord[]) => Promise<void>

/**
 * Strategy interface for processing a batch of records from a fetch
 */
export interface RecordProcessor {
	/**
	 * Process a batch of decoded messages from a single fetch
	 */
	processBatch(
		messages: Message<unknown, unknown>[],
		topic: string,
		partition: number,
		signal: AbortSignal,
		isRunning: () => boolean
	): Promise<void>
}

/**
 * Create a fetch callback that decodes records and delegates to a processor
 */
export function createFetchCallback(
	subscriptions: TopicSubscription<unknown, unknown>[],
	processor: RecordProcessor,
	signal: AbortSignal,
	isRunning: () => boolean
): FetchCallback {
	const decoders = buildDecoderMaps(subscriptions)

	return async (topic, partition, records) => {
		if (records.length === 0 || signal.aborted || !isRunning()) {
			return
		}

		// Decode all records into messages
		const messages = records.map(record => decodeRecord(topic, partition, record, decoders))

		// Delegate to processor
		await processor.processBatch(messages, topic, partition, signal, isRunning)
	}
}

/**
 * Error handler callback type
 */
export type ProcessorErrorHandler = (error: Error) => void

/**
 * Processor for "each" mode - processes one message at a time
 */
export class EachRecordProcessor implements RecordProcessor {
	constructor(
		private readonly handler: MessageHandler<unknown, unknown>,
		private readonly offsetManager: OffsetManager,
		private readonly commitOffsets: boolean,
		private readonly onError: ProcessorErrorHandler
	) {}

	async processBatch(
		messages: Message<unknown, unknown>[],
		topic: string,
		partition: number,
		signal: AbortSignal,
		isRunning: () => boolean
	): Promise<void> {
		for (const message of messages) {
			if (signal.aborted || !isRunning()) {
				break
			}

			const ctx: ConsumeContext = {
				signal,
				topic,
				partition,
				offset: message.offset,
			}

			try {
				await this.handler(message, ctx)

				if (this.commitOffsets) {
					this.offsetManager.markConsumed(message.topic, message.partition, message.offset)
				}
			} catch (error) {
				this.onError(error as Error)
				throw error
			}
		}
	}
}

/**
 * Processor for "batch" mode - passes entire batch to handler
 */
export class BatchRecordProcessor implements RecordProcessor {
	constructor(
		private readonly handler: BatchHandler<unknown, unknown>,
		private readonly offsetManager: OffsetManager,
		private readonly commitOffsets: boolean,
		private readonly onError: ProcessorErrorHandler
	) {}

	async processBatch(
		messages: Message<unknown, unknown>[],
		topic: string,
		partition: number,
		signal: AbortSignal
	): Promise<void> {
		if (messages.length === 0) {
			return
		}

		const firstMessage = messages[0]!
		const lastMessage = messages[messages.length - 1]!

		// Track offsets resolved by the handler
		const resolvedOffsets = new Set<bigint>()

		const ctx: BatchConsumeContext = {
			signal,
			topic,
			partition,
			offset: lastMessage.offset,
			firstOffset: firstMessage.offset,
			lastOffset: lastMessage.offset,
			resolveOffset: (offset: bigint) => {
				resolvedOffsets.add(offset)
			},
		}

		try {
			await this.handler(messages, ctx)

			// Mark offsets as consumed after handler succeeds
			if (this.commitOffsets) {
				if (resolvedOffsets.size > 0) {
					// User explicitly resolved some offsets - only commit those
					for (const msg of messages) {
						if (resolvedOffsets.has(msg.offset)) {
							this.offsetManager.markConsumed(msg.topic, msg.partition, msg.offset)
						}
					}
				} else {
					// No explicit resolves - commit all (backward compatible)
					for (const msg of messages) {
						this.offsetManager.markConsumed(msg.topic, msg.partition, msg.offset)
					}
				}
			}
		} catch (error) {
			// On error, still commit any offsets that were explicitly resolved
			// This enables partial progress even on failure
			if (this.commitOffsets && resolvedOffsets.size > 0) {
				for (const msg of messages) {
					if (resolvedOffsets.has(msg.offset)) {
						this.offsetManager.markConsumed(msg.topic, msg.partition, msg.offset)
					}
				}
			}
			this.onError(error as Error)
			throw error
		}
	}
}
