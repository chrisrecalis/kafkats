import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { RecordAccumulator } from '@/producer/accumulator.js'
import type { QueuedMessage, PartitionBatch, ResolvedProducerConfig } from '@/producer/types.js'
import type { Logger } from '@/logger.js'

function createConfig(overrides: Partial<ResolvedProducerConfig> = {}): ResolvedProducerConfig {
	return {
		lingerMs: 5,
		maxBatchBytes: 16384,
		acks: -1,
		retries: 3,
		retryBackoffMs: 100,
		maxRetryBackoffMs: 1000,
		compression: 0,
		partitioner: () => 0,
		requestTimeoutMs: 30000,
		transactionalId: null,
		idempotent: false,
		maxInFlight: 5,
		transactionTimeoutMs: 60000,
		...overrides,
	}
}

function createLogger(): Logger {
	const logger: Logger = {
		debug: vi.fn(),
		info: vi.fn(),
		warn: vi.fn(),
		error: vi.fn(),
		child: () => logger,
	}
	return logger
}

function createMessage(topic: string, partition: number, value: string, key?: string): QueuedMessage {
	return {
		topic,
		partition,
		key: key ? Buffer.from(key) : null,
		value: Buffer.from(value),
		headers: {},
		timestamp: BigInt(Date.now()),
		resolve: vi.fn(),
		reject: vi.fn(),
	}
}

describe('RecordAccumulator', () => {
	beforeEach(() => {
		vi.useFakeTimers()
	})

	afterEach(() => {
		vi.useRealTimers()
	})

	describe('basic batching', () => {
		it('creates a batch for the first message', () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())
			const message = createMessage('topic', 0, 'value')

			accumulator.append(message)

			expect(accumulator.batchCount).toBe(1)
		})

		it('groups messages by topic-partition', () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())

			accumulator.append(createMessage('topic', 0, 'v1'))
			accumulator.append(createMessage('topic', 0, 'v2'))
			accumulator.append(createMessage('topic', 1, 'v3'))
			accumulator.append(createMessage('other', 0, 'v4'))

			// 3 unique topic-partition combinations
			expect(accumulator.batchCount).toBe(3)
		})

		it('emits batchReady when flushed', () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())
			const batches: PartitionBatch[] = []
			accumulator.on('batchReady', batch => batches.push(batch))

			accumulator.append(createMessage('topic', 0, 'v1'))
			accumulator.append(createMessage('topic', 0, 'v2'))
			accumulator.flush()

			expect(batches).toHaveLength(1)
			expect(batches[0]!.topic).toBe('topic')
			expect(batches[0]!.partition).toBe(0)
			expect(batches[0]!.messages).toHaveLength(2)
		})

		it('estimates message size including overhead', () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())
			const batches: PartitionBatch[] = []
			accumulator.on('batchReady', batch => batches.push(batch))

			const message = createMessage('topic', 0, 'hello')
			accumulator.append(message)
			accumulator.flush()

			// Size should be: value(5) + overhead(50) = 55
			expect(batches[0]!.sizeBytes).toBe(55)
		})

		it('includes key and headers in size estimation', () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())
			const batches: PartitionBatch[] = []
			accumulator.on('batchReady', batch => batches.push(batch))

			const message: QueuedMessage = {
				topic: 'topic',
				partition: 0,
				key: Buffer.from('key'),
				value: Buffer.from('value'),
				headers: { h1: Buffer.from('header') },
				timestamp: BigInt(Date.now()),
				resolve: vi.fn(),
				reject: vi.fn(),
			}
			accumulator.append(message)
			accumulator.flush()

			// Size should be: key(3) + value(5) + header(6) + overhead(50) = 64
			expect(batches[0]!.sizeBytes).toBe(64)
		})
	})

	describe('flush triggers', () => {
		it('flushes immediately when lingerMs is 0', () => {
			const config = createConfig({ lingerMs: 0 })
			const accumulator = new RecordAccumulator(config, createLogger())
			const batches: PartitionBatch[] = []
			accumulator.on('batchReady', batch => batches.push(batch))

			accumulator.append(createMessage('topic', 0, 'value'))

			expect(batches).toHaveLength(1)
			expect(accumulator.batchCount).toBe(0)
		})

		it('flushes when batch size exceeds maxBatchBytes', () => {
			const config = createConfig({ maxBatchBytes: 100, lingerMs: 1000 })
			const accumulator = new RecordAccumulator(config, createLogger())
			const batches: PartitionBatch[] = []
			accumulator.on('batchReady', batch => batches.push(batch))

			// First message: ~55 bytes, should accumulate
			accumulator.append(createMessage('topic', 0, 'value'))
			expect(batches).toHaveLength(0)

			// Second message: ~55 bytes more, total ~110 > 100, should flush
			accumulator.append(createMessage('topic', 0, 'value'))
			expect(batches).toHaveLength(1)
		})

		it('flushes when lingerMs timer expires', async () => {
			const config = createConfig({ lingerMs: 100 })
			const accumulator = new RecordAccumulator(config, createLogger())
			const batches: PartitionBatch[] = []
			accumulator.on('batchReady', batch => batches.push(batch))

			accumulator.append(createMessage('topic', 0, 'value'))
			expect(batches).toHaveLength(0)

			vi.advanceTimersByTime(100)
			expect(batches).toHaveLength(1)
		})

		it('cancels timer when batch is flushed manually', () => {
			const config = createConfig({ lingerMs: 100 })
			const accumulator = new RecordAccumulator(config, createLogger())
			const batches: PartitionBatch[] = []
			accumulator.on('batchReady', batch => batches.push(batch))

			accumulator.append(createMessage('topic', 0, 'value'))
			accumulator.flush()
			expect(batches).toHaveLength(1)

			// Timer should be cancelled, no double flush
			vi.advanceTimersByTime(100)
			expect(batches).toHaveLength(1)
		})

		it('flush() flushes all batches', () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())
			const batches: PartitionBatch[] = []
			accumulator.on('batchReady', batch => batches.push(batch))

			accumulator.append(createMessage('topic1', 0, 'v1'))
			accumulator.append(createMessage('topic2', 0, 'v2'))
			accumulator.append(createMessage('topic1', 1, 'v3'))

			accumulator.flush()

			expect(batches).toHaveLength(3)
			expect(accumulator.batchCount).toBe(0)
		})

		it('does not flush empty batches', () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())
			const batches: PartitionBatch[] = []
			accumulator.on('batchReady', batch => batches.push(batch))

			accumulator.flush()

			expect(batches).toHaveLength(0)
		})
	})

	describe('append transaction', () => {
		it('suppresses auto-flush during append transaction', () => {
			const config = createConfig({ maxBatchBytes: 100 })
			const accumulator = new RecordAccumulator(config, createLogger())
			const batches: PartitionBatch[] = []
			accumulator.on('batchReady', batch => batches.push(batch))

			accumulator.beginAppendTransaction()

			// Would normally trigger flush at ~110 bytes
			accumulator.append(createMessage('topic', 0, 'value'))
			accumulator.append(createMessage('topic', 0, 'value'))
			accumulator.append(createMessage('topic', 0, 'value'))

			expect(batches).toHaveLength(0)
			expect(accumulator.batchCount).toBe(1)
		})

		it('flushes all batches when append transaction ends', () => {
			const config = createConfig({ maxBatchBytes: 100 })
			const accumulator = new RecordAccumulator(config, createLogger())
			const batches: PartitionBatch[] = []
			accumulator.on('batchReady', batch => batches.push(batch))

			accumulator.beginAppendTransaction()
			accumulator.append(createMessage('topic', 0, 'v1'))
			accumulator.append(createMessage('topic', 1, 'v2'))
			accumulator.endAppendTransaction()

			expect(batches).toHaveLength(2)
		})

		it('suppresses lingerMs=0 auto-flush during transaction', () => {
			const config = createConfig({ lingerMs: 0 })
			const accumulator = new RecordAccumulator(config, createLogger())
			const batches: PartitionBatch[] = []
			accumulator.on('batchReady', batch => batches.push(batch))

			accumulator.beginAppendTransaction()
			accumulator.append(createMessage('topic', 0, 'v1'))
			accumulator.append(createMessage('topic', 0, 'v2'))

			expect(batches).toHaveLength(0)

			accumulator.endAppendTransaction()
			expect(batches).toHaveLength(1)
		})
	})

	describe('fencing', () => {
		it('rejects messages when fenced', () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())
			const message = createMessage('topic', 0, 'value')

			accumulator.setFenced(true)
			accumulator.append(message)

			expect(message.reject).toHaveBeenCalledWith(expect.any(Error))
			expect((message.reject as ReturnType<typeof vi.fn>).mock.calls[0]![0].message).toContain('fenced')
			expect(accumulator.batchCount).toBe(0)
		})

		it('accepts messages after unfencing', () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())

			accumulator.setFenced(true)
			accumulator.setFenced(false)

			const message = createMessage('topic', 0, 'value')
			accumulator.append(message)

			expect(message.reject).not.toHaveBeenCalled()
			expect(accumulator.batchCount).toBe(1)
		})
	})

	describe('drain', () => {
		it('flushes all batches when draining', async () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())
			const batches: PartitionBatch[] = []
			accumulator.on('batchReady', batch => batches.push(batch))

			accumulator.append(createMessage('topic', 0, 'v1'))
			accumulator.append(createMessage('topic', 1, 'v2'))

			const drainPromise = accumulator.drain()

			// Simulate batch completion
			accumulator.batchCompleted()
			accumulator.batchCompleted()

			await drainPromise

			expect(batches).toHaveLength(2)
		})

		it('resolves immediately if no pending batches', async () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())

			await accumulator.drain()
			// Should not hang
		})

		it('waits for pending batches to complete', async () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())
			accumulator.on('batchReady', () => {})

			accumulator.append(createMessage('topic', 0, 'value'))
			accumulator.flush()

			let drained = false
			const drainPromise = accumulator.drain().then(() => {
				drained = true
			})

			// Not drained yet
			await Promise.resolve()
			expect(drained).toBe(false)

			// Complete the batch
			accumulator.batchCompleted()
			await drainPromise

			expect(drained).toBe(true)
		})
	})

	describe('batchCompleted', () => {
		it('decrements pending count', () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())
			accumulator.on('batchReady', () => {})

			accumulator.append(createMessage('topic', 0, 'v1'))
			accumulator.append(createMessage('topic', 1, 'v2'))
			accumulator.flush()

			expect(accumulator.pendingCount).toBe(2)

			accumulator.batchCompleted()
			expect(accumulator.pendingCount).toBe(1)

			accumulator.batchCompleted()
			expect(accumulator.pendingCount).toBe(0)
		})
	})

	describe('clear', () => {
		it('removes all batches', () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())

			accumulator.append(createMessage('topic', 0, 'v1'))
			accumulator.append(createMessage('topic', 1, 'v2'))
			expect(accumulator.batchCount).toBe(2)

			accumulator.clear()

			expect(accumulator.batchCount).toBe(0)
		})

		it('cancels all timers', () => {
			const config = createConfig({ lingerMs: 100 })
			const accumulator = new RecordAccumulator(config, createLogger())
			const batches: PartitionBatch[] = []
			accumulator.on('batchReady', batch => batches.push(batch))

			accumulator.append(createMessage('topic', 0, 'value'))
			accumulator.clear()

			vi.advanceTimersByTime(100)
			expect(batches).toHaveLength(0)
		})
	})

	describe('clearWithRejection', () => {
		it('rejects all pending messages with the given error', () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())
			const messages = [
				createMessage('topic', 0, 'v1'),
				createMessage('topic', 0, 'v2'),
				createMessage('topic', 1, 'v3'),
			]

			for (const msg of messages) {
				accumulator.append(msg)
			}

			const error = new Error('Producer fenced')
			accumulator.clearWithRejection(error)

			for (const msg of messages) {
				expect(msg.reject).toHaveBeenCalledWith(error)
			}
			expect(accumulator.batchCount).toBe(0)
		})

		it('cancels all timers', () => {
			const config = createConfig({ lingerMs: 100 })
			const accumulator = new RecordAccumulator(config, createLogger())
			const batches: PartitionBatch[] = []
			accumulator.on('batchReady', batch => batches.push(batch))

			accumulator.append(createMessage('topic', 0, 'value'))
			accumulator.clearWithRejection(new Error('error'))

			vi.advanceTimersByTime(100)
			expect(batches).toHaveLength(0)
		})
	})

	describe('state accessors', () => {
		it('batchCount returns number of batches awaiting flush', () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())

			expect(accumulator.batchCount).toBe(0)

			accumulator.append(createMessage('topic', 0, 'v1'))
			expect(accumulator.batchCount).toBe(1)

			accumulator.append(createMessage('topic', 1, 'v2'))
			expect(accumulator.batchCount).toBe(2)

			accumulator.flush()
			expect(accumulator.batchCount).toBe(0)
		})

		it('pendingCount returns number of batches being sent', () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())
			accumulator.on('batchReady', () => {})

			expect(accumulator.pendingCount).toBe(0)

			accumulator.append(createMessage('topic', 0, 'v1'))
			accumulator.flush()
			expect(accumulator.pendingCount).toBe(1)

			accumulator.append(createMessage('topic', 1, 'v2'))
			accumulator.flush()
			expect(accumulator.pendingCount).toBe(2)
		})
	})

	describe('batch metadata', () => {
		it('includes createdAt timestamp', () => {
			const now = Date.now()
			vi.setSystemTime(now)

			const accumulator = new RecordAccumulator(createConfig(), createLogger())
			const batches: PartitionBatch[] = []
			accumulator.on('batchReady', batch => batches.push(batch))

			accumulator.append(createMessage('topic', 0, 'value'))
			accumulator.flush()

			expect(batches[0]!.createdAt).toBe(now)
		})

		it('preserves message order within batch', () => {
			const accumulator = new RecordAccumulator(createConfig(), createLogger())
			const batches: PartitionBatch[] = []
			accumulator.on('batchReady', batch => batches.push(batch))

			accumulator.append(createMessage('topic', 0, 'first'))
			accumulator.append(createMessage('topic', 0, 'second'))
			accumulator.append(createMessage('topic', 0, 'third'))
			accumulator.flush()

			const values = batches[0]!.messages.map(m => m.value!.toString())
			expect(values).toEqual(['first', 'second', 'third'])
		})
	})
})
