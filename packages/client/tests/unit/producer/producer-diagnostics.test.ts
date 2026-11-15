import { describe, expect, it, beforeEach, afterEach, vi } from 'vitest'

import { diagnostics, type ProduceRequestDiagnostics, type SendDiagnostics } from '@/diagnostics.js'
import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { ProduceRequest } from '@/protocol/messages/requests/produce.js'
import { decodeRecordBatch } from '@/protocol/records/record-batch.js'
import { createMockBroker, createMockCluster, buildProduceResponse } from './_helpers.js'

describe('Producer diagnostics', () => {
	let mockCluster: ReturnType<typeof createMockCluster>
	let mockBroker: ReturnType<typeof createMockBroker>
	let produced: ProduceRequest[]
	const unsubscribe: Array<() => void> = []

	beforeEach(() => {
		mockCluster = createMockCluster()
		mockBroker = createMockBroker(1)
		mockCluster.getLeaderForPartition.mockResolvedValue(mockBroker)
		produced = []
		mockBroker.produce.mockImplementation(async (request: ProduceRequest) => {
			produced.push(request)
			return buildProduceResponse([
				{ name: 'test-topic', partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 7n }] },
			])
		})
	})

	afterEach(() => {
		for (const stop of unsubscribe.splice(0)) stop()
		vi.restoreAllMocks()
	})

	/** Subscribe to a tracing channel for the duration of the test. */
	function subscribe<C extends object>(
		channel: { subscribe(handlers: object): void; unsubscribe(handlers: object): void },
		handlers: { start?(context: C): void; asyncEnd?(context: C): void; error?(context: C): void }
	): void {
		channel.subscribe(handlers)
		unsubscribe.push(() => channel.unsubscribe(handlers))
	}

	it('publishes a send from the call until the acknowledgement, with the result', async () => {
		const events: string[] = []
		let finished: SendDiagnostics | undefined
		subscribe<SendDiagnostics>(diagnostics.send, {
			start: context => events.push(`start ${context.topic} ${context.messages.length}`),
			asyncEnd: context => {
				events.push('asyncEnd')
				finished = context
			},
		})
		const producer = new Producer(mockCluster, { retries: 0 })

		const result = await producer.send('test-topic', { value: Buffer.from('v'), partition: 0 })

		expect(events).toEqual(['start test-topic 1', 'asyncEnd'])
		expect(finished?.result).toBe(result)
		expect(finished?.transactional).toBe(false)
		await producer.disconnect()
	})

	it('sends the headers a subscriber adds, without touching the message the caller passed', async () => {
		subscribe<SendDiagnostics>(diagnostics.send, {
			start: context => context.addHeaders({ traceparent: '00-abc-def-01' }),
		})
		const producer = new Producer(mockCluster, { retries: 0 })
		const message = { value: Buffer.from('v'), partition: 0, headers: { mine: 'kept' } }

		await producer.send('test-topic', message)

		const { records } = await decodeRecordBatch(produced[0]!.topics[0]!.partitions[0]!.records)
		const headers = Object.fromEntries(records[0]!.headers.map(header => [header.key, header.value?.toString()]))
		expect(headers).toEqual({ mine: 'kept', traceparent: '00-abc-def-01' })
		expect(message.headers).toEqual({ mine: 'kept' })
		await producer.disconnect()
	})

	it('adds headers to one message of a call when given its index', async () => {
		subscribe<SendDiagnostics>(diagnostics.send, {
			start: context => {
				context.addHeaders({ all: 'yes' })
				context.addHeaders({ second: 'only' }, 1)
			},
		})
		const producer = new Producer(mockCluster, { retries: 0 })

		await producer.send('test-topic', [
			{ value: Buffer.from('a'), partition: 0 },
			{ value: Buffer.from('b'), partition: 0 },
		])

		const { records } = await decodeRecordBatch(produced[0]!.topics[0]!.partitions[0]!.records)
		expect(records.map(record => record.headers.map(header => header.key))).toEqual([['all'], ['all', 'second']])
		await producer.disconnect()
	})

	it('reports the partitions a broker rejected, on a request that did not fail as a whole', async () => {
		mockBroker.produce.mockResolvedValue(
			buildProduceResponse([
				{
					name: 'test-topic',
					partitions: [{ partitionIndex: 0, errorCode: ErrorCode.MessageTooLarge, baseOffset: -1n }],
				},
			])
		)
		let finished: ProduceRequestDiagnostics | undefined
		subscribe<ProduceRequestDiagnostics>(diagnostics.produceRequest, { asyncEnd: context => (finished = context) })
		const producer = new Producer(mockCluster, { retries: 0 })

		await expect(producer.send('test-topic', { value: Buffer.from('v'), partition: 0 })).rejects.toThrow()

		expect(finished?.error).toBeUndefined()
		expect(finished?.rejected).toEqual([
			{ topic: 'test-topic', partition: 0, errorCode: ErrorCode.MessageTooLarge },
		])
		await producer.disconnect().catch(() => {})
	})

	it('publishes each produce request with what it carried', async () => {
		const requests: ProduceRequestDiagnostics[] = []
		subscribe<ProduceRequestDiagnostics>(diagnostics.produceRequest, { start: context => requests.push(context) })
		const producer = new Producer(mockCluster, { retries: 0 })

		await producer.send('test-topic', [
			{ value: Buffer.from('a'), partition: 0 },
			{ value: Buffer.from('b'), partition: 0 },
		])

		expect(requests).toHaveLength(1)
		expect(requests[0]).toMatchObject({ brokerId: 1, partitions: 1, records: 2 })
		expect(requests[0]!.bytes).toBeGreaterThan(0)
		expect(requests[0]!.rejected).toEqual([])
		await producer.disconnect()
	})

	it('publishes a failed send with its error', async () => {
		mockBroker.produce.mockRejectedValue(new Error('broker gone'))
		let failed: SendDiagnostics | undefined
		subscribe<SendDiagnostics>(diagnostics.send, { error: context => (failed = context) })
		const producer = new Producer(mockCluster, { retries: 0 })

		await expect(producer.send('test-topic', { value: Buffer.from('v'), partition: 0 })).rejects.toThrow()

		expect(failed?.error).toBeInstanceOf(Error)
		await producer.disconnect().catch(() => {})
	})

	it('reports what is waiting inside the producer', async () => {
		let release!: () => void
		const held = new Promise<void>(resolve => (release = resolve))
		mockBroker.produce.mockImplementation(async () => {
			await held
			return buildProduceResponse([
				{ name: 'test-topic', partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None, baseOffset: 0n }] },
			])
		})
		const producer = new Producer(mockCluster, { retries: 0 })
		expect(producer.metrics()).toEqual({
			queuedBatches: 0,
			queuedRecords: 0,
			queuedBytes: 0,
			inFlightRequests: 0,
			unacknowledgedSends: 0,
		})

		const sends = [0, 1, 2].map(i => producer.send('test-topic', { value: Buffer.from(String(i)), partition: 0 }))
		await new Promise(resolve => setTimeout(resolve, 20))

		// The first is on the wire; the other two wait behind it for the same partition.
		const waiting = producer.metrics()
		expect(waiting.inFlightRequests).toBe(1)
		expect(waiting.unacknowledgedSends).toBe(3)
		expect(waiting.queuedRecords).toBe(2)
		expect(waiting.queuedBytes).toBeGreaterThan(0)

		release()
		await Promise.all(sends)
		expect(producer.metrics().unacknowledgedSends).toBe(0)
		await producer.disconnect()
	})
})
