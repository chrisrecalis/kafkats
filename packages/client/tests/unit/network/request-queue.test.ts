import { afterEach, describe, expect, it, vi } from 'vitest'

import { RequestQueue } from '@/network/request-queue.js'
import { ConnectionClosedError, RequestTimeoutError } from '@/network/errors.js'
import { ApiKey } from '@/protocol/messages/api-keys.js'

describe('RequestQueue', () => {
	afterEach(() => {
		vi.useRealTimers()
	})

	it('queues when max in-flight is reached and drains on complete', async () => {
		const sent: Buffer[] = []
		const queue = new RequestQueue({ maxInFlight: 1, defaultTimeoutMs: 1000 })
		queue.setSendFunction(buffer => {
			sent.push(buffer)
		})

		const first = queue.enqueue(ApiKey.Metadata, 0, correlationId => Buffer.from([correlationId]))
		const second = queue.enqueue(ApiKey.Metadata, 0, correlationId => Buffer.from([correlationId]))

		expect(queue.inFlightCount).toBe(1)
		expect(queue.queuedCount).toBe(1)
		expect(sent).toHaveLength(1)

		const firstCorrelationId = sent[0]![0]!
		queue.complete(firstCorrelationId, Buffer.from('first'))

		expect(queue.inFlightCount).toBe(1)
		expect(queue.queuedCount).toBe(0)
		expect(sent).toHaveLength(2)

		const secondCorrelationId = sent[1]![0]!
		queue.complete(secondCorrelationId, Buffer.from('second'))

		await expect(first).resolves.toEqual(Buffer.from('first'))
		await expect(second).resolves.toEqual(Buffer.from('second'))
		expect(queue.totalPending).toBe(0)
	})

	it('rejects when send function is not set', async () => {
		const queue = new RequestQueue({ maxInFlight: 1 })
		await expect(queue.enqueue(ApiKey.Metadata, 0, () => Buffer.from('x'))).rejects.toThrow('Send function not set')
	})

	it('times out requests and frees queue slots', async () => {
		vi.useFakeTimers()
		const queue = new RequestQueue({ maxInFlight: 1, defaultTimeoutMs: 10 })
		queue.setSendFunction(() => {})

		const promise = queue.enqueue(ApiKey.Metadata, 0, () => Buffer.from('x'))
		const handled = promise.catch(error => error)

		await vi.advanceTimersByTimeAsync(11)

		const error = await handled
		expect(error).toBeInstanceOf(RequestTimeoutError)
		expect(queue.totalPending).toBe(0)
	})

	it('returns null when completing unknown correlation ids', () => {
		const queue = new RequestQueue({ maxInFlight: 1 })
		expect(queue.complete(999, Buffer.from('x'))).toBeNull()
	})

	it('rejects when buildRequest throws', async () => {
		const queue = new RequestQueue({ maxInFlight: 1 })
		queue.setSendFunction(() => {})
		await expect(
			queue.enqueue(ApiKey.Metadata, 0, () => {
				throw new Error('boom')
			})
		).rejects.toThrow('boom')
	})

	it('rejects when send function throws', async () => {
		const queue = new RequestQueue({ maxInFlight: 1 })
		queue.setSendFunction(() => {
			throw new Error('send fail')
		})
		await expect(queue.enqueue(ApiKey.Metadata, 0, () => Buffer.from('x'))).rejects.toThrow('send fail')
	})

	it('rejects all pending and queued requests', async () => {
		const queue = new RequestQueue({ maxInFlight: 1 })
		queue.setSendFunction(() => {})
		const first = queue.enqueue(ApiKey.Metadata, 0, () => Buffer.from('x'))
		const second = queue.enqueue(ApiKey.Metadata, 0, () => Buffer.from('y'))
		queue.rejectAll()
		await expect(first).rejects.toBeInstanceOf(ConnectionClosedError)
		await expect(second).rejects.toBeInstanceOf(ConnectionClosedError)
		expect(queue.totalPending).toBe(0)
	})

	it('clears pending requests without rejecting', async () => {
		const queue = new RequestQueue({ maxInFlight: 1 })
		queue.setSendFunction(() => {})
		const promise = queue.enqueue(ApiKey.Metadata, 0, () => Buffer.from('x'))
		queue.clear()
		await expect(Promise.race([promise, Promise.resolve('cleared')])).resolves.toBe('cleared')
		expect(queue.totalPending).toBe(0)
	})

	it('reports ability to send immediately', () => {
		const queue = new RequestQueue({ maxInFlight: 1 })
		queue.setSendFunction(() => {})
		expect(queue.canSendImmediately()).toBe(true)
		queue.enqueue(ApiKey.Metadata, 0, () => Buffer.from('x'))
		expect(queue.canSendImmediately()).toBe(false)
	})
})
