import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from 'vitest'
import type { Producer } from '@kafkats/client'
import { KafkaProducer, PRODUCER_CLOSE_TIMEOUT_MS } from '@/runtime/kafka-producer.js'
import { deferred } from '../support/fake-io.js'

const result = { topic: 't', partition: 1, offset: 4n, timestamp: new Date(5) }
const message = {
	key: Buffer.from('k'),
	value: null,
	partition: 1,
	timestamp: new Date(5),
	headers: { h: Buffer.from('x') },
}
const signal = () => new AbortController().signal

function stubHandle() {
	return {
		send: vi.fn(() => Promise.resolve(result)),
		sendOffsets: vi.fn(() => Promise.resolve()),
		commit: vi.fn(() => Promise.resolve()),
		abort: vi.fn(() => Promise.resolve()),
	}
}

function stubProducer() {
	const handle = stubHandle()
	const stub = {
		send: vi.fn(() => Promise.resolve(result)),
		beginTransaction: vi.fn(() => Promise.resolve(handle)),
		partitionCount: vi.fn(() => Promise.resolve(3)),
		disconnect: vi.fn(() => Promise.resolve()),
	}
	return { stub, handle, port: new KafkaProducer(stub as unknown as Producer) }
}

let unhandled: Mock<(reason: unknown) => void>
beforeEach(() => {
	unhandled = vi.fn()
	process.on('unhandledRejection', unhandled)
})
afterEach(async () => {
	await new Promise(resolve => setImmediate(resolve))
	process.off('unhandledRejection', unhandled)
	expect(unhandled).not.toHaveBeenCalled()
})

describe('KafkaProducer', () => {
	it('sends at-least-once records as they are', async () => {
		const { stub, port } = stubProducer()
		await expect(port.send('t', message)).resolves.toBe(result)
		expect(stub.send).toHaveBeenCalledWith('t', message)
	})

	it("opens a transaction whose sends, offsets, commit and abort reach the client's handle", async () => {
		const { stub, handle, port } = stubProducer()
		const transaction = await port.beginTransaction({ signal: signal() })
		// No client signal: it would abort the open transaction once it fired.
		expect(stub.beginTransaction).toHaveBeenCalledWith()

		await expect(transaction.send('t', message)).resolves.toBe(result)
		expect(handle.send).toHaveBeenCalledWith('t', message)

		const group = { groupId: 'app', generationId: 2, memberId: 'm' }
		const offsets = [{ topic: 'in', partition: 0, offset: 9n, metadata: 'AgAA' }]
		await transaction.sendOffsets({ consumerGroupMetadata: group, offsets })
		expect(handle.sendOffsets).toHaveBeenCalledWith({ consumerGroupMetadata: group, offsets })

		const commitSignal = signal()
		await transaction.commit({ signal: commitSignal })
		expect(handle.commit).toHaveBeenCalledWith({ signal: commitSignal })
		const abortSignal = signal()
		await transaction.abort({ signal: abortSignal })
		expect(handle.abort).toHaveBeenCalledWith({ signal: abortSignal })
	})

	it('aborts a transaction that opens after the caller stopped waiting for it', async () => {
		const { stub, handle, port } = stubProducer()
		const opening = deferred<typeof handle>()
		stub.beginTransaction.mockReturnValue(opening.promise)
		const controller = new AbortController()
		const begun = port.beginTransaction({ signal: controller.signal })
		controller.abort()
		await expect(begun).rejects.toMatchObject({ name: 'AbortError' })
		handle.abort.mockReturnValue(Promise.reject(new Error('abort failed')))
		opening.resolve(handle)
		await new Promise(resolve => setImmediate(resolve))
		expect(handle.abort).toHaveBeenCalledWith({ timeoutMs: PRODUCER_CLOSE_TIMEOUT_MS })
	})

	it('rejects as the client does when the transaction cannot begin', async () => {
		const { stub, port } = stubProducer()
		stub.beginTransaction.mockReturnValue(Promise.reject(new Error('fenced')))
		await expect(port.beginTransaction({ signal: signal() })).rejects.toThrow('fenced')
	})

	it('looks up partition counts with the signal of the call', async () => {
		const { stub, port } = stubProducer()
		const options = { signal: signal() }
		await expect(port.partitionCount('t', options)).resolves.toBe(3)
		expect(stub.partitionCount).toHaveBeenCalledWith('t', options)
	})

	it('closes within the bound of the client, and stops waiting for it when the signal aborts', async () => {
		const { stub, port } = stubProducer()
		await port.close({ signal: signal() })
		expect(stub.disconnect).toHaveBeenCalledWith({ timeoutMs: PRODUCER_CLOSE_TIMEOUT_MS })

		const disconnecting = deferred()
		stub.disconnect.mockReturnValue(disconnecting.promise)
		const controller = new AbortController()
		const closing = port.close({ signal: controller.signal })
		controller.abort()
		await expect(closing).rejects.toMatchObject({ name: 'AbortError' })
		disconnecting.reject(new Error('late'))
	})
})
