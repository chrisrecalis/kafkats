import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'
import { EventEmitter } from 'node:events'
import type * as net from 'node:net'

import { Connection } from '@/network/connection.js'
import { ConnectionClosedError } from '@/network/errors.js'
import { ApiKey } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { Encoder } from '@/protocol/primitives/encoder.js'

/**
 * Mock socket that emulates net.Socket behavior
 */
class MockSocket extends EventEmitter {
	public destroyed = false
	public writtenData: Buffer[] = []
	private correlationCounter = 0

	write(data: Buffer | string): boolean {
		if (this.destroyed) {
			throw new Error('Socket is destroyed')
		}
		this.writtenData.push(Buffer.isBuffer(data) ? data : Buffer.from(data))
		return true
	}

	destroy(): void {
		this.destroyed = true
		this.emit('close', false)
	}

	/**
	 * Simulate receiving a response from the broker
	 */
	simulateResponse(correlationId: number, payload: Buffer): void {
		// Build a framed response: 4-byte length + 4-byte correlation ID + payload
		const length = 4 + payload.length
		const response = Buffer.alloc(4 + length)
		response.writeInt32BE(length, 0)
		response.writeInt32BE(correlationId, 4)
		payload.copy(response, 8)
		this.emit('data', response)
	}

	/**
	 * Simulate socket close
	 */
	simulateClose(hadError = false): void {
		this.destroyed = true
		this.emit('close', hadError)
	}

	/**
	 * Simulate socket error
	 */
	simulateError(error: Error): void {
		this.emit('error', error)
	}

	/**
	 * Extract correlation ID from the last written request
	 */
	getLastCorrelationId(): number | null {
		if (this.writtenData.length === 0) return null
		const lastWrite = this.writtenData[this.writtenData.length - 1]!
		// Skip 4-byte length prefix, then read 2-byte API key, 2-byte API version, then 4-byte correlation ID
		if (lastWrite.length < 12) return null
		return lastWrite.readInt32BE(8)
	}
}

describe('Connection', () => {
	let mockSocket: MockSocket

	beforeEach(() => {
		mockSocket = new MockSocket()
		vi.useFakeTimers()
	})

	afterEach(() => {
		vi.useRealTimers()
		vi.restoreAllMocks()
	})

	describe('socket close handling', () => {
		it('rejects pending requests when socket closes unexpectedly', async () => {
			// Create a mock connection with our test socket
			const connection = new Connection({
				host: 'localhost',
				port: 9092,
				clientId: 'test-client',
			})

			// Inject the mock socket by accessing internals
			// Note: In real tests, we'd use dependency injection
			const internalSocket = mockSocket as unknown as net.Socket

			// Simulate connection established state
			;(connection as unknown as { _state: string })._state = 'connected'
			;(connection as unknown as { socket: net.Socket }).socket = internalSocket

			// Set up the request queue's send function
			const requestQueue = (
				connection as unknown as { requestQueue: { setSendFunction: (fn: (data: Buffer) => void) => void } }
			).requestQueue
			requestQueue.setSendFunction((data: Buffer) => {
				mockSocket.write(data)
			})

			// Set up data handler
			const handleData = (connection as unknown as { handleData: (data: Buffer) => void }).handleData.bind(
				connection
			)
			mockSocket.on('data', handleData)

			// Set up close handler
			const handleClose = (
				connection as unknown as { handleClose: (hadError: boolean) => void }
			).handleClose.bind(connection)
			mockSocket.on('close', handleClose)

			// Send a request that will be pending
			const sendPromise = connection.send(ApiKey.Metadata, 0, encoder => {
				encoder.writeInt32(0) // Empty metadata request
			})

			// Socket closes before response
			mockSocket.simulateClose(false)

			// Request should be rejected with ConnectionClosedError
			await expect(sendPromise).rejects.toBeInstanceOf(ConnectionClosedError)
		})

		it('rejects all queued requests when socket closes', async () => {
			const connection = new Connection({
				host: 'localhost',
				port: 9092,
				clientId: 'test-client',
				maxInFlightRequests: 1, // Only 1 in-flight, rest queued
			})

			const internalSocket = mockSocket as unknown as net.Socket
			;(connection as unknown as { _state: string })._state = 'connected'
			;(connection as unknown as { socket: net.Socket }).socket = internalSocket

			const requestQueue = (
				connection as unknown as { requestQueue: { setSendFunction: (fn: (data: Buffer) => void) => void } }
			).requestQueue
			requestQueue.setSendFunction((data: Buffer) => {
				mockSocket.write(data)
			})

			const handleClose = (
				connection as unknown as { handleClose: (hadError: boolean) => void }
			).handleClose.bind(connection)
			mockSocket.on('close', handleClose)

			// Send multiple requests (first in-flight, rest queued)
			const promises = [
				connection.send(ApiKey.Metadata, 0, encoder => encoder.writeInt32(0)),
				connection.send(ApiKey.Metadata, 0, encoder => encoder.writeInt32(1)),
				connection.send(ApiKey.Metadata, 0, encoder => encoder.writeInt32(2)),
			]

			// Close socket
			mockSocket.simulateClose(true)

			// All requests should be rejected
			await expect(promises[0]).rejects.toBeInstanceOf(ConnectionClosedError)
			await expect(promises[1]).rejects.toBeInstanceOf(ConnectionClosedError)
			await expect(promises[2]).rejects.toBeInstanceOf(ConnectionClosedError)
		})

		it('emits disconnect event when connection closes', async () => {
			const connection = new Connection({
				host: 'localhost',
				port: 9092,
				clientId: 'test-client',
			})

			const internalSocket = mockSocket as unknown as net.Socket
			;(connection as unknown as { _state: string })._state = 'connected'
			;(connection as unknown as { socket: net.Socket }).socket = internalSocket

			const handleClose = (
				connection as unknown as { handleClose: (hadError: boolean) => void }
			).handleClose.bind(connection)
			mockSocket.on('close', handleClose)

			const disconnectHandler = vi.fn()
			connection.on('disconnect', disconnectHandler)

			mockSocket.simulateClose(false)

			expect(disconnectHandler).toHaveBeenCalledTimes(1)
			expect(connection.state).toBe('disconnected')
		})

		it('emits disconnect with error when socket closes with error', async () => {
			const connection = new Connection({
				host: 'localhost',
				port: 9092,
				clientId: 'test-client',
			})

			const internalSocket = mockSocket as unknown as net.Socket
			;(connection as unknown as { _state: string })._state = 'connected'
			;(connection as unknown as { socket: net.Socket }).socket = internalSocket

			const handleClose = (
				connection as unknown as { handleClose: (hadError: boolean) => void }
			).handleClose.bind(connection)
			mockSocket.on('close', handleClose)

			const disconnectHandler = vi.fn()
			connection.on('disconnect', disconnectHandler)

			mockSocket.simulateClose(true) // hadError = true

			expect(disconnectHandler).toHaveBeenCalledWith(expect.any(Error))
		})
	})

	describe('request-response correlation', () => {
		it('completes request when matching response arrives', async () => {
			const connection = new Connection({
				host: 'localhost',
				port: 9092,
				clientId: 'test-client',
			})

			const internalSocket = mockSocket as unknown as net.Socket
			;(connection as unknown as { _state: string })._state = 'connected'
			;(connection as unknown as { socket: net.Socket }).socket = internalSocket

			const requestQueue = (
				connection as unknown as { requestQueue: { setSendFunction: (fn: (data: Buffer) => void) => void } }
			).requestQueue
			requestQueue.setSendFunction((data: Buffer) => {
				mockSocket.write(data)
			})

			const handleData = (connection as unknown as { handleData: (data: Buffer) => void }).handleData.bind(
				connection
			)
			mockSocket.on('data', handleData)

			// Start a request
			const sendPromise = connection.send(ApiKey.Metadata, 0, encoder => {
				encoder.writeInt32(0)
			})

			// Get the correlation ID from the sent request
			const correlationId = mockSocket.getLastCorrelationId()!
			expect(correlationId).toBeGreaterThanOrEqual(0)

			// Simulate broker response
			const responsePayload = Buffer.from([0, 0, 0, 0]) // Empty success response
			mockSocket.simulateResponse(correlationId, responsePayload)

			// Request should complete successfully
			const response = await sendPromise
			expect(response).toBeInstanceOf(Buffer)
		})

		it('ignores responses with unknown correlation IDs', async () => {
			const connection = new Connection({
				host: 'localhost',
				port: 9092,
				clientId: 'test-client',
			})

			const internalSocket = mockSocket as unknown as net.Socket
			;(connection as unknown as { _state: string })._state = 'connected'
			;(connection as unknown as { socket: net.Socket }).socket = internalSocket

			const handleData = (connection as unknown as { handleData: (data: Buffer) => void }).handleData.bind(
				connection
			)
			mockSocket.on('data', handleData)

			// Simulate response with unknown correlation ID
			const unknownCorrelationId = 99999
			const responsePayload = Buffer.from([0, 0, 0, 0])

			// This should not throw
			expect(() => {
				mockSocket.simulateResponse(unknownCorrelationId, responsePayload)
			}).not.toThrow()
		})
	})

	describe('SASL reauthentication', () => {
		it('refreshes OAUTHBEARER session before expiry', async () => {
			const oauthBearerProvider = vi.fn().mockResolvedValue({ value: 'token-1' })

			const connection = new Connection({
				host: 'localhost',
				port: 9092,
				clientId: 'test-client',
				sasl: {
					mechanism: 'OAUTHBEARER',
					oauthBearerProvider,
					reauthenticationThresholdMs: 0,
				},
			})

			const internalSocket = mockSocket as unknown as net.Socket
			;(connection as unknown as { _state: string })._state = 'connected'
			;(connection as unknown as { socket: net.Socket }).socket = internalSocket

			const requestQueue = (
				connection as unknown as { requestQueue: { setSendFunction: (fn: (data: Buffer) => void) => void } }
			).requestQueue
			requestQueue.setSendFunction((data: Buffer) => {
				mockSocket.write(data)
			})

			const handleData = (connection as unknown as { handleData: (data: Buffer) => void }).handleData.bind(
				connection
			)
			mockSocket.on('data', handleData)

			// Schedule reauth soon.
			;(
				connection as unknown as { scheduleSaslReauthentication: (ms: bigint) => void }
			).scheduleSaslReauthentication(5n)

			await vi.advanceTimersByTimeAsync(5)

			const request = mockSocket.writtenData[mockSocket.writtenData.length - 1]!
			expect(request.readInt16BE(4)).toBe(ApiKey.SaslAuthenticate)

			const correlationId = mockSocket.getLastCorrelationId()!

			const encoder = new Encoder()
			encoder.writeInt16(ErrorCode.None)
			encoder.writeNullableString(null)
			encoder.writeBytes(Buffer.alloc(0))
			encoder.writeInt64(0n) // no further reauth scheduling

			const reauthPromise = (connection as unknown as { saslReauthPromise: Promise<void> | null })
				.saslReauthPromise
			expect(reauthPromise).not.toBeNull()

			mockSocket.simulateResponse(correlationId, encoder.toBuffer())

			await reauthPromise

			expect(oauthBearerProvider).toHaveBeenCalledTimes(1)
		})
	})

	describe('error handling', () => {
		it('emits error event when socket error occurs', async () => {
			const connection = new Connection({
				host: 'localhost',
				port: 9092,
				clientId: 'test-client',
			})

			const internalSocket = mockSocket as unknown as net.Socket
			;(connection as unknown as { _state: string })._state = 'connected'
			;(connection as unknown as { socket: net.Socket }).socket = internalSocket

			const handleError = (connection as unknown as { handleError: (error: Error) => void }).handleError.bind(
				connection
			)
			mockSocket.on('error', handleError)

			const errorHandler = vi.fn()
			connection.on('error', errorHandler)

			const socketError = new Error('ECONNRESET')
			mockSocket.simulateError(socketError)

			expect(errorHandler).toHaveBeenCalledWith(socketError)
		})

		it('throws ConnectionClosedError when sending on disconnected connection', async () => {
			const connection = new Connection({
				host: 'localhost',
				port: 9092,
				clientId: 'test-client',
			})

			// Connection is in 'disconnected' state by default
			expect(connection.isConnected).toBe(false)

			await expect(
				connection.send(ApiKey.Metadata, 0, encoder => {
					encoder.writeInt32(0)
				})
			).rejects.toBeInstanceOf(ConnectionClosedError)
		})
	})

	describe('state management', () => {
		it('transitions to disconnected state when socket closes', () => {
			const connection = new Connection({
				host: 'localhost',
				port: 9092,
				clientId: 'test-client',
			})

			const internalSocket = mockSocket as unknown as net.Socket
			;(connection as unknown as { _state: string })._state = 'connected'
			;(connection as unknown as { socket: net.Socket }).socket = internalSocket

			const handleClose = (
				connection as unknown as { handleClose: (hadError: boolean) => void }
			).handleClose.bind(connection)
			mockSocket.on('close', handleClose)

			expect(connection.state).toBe('connected')

			mockSocket.simulateClose(false)

			expect(connection.state).toBe('disconnected')
			expect(connection.isConnected).toBe(false)
		})

		it('reports pending request count accurately', async () => {
			const connection = new Connection({
				host: 'localhost',
				port: 9092,
				clientId: 'test-client',
				maxInFlightRequests: 2,
			})

			const internalSocket = mockSocket as unknown as net.Socket
			;(connection as unknown as { _state: string })._state = 'connected'
			;(connection as unknown as { socket: net.Socket }).socket = internalSocket

			const requestQueue = (
				connection as unknown as { requestQueue: { setSendFunction: (fn: (data: Buffer) => void) => void } }
			).requestQueue
			requestQueue.setSendFunction((data: Buffer) => {
				mockSocket.write(data)
			})

			expect(connection.pendingRequests).toBe(0)

			// Send two requests
			connection.send(ApiKey.Metadata, 0, encoder => encoder.writeInt32(0))
			connection.send(ApiKey.Metadata, 0, encoder => encoder.writeInt32(1))

			expect(connection.pendingRequests).toBe(2)
			expect(connection.inFlightRequests).toBe(2)
			expect(connection.queuedRequests).toBe(0)

			// Send a third (will be queued since maxInFlight=2)
			connection.send(ApiKey.Metadata, 0, encoder => encoder.writeInt32(2))

			expect(connection.pendingRequests).toBe(3)
			expect(connection.inFlightRequests).toBe(2)
			expect(connection.queuedRequests).toBe(1)
		})
	})

	describe('graceful close', () => {
		it('close() rejects all pending requests', async () => {
			const connection = new Connection({
				host: 'localhost',
				port: 9092,
				clientId: 'test-client',
			})

			const internalSocket = mockSocket as unknown as net.Socket
			;(connection as unknown as { _state: string })._state = 'connected'
			;(connection as unknown as { socket: net.Socket }).socket = internalSocket

			const requestQueue = (
				connection as unknown as { requestQueue: { setSendFunction: (fn: (data: Buffer) => void) => void } }
			).requestQueue
			requestQueue.setSendFunction((data: Buffer) => {
				mockSocket.write(data)
			})

			// Send a request
			const sendPromise = connection.send(ApiKey.Metadata, 0, encoder => {
				encoder.writeInt32(0)
			})

			// Close the connection (waitForPending=false to close immediately)
			await connection.close(false)

			// Request should be rejected
			await expect(sendPromise).rejects.toBeInstanceOf(ConnectionClosedError)
			expect(connection.state).toBe('closed')
		})
	})
})
