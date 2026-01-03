/**
 * Connection class for managing a single TCP connection to a Kafka broker
 */

import type * as net from 'node:net'
import type * as tls from 'node:tls'
import { EventEmitter } from 'node:events'
import type { ConnectionConfig, ConnectionState, SaslConfig } from '@/network/types.js'
import type { ApiKey } from '@/protocol/messages/api-keys.js'
import { SocketFactory, type SocketFactoryOptions } from '@/network/socket-factory.js'
import { RequestQueue, type RequestQueueOptions } from '@/network/request-queue.js'
import { ConnectionClosedError, NetworkError } from '@/network/errors.js'
import { KafkaFrameDecoder } from '@/network/kafka-frame-decoder.js'
import { Encoder } from '@/protocol/primitives/index.js'
import { encodeRequestHeader } from '@/protocol/messages/headers.js'
import { noopLogger, type Logger } from '@/logger.js'
import { SaslAuthenticator } from '@/auth/sasl-authenticator.js'
import { sleep } from '@/utils/sleep.js'

export interface ConnectionOptions extends ConnectionConfig {
	host: string
	port: number
}

export interface ConnectionEvents {
	connect: []
	disconnect: [error?: Error]
	error: [error: Error]
}

/**
 * Manages a single TCP connection to a Kafka broker
 *
 * Features:
 * - Kafka message framing (4-byte length prefix)
 * - Request/response correlation via correlation IDs
 * - Partial message buffering
 * - Request pipelining with backpressure
 * - Automatic header version selection (v1/v2 based on flexible version)
 */
export class Connection extends EventEmitter<ConnectionEvents> {
	private socket: net.Socket | tls.TLSSocket | null = null
	private readonly socketFactory: SocketFactory
	private readonly requestQueue: RequestQueue
	private readonly logger: Logger
	private readonly saslConfig?: SaslConfig

	private readonly frameDecoder = new KafkaFrameDecoder()

	private _state: ConnectionState = 'disconnected'
	private connectPromise: Promise<void> | null = null

	readonly host: string
	readonly port: number
	readonly clientId: string

	constructor(options: ConnectionOptions) {
		super()
		this.host = options.host
		this.port = options.port
		this.clientId = options.clientId
		this.saslConfig = options.sasl
		this.logger =
			options.logger?.child({ component: 'connection', host: options.host, port: options.port }) ?? noopLogger

		const socketOptions: SocketFactoryOptions = {
			connectionTimeoutMs: options.connectionTimeoutMs,
			keepAlive: options.keepAlive,
			keepAliveInitialDelayMs: options.keepAliveInitialDelayMs,
			noDelay: options.noDelay,
			tls: options.tls,
		}
		this.socketFactory = new SocketFactory(socketOptions)

		const queueOptions: RequestQueueOptions = {
			maxInFlight: options.maxInFlightRequests ?? 5,
			defaultTimeoutMs: options.requestTimeoutMs ?? 30000,
		}
		this.requestQueue = new RequestQueue(queueOptions)
	}

	get state(): ConnectionState {
		return this._state
	}

	get isConnected(): boolean {
		return this._state === 'connected'
	}

	get pendingRequests(): number {
		return this.requestQueue.totalPending
	}

	get inFlightRequests(): number {
		return this.requestQueue.inFlightCount
	}

	get queuedRequests(): number {
		return this.requestQueue.queuedCount
	}

	/**
	 * Establish connection to the broker
	 */
	async connect(): Promise<void> {
		// Already connected
		if (this._state === 'connected') {
			return
		}

		// Connection in progress - wait for it
		if (this._state === 'connecting' && this.connectPromise) {
			return this.connectPromise
		}

		this._state = 'connecting'
		const startTime = Date.now()
		this.logger.debug('connecting')

		this.connectPromise = this.doConnect(startTime)

		try {
			await this.connectPromise
		} finally {
			this.connectPromise = null
		}
	}

	private async doConnect(startTime: number): Promise<void> {
		try {
			this.socket = await this.socketFactory.connect(this.host, this.port)

			// Perform SASL authentication if configured
			// This must happen before setting up the normal request queue
			if (this.saslConfig) {
				await this.performSaslAuthentication()
			}

			this._state = 'connected'

			this.requestQueue.setSendFunction((data: Buffer) => {
				this.socket!.write(data)
			})

			this.socket.on('data', this.handleData.bind(this))
			this.socket.on('error', this.handleError.bind(this))
			this.socket.on('close', this.handleClose.bind(this))

			const durationMs = Date.now() - startTime
			this.logger.debug('connected', { durationMs })
			this.emit('connect')
		} catch (error) {
			this._state = 'disconnected'
			if (this.socket) {
				this.socket.destroy()
				this.socket = null
			}
			this.logger.error('connection failed', { error: (error as Error).message })
			throw error
		}
	}

	/**
	 * Perform SASL authentication
	 */
	private async performSaslAuthentication(): Promise<void> {
		const authenticator = new SaslAuthenticator({
			config: this.saslConfig!,
			clientId: this.clientId,
			logger: this.logger,
			sendRaw: requestBuffer => this.sendRaw(requestBuffer),
		})

		await authenticator.authenticate()
	}

	/** Authentication timeout in milliseconds (default: 30 seconds) */
	private static readonly AUTH_TIMEOUT_MS = 30000

	/**
	 * Send a raw request during authentication phase
	 *
	 * This bypasses the normal request queue and handles a single
	 * request/response exchange directly on the socket.
	 *
	 * Includes timeout and close handling to prevent hanging if the broker
	 * stalls or closes mid-auth.
	 */
	private sendRaw(requestBuffer: Buffer): Promise<Buffer> {
		return new Promise((resolve, reject) => {
			if (!this.socket) {
				reject(new ConnectionClosedError('Socket not connected'))
				return
			}

			let responseBuffer = Buffer.alloc(0)
			let expectedLength = 0
			let settled = false

			const cleanup = (): void => {
				if (timeoutHandle) {
					clearTimeout(timeoutHandle)
				}
				this.socket?.removeListener('data', onData)
				this.socket?.removeListener('error', onError)
				this.socket?.removeListener('close', onClose)
			}

			const settle = (cb: () => void): void => {
				if (settled) return
				settled = true
				cleanup()
				cb()
			}

			const onData = (data: Buffer): void => {
				responseBuffer = Buffer.concat([responseBuffer, data])

				if (responseBuffer.length < 4) {
					return
				}

				if (expectedLength === 0) {
					expectedLength = responseBuffer.readInt32BE(0)
				}

				const totalLength = 4 + expectedLength
				if (responseBuffer.length < totalLength) {
					return
				}

				// Extract the complete message (without length prefix)
				const messageBuffer = responseBuffer.subarray(4, totalLength)

				settle(() => resolve(messageBuffer))
			}

			const onError = (error: Error): void => {
				settle(() => reject(new NetworkError(`SASL authentication failed: ${error.message}`)))
			}

			const onClose = (): void => {
				settle(() => reject(new ConnectionClosedError('Connection closed during SASL authentication')))
			}

			const onTimeout = (): void => {
				settle(() =>
					reject(new NetworkError(`SASL authentication timed out after ${Connection.AUTH_TIMEOUT_MS}ms`))
				)
			}

			const timeoutHandle = setTimeout(onTimeout, Connection.AUTH_TIMEOUT_MS)

			this.socket.on('data', onData)
			this.socket.on('error', onError)
			this.socket.on('close', onClose)

			this.socket.write(requestBuffer)
		})
	}

	/**
	 * Send a request and wait for response
	 *
	 * @param apiKey - The API key for this request
	 * @param apiVersion - The API version to use
	 * @param encodePayload - Function that encodes the request body (receives encoder positioned after header)
	 * @param timeoutMs - Optional request-specific timeout
	 * @returns Promise resolving to the complete response buffer (including header)
	 */
	async send(
		apiKey: ApiKey,
		apiVersion: number,
		encodePayload: (encoder: Encoder) => void,
		timeoutMs?: number
	): Promise<Buffer> {
		if (!this.isConnected) {
			throw new ConnectionClosedError('Cannot send on disconnected connection')
		}

		// Build function receives correlation ID from the request queue
		const buildRequest = (correlationId: number): Buffer => {
			const encoder = new Encoder()

			// Leave space for 4-byte length prefix (filled in at the end)
			encoder.writeInt32(0) // Placeholder

			// Encode request header
			encodeRequestHeader(encoder, {
				apiKey,
				apiVersion,
				correlationId,
				clientId: this.clientId,
			})

			// Encode request body
			encodePayload(encoder)

			// Get buffer and fill in length prefix
			const buffer = encoder.toBuffer()
			const messageLength = buffer.length - 4 // Exclude length prefix itself
			buffer.writeInt32BE(messageLength, 0)

			return buffer
		}

		return this.requestQueue.enqueue(apiKey, apiVersion, buildRequest, timeoutMs)
	}

	/**
	 * Handle incoming data from socket
	 *
	 * Kafka uses a simple framing protocol:
	 * - 4-byte big-endian length prefix
	 * - Message payload
	 *
	 * Data may arrive in chunks or multiple messages in one TCP segment.
	 */
	private handleData(data: Buffer): void {
		for (const messageBuffer of this.frameDecoder.push(data)) {
			this.handleMessage(messageBuffer)
		}
	}

	/**
	 * Process a complete response message
	 */
	private handleMessage(data: Buffer): void {
		// First 4 bytes are always the correlation ID
		if (data.length < 4) {
			this.emit('error', new NetworkError('Response too short to contain correlation ID'))
			return
		}

		const correlationId = data.readInt32BE(0)

		// Complete the pending request
		// The response buffer includes correlation ID - the caller will decode the full response
		const result = this.requestQueue.complete(correlationId, data)

		if (!result) {
			// Unknown correlation ID - could be timed out request
			// Log but don't error - this is expected in some cases
		}
	}

	private handleError(error: Error): void {
		this.logger.error('socket error', { error: error.message })
		this.emit('error', error)
	}

	private handleClose(hadError: boolean): void {
		const wasConnected = this._state === 'connected'
		this._state = 'disconnected'

		this.logger.debug('connection closed', { hadError, pendingRequests: this.requestQueue.totalPending })

		this.requestQueue.rejectAll(new ConnectionClosedError())

		this.frameDecoder.reset()

		if (wasConnected) {
			this.emit('disconnect', hadError ? new Error('Connection closed with error') : undefined)
		}
	}

	/**
	 * Close the connection gracefully
	 *
	 * @param waitForPending - If true, wait for pending requests (with timeout)
	 * @param waitTimeoutMs - Maximum time to wait for pending requests (default: 5000)
	 */
	async close(waitForPending = true, waitTimeoutMs = 5000): Promise<void> {
		if (this._state === 'closed' || this._state === 'closing') {
			return
		}

		this._state = 'closing'
		this.logger.debug('closing connection', { waitForPending, pendingRequests: this.requestQueue.totalPending })

		if (waitForPending && this.requestQueue.totalPending > 0) {
			const startTime = Date.now()
			while (this.requestQueue.totalPending > 0 && Date.now() - startTime < waitTimeoutMs) {
				await sleep(50)
			}
		}

		this.requestQueue.rejectAll(new ConnectionClosedError('Connection closing'))

		if (this.socket) {
			this.socket.destroy()
			this.socket = null
		}

		this._state = 'closed'
		this.frameDecoder.reset()
		this.logger.debug('connection closed')
	}

	/**
	 * Get a string representation of this connection
	 */
	override toString(): string {
		return `Connection(${this.host}:${this.port}, state=${this._state})`
	}
}
