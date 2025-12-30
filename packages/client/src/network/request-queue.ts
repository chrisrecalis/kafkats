/**
 * Request queue for managing request pipelining, correlation, and backpressure
 */

import type { ApiKey } from '@/protocol/messages/api-keys.js'
import type { PendingRequest, QueuedRequest } from '@/network/types.js'
import { RequestTimeoutError, ConnectionClosedError } from '@/network/errors.js'

export interface RequestQueueOptions {
	/** Maximum in-flight requests (default: 5) */
	maxInFlight?: number
	/** Default request timeout in ms (default: 30000) */
	defaultTimeoutMs?: number
}

/**
 * Manages request pipelining and response correlation
 *
 * Features:
 * - Correlates requests to responses via correlation IDs
 * - Limits in-flight requests with internal queuing for backpressure
 * - Handles request timeouts
 * - Automatically sends queued requests when slots become available
 */
export class RequestQueue {
	private readonly pending = new Map<number, PendingRequest>()
	private readonly waiting: QueuedRequest[] = []
	private waitingHead = 0
	private correlationId = 0
	private readonly maxInFlight: number
	private readonly defaultTimeoutMs: number
	private sendFn: ((data: Buffer) => void) | null = null

	constructor(options: RequestQueueOptions = {}) {
		this.maxInFlight = options.maxInFlight ?? 5
		this.defaultTimeoutMs = options.defaultTimeoutMs ?? 30000
	}

	/**
	 * Set the send function used to write to the socket
	 * Must be called before enqueue()
	 */
	setSendFunction(fn: (data: Buffer) => void): void {
		this.sendFn = fn
	}

	/**
	 * Generate next correlation ID (wraps at 2^31 - 1)
	 */
	nextCorrelationId(): number {
		this.correlationId = (this.correlationId + 1) & 0x7fffffff
		return this.correlationId
	}

	/**
	 * Number of in-flight requests (sent, awaiting response)
	 */
	get inFlightCount(): number {
		return this.pending.size
	}

	/**
	 * Number of queued requests (waiting to be sent)
	 */
	get queuedCount(): number {
		return this.waiting.length - this.waitingHead
	}

	/**
	 * Total pending requests (in-flight + queued)
	 */
	get totalPending(): number {
		return this.pending.size + this.queuedCount
	}

	/**
	 * Check if we can send more requests immediately
	 */
	canSendImmediately(): boolean {
		return this.pending.size < this.maxInFlight
	}

	/**
	 * Enqueue a request for sending
	 *
	 * If under the in-flight limit, sends immediately.
	 * Otherwise, queues the request and sends when a slot becomes available.
	 *
	 * @param apiKey - The API key for this request
	 * @param apiVersion - The API version to use
	 * @param buildRequest - Function that builds the request buffer (receives correlationId)
	 * @param timeoutMs - Optional request-specific timeout (starts when sent, not queued)
	 * @returns Promise resolving to the response payload
	 */
	enqueue(
		apiKey: ApiKey,
		apiVersion: number,
		buildRequest: (correlationId: number) => Buffer,
		timeoutMs?: number
	): Promise<Buffer> {
		const timeout = timeoutMs ?? this.defaultTimeoutMs

		return new Promise((resolve, reject) => {
			const queuedRequest: QueuedRequest = {
				apiKey,
				apiVersion,
				timeoutMs: timeout,
				buildRequest,
				resolve,
				reject,
			}

			if (this.canSendImmediately()) {
				this.sendRequest(queuedRequest)
			} else {
				this.waiting.push(queuedRequest)
			}
		})
	}

	/**
	 * Complete a request with its response
	 *
	 * @param correlationId - The correlation ID from the response
	 * @param responsePayload - The complete response buffer
	 * @returns The API key and version for response decoding, or null if unknown
	 */
	complete(correlationId: number, responsePayload: Buffer): { apiKey: ApiKey; apiVersion: number } | null {
		const request = this.pending.get(correlationId)
		if (!request) {
			// Response for unknown/timed-out request
			return null
		}

		this.pending.delete(correlationId)
		clearTimeout(request.timeoutHandle)
		request.resolve(responsePayload)

		this.processQueue()

		return { apiKey: request.apiKey, apiVersion: request.apiVersion }
	}

	/**
	 * Reject all pending and queued requests
	 *
	 * @param error - The error to reject with (defaults to ConnectionClosedError)
	 */
	rejectAll(error?: Error): void {
		const rejectError = error ?? new ConnectionClosedError()

		for (const request of this.pending.values()) {
			clearTimeout(request.timeoutHandle)
			request.reject(rejectError)
		}
		this.pending.clear()

		for (const request of this.waiting) {
			request.reject(rejectError)
		}
		for (let i = this.waitingHead; i < this.waiting.length; i++) {
			this.waiting[i]!.reject(rejectError)
		}
		this.waiting.length = 0
		this.waitingHead = 0
	}

	/**
	 * Clear all pending requests without rejection
	 */
	clear(): void {
		for (const request of this.pending.values()) {
			clearTimeout(request.timeoutHandle)
		}
		this.pending.clear()
		this.waiting.length = 0
		this.waitingHead = 0
	}

	/**
	 * Send a request immediately
	 */
	private sendRequest(queuedRequest: QueuedRequest): void {
		if (!this.sendFn) {
			queuedRequest.reject(new Error('Send function not set'))
			return
		}

		const correlationId = this.nextCorrelationId()

		// Build the request with the correlation ID
		let requestBuffer: Buffer
		try {
			requestBuffer = queuedRequest.buildRequest(correlationId)
		} catch (error) {
			queuedRequest.reject(error instanceof Error ? error : new Error(String(error)))
			return
		}

		const timeoutHandle = setTimeout(() => {
			this.pending.delete(correlationId)
			queuedRequest.reject(new RequestTimeoutError(correlationId, queuedRequest.timeoutMs))
			this.processQueue()
		}, queuedRequest.timeoutMs)

		// Track the pending request
		const pendingRequest: PendingRequest = {
			correlationId,
			apiKey: queuedRequest.apiKey,
			apiVersion: queuedRequest.apiVersion,
			sentAt: Date.now(),
			timeoutMs: queuedRequest.timeoutMs,
			resolve: queuedRequest.resolve,
			reject: queuedRequest.reject,
			timeoutHandle,
		}
		this.pending.set(correlationId, pendingRequest)

		try {
			this.sendFn(requestBuffer)
		} catch (error) {
			this.pending.delete(correlationId)
			clearTimeout(timeoutHandle)
			queuedRequest.reject(error instanceof Error ? error : new Error(String(error)))
		}
	}

	/**
	 * Process the waiting queue, sending requests if slots are available
	 */
	private processQueue(): void {
		while (this.canSendImmediately() && this.waitingHead < this.waiting.length) {
			const next = this.waiting[this.waitingHead++]!
			this.sendRequest(next)
		}

		// Compact the queue to avoid unbounded growth and keep queuedCount accurate.
		if (this.waitingHead > 0 && this.waitingHead === this.waiting.length) {
			this.waiting.length = 0
			this.waitingHead = 0
			return
		}

		// Periodic compaction when head grows large.
		if (this.waitingHead > 1024 && this.waitingHead > this.waiting.length / 2) {
			this.waiting.splice(0, this.waitingHead)
			this.waitingHead = 0
		}
	}
}
