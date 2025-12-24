/**
 * Network layer error types for Kafka connections
 */

/**
 * Base class for all network-related errors
 */
export class NetworkError extends Error {
	override readonly cause?: Error

	constructor(message: string, cause?: Error) {
		super(message)
		this.name = 'NetworkError'
		this.cause = cause
		// Maintains proper stack trace in V8
		if (Error.captureStackTrace) {
			Error.captureStackTrace(this, this.constructor)
		}
	}
}

/**
 * Thrown when connection establishment fails
 */
export class ConnectionError extends NetworkError {
	constructor(
		public readonly host: string,
		public readonly port: number,
		message: string,
		cause?: Error
	) {
		super(`Connection to ${host}:${port} failed: ${message}`, cause)
		this.name = 'ConnectionError'
	}
}

/**
 * Thrown when connection establishment times out
 */
export class ConnectionTimeoutError extends ConnectionError {
	constructor(
		host: string,
		port: number,
		public readonly timeoutMs: number
	) {
		super(host, port, `timed out after ${timeoutMs}ms`)
		this.name = 'ConnectionTimeoutError'
	}
}

/**
 * Thrown when connection is unexpectedly closed
 */
export class ConnectionClosedError extends NetworkError {
	constructor(message: string = 'Connection closed unexpectedly') {
		super(message)
		this.name = 'ConnectionClosedError'
	}
}

/**
 * Thrown when a request times out waiting for response
 */
export class RequestTimeoutError extends NetworkError {
	constructor(
		public readonly correlationId: number,
		public readonly timeoutMs: number
	) {
		super(`Request ${correlationId} timed out after ${timeoutMs}ms`)
		this.name = 'RequestTimeoutError'
	}
}
