/**
 * Shared types and interfaces for the network layer
 */

import type { ApiKey } from '@/protocol/messages/api-keys.js'
import type { Logger } from '@/logger.js'

/**
 * TLS configuration options
 */
export interface TlsConfig {
	/** Enable TLS (default: false) */
	enabled: boolean
	/** CA certificate(s) for server verification */
	ca?: string | Buffer | Array<string | Buffer>
	/** Client certificate for mTLS */
	cert?: string | Buffer
	/** Client private key for mTLS */
	key?: string | Buffer
	/** Passphrase for private key */
	passphrase?: string
	/** Reject unauthorized certificates (default: true) */
	rejectUnauthorized?: boolean
	/** Server name for SNI */
	servername?: string
}

/**
 * SASL authentication configuration
 */
export interface SaslConfig {
	/** SASL mechanism to use */
	mechanism: 'PLAIN' | 'SCRAM-SHA-256' | 'SCRAM-SHA-512'
	/** Username for authentication */
	username: string
	/** Password for authentication */
	password: string
}

/**
 * Socket configuration options
 */
export interface SocketConfig {
	/** Connection timeout in milliseconds (default: 10000) */
	connectionTimeoutMs?: number
	/** Enable TCP keep-alive (default: true) */
	keepAlive?: boolean
	/** Keep-alive initial delay in milliseconds (default: 60000) */
	keepAliveInitialDelayMs?: number
	/** Enable TCP_NODELAY (default: true) */
	noDelay?: boolean
	/** TLS configuration */
	tls?: TlsConfig
	/** SASL authentication configuration */
	sasl?: SaslConfig
}

/**
 * Connection configuration options
 */
export interface ConnectionConfig extends SocketConfig {
	/** Client ID for Kafka protocol */
	clientId: string
	/** Request timeout in milliseconds (default: 30000) */
	requestTimeoutMs?: number
	/** Maximum number of in-flight requests per connection (default: 5) */
	maxInFlightRequests?: number
	/** Logger instance */
	logger?: Logger
}

/**
 * Connection pool configuration
 */
export interface ConnectionPoolConfig extends ConnectionConfig {
	/** Minimum connections per broker (default: 1) */
	minConnections?: number
	/** Maximum connections per broker (default: 5) */
	maxConnections?: number
	/** Idle timeout before connection is closed in milliseconds (default: 300000) */
	idleTimeoutMs?: number
	/** Acquire timeout when pool is exhausted in milliseconds (default: 5000) */
	acquireTimeoutMs?: number
}

/**
 * Reconnection strategy configuration
 */
export interface ReconnectionConfig {
	/** Maximum reconnection attempts (default: Infinity) */
	maxAttempts?: number
	/** Initial backoff delay in milliseconds (default: 100) */
	initialDelayMs?: number
	/** Maximum backoff delay in milliseconds (default: 30000) */
	maxDelayMs?: number
	/** Backoff multiplier (default: 2) */
	multiplier?: number
	/** Jitter factor 0-1 (default: 0.2) */
	jitter?: number
}

/**
 * Broker address
 */
export interface BrokerAddress {
	host: string
	port: number
	nodeId?: number
}

/**
 * Connection state
 */
export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'closing' | 'closed'

/**
 * Pending request tracking
 */
export interface PendingRequest {
	correlationId: number
	apiKey: ApiKey
	apiVersion: number
	sentAt: number
	timeoutMs: number
	resolve: (response: Buffer) => void
	reject: (error: Error) => void
	timeoutHandle: NodeJS.Timeout
}

/**
 * Queued request waiting to be sent
 */
export interface QueuedRequest {
	apiKey: ApiKey
	apiVersion: number
	timeoutMs: number
	/** Build request buffer - receives correlation ID to include in header */
	buildRequest: (correlationId: number) => Buffer
	resolve: (response: Buffer) => void
	reject: (error: Error) => void
}
