/**
 * Network layer for Kafka connections
 *
 * Provides TCP/TLS socket management, request pipelining,
 * connection pooling, and automatic reconnection.
 */

// Types
export type {
	TlsConfig,
	SocketConfig,
	ConnectionConfig,
	ConnectionPoolConfig,
	ReconnectionConfig,
	BrokerAddress,
	PendingRequest,
	QueuedRequest,
	ConnectionState,
} from '@/network/types.js'

// Errors
export {
	NetworkError,
	ConnectionError,
	ConnectionTimeoutError,
	ConnectionClosedError,
	RequestTimeoutError,
} from '@/network/errors.js'

// Socket Factory
export { SocketFactory, type SocketFactoryOptions } from '@/network/socket-factory.js'

// Request Queue
export { RequestQueue, type RequestQueueOptions } from '@/network/request-queue.js'

// Connection
export { Connection, type ConnectionOptions, type ConnectionEvents } from '@/network/connection.js'

// Connection Pool
export { ConnectionPool, type ConnectionPoolOptions, type ConnectionPoolStats } from '@/network/connection-pool.js'

// Reconnection
export {
	ReconnectionStrategy,
	ReconnectingConnection,
	type ReconnectionStrategyOptions,
} from '@/network/reconnection.js'
