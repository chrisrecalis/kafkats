/**
 * Connection pool for managing multiple connections to a Kafka broker
 */

import type { ConnectionPoolConfig, BrokerAddress } from '@/network/types.js'
import { Connection, type ConnectionOptions } from '@/network/connection.js'
import { NetworkError } from '@/network/errors.js'

interface PooledConnection {
	connection: Connection
	inUse: boolean
	lastUsed: number
	createdAt: number
}

interface Waiter {
	resolve: (conn: Connection) => void
	reject: (error: Error) => void
	timeoutHandle: NodeJS.Timeout
}

export type ConnectionPoolOptions = ConnectionPoolConfig

/**
 * Connection pool statistics
 */
export interface ConnectionPoolStats {
	/** Total connections in pool */
	total: number
	/** Available (idle) connections */
	available: number
	/** Connections currently in use */
	inUse: number
	/** Requests waiting for a connection */
	waiting: number
}

/**
 * Connection pool for a single broker
 *
 * Features:
 * - Acquire/release pattern for connection management
 * - Automatic scaling between min and max connections
 * - Idle connection cleanup
 * - Waiter queue when pool is exhausted
 */
export class ConnectionPool {
	private readonly connections: PooledConnection[] = []
	private readonly waitQueue: Waiter[] = []

	private readonly minConnections: number
	private readonly maxConnections: number
	private readonly idleTimeoutMs: number
	private readonly acquireTimeoutMs: number

	private idleCleanupInterval: NodeJS.Timeout | null = null
	private closed = false

	constructor(
		private readonly address: BrokerAddress,
		private readonly config: ConnectionPoolOptions
	) {
		this.minConnections = config.minConnections ?? 1
		this.maxConnections = config.maxConnections ?? 5
		this.idleTimeoutMs = config.idleTimeoutMs ?? 300000 // 5 minutes
		this.acquireTimeoutMs = config.acquireTimeoutMs ?? 5000
	}

	/**
	 * Initialize the pool with minimum connections
	 */
	async initialize(): Promise<void> {
		if (this.closed) {
			throw new NetworkError('Connection pool is closed')
		}

		// Start idle cleanup timer
		this.idleCleanupInterval = setInterval(
			() => this.cleanupIdleConnections(),
			Math.min(this.idleTimeoutMs / 2, 60000)
		)
		this.idleCleanupInterval.unref()

		const promises: Promise<void>[] = []
		for (let i = 0; i < this.minConnections; i++) {
			promises.push(this.createConnection())
		}
		await Promise.all(promises)
	}

	/**
	 * Acquire a connection from the pool
	 *
	 * Returns an available connection, creates a new one if under limit,
	 * or waits until one becomes available.
	 */
	async acquire(): Promise<Connection> {
		if (this.closed) {
			throw new NetworkError('Connection pool is closed')
		}

		const available = this.connections.find(pc => !pc.inUse && pc.connection.isConnected)
		if (available) {
			available.inUse = true
			available.lastUsed = Date.now()
			return available.connection
		}

		if (this.connections.length < this.maxConnections) {
			try {
				await this.createConnection()
				const newConn = this.connections[this.connections.length - 1]
				if (newConn && newConn.connection.isConnected) {
					newConn.inUse = true
					newConn.lastUsed = Date.now()
					return newConn.connection
				}
			} catch {
				// Fall through to wait queue
			}
		}

		return new Promise((resolve, reject) => {
			const timeoutHandle = setTimeout(() => {
				const idx = this.waitQueue.findIndex(w => w.resolve === resolve)
				if (idx >= 0) {
					this.waitQueue.splice(idx, 1)
				}
				reject(new NetworkError(`Connection acquire timeout after ${this.acquireTimeoutMs}ms`))
			}, this.acquireTimeoutMs)

			this.waitQueue.push({ resolve, reject, timeoutHandle })
		})
	}

	/**
	 * Release a connection back to the pool
	 */
	release(connection: Connection): void {
		const pooled = this.connections.find(pc => pc.connection === connection)
		if (!pooled) {
			// Connection not from this pool
			return
		}

		pooled.inUse = false
		pooled.lastUsed = Date.now()

		if (this.waitQueue.length > 0 && connection.isConnected) {
			const waiter = this.waitQueue.shift()!
			clearTimeout(waiter.timeoutHandle)
			pooled.inUse = true
			waiter.resolve(connection)
		}
	}

	/**
	 * Get pool statistics
	 */
	get stats(): ConnectionPoolStats {
		const available = this.connections.filter(pc => !pc.inUse && pc.connection.isConnected).length
		const inUse = this.connections.filter(pc => pc.inUse).length

		return {
			total: this.connections.length,
			available,
			inUse,
			waiting: this.waitQueue.length,
		}
	}

	/**
	 * Get the broker address for this pool
	 */
	get brokerAddress(): BrokerAddress {
		return this.address
	}

	/**
	 * Check if the pool is closed
	 */
	get isClosed(): boolean {
		return this.closed
	}

	/**
	 * Close the pool and all connections
	 */
	async close(): Promise<void> {
		if (this.closed) {
			return
		}

		this.closed = true

		// Stop idle cleanup
		if (this.idleCleanupInterval) {
			clearInterval(this.idleCleanupInterval)
			this.idleCleanupInterval = null
		}

		for (const waiter of this.waitQueue) {
			clearTimeout(waiter.timeoutHandle)
			waiter.reject(new NetworkError('Connection pool is closing'))
		}
		this.waitQueue.length = 0

		await Promise.all(this.connections.map(pc => pc.connection.close(true)))
		this.connections.length = 0
	}

	/**
	 * Create a new connection and add to pool
	 */
	private async createConnection(): Promise<void> {
		const options: ConnectionOptions = {
			host: this.address.host,
			port: this.address.port,
			clientId: this.config.clientId,
			connectionTimeoutMs: this.config.connectionTimeoutMs,
			requestTimeoutMs: this.config.requestTimeoutMs,
			maxInFlightRequests: this.config.maxInFlightRequests,
			keepAlive: this.config.keepAlive,
			keepAliveInitialDelayMs: this.config.keepAliveInitialDelayMs,
			noDelay: this.config.noDelay,
			tls: this.config.tls,
			sasl: this.config.sasl,
		}

		const connection = new Connection(options)

		connection.on('disconnect', () => {
			this.handleDisconnection(connection)
		})

		connection.on('error', error => {
			// Log error but don't remove connection immediately
			// The disconnect event will handle cleanup if needed
			console.error(`Connection error for ${this.address.host}:${this.address.port}:`, error.message)
		})

		await connection.connect()

		this.connections.push({
			connection,
			inUse: false,
			lastUsed: Date.now(),
			createdAt: Date.now(),
		})
	}

	/**
	 * Handle disconnection of a pooled connection
	 */
	private handleDisconnection(connection: Connection): void {
		const idx = this.connections.findIndex(pc => pc.connection === connection)
		if (idx >= 0) {
			const pooled = this.connections[idx]!
			this.connections.splice(idx, 1)

			// If connection was in use, it might have a waiter
			// Nothing to do since the caller's promise will be rejected
			// via the request queue

			if (!pooled.inUse && this.waitQueue.length > 0) {
				this.tryFulfillWaiter()
			}
		}

		// Attempt to maintain minimum connections
		if (!this.closed && this.connections.length < this.minConnections) {
			this.createConnection().catch(() => {
				// Will be retried on next acquire or by cleanup
			})
		}
	}

	/**
	 * Try to fulfill a waiting acquire request
	 */
	private tryFulfillWaiter(): void {
		if (this.waitQueue.length === 0) {
			return
		}

		const available = this.connections.find(pc => !pc.inUse && pc.connection.isConnected)
		if (available) {
			const waiter = this.waitQueue.shift()!
			clearTimeout(waiter.timeoutHandle)
			available.inUse = true
			available.lastUsed = Date.now()
			waiter.resolve(available.connection)
		}
	}

	/**
	 * Cleanup idle connections
	 */
	private cleanupIdleConnections(): void {
		if (this.closed) {
			return
		}

		const now = Date.now()
		const toRemove: number[] = []

		for (let i = 0; i < this.connections.length; i++) {
			const pooled = this.connections[i]!

			if (pooled.inUse) continue
			if (this.connections.length - toRemove.length <= this.minConnections) break

			if (now - pooled.lastUsed > this.idleTimeoutMs) {
				toRemove.push(i)
			}
		}

		for (let i = toRemove.length - 1; i >= 0; i--) {
			const idx = toRemove[i]!
			const pooled = this.connections[idx]!
			this.connections.splice(idx, 1)
			pooled.connection.close().catch(() => {
				// Ignore close errors
			})
		}
	}
}
