import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { ConnectionPool } from '@/network/connection-pool.js'
import { Connection } from '@/network/connection.js'

vi.mock('@/network/connection.js', () => {
	let counter = 0
	let dieAfterConnect = false
	class MockConnection {
		readonly id: number
		isConnected = false
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		private listeners: Record<string, Array<(...args: any[]) => void>> = {}

		constructor() {
			this.id = ++counter
		}

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		on(event: string, fn: (...args: any[]) => void): void {
			;(this.listeners[event] ??= []).push(fn)
		}

		async connect(): Promise<void> {
			// Simulate async connect — yields to event loop so concurrent acquirers
			// can interleave between the await in createConnection and the post-await read.
			await Promise.resolve()
			await Promise.resolve()
			// dieAfterConnect simulates a connection that drops between connect()
			// resolving and the pool registering it.
			this.isConnected = !dieAfterConnect
		}

		async close(): Promise<void> {
			this.isConnected = false
		}
	}
	return {
		Connection: MockConnection,
		__resetCounter: () => {
			counter = 0
		},
		__setDieAfterConnect: (value: boolean) => {
			dieAfterConnect = value
		},
	}
})

const setDieAfterConnect = async (value: boolean): Promise<void> => {
	const module = (await import('@/network/connection.js')) as unknown as {
		__setDieAfterConnect: (value: boolean) => void
	}
	module.__setDieAfterConnect(value)
}

describe('ConnectionPool', () => {
	beforeEach(async () => {
		// Reset the per-test connection counter
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(Connection as any).__resetCounter?.()
		await setDieAfterConnect(false)
	})

	afterEach(() => {
		vi.useRealTimers()
	})

	it('hands distinct connections to concurrent acquirers (no race)', async () => {
		const pool = new ConnectionPool(
			{ host: 'localhost', port: 9092 },
			{ clientId: 'test', minConnections: 0, maxConnections: 5 }
		)
		await pool.initialize()

		// Three concurrent acquires before any connection exists.
		// Without the fix, all three see connections[length-1] after their await
		// and all flip the same PooledConnection to inUse=true.
		const [a, b, c] = await Promise.all([pool.acquire(), pool.acquire(), pool.acquire()])

		expect(a).not.toBe(b)
		expect(b).not.toBe(c)
		expect(a).not.toBe(c)
		const stats = pool.stats
		expect(stats.inUse).toBe(3)
		expect(stats.total).toBe(3)

		await pool.close()
	})

	it('does not let a late acquirer grab a connection between push and inUse=true', async () => {
		// Reproduces the narrower race: caller A awaits createConnection(),
		// the entry is pushed but not yet marked inUse. Caller B enters acquire()
		// in that gap, runs the find() at the top, and would grab the same entry.
		const pool = new ConnectionPool(
			{ host: 'localhost', port: 9092 },
			{ clientId: 'test', minConnections: 0, maxConnections: 5 }
		)
		await pool.initialize()

		// Two concurrent acquires will trigger this scenario.
		const [a, b] = await Promise.all([pool.acquire(), pool.acquire()])
		expect(a).not.toBe(b)
		expect(pool.stats.inUse).toBe(2)

		await pool.close()
	})

	it('never exceeds maxConnections under concurrent acquire pressure', async () => {
		const pool = new ConnectionPool(
			{ host: 'localhost', port: 9092 },
			{ clientId: 'test', minConnections: 0, maxConnections: 2, acquireTimeoutMs: 20 }
		)
		await pool.initialize()

		// 10 concurrent acquirers all pass the `connections.length < maxConnections`
		// check before any creation completes — without a synchronous reservation,
		// every one of them creates a connection and the pool balloons to 10.
		const results = await Promise.allSettled(Array.from({ length: 10 }, () => pool.acquire()))

		expect(pool.stats.total).toBeLessThanOrEqual(2)
		const fulfilled = results.filter(result => result.status === 'fulfilled')
		expect(fulfilled).toHaveLength(2)

		await pool.close()
	})

	it('does not register a connection that died between connect() and registration', async () => {
		await setDieAfterConnect(true)

		const pool = new ConnectionPool(
			{ host: 'localhost', port: 9092 },
			{ clientId: 'test', minConnections: 0, maxConnections: 2, acquireTimeoutMs: 20 }
		)
		await pool.initialize()

		const [result] = await Promise.allSettled([pool.acquire()])
		expect(result!.status).toBe('rejected')

		// The dead connection must not linger in the pool.
		expect(pool.stats.total).toBe(0)

		await pool.close()
	})
})
