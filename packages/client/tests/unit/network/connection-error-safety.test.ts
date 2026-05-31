import { describe, expect, it } from 'vitest'
import { EventEmitter } from 'node:events'
import type * as net from 'node:net'

import { Connection } from '@/network/connection.js'

/**
 * Regression: a socket error (e.g. ECONNRESET when a broker dies/restarts) used to be
 * re-emitted as an unhandled 'error' event on the Connection EventEmitter, which crashes
 * the host process. Broker owns its Connections internally and callers cannot attach a
 * listener, so the Connection must surface internal errors crash-safely (emitError guards
 * on listenerCount and logs when nobody is listening).
 */
function makeConnection(): Connection {
	return new Connection({ host: 'localhost', port: 9092, clientId: 'test-client' })
}

describe('Connection error-event safety', () => {
	it('does not throw when an internal error is surfaced with no error listener', () => {
		const connection = makeConnection()
		expect(() =>
			(connection as unknown as { emitError(e: Error): void }).emitError(new Error('ECONNRESET'))
		).not.toThrow()
	})

	it('a socket error with no error listener does not throw (was: unhandled "error" crashed the process)', () => {
		const connection = makeConnection()
		const socket = new EventEmitter() as unknown as net.Socket
		;(connection as unknown as { socket: net.Socket }).socket = socket
		// Wire the real handler exactly as connect() does.
		socket.on('error', (connection as unknown as { handleError(e: Error): void }).handleError.bind(connection))
		expect(() => socket.emit('error', new Error('read ECONNRESET'))).not.toThrow()
	})

	it('still delivers to an attached error listener', () => {
		const connection = makeConnection()
		const seen: Error[] = []
		connection.on('error', e => seen.push(e))
		;(connection as unknown as { emitError(e: Error): void }).emitError(new Error('boom'))
		expect(seen.map(e => e.message)).toEqual(['boom'])
	})
})
