/**
 * Socket factory for creating TCP and TLS connections
 */

import * as net from 'node:net'
import * as tls from 'node:tls'
import type { SocketConfig, TlsConfig } from '@/network/types.js'
import { ConnectionError, ConnectionTimeoutError } from '@/network/errors.js'

export type SocketFactoryOptions = SocketConfig

/**
 * Resolved socket configuration with defaults applied
 */
interface ResolvedSocketConfig {
	connectionTimeoutMs: number
	keepAlive: boolean
	keepAliveInitialDelayMs: number
	noDelay: boolean
	tls?: TlsConfig
}

/**
 * Factory for creating TCP and TLS sockets with consistent configuration
 */
export class SocketFactory {
	private readonly config: ResolvedSocketConfig

	constructor(options: SocketFactoryOptions = {}) {
		this.config = {
			connectionTimeoutMs: options.connectionTimeoutMs ?? 10000,
			keepAlive: options.keepAlive ?? true,
			keepAliveInitialDelayMs: options.keepAliveInitialDelayMs ?? 60000,
			noDelay: options.noDelay ?? true,
			tls: options.tls,
		}
	}

	/**
	 * Create a connected socket to the specified host and port
	 *
	 * @param host - The host to connect to
	 * @param port - The port to connect to
	 * @returns A connected socket (TCP or TLS depending on configuration)
	 */
	async connect(host: string, port: number): Promise<net.Socket | tls.TLSSocket> {
		return new Promise((resolve, reject) => {
			let socket: net.Socket | tls.TLSSocket
			let settled = false

			const timeoutHandle = setTimeout(() => {
				if (!settled) {
					settled = true
					socket.destroy()
					reject(new ConnectionTimeoutError(host, port, this.config.connectionTimeoutMs))
				}
			}, this.config.connectionTimeoutMs)

			const onConnect = () => {
				if (settled) return
				settled = true
				clearTimeout(timeoutHandle)
				socket.removeListener('error', onError)

				// Configure socket options after connection
				this.configureSocket(socket)

				resolve(socket)
			}

			const onError = (error: Error) => {
				if (settled) return
				settled = true
				clearTimeout(timeoutHandle)
				socket.destroy()
				reject(new ConnectionError(host, port, error.message, error))
			}

			if (this.config.tls?.enabled) {
				socket = tls.connect({
					host,
					port,
					ca: this.config.tls.ca,
					cert: this.config.tls.cert,
					key: this.config.tls.key,
					passphrase: this.config.tls.passphrase,
					rejectUnauthorized: this.config.tls.rejectUnauthorized ?? true,
					servername: this.config.tls.servername ?? host,
				})
				socket.once('secureConnect', onConnect)
			} else {
				socket = net.connect({ host, port })
				socket.once('connect', onConnect)
			}

			socket.once('error', onError)
		})
	}

	/**
	 * Configure socket options after connection
	 */
	private configureSocket(socket: net.Socket | tls.TLSSocket): void {
		if (this.config.keepAlive) {
			socket.setKeepAlive(true, this.config.keepAliveInitialDelayMs)
		}
		if (this.config.noDelay) {
			socket.setNoDelay(true)
		}
	}
}
