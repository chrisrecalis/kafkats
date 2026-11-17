import { setTimeout as sleep } from 'node:timers/promises'
import type { StreamState } from '@/legacy/types.js'

/**
 * Lifecycle status of a flow app: its reported state and last error, whether it is starting,
 * running or closing, and the shutdown signal that interrupts waits.
 *
 * Every runtime component reads it; components report failures and state changes through it.
 */
export class AppStatus {
	state: StreamState = 'CREATED'
	lastError: Error | null = null
	/** True once start() has completed, until shutdown. */
	started = false
	/** True once the cluster was reachable at startup (false only with mocked/offline clients). */
	clusterAvailable = false
	/** True once processing was stopped for good by a failure (see halt), until the next start(). */
	halted = false
	private startupFailure: Error | null = null
	private shutdown = new AbortController()

	/** True from close() (or a failed start) until the next start(). */
	get closing(): boolean {
		return this.shutdown.signal.aborted
	}

	/** Aborted when the app starts shutting down. */
	get shutdownSignal(): AbortSignal {
		return this.shutdown.signal
	}

	/** Reset for a (re)start. */
	beginStart(): void {
		this.state = 'CREATED'
		this.lastError = null
		this.started = false
		this.halted = false
		this.startupFailure = null
		this.shutdown = new AbortController()
	}

	/**
	 * Stop processing for good after a failure that restarting a worker cannot fix. The app reports
	 * 'ERROR' until it is closed, and no worker (re)starts: not one that is running, and not one
	 * that is waiting out a restart backoff either.
	 */
	halt(error: Error): void {
		this.lastError = error
		this.state = 'ERROR'
		this.halted = true
	}

	/** Stop starting new work and wake up everything waiting in sleepUnlessClosing(). */
	beginShutdown(): void {
		this.shutdown.abort()
	}

	/** Record a failure that must fail an in-flight start(); ignored once the app has started. */
	failStartup(error: Error): void {
		if (!this.started) this.startupFailure ??= error
	}

	/** Throw if start() must not continue: the app is closing or a background task failed startup. */
	assertNotClosing(): void {
		if (this.closing) {
			throw new Error('Flow was closed while starting')
		}
		if (this.startupFailure) {
			throw this.startupFailure
		}
	}

	/** Sleep for `ms`, returning early when the app starts shutting down. */
	async sleepUnlessClosing(ms: number): Promise<void> {
		await sleep(ms, undefined, { signal: this.shutdown.signal }).catch(() => {})
	}
}
