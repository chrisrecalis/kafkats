import { setTimeout as sleep } from 'node:timers/promises'
import type { StreamState } from '@/types.js'

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
		this.startupFailure = null
		this.shutdown = new AbortController()
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
