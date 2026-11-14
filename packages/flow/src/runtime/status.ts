import type { StreamState } from '@/types.js'

/**
 * Lifecycle status of a flow app: its reported state and last error, whether it is starting,
 * running or closing, and the shutdown signal that interrupts waits.
 *
 * Every runtime component reads it; components report failures and state changes through it.
 */
export class AppStatus {
	private currentState: StreamState = 'CREATED'
	private error: Error | null = null
	private closingFlag = false
	private startedFlag = false
	private startupFailure: Error | null = null
	private clusterReachable = false
	private shutdownController = new AbortController()

	get state(): StreamState {
		return this.currentState
	}

	setState(state: StreamState): void {
		this.currentState = state
	}

	get lastError(): Error | null {
		return this.error
	}

	recordError(error: Error): void {
		this.error = error
	}

	/** True from close() (or a failed start) until the next start(). */
	get closing(): boolean {
		return this.closingFlag
	}

	/** True once start() has completed, until shutdown. */
	get started(): boolean {
		return this.startedFlag
	}

	/** True once the cluster was reachable at startup (false only with mocked/offline clients). */
	get clusterAvailable(): boolean {
		return this.clusterReachable
	}

	setClusterAvailable(available: boolean): void {
		this.clusterReachable = available
	}

	get shutdownSignal(): AbortSignal {
		return this.shutdownController.signal
	}

	/** Reset for a (re)start. */
	beginStart(): void {
		this.closingFlag = false
		this.startedFlag = false
		this.currentState = 'CREATED'
		this.startupFailure = null
		this.error = null
		this.shutdownController = new AbortController()
	}

	markStarted(): void {
		this.startedFlag = true
	}

	markShutDown(): void {
		this.startedFlag = false
	}

	/** Stop starting new work and wake up everything waiting in sleepUnlessClosing(). */
	beginShutdown(): void {
		this.closingFlag = true
		this.shutdownController.abort()
	}

	/** Record a failure that must fail an in-flight start(); ignored once the app has started. */
	failStartup(error: Error): void {
		if (!this.startedFlag) this.startupFailure ??= error
	}

	/** Throw if start() must not continue: the app is closing or a background task failed startup. */
	assertNotClosing(): void {
		if (this.closingFlag) {
			throw new Error('Flow was closed while starting')
		}
		if (this.startupFailure) {
			throw this.startupFailure
		}
	}

	sleepUnlessClosing(ms: number): Promise<void> {
		const signal = this.shutdownController.signal
		if (signal.aborted) return Promise.resolve()
		return new Promise(resolve => {
			const timer = setTimeout(() => {
				signal.removeEventListener('abort', onAbort)
				resolve()
			}, ms)
			const onAbort = () => {
				clearTimeout(timer)
				resolve()
			}
			signal.addEventListener('abort', onAbort, { once: true })
		})
	}
}
