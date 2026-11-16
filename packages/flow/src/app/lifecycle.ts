/**
 * The app's lifecycle (3.15, 4.2, 4.3.5, 4.10.3, 4.11): its StreamState, the error getError() reports, and the
 * memoized start and close promises. What a start or a close does is the caller's (FlowRuntime, from Stage 2).
 *
 * States: CREATED (until a start succeeds) -> RUNNING <-> REBALANCING; ERROR after a fatal failure (sticky until
 * close) or a failed start; STOPPED after close. getError(): the fatal error while there is one, else the most recent
 * recoverable error (a failed start, a recovery's cause), which never changes state(). A new start clears both.
 */
import type { StreamState } from '@/api/app.js'

const toError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)))

export class AppLifecycle {
	private current: StreamState = 'CREATED'
	private fatal: Error | null = null
	private recoverable: Error | null = null
	private startPromise: Promise<void> | null = null
	private closePromise: Promise<void> | null = null
	private isStarted = false
	private isClosing = false

	get state(): StreamState {
		return this.current
	}

	get error(): Error | null {
		return this.fatal ?? this.recoverable
	}

	/** The last start succeeded and no close happened since. */
	get started(): boolean {
		return this.isStarted
	}

	get closing(): boolean {
		return this.isClosing
	}

	/**
	 * Runs `start` unless a start is in flight or succeeded (both share its promise; so does start() while ERROR after
	 * a fatal failure, until close()). A failed start leaves state ERROR (STOPPED if a close came meanwhile) and can be
	 * retried.
	 */
	start(start: () => Promise<void>): Promise<void> {
		if (this.startPromise) {
			return this.startPromise
		}
		this.current = 'CREATED'
		this.fatal = null
		this.recoverable = null
		this.isClosing = false
		this.closePromise = null
		const promise = new Promise<void>(resolve => resolve(start())).then(
			() => {
				this.isStarted = true
				if (this.current !== 'ERROR') {
					this.current = 'RUNNING'
				}
			},
			(error: unknown) => {
				this.recoverable = toError(error)
				this.current = this.isClosing ? 'STOPPED' : 'ERROR'
				if (this.startPromise === promise) {
					this.startPromise = null
				}
				throw error
			}
		)
		this.startPromise = promise
		return promise
	}

	/**
	 * Runs `close` once (later calls share its promise), then waits for a start still in flight; the state is STOPPED
	 * afterwards even if `close` failed. Closing an app that never started only marks it STOPPED.
	 */
	close(close: () => Promise<void>): Promise<void> {
		if (this.closePromise) {
			return this.closePromise
		}
		if (this.current === 'STOPPED' || (this.current === 'CREATED' && this.startPromise === null)) {
			this.current = 'STOPPED'
			return Promise.resolve()
		}
		this.isClosing = true
		const inFlight = this.startPromise
		this.closePromise = (async () => {
			try {
				await close()
				await inFlight?.catch(() => {})
			} finally {
				this.current = 'STOPPED'
				this.isStarted = false
				this.startPromise = null
			}
		})()
		return this.closePromise
	}

	/** Every assigned task is RUNNING: REBALANCING -> RUNNING, once started, unless failed or closing. */
	setRunning(): void {
		if (this.isStarted && !this.isClosing && this.current === 'REBALANCING') {
			this.current = 'RUNNING'
		}
	}

	/** A rebalance, a restore or a recovery began: RUNNING -> REBALANCING. */
	setRebalancing(): void {
		if (this.current === 'RUNNING') {
			this.current = 'REBALANCING'
		}
	}

	/** A recoverable failure: reported by getError() unless a fatal one is; the state does not change. */
	recordError(error: unknown): void {
		this.recoverable = toError(error)
	}

	/**
	 * A fatal failure after start: state ERROR (unless closing) and the error reported until close. The first fatal
	 * failure wins: false if one was recorded already.
	 */
	halt(error: unknown): boolean {
		if (this.fatal !== null) {
			return false
		}
		this.fatal = toError(error)
		if (!this.isClosing) {
			this.current = 'ERROR'
		}
		return true
	}
}
