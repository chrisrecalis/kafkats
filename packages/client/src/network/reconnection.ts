/**
 * Reconnection strategy with exponential backoff and jitter
 */

import type { ReconnectionConfig } from '@/network/types.js'

export type ReconnectionStrategyOptions = ReconnectionConfig

/**
 * Exponential backoff with jitter for reconnection attempts
 *
 * Backoff formula:
 *   delay = min(initialDelay * multiplier^attempts, maxDelay)
 *   jitteredDelay = delay + random(-jitter, +jitter) * delay
 */
export class ReconnectionStrategy {
	private readonly maxAttempts: number
	private readonly initialDelayMs: number
	private readonly maxDelayMs: number
	private readonly multiplier: number
	private readonly jitter: number

	private attempts = 0
	private lastSuccessTime: number | null = null

	constructor(options: ReconnectionStrategyOptions = {}) {
		this.maxAttempts = options.maxAttempts ?? Infinity
		this.initialDelayMs = options.initialDelayMs ?? 100
		this.maxDelayMs = options.maxDelayMs ?? 30000
		this.multiplier = options.multiplier ?? 2
		this.jitter = Math.min(1, Math.max(0, options.jitter ?? 0.2))
	}

	/**
	 * Check if we should attempt reconnection
	 */
	shouldReconnect(): boolean {
		return this.attempts < this.maxAttempts
	}

	/**
	 * Get the delay before next reconnection attempt
	 */
	nextDelay(): number {
		// Calculate base delay with exponential backoff
		const exponentialDelay = this.initialDelayMs * Math.pow(this.multiplier, this.attempts)
		const cappedDelay = Math.min(exponentialDelay, this.maxDelayMs)

		// Apply jitter (random factor to prevent thundering herd)
		const jitterRange = cappedDelay * this.jitter
		const jitterValue = (Math.random() * 2 - 1) * jitterRange

		return Math.max(0, Math.round(cappedDelay + jitterValue))
	}

	/**
	 * Record a failed attempt
	 */
	recordFailure(): void {
		this.attempts++
	}

	/**
	 * Record successful connection
	 */
	recordSuccess(): void {
		this.attempts = 0
		this.lastSuccessTime = Date.now()
	}

	/**
	 * Reset the strategy
	 */
	reset(): void {
		this.attempts = 0
	}

	/**
	 * Get current attempt count
	 */
	get currentAttempts(): number {
		return this.attempts
	}

	/**
	 * Get time since last successful connection
	 */
	get timeSinceLastSuccess(): number | null {
		return this.lastSuccessTime !== null ? Date.now() - this.lastSuccessTime : null
	}
}

/**
 * Wrapper that provides reconnecting behavior for a connection
 */
export class ReconnectingConnection {
	private readonly strategy: ReconnectionStrategy
	private reconnecting = false
	private shouldStop = false

	constructor(
		private readonly connect: () => Promise<void>,
		private readonly onReconnect?: () => void,
		private readonly onReconnectFailed?: (error: Error) => void,
		options: ReconnectionStrategyOptions = {}
	) {
		this.strategy = new ReconnectionStrategy(options)
	}

	/**
	 * Check if currently in reconnection loop
	 */
	get isReconnecting(): boolean {
		return this.reconnecting
	}

	/**
	 * Get the underlying strategy for inspection
	 */
	getStrategy(): ReconnectionStrategy {
		return this.strategy
	}

	/**
	 * Start reconnection loop (called when connection is lost)
	 *
	 * @returns true if reconnection succeeded, false if stopped or max attempts reached
	 */
	async startReconnecting(): Promise<boolean> {
		if (this.reconnecting || this.shouldStop) {
			return false
		}

		this.reconnecting = true

		while (this.strategy.shouldReconnect() && !this.shouldStop) {
			const delay = this.strategy.nextDelay()

			await this.sleep(delay)

			if (this.shouldStop) {
				break
			}

			try {
				await this.connect()
				this.strategy.recordSuccess()
				this.reconnecting = false
				this.onReconnect?.()
				return true
			} catch (error) {
				this.strategy.recordFailure()
				this.onReconnectFailed?.(error instanceof Error ? error : new Error(String(error)))
			}
		}

		this.reconnecting = false
		return false
	}

	/**
	 * Attempt a single reconnection (non-loop)
	 *
	 * @returns true if reconnection succeeded
	 */
	async attemptReconnect(): Promise<boolean> {
		if (!this.strategy.shouldReconnect()) {
			return false
		}

		try {
			await this.connect()
			this.strategy.recordSuccess()
			this.onReconnect?.()
			return true
		} catch (error) {
			this.strategy.recordFailure()
			this.onReconnectFailed?.(error instanceof Error ? error : new Error(String(error)))
			return false
		}
	}

	/**
	 * Stop reconnection attempts
	 */
	stop(): void {
		this.shouldStop = true
	}

	/**
	 * Reset and allow reconnection again
	 */
	reset(): void {
		this.shouldStop = false
		this.strategy.reset()
	}

	private sleep(ms: number): Promise<void> {
		return new Promise(resolve => setTimeout(resolve, ms))
	}
}
