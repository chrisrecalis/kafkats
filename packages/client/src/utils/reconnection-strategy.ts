/**
 * Exponential backoff with jitter for retry/reconnection attempts.
 */

export interface ReconnectionStrategyOptions {
	/** Maximum attempts before giving up (default: Infinity) */
	maxAttempts?: number
	/** Initial backoff delay in ms (default: 100) */
	initialDelayMs?: number
	/** Maximum backoff delay in ms (default: 30000) */
	maxDelayMs?: number
	/** Backoff multiplier (default: 2) */
	multiplier?: number
	/** Jitter factor 0-1 (default: 0.2) */
	jitter?: number
}

/**
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
	 * Check if we should attempt another try.
	 */
	shouldReconnect(): boolean {
		return this.attempts < this.maxAttempts
	}

	/**
	 * Get the delay before the next attempt.
	 */
	nextDelay(): number {
		const exponentialDelay = this.initialDelayMs * Math.pow(this.multiplier, this.attempts)
		const cappedDelay = Math.min(exponentialDelay, this.maxDelayMs)

		const jitterRange = cappedDelay * this.jitter
		const jitterValue = (Math.random() * 2 - 1) * jitterRange

		return Math.max(0, Math.round(cappedDelay + jitterValue))
	}

	/**
	 * Record a failed attempt.
	 */
	recordFailure(): void {
		this.attempts++
	}

	/**
	 * Record a successful attempt.
	 */
	recordSuccess(): void {
		this.attempts = 0
		this.lastSuccessTime = Date.now()
	}

	/**
	 * Reset the strategy.
	 */
	reset(): void {
		this.attempts = 0
	}

	/**
	 * Get current attempt count.
	 */
	get currentAttempts(): number {
		return this.attempts
	}

	/**
	 * Get time since last successful attempt.
	 */
	get timeSinceLastSuccess(): number | null {
		return this.lastSuccessTime !== null ? Date.now() - this.lastSuccessTime : null
	}
}
