/**
 * Reconnection strategy with exponential backoff and jitter
 */

import { sleep } from '@/utils/sleep.js'
import { ReconnectionStrategy } from '@/utils/reconnection-strategy.js'
import type { ReconnectionConfig } from '@/network/types.js'

export type ReconnectionStrategyOptions = ReconnectionConfig
export { ReconnectionStrategy }

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

			await sleep(delay)

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
}
