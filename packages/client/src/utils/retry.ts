import { ReconnectionStrategy, type ReconnectionStrategyOptions } from '@/utils/reconnection-strategy.js'
import { sleep } from '@/utils/sleep.js'

export type RetryBackoffConfig = {
	retries: number
	retryBackoffMs: number
	maxRetryBackoffMs: number
}

export type RetryOptions = ReconnectionStrategyOptions & {
	signal?: AbortSignal
	resolveOnAbort?: boolean
	shouldRetry?: (error: unknown) => boolean
	onRetry?: (params: { attempt: number; delayMs: number; error: unknown }) => void | Promise<void>
	/**
	 * Wall-clock deadline (ms) for the whole retry loop. Once exceeded, retries stop and the last
	 * error is rethrown even if attempts remain. Leave undefined for purely attempt-bounded retries.
	 */
	maxElapsedMs?: number
}

export function createRetryStrategyOptions(config: RetryBackoffConfig): ReconnectionStrategyOptions {
	return {
		maxAttempts: config.retries + 1,
		initialDelayMs: config.retryBackoffMs,
		maxDelayMs: config.maxRetryBackoffMs,
	}
}

export async function retry<T>(fn: (attempt: number) => Promise<T>, options: RetryOptions = {}): Promise<T> {
	const { signal, resolveOnAbort, shouldRetry, onRetry, maxElapsedMs, ...strategyOptions } = options
	const strategy = new ReconnectionStrategy(strategyOptions)
	const startedAt = Date.now()

	while (strategy.shouldReconnect()) {
		const attempt = strategy.currentAttempts + 1
		try {
			return await fn(attempt)
		} catch (error) {
			if (shouldRetry && !shouldRetry(error)) {
				throw error
			}

			const delayMs = strategy.nextDelay()
			strategy.recordFailure()
			// Stop on attempt budget OR on the wall-clock deadline (would elapse past it after sleeping).
			const deadlineExceeded = maxElapsedMs !== undefined && Date.now() - startedAt + delayMs >= maxElapsedMs
			if (!strategy.shouldReconnect() || deadlineExceeded) {
				throw error
			}

			await onRetry?.({ attempt, delayMs, error })
			await sleep(delayMs, { signal, resolveOnAbort })
		}
	}

	// Should never happen, but keeps TS happy.
	throw new Error('Retry loop exited unexpectedly')
}
