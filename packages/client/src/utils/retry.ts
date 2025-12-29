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
}

export function createRetryStrategyOptions(config: RetryBackoffConfig): ReconnectionStrategyOptions {
	return {
		maxAttempts: config.retries + 1,
		initialDelayMs: config.retryBackoffMs,
		maxDelayMs: config.maxRetryBackoffMs,
	}
}

export async function retry<T>(fn: (attempt: number) => Promise<T>, options: RetryOptions = {}): Promise<T> {
	const { signal, resolveOnAbort, shouldRetry, onRetry, ...strategyOptions } = options
	const strategy = new ReconnectionStrategy(strategyOptions)

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
			if (!strategy.shouldReconnect()) {
				throw error
			}

			await onRetry?.({ attempt, delayMs, error })
			await sleep(delayMs, { signal, resolveOnAbort })
		}
	}

	// Should never happen, but keeps TS happy.
	throw new Error('Retry loop exited unexpectedly')
}
