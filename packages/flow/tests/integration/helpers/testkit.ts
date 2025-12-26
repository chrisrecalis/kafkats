import { randomUUID } from 'node:crypto'
import { vi } from 'vitest'
import type { FlowApp } from '@kafkats/flow'

// Re-export waitFor for direct use in tests
export const waitFor = vi.waitFor.bind(vi)

export function uniqueName(prefix: string): string {
	return `${prefix}-${randomUUID().replace(/-/g, '').slice(0, 12)}`
}

/**
 * Wait for the FlowApp to be ready (consumer joined and partitions assigned).
 * This replaces fixed sleeps after app.start().
 */
export async function waitForAppReady(app: FlowApp, opts?: { timeoutMs?: number }): Promise<void> {
	const { timeoutMs = 15000 } = opts ?? {}
	await waitFor(
		() => {
			if (app.state() !== 'RUNNING') {
				throw new Error('App not yet RUNNING')
			}
		},
		{ timeout: timeoutMs, interval: 100 }
	)
}

/**
 * Consume messages from a topic with a timeout.
 * Returns collected messages when either:
 * - expectedCount messages are received
 * - timeout is reached
 */
export async function consumeWithTimeout<T>(
	// eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
	consumer: { runEach: Function; stop: () => void },
	topic: string,
	opts: {
		expectedCount: number
		timeoutMs?: number
		parse?: (buf: Buffer) => T
	}
): Promise<T[]> {
	const { expectedCount, timeoutMs = 10000, parse = (buf: Buffer) => JSON.parse(buf.toString()) as T } = opts
	const results: T[] = []
	let consuming = true

	const consumePromise = consumer.runEach(
		topic,
		async (msg: { value: Buffer }) => {
			results.push(parse(msg.value))
			if (results.length >= expectedCount) {
				consumer.stop()
			}
		},
		{ autoCommit: false }
	)

	// Use waitFor to poll for expected message count
	try {
		await waitFor(
			() => {
				if (results.length < expectedCount && consuming) {
					throw new Error(`Only ${results.length}/${expectedCount} messages received`)
				}
			},
			{ timeout: timeoutMs, interval: 100 }
		)
	} catch {
		// Timeout reached, stop consumer
	} finally {
		consuming = false
		consumer.stop()
	}

	await consumePromise.catch(() => {
		// Consumer stopped
	})

	return results
}

/**
 * Poll for expected output using waitFor, retrying until results appear or timeout.
 * This is more robust than sleep + consume for testing async pipelines.
 *
 * Creates a new consumer for each poll attempt to ensure fresh reads.
 */
export async function pollForOutput<T>(
	// eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
	createConsumer: () => { runEach: Function; stop: () => void },
	topic: string,
	opts: {
		expectedCount: number
		timeoutMs?: number
		pollIntervalMs?: number
		parse?: (buf: Buffer) => T
	}
): Promise<T[]> {
	const {
		expectedCount,
		timeoutMs = 15000,
		pollIntervalMs = 500,
		parse = (buf: Buffer) => JSON.parse(buf.toString()) as T,
	} = opts

	let results: T[] = []
	let currentConsumer: { runEach: Function; stop: () => void } | null = null

	try {
		await waitFor(
			async () => {
				// Stop previous consumer if any
				if (currentConsumer) {
					currentConsumer.stop()
				}

				currentConsumer = createConsumer()
				const pollResults: T[] = []
				let done = false

				const consumePromise = currentConsumer.runEach(
					topic,
					async (msg: { value: Buffer }) => {
						pollResults.push(parse(msg.value))
						if (pollResults.length >= expectedCount) {
							done = true
							currentConsumer?.stop()
						}
					},
					{ autoCommit: false }
				)

				// Give this poll attempt a short window to collect messages
				await Promise.race([
					consumePromise.catch(() => {}),
					new Promise<void>(resolve => setTimeout(resolve, pollIntervalMs * 2)),
				])

				if (!done) {
					currentConsumer.stop()
				}

				results = pollResults

				if (results.length < expectedCount) {
					throw new Error(`Only ${results.length}/${expectedCount} messages received`)
				}
			},
			{ timeout: timeoutMs, interval: pollIntervalMs }
		)
	} catch {
		// Timeout - return what we have
	} finally {
		if (currentConsumer) {
			currentConsumer.stop()
		}
	}

	if (results.length < expectedCount) {
		throw new Error(`Timeout waiting for ${expectedCount} messages on ${topic} after ${timeoutMs}ms`)
	}

	return results
}
