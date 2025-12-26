import { randomUUID } from 'node:crypto'
import { vi } from 'vitest'
import type { FlowApp } from '@kafkats/flow'

/**
 * Wrapper around vi.waitFor for polling conditions in tests.
 * Retries the callback until it stops throwing or timeout is reached.
 */
export async function waitFor(
	callback: () => void | Promise<void>,
	options?: { timeout?: number; interval?: number }
): Promise<void> {
	await vi.waitFor(callback, options)
}

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
