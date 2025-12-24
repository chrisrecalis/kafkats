import { randomUUID } from 'node:crypto'
import type { FlowApp } from '@kafkats/flow'

export function uniqueName(prefix: string): string {
	return `${prefix}-${randomUUID().replace(/-/g, '').slice(0, 12)}`
}

export function sleep(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms))
}

/**
 * Wait for a condition to be true, polling at intervals.
 * Throws if timeout is reached before condition is satisfied.
 */
export async function waitForCondition(
	condition: () => boolean | Promise<boolean>,
	opts?: { timeoutMs?: number; pollIntervalMs?: number; description?: string }
): Promise<void> {
	const { timeoutMs = 10000, pollIntervalMs = 100, description = 'condition' } = opts ?? {}
	const startTime = Date.now()

	while (Date.now() - startTime < timeoutMs) {
		if (await condition()) {
			return
		}
		await sleep(pollIntervalMs)
	}

	throw new Error(`Timeout waiting for ${description} after ${timeoutMs}ms`)
}

/**
 * Wait for the FlowApp to be ready (consumer joined and partitions assigned).
 * This replaces fixed sleeps after app.start().
 */
export async function waitForAppReady(app: FlowApp, opts?: { timeoutMs?: number }): Promise<void> {
	const { timeoutMs = 15000 } = opts ?? {}
	await waitForCondition(() => app.state() === 'RUNNING', {
		timeoutMs,
		pollIntervalMs: 100,
		description: 'app to be RUNNING',
	})
}

/**
 * Consume messages from a topic with a timeout.
 * Returns collected messages when either:
 * - expectedCount messages are received
 * - timeout is reached
 */
export async function consumeWithTimeout<T>(
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

	const timeoutPromise = sleep(timeoutMs).then(() => {
		consumer.stop()
	})

	await Promise.race([consumePromise, timeoutPromise])
	return results
}

/**
 * Poll for expected output, retrying until results appear or timeout.
 * This is more robust than sleep + consume for testing async pipelines.
 *
 * Creates a new consumer for each poll attempt to ensure fresh reads.
 */
export async function pollForOutput<T>(
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

	const startTime = Date.now()

	while (Date.now() - startTime < timeoutMs) {
		const consumer = createConsumer()
		const results: T[] = []

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

		// Give this poll attempt a short window to collect messages
		const pollTimeout = Math.min(pollIntervalMs * 2, timeoutMs - (Date.now() - startTime))
		await Promise.race([consumePromise, sleep(pollTimeout).then(() => consumer.stop())])

		if (results.length >= expectedCount) {
			return results
		}

		// Wait before next poll
		if (Date.now() - startTime < timeoutMs) {
			await sleep(pollIntervalMs)
		}
	}

	throw new Error(`Timeout waiting for ${expectedCount} messages on ${topic} after ${timeoutMs}ms`)
}
