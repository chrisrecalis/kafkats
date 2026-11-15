import type { TracingChannel } from 'node:diagnostics_channel'

/**
 * Follow the operations of one of the client's tracing channels from start to finish.
 *
 * `started` may return a value (a span, a start time) that `finished` gets back when the
 * operation has settled, with the failure if there was one. Returns the function that stops it.
 */
export function onOperation<C extends { error?: unknown }, S>(
	channel: TracingChannel<unknown, C>,
	hooks: { started(context: C): S; finished(context: C, state: S, error: unknown): void }
): () => void {
	const running = new WeakMap<C, { state: S }>()
	let inFlight = 0
	let stopping = false
	const start = (message: unknown): void => {
		const context = message as C
		running.set(context, { state: hooks.started(context) })
		inFlight++
	}
	// The client settles every operation asynchronously, so this is always the last event.
	const asyncEnd = (message: unknown): void => {
		const context = message as C
		const operation = running.get(context)
		if (!operation) return
		running.delete(context)
		hooks.finished(context, operation.state, context.error)
		if (--inFlight === 0 && stopping) channel.asyncEnd.unsubscribe(asyncEnd)
	}
	channel.start.subscribe(start)
	channel.asyncEnd.subscribe(asyncEnd)
	return () => {
		// No new operations, but the ones under way are still followed to their end: a span that
		// was started is ended, also when instrumentation stops during shutdown.
		channel.start.unsubscribe(start)
		stopping = true
		if (inFlight === 0) channel.asyncEnd.unsubscribe(asyncEnd)
	}
}

/** Seconds since `startedAt` (a `performance.now()` reading): the unit of the duration metrics. */
export const secondsSince = (startedAt: number): number => (performance.now() - startedAt) / 1000
