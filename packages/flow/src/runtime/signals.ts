import { setImmediate as nextTurn, setTimeout as sleepFor } from 'node:timers/promises'
import type { MaybePromise } from '@/processor/api.js'
import type { Clock } from '@/runtime/io.js'

/** Longest stretch of synchronous work between two yields to the event loop. */
export const YIELD_AFTER_MS = 10

/** `parent`, aborted after `ms` at the latest. */
export function cap(parent: AbortSignal, ms: number): AbortSignal {
	return AbortSignal.any([parent, AbortSignal.timeout(ms)])
}

/** The AbortError an operation rejects with once `signal` aborted. */
export function abortError(signal: AbortSignal): DOMException {
	return new DOMException('The operation was aborted', { name: 'AbortError', cause: signal.reason })
}

/**
 * Settles like `promise`, or rejects with an AbortError once `signal` aborts. A rejection of the
 * abandoned promise is caught, so it never surfaces as unhandled.
 */
export function raceSignal<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
	promise.catch(() => {})
	if (signal.aborted) {
		return Promise.reject(abortError(signal))
	}
	return new Promise<T>((resolve, reject) => {
		const onAbort = () => reject(abortError(signal))
		signal.addEventListener('abort', onAbort, { once: true })
		promise.then(
			value => {
				signal.removeEventListener('abort', onAbort)
				resolve(value)
			},
			(error: unknown) => {
				signal.removeEventListener('abort', onAbort)
				reject(error)
			}
		)
	})
}

/** Resolves after `ms`; rejects with an AbortError once `signal` aborts. */
export function sleep(ms: number, signal: AbortSignal): Promise<void> {
	return sleepFor(ms, undefined, { signal })
}

/** Resolves after `ms` of `clock`; rejects with an AbortError once `signal` aborts. */
export function delay(clock: Clock, ms: number, signal: AbortSignal): Promise<void> {
	let cancel = (): void => {}
	const elapsed = new Promise<void>(resolve => {
		cancel = clock.setTimer(clock.now() + ms, resolve)
	})
	return raceSignal(elapsed, signal).finally(cancel)
}

let lastYield = performance.now()

/** Yields to the event loop (setImmediate) once YIELD_AFTER_MS passed since the last yield; otherwise nothing. */
export function yieldIfDue(): MaybePromise<void> {
	if (performance.now() - lastYield < YIELD_AFTER_MS) {
		return
	}
	return nextTurn().then(() => {
		lastYield = performance.now()
	})
}
