/**
 * Upper bound (ms) for the best-effort work done while shutting down against a possibly
 * unresponsive broker (final offset commit, LeaveGroup, flush). After it, sockets are force-closed.
 */
export const SHUTDOWN_DEADLINE_MS = 5000

/** The longest delay `setTimeout` keeps: Node runs a longer one (`Infinity` included) after 1 ms. */
const MAX_TIMER_DELAY_MS = 2 ** 31 - 1

/**
 * Run `callback` once `timeoutMs` passed, like `setTimeout`, except that a timeout that is not finite (`Infinity`: no
 * deadline) arms no timer and one too long for a timer waits the longest a timer can (about 24.8 days) instead of
 * firing at once. Clear it with `clearTimeout`.
 */
export function setDeadline(callback: () => void, timeoutMs: number): ReturnType<typeof setTimeout> | undefined {
	return Number.isFinite(timeoutMs) ? setTimeout(callback, Math.min(timeoutMs, MAX_TIMER_DELAY_MS)) : undefined
}

/** Rejection of {@link withTimeout} when the deadline passes first. */
export class DeadlineExceededError extends Error {
	constructor(message: string) {
		super(message)
		this.name = 'DeadlineExceededError'
	}
}

/**
 * Settle with `promise`, or reject with {@link DeadlineExceededError} after `ms`. The underlying
 * operation is not cancelled; its eventual outcome is swallowed so it cannot become an unhandled
 * rejection.
 */
export function withTimeout<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
	return new Promise<T>((resolve, reject) => {
		const timer = setDeadline(() => reject(new DeadlineExceededError(`${what} timed out after ${ms}ms`)), ms)
		promise.then(
			value => {
				clearTimeout(timer)
				resolve(value)
			},
			error => {
				clearTimeout(timer)
				reject(error)
			}
		)
	})
}
