/**
 * Upper bound (ms) for the best-effort work done while shutting down against a possibly
 * unresponsive broker (final offset commit, LeaveGroup, flush). After it, sockets are force-closed.
 */
export const SHUTDOWN_DEADLINE_MS = 5000

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
		const timer = setTimeout(() => reject(new DeadlineExceededError(`${what} timed out after ${ms}ms`)), ms)
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
