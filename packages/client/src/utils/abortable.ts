/**
 * Settle as `promise` does, or reject with the reason of `signal` once it aborts; the promise then runs on unobserved
 * (its later rejection is handled, not reported as unhandled). Without a signal, `promise` is returned as is.
 */
export function abortable<T>(promise: Promise<T>, signal: AbortSignal | undefined): Promise<T> {
	if (!signal) return promise
	return new Promise<T>((resolve, reject) => {
		const onAbort = () => reject(signal.reason)
		if (signal.aborted) onAbort()
		else signal.addEventListener('abort', onAbort, { once: true })
		void promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', onAbort))
	})
}

/**
 * A signal that aborts once `timeoutMs` passed or `signal` aborted (already aborted: at once), the bound of a whole
 * operation: its requests, lookups and backoffs. Tell the two apart by checking `signal` first. `clear()` releases the
 * timer and the listener on `signal`.
 */
export function deadlineSignal(timeoutMs: number, signal?: AbortSignal): { signal: AbortSignal; clear(): void } {
	const deadline = new AbortController()
	const timer = setTimeout(() => deadline.abort(), timeoutMs)
	const onAbort = (): void => deadline.abort()
	if (signal?.aborted) deadline.abort()
	else signal?.addEventListener('abort', onAbort, { once: true })
	return {
		signal: deadline.signal,
		clear: () => {
			clearTimeout(timer)
			signal?.removeEventListener('abort', onAbort)
		},
	}
}
