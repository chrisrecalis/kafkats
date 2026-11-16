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
