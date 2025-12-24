/**
 * Options for the sleep function
 */
export interface SleepOptions {
	/**
	 * AbortSignal to cancel the sleep early
	 */
	signal?: AbortSignal
	/**
	 * If true, resolves instead of rejecting when aborted.
	 * Useful for graceful shutdown scenarios.
	 * @default false
	 */
	resolveOnAbort?: boolean
}

/**
 * Sleep helper that optionally respects an abort signal.
 *
 * @param ms - Duration to sleep in milliseconds
 * @param options - Optional configuration for signal and abort behavior
 * @returns Promise that resolves after the timeout or when aborted
 * @throws Error with message "Aborted" if signal is aborted and resolveOnAbort is false
 *
 * @example
 * // Simple sleep
 * await sleep(1000)
 *
 * @example
 * // Sleep with abort signal (throws on abort)
 * const controller = new AbortController()
 * await sleep(1000, { signal: controller.signal })
 *
 * @example
 * // Sleep with abort signal (resolves on abort for graceful shutdown)
 * await sleep(1000, { signal: controller.signal, resolveOnAbort: true })
 */
export function sleep(ms: number, options?: SleepOptions): Promise<void> {
	const { signal, resolveOnAbort = false } = options ?? {}

	return new Promise((resolve, reject) => {
		if (!signal) {
			setTimeout(resolve, ms)
			return
		}

		if (signal.aborted) {
			if (resolveOnAbort) {
				resolve()
			} else {
				reject(new Error('Aborted'))
			}
			return
		}

		const timeout = setTimeout(() => {
			signal.removeEventListener('abort', onAbort)
			resolve()
		}, ms)

		function onAbort() {
			clearTimeout(timeout)
			signal!.removeEventListener('abort', onAbort)
			if (resolveOnAbort) {
				resolve()
			} else {
				reject(new Error('Aborted'))
			}
		}

		signal.addEventListener('abort', onAbort)
	})
}
