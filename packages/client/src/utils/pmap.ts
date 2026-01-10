/**
 * Parallel map utilities with concurrency limiting.
 */

/**
 * Execute async functions on items with concurrency limit (fire-and-forget).
 *
 * Optimized for void-returning handlers where results aren't needed.
 * Stops processing on first error or abort signal.
 *
 * @param items - Array of items to process
 * @param fn - Async function to apply to each item
 * @param concurrency - Maximum concurrent operations
 * @param signal - Optional abort signal
 */
export async function pmapVoid<T>(
	items: readonly T[],
	fn: (item: T) => Promise<void>,
	concurrency: number,
	signal?: AbortSignal
): Promise<void> {
	const len = items.length
	if (len === 0 || signal?.aborted) return

	const limit = Math.max(1, concurrency)

	// Fast path: sequential execution
	if (limit === 1) {
		for (const item of items) {
			if (signal?.aborted) return
			await fn(item)
		}
		return
	}

	// Fast path: unlimited concurrency
	if (limit >= len) {
		await Promise.all(items.map(fn))
		return
	}

	// Concurrent execution with limit
	let nextIndex = 0
	let active = 0
	let error: Error | null = null
	let resolvePromise: () => void
	let rejectPromise: (err: Error) => void

	const promise = new Promise<void>((resolve, reject) => {
		resolvePromise = resolve
		rejectPromise = reject
	})

	const runNext = () => {
		while (error === null && !signal?.aborted && nextIndex < len && active < limit) {
			const item = items[nextIndex++]!
			active++

			fn(item)
				.then(() => {
					active--
					if (error === null && !signal?.aborted) {
						if (nextIndex < len) {
							runNext()
						} else if (active === 0) {
							resolvePromise()
						}
					} else if (active === 0) {
						if (error) {
							rejectPromise(error)
						} else {
							resolvePromise()
						}
					}
				})
				.catch(err => {
					active--
					if (error === null) {
						error = err instanceof Error ? err : new Error(String(err))
					}
					if (active === 0) {
						rejectPromise(error)
					}
				})
		}

		// Check immediate completion
		if (active === 0) {
			if (error) {
				rejectPromise(error)
			} else {
				resolvePromise()
			}
		}
	}

	runNext()
	return promise
}

/**
 * Parallel map with concurrency limit, preserving result order.
 *
 * @param items - Array of items to process
 * @param fn - Async function to apply to each item
 * @param concurrency - Maximum concurrent operations
 * @param signal - Optional abort signal
 * @returns Array of results in input order (excludes items not run due to abort)
 */
export async function pmap<T, R>(
	items: readonly T[],
	fn: (item: T) => Promise<R>,
	concurrency: number,
	signal?: AbortSignal
): Promise<Array<Awaited<R>>> {
	const len = items.length
	if (len === 0 || signal?.aborted) return []

	const limit = Math.max(1, concurrency)

	// Fast path: sequential execution
	if (limit === 1) {
		const results: Array<Awaited<R>> = []
		for (const item of items) {
			if (signal?.aborted) break
			results.push(await fn(item))
		}
		return results
	}

	// Fast path: unlimited concurrency
	if (limit >= len) {
		return Promise.all(items.map(fn))
	}

	// Concurrent execution with limit
	const results = new Array<Awaited<R> | undefined>(len)
	let nextIndex = 0
	let completed = 0
	let active = 0
	let error: Error | null = null
	let resolvePromise: (results: Array<Awaited<R>>) => void
	let rejectPromise: (err: Error) => void

	const promise = new Promise<Array<Awaited<R>>>((resolve, reject) => {
		resolvePromise = resolve
		rejectPromise = reject
	})

	const finish = () => {
		if (error) {
			rejectPromise(error)
		} else {
			// Filter out undefined slots (items skipped due to abort)
			const out: Array<Awaited<R>> = []
			for (let i = 0; i < completed; i++) {
				out.push(results[i]!)
			}
			resolvePromise(out)
		}
	}

	const runNext = () => {
		while (error === null && !signal?.aborted && nextIndex < len && active < limit) {
			const index = nextIndex++
			const item = items[index]!
			active++

			fn(item)
				.then(result => {
					if (error === null) {
						results[index] = result as Awaited<R>
						completed = Math.max(completed, index + 1)
					}
					active--
					if (error === null && !signal?.aborted) {
						if (nextIndex < len) {
							runNext()
						} else if (active === 0) {
							finish()
						}
					} else if (active === 0) {
						finish()
					}
				})
				.catch(err => {
					active--
					if (error === null) {
						error = err instanceof Error ? err : new Error(String(err))
					}
					if (active === 0) {
						finish()
					}
				})
		}

		// Check immediate completion
		if (active === 0) {
			finish()
		}
	}

	runNext()
	return promise
}
