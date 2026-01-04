/**
 * Parallel map with concurrency limit.
 *
 * Maps items through an async function with at most `concurrency` operations running
 * simultaneously. Results preserve input order.
 *
 * @param items - Array of items to process
 * @param fn - Async function to apply to each item
 * @param concurrency - Maximum number of concurrent operations
 * @param signal - Optional abort signal to stop processing
 * @returns Array of results in the same order as input
 */

/** Sentinel value for slots that were never executed (due to abort) */
const NOT_RUN = Symbol('NOT_RUN')

export async function pmap<T, R>(
	items: readonly T[],
	fn: (item: T) => Promise<R>,
	concurrency: number,
	signal?: AbortSignal
): Promise<Array<Awaited<R>>> {
	const limit = Math.max(1, concurrency)

	// Fast path: empty input
	if (items.length === 0) {
		return []
	}

	// Fast path: already aborted
	if (signal?.aborted) {
		return []
	}

	const results = new Array<Awaited<R> | typeof NOT_RUN>(items.length).fill(NOT_RUN)
	let nextIndex = 0
	let active = 0
	let aborted = false
	let error: Error | null = null

	const onAbort = () => {
		aborted = true
	}
	signal?.addEventListener('abort', onAbort, { once: true })

	return new Promise((resolve, reject) => {
		const cleanup = () => {
			signal?.removeEventListener('abort', onAbort)
		}

		const tryResolve = () => {
			// Can only resolve when no active tasks remain
			if (active > 0) {
				return
			}

			cleanup()

			if (error) {
				reject(error)
				return
			}

			const out: Array<Awaited<R>> = []
			for (const r of results) {
				if (r !== NOT_RUN) {
					out.push(r)
				}
			}
			resolve(out)
		}

		const runNext = () => {
			// Don't start new work if aborted or errored or done
			while (!aborted && error === null && nextIndex < items.length && active < limit) {
				const index = nextIndex++
				const item = items[index]!
				active++

				fn(item)
					.then(result => {
						if (error === null) {
							results[index] = result as Awaited<R>
						}
						active--
						runNext()
					})
					.catch(err => {
						if (error === null) {
							error = err instanceof Error ? err : new Error(String(err))
						}
						active--
						tryResolve()
					})
			}

			tryResolve()
		}

		runNext()
	})
}
