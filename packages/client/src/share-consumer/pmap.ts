import { Semaphore } from './semaphore.js'

export async function pmap<T, R>(
	items: readonly T[],
	fn: (item: T) => Promise<R>,
	concurrency: number,
	signal?: AbortSignal
): Promise<Array<Awaited<R>>> {
	const semaphore = new Semaphore(Math.max(1, concurrency))
	const tasks = items.map(async item => {
		const acquired = await semaphore.acquire(signal)
		if (!acquired) {
			return null
		}
		try {
			return await fn(item)
		} finally {
			semaphore.release()
		}
	})

	const results = await Promise.all(tasks)
	const out: Array<Awaited<R>> = []
	for (const r of results) {
		if (r !== null) {
			out.push(r)
		}
	}
	return out
}
