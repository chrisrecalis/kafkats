import { describe, expect, it, vi } from 'vitest'

import { pmap, pmapVoid } from '@/utils/pmap.js'

describe('pmap', () => {
	it('maps all items with the given function', async () => {
		const items = [1, 2, 3, 4, 5]
		const fn = async (x: number) => x * 2

		const results = await pmap(items, fn, 2)

		expect(results).toEqual([2, 4, 6, 8, 10])
	})

	it('returns empty array for empty input', async () => {
		const results = await pmap([], async (x: number) => x * 2, 2)

		expect(results).toEqual([])
	})

	it('respects concurrency limit', async () => {
		const concurrency = 2
		let currentActive = 0
		let maxActive = 0

		const items = [1, 2, 3, 4, 5, 6]
		const fn = async (x: number) => {
			currentActive++
			maxActive = Math.max(maxActive, currentActive)
			await new Promise(r => setTimeout(r, 10))
			currentActive--
			return x
		}

		await pmap(items, fn, concurrency)

		expect(maxActive).toBeLessThanOrEqual(concurrency)
	})

	it('handles concurrency of 1 (sequential)', async () => {
		const order: number[] = []
		const items = [1, 2, 3]

		const fn = async (x: number) => {
			order.push(x)
			await new Promise(r => setTimeout(r, 5))
			return x
		}

		const results = await pmap(items, fn, 1)

		expect(results).toEqual([1, 2, 3])
		expect(order).toEqual([1, 2, 3])
	})

	it('handles concurrency greater than items length', async () => {
		const items = [1, 2, 3]
		const fn = async (x: number) => x * 2

		const results = await pmap(items, fn, 10)

		expect(results).toEqual([2, 4, 6])
	})

	it('stops processing when signal is aborted', async () => {
		const controller = new AbortController()
		const processed: number[] = []
		const items = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

		const fn = async (x: number) => {
			processed.push(x)
			await new Promise(r => setTimeout(r, 20))
			if (x === 2) {
				controller.abort()
			}
			return x
		}

		const results = await pmap(items, fn, 2, controller.signal)

		// Some items may have started but not all should complete
		expect(results.length).toBeLessThan(items.length)
	})

	it('handles already aborted signal', async () => {
		const controller = new AbortController()
		controller.abort()

		const items = [1, 2, 3, 4, 5]
		const fn = vi.fn(async (x: number) => {
			await new Promise(r => setTimeout(r, 5))
			return x
		})

		const results = await pmap(items, fn, 2, controller.signal)

		// Should not process any items when signal is already aborted
		expect(results.length).toBe(0)
		expect(fn).not.toHaveBeenCalled()
	})

	it('propagates errors from the mapping function', async () => {
		const items = [1, 2, 3]
		const fn = async (x: number) => {
			if (x === 2) {
				throw new Error('test error')
			}
			return x
		}

		await expect(pmap(items, fn, 2)).rejects.toThrow('test error')
	})

	it('preserves order of results', async () => {
		const items = [1, 2, 3, 4, 5]
		// Add varying delays to test order preservation
		const fn = async (x: number) => {
			await new Promise(r => setTimeout(r, (6 - x) * 5))
			return x * 10
		}

		const results = await pmap(items, fn, 3)

		expect(results).toEqual([10, 20, 30, 40, 50])
	})

	it('handles null return values from function', async () => {
		const items = [1, 2, 3]
		const fn = async (x: number): Promise<number | null> => {
			if (x === 2) return null
			return x
		}

		const results = await pmap(items, fn, 2)

		// Current implementation filters out nulls - verify behavior
		expect(results).toContain(1)
		expect(results).toContain(3)
	})

	it('processes items efficiently with high concurrency', async () => {
		const items = Array.from({ length: 100 }, (_, i) => i)
		const delayMs = 10
		const concurrency = 10

		const start = performance.now()
		const fn = async (x: number) => {
			await new Promise(r => setTimeout(r, delayMs))
			return x
		}

		await pmap(items, fn, concurrency)
		const elapsed = performance.now() - start

		// With 100 items, 10ms each, 10 concurrency = ~100ms minimum
		// Allow some overhead but should be much less than sequential (1000ms)
		expect(elapsed).toBeLessThan(500)
	})

	it('preserves completed results that finish after abort', async () => {
		// Regression test: completed results should not be discarded just because
		// abort happened while they were in-flight
		const controller = new AbortController()
		const items = [1, 2, 3]

		// Item 1 completes quickly, item 2 triggers abort, item 3 is slow
		// All three start concurrently. Items 1 and 2 should be in results
		// even though abort fires while item 3 is still running.
		const fn = async (x: number) => {
			if (x === 1) {
				await new Promise(r => setTimeout(r, 5))
				return x * 10 // completes at ~5ms
			}
			if (x === 2) {
				await new Promise(r => setTimeout(r, 10))
				controller.abort() // abort at ~10ms
				return x * 10 // completes at ~10ms
			}
			// x === 3
			await new Promise(r => setTimeout(r, 50))
			return x * 10 // would complete at ~50ms but abort already fired
		}

		const results = await pmap(items, fn, 3, controller.signal)

		// Items 1 and 2 both completed before/at abort time
		// They must be in the results
		expect(results).toContain(10) // item 1's result
		expect(results).toContain(20) // item 2's result
	})

	it('includes results from in-flight tasks that complete after abort', async () => {
		// More explicit test: task completes AFTER abort is signaled but before
		// the promise settles. The result should still be captured.
		const controller = new AbortController()
		let abortTime = 0
		let task2CompleteTime = 0

		const items = [1, 2]
		const fn = async (x: number) => {
			if (x === 1) {
				await new Promise(r => setTimeout(r, 5))
				controller.abort()
				abortTime = performance.now()
				return 'first'
			}
			// x === 2: starts immediately, takes longer than task 1
			await new Promise(r => setTimeout(r, 20))
			task2CompleteTime = performance.now()
			return 'second'
		}

		const results = await pmap(items, fn, 2, controller.signal)

		// Task 2 completed after abort was signaled
		expect(task2CompleteTime).toBeGreaterThan(abortTime)
		// But it was already in-flight, so its result should be captured
		expect(results).toContain('first')
		expect(results).toContain('second')
	})
})

describe('pmapVoid', () => {
	it('executes function for all items', async () => {
		const items = [1, 2, 3, 4, 5]
		const processed: number[] = []
		const fn = async (x: number) => {
			processed.push(x)
		}

		await pmapVoid(items, fn, 2)

		expect(processed.sort()).toEqual([1, 2, 3, 4, 5])
	})

	it('returns void (undefined)', async () => {
		const items = [1, 2, 3]
		const result = await pmapVoid(items, async () => {}, 2)
		expect(result).toBeUndefined()
	})

	it('respects concurrency limit', async () => {
		const concurrency = 2
		let currentActive = 0
		let maxActive = 0

		const items = [1, 2, 3, 4, 5, 6]
		const fn = async (_x: number) => {
			currentActive++
			maxActive = Math.max(maxActive, currentActive)
			await new Promise(r => setTimeout(r, 10))
			currentActive--
		}

		await pmapVoid(items, fn, concurrency)

		expect(maxActive).toBeLessThanOrEqual(concurrency)
	})

	it('handles concurrency of 1 (sequential)', async () => {
		const order: number[] = []
		const items = [1, 2, 3]

		const fn = async (x: number) => {
			order.push(x)
			await new Promise(r => setTimeout(r, 5))
		}

		await pmapVoid(items, fn, 1)

		expect(order).toEqual([1, 2, 3])
	})

	it('handles empty input', async () => {
		const fn = vi.fn(async () => {})
		await pmapVoid([], fn, 2)
		expect(fn).not.toHaveBeenCalled()
	})

	it('handles already aborted signal', async () => {
		const controller = new AbortController()
		controller.abort()

		const fn = vi.fn(async () => {})
		await pmapVoid([1, 2, 3], fn, 2, controller.signal)

		expect(fn).not.toHaveBeenCalled()
	})

	it('stops processing when signal is aborted', async () => {
		const controller = new AbortController()
		const processed: number[] = []
		const items = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

		const fn = async (x: number) => {
			processed.push(x)
			await new Promise(r => setTimeout(r, 20))
			if (x === 2) {
				controller.abort()
			}
		}

		await pmapVoid(items, fn, 2, controller.signal)

		expect(processed.length).toBeLessThan(items.length)
	})

	it('propagates errors from the function', async () => {
		const items = [1, 2, 3]
		const fn = async (x: number) => {
			if (x === 2) {
				throw new Error('test error')
			}
		}

		await expect(pmapVoid(items, fn, 2)).rejects.toThrow('test error')
	})

	it('handles unlimited concurrency fast path', async () => {
		const items = [1, 2, 3]
		const processed: number[] = []

		await pmapVoid(
			items,
			async x => {
				processed.push(x)
			},
			10
		)

		expect(processed.sort()).toEqual([1, 2, 3])
	})
})
