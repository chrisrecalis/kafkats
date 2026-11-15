import { describe, expect, it } from 'vitest'
import { Coalesced } from '@/coalesced.js'

/** A task whose runs are finished by the test. */
function manualTask() {
	const finish: Array<(error?: Error) => void> = []
	let running = 0
	let maxRunning = 0
	const task = () =>
		new Promise<void>((resolve, reject) => {
			maxRunning = Math.max(maxRunning, ++running)
			finish.push(error => {
				running--
				if (error) reject(error)
				else resolve()
			})
		})
	return { task, finish, started: () => finish.length, maxRunning: () => maxRunning }
}

const turn = () => new Promise(resolve => setImmediate(resolve))

describe('Coalesced', () => {
	it('runs once for a single request', async () => {
		const { task, finish, started } = manualTask()
		const requested = new Coalesced(task).request()
		finish[0]!()
		await requested
		expect(started()).toBe(1)
	})

	it('gives requests made during a run one shared run after it', async () => {
		const { task, finish, started } = manualTask()
		const coalesced = new Coalesced(task)
		const first = coalesced.request()
		const later = [coalesced.request(), coalesced.request(), coalesced.request()]

		let laterDone = false
		void Promise.all(later).then(() => (laterDone = true))
		finish[0]!()
		await first
		await turn()
		// The run that was already going does not count for them.
		expect(laterDone).toBe(false)
		expect(started()).toBe(2)

		finish[1]!()
		await Promise.all(later)
		expect(started()).toBe(2)
	})

	it('never runs the task twice at the same time', async () => {
		const { task, finish, started, maxRunning } = manualTask()
		const coalesced = new Coalesced(task)
		const requests = [coalesced.request(), coalesced.request()]
		finish[0]!()
		await requests[0]
		// Asked right as the first run ends, before the queued one has started.
		requests.push(coalesced.request())
		await turn()
		while (started() > 0 && finish.length > 0) {
			const next = finish.pop()!
			next()
			await turn()
		}
		await Promise.all(requests)
		expect(maxRunning()).toBe(1)
	})

	it('a failed run fails its own requests only', async () => {
		const { task, finish } = manualTask()
		const coalesced = new Coalesced(task)
		const first = coalesced.request()
		const second = coalesced.request()
		finish[0]!(new Error('sync failed'))
		await expect(first).rejects.toThrow('sync failed')
		await turn()
		finish[1]!()
		await expect(second).resolves.toBeUndefined()
		// And it runs again afterwards.
		const third = coalesced.request()
		await turn()
		finish[2]!()
		await expect(third).resolves.toBeUndefined()
	})
})
