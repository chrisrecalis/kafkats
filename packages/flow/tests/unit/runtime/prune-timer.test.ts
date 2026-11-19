/**
 * PruneTimer (4.10.4): the prune of unowned state runs outside the scheduler's exclusive section, one instance after
 * the other, each once it is due and the run has been RUNNING for 30 s, and stops before the next instance once
 * disarmed.
 */
import { describe, expect, it, vi } from 'vitest'
import { PRUNE_DELAY_MS, PruneTimer } from '@/runtime/prune-timer.js'
import type { KeyValueStore } from '@/state.js'
import type { UnleasedInstance } from '@/state/state-directory.js'
import { keyValueStoreBuilder } from '@/state/store-builders.js'
import { DEFAULTS, directory as stateDirectory, str, taskId } from '../state/support.js'
import { deferred } from '../support/fake-io.js'
import { ManualClock } from '../support/manual-clock.js'
import { RecordingLogger } from '../support/task-harness.js'

const counts = keyValueStoreBuilder(DEFAULTS, { name: 'counts', keyCodec: str, valueCodec: str })

const tick = () => new Promise(resolve => setImmediate(resolve))

/** In-memory state released before the run became RUNNING. */
const released = (store: string, partition: number): UnleasedInstance => ({ store, partition, prunableAt: 0 })

function timer(initial: UnleasedInstance[]) {
	const pruned: string[] = []
	const gates = new Map<string, Promise<void>>()
	const clock = new ManualClock()
	const logger = new RecordingLogger()
	let unleased = initial
	const prune = new PruneTimer({
		stateDirectory: {
			unleased: () => unleased,
			prune: async (store, partition) => {
				await gates.get(`${store}:${partition}`)
				pruned.push(`${store}:${partition}`)
				unleased = unleased.filter(u => u.store !== store || u.partition !== partition)
			},
		},
		clock,
		signal: new AbortController().signal,
		logger,
	})
	return { prune, pruned, gates, clock, logger }
}

describe('PruneTimer', () => {
	it('prunes every unowned instance once the run has been RUNNING for 30 s', async () => {
		const t = timer([released('a', 0), released('a', 1)])
		t.prune.arm()
		t.clock.advance(PRUNE_DELAY_MS - 1)
		await tick()
		expect(t.pruned).toEqual([])
		t.clock.advance(1)
		await tick()
		expect(t.pruned).toEqual(['a:0', 'a:1'])
		expect(t.clock.pendingTimers).toBe(0)
	})

	it('nothing unowned, no timer', () => {
		const t = timer([])
		t.prune.arm()
		expect(t.clock.pendingTimers).toBe(0)
	})

	it('durable state waits for its own time (the cleanup delay since its release), the rest goes at 30 s', async () => {
		const durable = { store: 'd', partition: 0, prunableAt: 600_000 }
		const t = timer([released('a', 0), durable])
		t.prune.arm()
		t.clock.advance(PRUNE_DELAY_MS)
		await tick()
		expect(t.pruned).toEqual(['a:0'])
		t.clock.advance(600_000 - PRUNE_DELAY_MS - 1)
		await tick()
		expect(t.pruned).toEqual(['a:0'])
		t.clock.advance(1)
		await tick()
		expect(t.pruned).toEqual(['a:0', 'd:0'])
	})

	it('a disarm while it runs stops it before the next instance (a rebalance or a revival began)', async () => {
		const t = timer([released('a', 0), released('a', 1)])
		const first = deferred()
		t.gates.set('a:0', first.promise)
		t.prune.arm()
		t.clock.advance(PRUNE_DELAY_MS)
		await tick()
		t.prune.disarm()
		first.resolve()
		await tick()
		expect(t.pruned).toEqual(['a:0'])
		expect(t.clock.pendingTimers).toBe(0)
	})

	it('a failed prune is logged, the others go on, and it is tried again 30 s later (KS: the next cleaner run)', async () => {
		const t = timer([released('a', 0), released('a', 1)])
		t.gates.set('a:0', Promise.reject(new Error('disk full')))
		t.prune.arm()
		t.clock.advance(PRUNE_DELAY_MS)
		await tick()
		expect(t.logger.warnings).toEqual(['dropping unowned state failed: it is tried again in 30 s'])
		expect(t.pruned).toEqual(['a:1'])
		t.gates.delete('a:0')
		t.clock.advance(PRUNE_DELAY_MS - 1)
		await tick()
		expect(t.pruned).toEqual(['a:1'])
		t.clock.advance(1)
		await tick()
		expect(t.pruned).toEqual(['a:1', 'a:0'])
		expect(t.clock.pendingTimers).toBe(0)
	})

	it('a prune still clearing after a disarm and an arm is not waited for again (no timer spins on it)', async () => {
		const clock = new ManualClock()
		const { directory } = stateDirectory({ cleanup: { delayMs: 0, now: () => clock.now() } })
		const owner = taskId(0, 0)
		const store = (await directory.lease(owner, counts, 0)) as KeyValueStore<string, string>
		await store.put('k', 'v')
		await directory.release(owner, 'counts', 0)
		const deleting = deferred()
		const remove = store.delete.bind(store)
		vi.spyOn(store, 'delete').mockImplementation(async key => {
			await deleting.promise
			await remove(key)
		})
		const unleased = vi.spyOn(directory, 'unleased')
		const prune = new PruneTimer({
			stateDirectory: directory,
			clock,
			signal: new AbortController().signal,
			logger: new RecordingLogger(),
		})
		prune.arm()
		clock.advance(PRUNE_DELAY_MS)
		await tick()
		// A rebalance during the clear, then every task RUNNING again.
		prune.disarm()
		prune.arm()
		expect(clock.pendingTimers).toBe(0)
		clock.advance(PRUNE_DELAY_MS)
		await tick()
		const calls = unleased.mock.calls.length
		for (let i = 0; i < 10; i++) {
			clock.advance(0)
			await tick()
		}
		expect(unleased.mock.calls.length).toBe(calls)
		deleting.resolve()
		await tick()
		expect(directory.unleased()).toEqual([])
		expect(clock.pendingTimers).toBe(0)
	})
})
