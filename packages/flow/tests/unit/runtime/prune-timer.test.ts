/**
 * PruneTimer (4.10.4): the prune of unowned state runs outside the scheduler's exclusive section, one instance after
 * the other, each once it is due and the run has been RUNNING for 30 s, and stops before the next instance once
 * disarmed.
 */
import { describe, expect, it } from 'vitest'
import { PRUNE_DELAY_MS, PruneTimer } from '@/runtime/prune-timer.js'
import type { UnleasedInstance } from '@/state/state-directory.js'
import { deferred } from '../support/fake-io.js'
import { ManualClock } from '../support/manual-clock.js'
import { RecordingLogger } from '../support/task-harness.js'

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

	it('a failed prune is logged; the next owner rebuilds the instance', async () => {
		const t = timer([released('a', 0)])
		t.gates.set('a:0', Promise.reject(new Error('disk full')))
		t.prune.arm()
		t.clock.advance(PRUNE_DELAY_MS)
		await tick()
		expect(t.logger.warnings).toEqual(['dropping unowned state failed: its next owner rebuilds it'])
		expect(t.clock.pendingTimers).toBe(0)
	})
})
