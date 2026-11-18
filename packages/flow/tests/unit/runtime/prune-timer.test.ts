/**
 * PruneTimer (4.10.4): the prune of unowned in-memory state runs outside the scheduler's exclusive section, one
 * instance after the other, and stops before the next instance once disarmed.
 */
import { describe, expect, it } from 'vitest'
import { PRUNE_DELAY_MS, PruneTimer } from '@/runtime/prune-timer.js'
import { deferred } from '../support/fake-io.js'
import { ManualClock } from '../support/manual-clock.js'
import { RecordingLogger } from '../support/task-harness.js'

const tick = () => new Promise(resolve => setImmediate(resolve))

function timer(unleased: { store: string; partition: number }[]) {
	const pruned: string[] = []
	const gates = new Map<string, Promise<void>>()
	const clock = new ManualClock()
	const logger = new RecordingLogger()
	const prune = new PruneTimer({
		stateDirectory: {
			unleased: () => unleased,
			prune: async (store, partition) => {
				await gates.get(`${store}:${partition}`)
				pruned.push(`${store}:${partition}`)
			},
		},
		clock,
		signal: new AbortController().signal,
		logger,
	})
	return { prune, pruned, gates, clock, logger }
}

describe('PruneTimer', () => {
	it('prunes every unowned instance once the delay passed', async () => {
		const t = timer([
			{ store: 'a', partition: 0 },
			{ store: 'a', partition: 1 },
		])
		t.prune.arm()
		t.clock.advance(PRUNE_DELAY_MS - 1)
		await tick()
		expect(t.pruned).toEqual([])
		t.clock.advance(1)
		await tick()
		expect(t.pruned).toEqual(['a:0', 'a:1'])
	})

	it('a disarm while it runs stops it before the next instance (a rebalance or a revival began)', async () => {
		const t = timer([
			{ store: 'a', partition: 0 },
			{ store: 'a', partition: 1 },
		])
		const first = deferred()
		t.gates.set('a:0', first.promise)
		t.prune.arm()
		t.clock.advance(PRUNE_DELAY_MS)
		await tick()
		t.prune.disarm()
		first.resolve()
		await tick()
		expect(t.pruned).toEqual(['a:0'])
	})

	it('a failed prune is logged; the next owner rebuilds the instance', async () => {
		const t = timer([{ store: 'a', partition: 0 }])
		t.gates.set('a:0', Promise.reject(new Error('disk full')))
		t.prune.arm()
		t.clock.advance(PRUNE_DELAY_MS)
		await tick()
		expect(t.logger.warnings).toEqual(['dropping unowned state failed: its next owner rebuilds it'])
	})
})
