import { describe, expect, it } from 'vitest'
import { string } from '@/codec.js'
import type { InternalProcessorContext, ProcessorSupplier } from '@/processor/api.js'
import { ObservedStreamTime } from '@/processor/time-tracker.js'
import type { PartitionStart, StreamTask } from '@/runtime/task.js'
import type { InternalTopologyBuilder } from '@/topology/builder.js'
import { kvStore, TaskHarness } from './support/task-harness.js'

const WINDOW_MS = 1_000
const GRACE_MS = 5_000
const BASE = 1_000_000_000_000
const LAST = BASE + 30_530

/**
 * A windowed operator's lateness rule with an explicit grace (KS): a record is late when the newest window that
 * contains it ended at or before `streamTime - grace`. Its clock is the processor clock of store `w`, persisted in
 * the commit metadata.
 */
function windowed(advanceMs: number): ProcessorSupplier<string, string, string, string> {
	return () => {
		let context: InternalProcessorContext<string, string>
		let clock: ObservedStreamTime
		return {
			init: ctx => {
				context = ctx as InternalProcessorContext<string, string>
				clock = new ObservedStreamTime(context, 'w')
			},
			process: record => {
				const streamTime = clock.advance(record.timestamp)
				const windowEnd = record.timestamp - (record.timestamp % advanceMs) + WINDOW_MS
				if (windowEnd <= streamTime - GRACE_MS) {
					context.recordDropped('late')
					return
				}
				return context.forward(record)
			},
		}
	}
}

function topology(advanceMs: number) {
	return (b: InternalTopologyBuilder) => {
		b.addSource({ name: 'source', topics: ['input'], keyCodec: string(), valueCodec: string(), internal: false })
		b.addStateStore(kvStore('w', { clock: {} }))
		b.addProcessor({
			name: 'window',
			parents: ['source'],
			stores: ['w'],
			supplier: windowed(advanceMs) as ProcessorSupplier<unknown, unknown, unknown, unknown>,
		})
		b.addSink({
			name: 'sink',
			parents: ['window'],
			topic: 'output',
			keyCodec: string(),
			valueCodec: string(),
			internal: false,
		})
	}
}

/** Every timestamp around the lateness edge. */
const CANDIDATES = Array.from({ length: 80 }, (_, i) => LAST - GRACE_MS - WINDOW_MS - 2_000 + i * 50)

/** Whether each candidate is processed (not late) by `task`; candidates are older than its stream time. */
async function decide(h: TaskHarness, task: StreamTask): Promise<boolean[]> {
	const decisions: boolean[] = []
	for (const timestamp of CANDIDATES) {
		const before = h.producer.sent.length
		h.enqueue(task, 'input', [{ key: 'k', value: 'v', timestamp }])
		await h.process(task)
		decisions.push(h.producer.sent.length > before)
	}
	return decisions
}

describe('windowed lateness after the task is re-created from its commits (D5)', () => {
	for (const eos of [false, true]) {
		for (const [name, advanceMs] of [
			['tumbling', WINDOW_MS],
			['hopping', 250],
		] as const) {
			it(`${eos ? 'exactly-once' : 'at-least-once'}, ${name} windows: the same late-or-not answers`, async () => {
				const h = new TaskHarness({ eos, build: topology(advanceMs) })
				const first = await h.start(h.task())
				h.enqueue(first, 'input', [{ key: 'k', value: 'v', timestamp: LAST }])
				await h.process(first)
				expect(await h.commit('revocation')).toBe('committed')
				const committed = h.committedOffsets()

				const before = await decide(h, first)
				await first.suspend()
				await first.close('dirty')

				// The task is re-created from the committed offsets and their metadata (a restart or a rebalance).
				const restarted = await h.restart()
				expect(restarted.streamTime).toBe(LAST)
				const after = await decide(h, restarted)

				expect(after).toEqual(before)
				// The candidates straddle the edge: some are late, some are not.
				expect(new Set(before).size).toBe(2)

				// Mutation check: without the commit metadata the clock starts unknown and the answers differ.
				await restarted.close('dirty')
				const unseeded = h.task()
				const starts = new Map<string, PartitionStart>(
					committed.map(o => [`${o.topic}:${o.partition}`, { committed: null, resolved: o.offset }])
				)
				await h.start(unseeded, starts)
				expect(await decide(h, unseeded)).not.toEqual(before)
			})
		}
	}
})
