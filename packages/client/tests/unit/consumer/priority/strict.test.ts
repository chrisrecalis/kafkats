import { describe, expect, it } from 'vitest'
import { strict } from '@/consumer/priority.js'
import type { SchedulerPartition, SchedulerState } from '@/consumer/priority.js'

function partition(topic: string, partition: number, bufferedRecords: number, busy = false): SchedulerPartition {
	return {
		topic,
		partition,
		bufferedRecords,
		bufferedBytes: bufferedRecords * 10,
		busy,
	}
}

function state(partitions: SchedulerPartition[], recordBudget = 10): SchedulerState {
	return { partitions, recordBudget, bufferCapacityBytes: 1000, bufferedBytes: 100 }
}

describe('strict topic priority', () => {
	it('drains configured topics first and leaves unlisted topics in state order', () => {
		const decision = strict({ order: ['live', 'bulk'] }).schedule(
			state([
				partition('other', 0, 3),
				partition('bulk', 0, 3),
				partition('live', 0, 3),
				partition('other', 1, 3),
			])
		)
		expect(decision.drain.map(p => `${p.topic}:${p.partition}`)).toEqual(['live:0', 'bulk:0', 'other:0', 'other:1'])
	})

	it('skips busy partitions and gates lower-tier fetches by available higher-priority records', () => {
		const strategy = strict({ order: ['live', 'bulk'] })
		expect(strategy.schedule(state([partition('live', 0, 10), partition('bulk', 0, 0)])).fetch).toEqual([
			{ topic: 'live', partition: 0 },
		])
		expect(strategy.schedule(state([partition('live', 0, 10, true), partition('bulk', 0, 0)])).fetch).toEqual([
			{ topic: 'live', partition: 0 },
			{ topic: 'bulk', partition: 0 },
		])
	})

	it('validates its order', () => {
		expect(() => strict({ order: [] })).toThrow('must not be empty')
		expect(() => strict({ order: ['live', 'live'] })).toThrow('duplicates')
	})
})
