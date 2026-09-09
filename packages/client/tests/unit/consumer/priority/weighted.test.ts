import { describe, expect, it } from 'vitest'
import { weighted } from '@/consumer/priority.js'
import type { SchedulerPartition, SchedulerState } from '@/consumer/priority.js'

function partition(topic: string, bufferedRecords: number, bufferedBytes = 0): SchedulerPartition {
	return { topic, partition: 0, bufferedRecords, bufferedBytes, busy: false }
}

function state(partitions: SchedulerPartition[], recordBudget = 10): SchedulerState {
	return { partitions, recordBudget, bufferCapacityBytes: 1000, bufferedBytes: 0 }
}

describe('weighted topic priority', () => {
	it('allocates proportionally and redistributes unused demand', () => {
		const strategy = weighted({ shares: { live: 3, bulk: 1 } })
		const proportional = strategy.schedule(state([partition('live', 100), partition('bulk', 100)]))
		expect(proportional.drain.map(p => [p.topic, p.maxRecords])).toEqual([
			['live', 8],
			['bulk', 2],
		])
		const redistributed = strategy.schedule(state([partition('live', 1), partition('bulk', 100)]))
		expect(redistributed.drain.map(p => [p.topic, p.maxRecords])).toEqual([
			['live', 1],
			['bulk', 9],
		])
	})

	it('uses share 1 for unlisted topics and gates fetch by byte share', () => {
		const strategy = weighted({ shares: { live: 3 } })
		const decision = strategy.schedule(state([partition('live', 100, 750), partition('other', 100, 200)]))
		expect(decision.drain.map(p => [p.topic, p.maxRecords])).toEqual([
			['live', 8],
			['other', 2],
		])
		expect(decision.fetch).toEqual([{ topic: 'other', partition: 0 }])
	})

	it('validates shares', () => {
		expect(() => weighted({ shares: {} })).toThrow('must not be empty')
		expect(() => weighted({ shares: { live: 0 } })).toThrow('greater than zero')
		expect(() => weighted({ shares: { live: Number.NaN } })).toThrow('finite')
	})
})
