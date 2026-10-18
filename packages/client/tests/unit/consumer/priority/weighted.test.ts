import { describe, expect, it } from 'vitest'
import { weighted } from '@/consumer/priority.js'
import type { SchedulerPartition, SchedulerState } from '@/consumer/priority.js'

function partition(topic: string, bufferedBytes: number): SchedulerPartition {
	return { topic, partition: 0, bufferedBytes, busy: false }
}

function state(partitions: SchedulerPartition[], byteBudget = 100): SchedulerState {
	return { partitions, byteBudget, bufferCapacityBytes: 1000, bufferedBytes: 0 }
}

describe('weighted topic priority', () => {
	it('allocates proportionally and redistributes unused demand', () => {
		const strategy = weighted({ shares: { live: 3, bulk: 1 } })
		const proportional = strategy.schedule(state([partition('live', 100), partition('bulk', 100)]))
		expect(proportional.drain.map(p => [p.topic, p.maxBytes])).toEqual([
			['live', 75],
			['bulk', 25],
		])
		const redistributed = strategy.schedule(state([partition('live', 1), partition('bulk', 100)]))
		expect(redistributed.drain.map(p => [p.topic, p.maxBytes])).toEqual([
			['live', 1],
			['bulk', 99],
		])
	})

	it('uses share 1 for unlisted topics and gates fetch by byte share', () => {
		const strategy = weighted({ shares: { live: 3 } })
		const decision = strategy.schedule(state([partition('live', 750), partition('other', 200)]))
		expect(decision.drain.map(p => [p.topic, p.maxBytes])).toEqual([
			['live', 75],
			['other', 25],
		])
		expect(decision.fetch).toEqual([{ topic: 'other', partition: 0 }])
	})

	it('validates shares', () => {
		expect(() => weighted({ shares: {} })).toThrow('must not be empty')
		expect(() => weighted({ shares: { live: 0 } })).toThrow('greater than zero')
		expect(() => weighted({ shares: { live: Number.NaN } })).toThrow('finite')
	})
})
