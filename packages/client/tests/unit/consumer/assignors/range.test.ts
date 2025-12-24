import { describe, expect, it } from 'vitest'

import { rangeAssignor } from '@/consumer/assignors/range.js'
import type { MemberSubscription } from '@/consumer/assignors/types.js'

function member(memberId: string, topics: string[]): MemberSubscription {
	return {
		memberId,
		metadata: {
			version: 0,
			topics,
			userData: null,
		},
	}
}

describe('RangeAssignor', () => {
	it('assigns partitions contiguously', () => {
		const members = [member('a', ['t']), member('b', ['t']), member('c', ['t'])]
		const partitions = new Map([['t', [0, 1, 2, 3, 4, 5, 6]]])
		const assignments = rangeAssignor.assign(members, partitions)
		expect(assignments.get('a')!.partitions[0]!.partitions).toEqual([0, 1, 2])
		expect(assignments.get('b')!.partitions[0]!.partitions).toEqual([3, 4])
		expect(assignments.get('c')!.partitions[0]!.partitions).toEqual([5, 6])
	})

	it('assigns extra partitions to early members', () => {
		const members = [member('a', ['t']), member('b', ['t'])]
		const partitions = new Map([['t', [0, 1, 2]]])
		const assignments = rangeAssignor.assign(members, partitions)
		expect(assignments.get('a')!.partitions[0]!.partitions).toEqual([0, 1])
		expect(assignments.get('b')!.partitions[0]!.partitions).toEqual([2])
	})

	it('respects member topic subscriptions', () => {
		const members = [member('a', ['t1']), member('b', ['t2'])]
		const partitions = new Map([
			['t1', [0, 1]],
			['t2', [0, 1]],
		])
		const assignments = rangeAssignor.assign(members, partitions)
		expect(assignments.get('a')!.partitions).toEqual([{ topic: 't1', partitions: [0, 1] }])
		expect(assignments.get('b')!.partitions).toEqual([{ topic: 't2', partitions: [0, 1] }])
	})

	it('handles topics with no partitions', () => {
		const members = [member('a', ['t'])]
		const partitions = new Map<string, number[]>([['t', []]])
		const assignments = rangeAssignor.assign(members, partitions)
		expect(assignments.get('a')!.partitions).toEqual([])
	})

	it('returns empty assignments when no members', () => {
		const assignments = rangeAssignor.assign([], new Map([['t', [0]]]))
		expect(assignments.size).toBe(0)
	})

	it('sorts members by memberId for stable assignment', () => {
		const members = [member('b', ['t']), member('a', ['t'])]
		const partitions = new Map([['t', [0, 1]]])
		const assignments = rangeAssignor.assign(members, partitions)
		expect(assignments.get('a')!.partitions[0]!.partitions).toEqual([0])
		expect(assignments.get('b')!.partitions[0]!.partitions).toEqual([1])
	})
})
