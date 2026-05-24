import { describe, expect, it } from 'vitest'

import { stickyAssignor } from '@/consumer/assignors/sticky.js'
import type { MemberSubscription } from '@/consumer/assignors/types.js'
import type { TopicPartitionList } from '@/consumer/assignors/consumer-protocol.js'

function member(memberId: string, topics: string[], ownedPartitions?: TopicPartitionList[]): MemberSubscription {
	return {
		memberId,
		metadata: {
			version: ownedPartitions ? 1 : 0,
			topics,
			userData: null,
			ownedPartitions,
		},
	}
}

function getPartitions(
	assignments: Map<string, { partitions: TopicPartitionList[] }>,
	memberId: string,
	topic: string
): number[] {
	const assignment = assignments.get(memberId)
	if (!assignment) return []
	const tp = assignment.partitions.find(p => p.topic === topic)
	return tp?.partitions ?? []
}

function getAllPartitions(
	assignments: Map<string, { partitions: TopicPartitionList[] }>,
	memberId: string
): Set<string> {
	const result = new Set<string>()
	const assignment = assignments.get(memberId)
	if (!assignment) return result
	for (const tp of assignment.partitions) {
		for (const p of tp.partitions) {
			result.add(`${tp.topic}-${p}`)
		}
	}
	return result
}

describe('StickyAssignor', () => {
	it('has correct name and protocol type', () => {
		expect(stickyAssignor.name).toBe('sticky')
		expect(stickyAssignor.protocolType).toBe('eager')
	})

	describe('basic assignment (no prior ownership)', () => {
		it('assigns partitions evenly to members', () => {
			const members = [member('a', ['t']), member('b', ['t'])]
			const partitions = new Map([['t', [0, 1, 2, 3]]])
			const assignments = stickyAssignor.assign(members, partitions)

			expect(getPartitions(assignments, 'a', 't')).toHaveLength(2)
			expect(getPartitions(assignments, 'b', 't')).toHaveLength(2)
		})

		it('assigns extra partitions when not evenly divisible', () => {
			const members = [member('a', ['t']), member('b', ['t'])]
			const partitions = new Map([['t', [0, 1, 2, 3, 4]]])
			const assignments = stickyAssignor.assign(members, partitions)

			const aCount = getPartitions(assignments, 'a', 't').length
			const bCount = getPartitions(assignments, 'b', 't').length
			expect(aCount + bCount).toBe(5)
			expect(Math.abs(aCount - bCount)).toBeLessThanOrEqual(1)
		})

		it('respects topic subscriptions', () => {
			const members = [member('a', ['t1']), member('b', ['t2'])]
			const partitions = new Map([
				['t1', [0, 1]],
				['t2', [0, 1]],
			])
			const assignments = stickyAssignor.assign(members, partitions)

			expect(getPartitions(assignments, 'a', 't1')).toEqual([0, 1])
			expect(getPartitions(assignments, 'a', 't2')).toEqual([])
			expect(getPartitions(assignments, 'b', 't1')).toEqual([])
			expect(getPartitions(assignments, 'b', 't2')).toEqual([0, 1])
		})

		it('handles overlapping subscriptions', () => {
			const members = [member('a', ['t1', 't2']), member('b', ['t2'])]
			const partitions = new Map([
				['t1', [0, 1]],
				['t2', [0, 1]],
			])
			const assignments = stickyAssignor.assign(members, partitions)

			// a should get all of t1 (only subscriber)
			expect(getPartitions(assignments, 'a', 't1')).toEqual([0, 1])
			// t2 should be split between a and b
			const t2Total = getPartitions(assignments, 'a', 't2').length + getPartitions(assignments, 'b', 't2').length
			expect(t2Total).toBe(2)
		})
	})

	describe('stickiness (preserving prior ownership)', () => {
		it('preserves existing assignments when possible', () => {
			const members = [
				member('a', ['t'], [{ topic: 't', partitions: [0, 1] }]),
				member('b', ['t'], [{ topic: 't', partitions: [2, 3] }]),
			]
			const partitions = new Map([['t', [0, 1, 2, 3]]])
			const assignments = stickyAssignor.assign(members, partitions)

			expect(getPartitions(assignments, 'a', 't')).toEqual([0, 1])
			expect(getPartitions(assignments, 'b', 't')).toEqual([2, 3])
		})

		it('does not preserve ownership for unsubscribed topics', () => {
			// a previously owned t1 partitions but is now only subscribed to t2
			const members = [member('a', ['t2'], [{ topic: 't1', partitions: [0, 1] }]), member('b', ['t1', 't2'], [])]
			const partitions = new Map([
				['t1', [0, 1]],
				['t2', [0, 1]],
			])
			const assignments = stickyAssignor.assign(members, partitions)

			// a should not have t1 anymore
			expect(getPartitions(assignments, 'a', 't1')).toEqual([])
			// b should get t1
			expect(getPartitions(assignments, 'b', 't1')).toEqual([0, 1])
		})

		it('rebalances to a joining member instead of starving it', () => {
			// a owns all partitions, c joins owning none. The eager assignor revokes all
			// partitions before reassignment, so it must rebalance — c must not be starved.
			const members = [member('a', ['t'], [{ topic: 't', partitions: [0, 1, 2, 3] }]), member('c', ['t'], [])]
			const partitions = new Map([['t', [0, 1, 2, 3]]])
			const assignments = stickyAssignor.assign(members, partitions)

			const aPartitions = getPartitions(assignments, 'a', 't')
			const cPartitions = getPartitions(assignments, 'c', 't')

			// All partitions assigned exactly once, balanced, and c is not starved.
			expect(aPartitions.length + cPartitions.length).toBe(4)
			expect(cPartitions.length).toBeGreaterThan(0)
			expect(Math.abs(aPartitions.length - cPartitions.length)).toBeLessThanOrEqual(1)
			const overlap = aPartitions.filter(p => cPartitions.includes(p))
			expect(overlap).toEqual([])
		})

		it('handles member leaving with sticky assignments', () => {
			// b left, only a remains
			const members = [member('a', ['t'], [{ topic: 't', partitions: [0, 1] }])]
			const partitions = new Map([['t', [0, 1, 2, 3]]])
			const assignments = stickyAssignor.assign(members, partitions)

			// a should get all partitions including previously unowned ones
			expect(getPartitions(assignments, 'a', 't')).toEqual([0, 1, 2, 3])
		})
	})

	describe('balancing', () => {
		it('balances partitions across three members', () => {
			const members = [member('a', ['t']), member('b', ['t']), member('c', ['t'])]
			const partitions = new Map([['t', [0, 1, 2, 3, 4, 5]]])
			const assignments = stickyAssignor.assign(members, partitions)

			expect(getPartitions(assignments, 'a', 't')).toHaveLength(2)
			expect(getPartitions(assignments, 'b', 't')).toHaveLength(2)
			expect(getPartitions(assignments, 'c', 't')).toHaveLength(2)
		})

		it('handles more members than partitions', () => {
			const members = [member('a', ['t']), member('b', ['t']), member('c', ['t'])]
			const partitions = new Map([['t', [0, 1]]])
			const assignments = stickyAssignor.assign(members, partitions)

			const total =
				getPartitions(assignments, 'a', 't').length +
				getPartitions(assignments, 'b', 't').length +
				getPartitions(assignments, 'c', 't').length
			expect(total).toBe(2)
		})

		it('rebalances a fully-owned topic to max-min <= 1 across many members', () => {
			// a owns all 6 partitions; b/c/d join. Must balance to 2/2/1/1 (max-min <= 1),
			// not stop early at e.g. 3/1/1/1.
			const members = [
				member('a', ['t'], [{ topic: 't', partitions: [0, 1, 2, 3, 4, 5] }]),
				member('b', ['t'], []),
				member('c', ['t'], []),
				member('d', ['t'], []),
			]
			const partitions = new Map([['t', [0, 1, 2, 3, 4, 5]]])
			const assignments = stickyAssignor.assign(members, partitions)

			const counts = ['a', 'b', 'c', 'd'].map(m => getAllPartitions(assignments, m).size)
			expect(counts.reduce((s, n) => s + n, 0)).toBe(6)
			expect(Math.max(...counts) - Math.min(...counts)).toBeLessThanOrEqual(1)
		})

		it('avoids avoidable starvation when members outnumber partitions and one owns all', () => {
			// a owns both partitions; b/c join. Two members should get 1 and one gets 0
			// (unavoidable), not a=2 / b=0 / c=0.
			const members = [
				member('a', ['t'], [{ topic: 't', partitions: [0, 1] }]),
				member('b', ['t'], []),
				member('c', ['t'], []),
			]
			const partitions = new Map([['t', [0, 1]]])
			const assignments = stickyAssignor.assign(members, partitions)

			const counts = ['a', 'b', 'c'].map(m => getAllPartitions(assignments, m).size).sort()
			expect(counts).toEqual([0, 1, 1])
		})
	})

	describe('edge cases', () => {
		it('handles empty members list', () => {
			const assignments = stickyAssignor.assign([], new Map([['t', [0, 1]]]))
			expect(assignments.size).toBe(0)
		})

		it('handles empty partitions', () => {
			const members = [member('a', ['t'])]
			const assignments = stickyAssignor.assign(members, new Map([['t', []]]))
			expect(assignments.get('a')!.partitions).toEqual([])
		})

		it('handles topic with no subscribers', () => {
			const members = [member('a', ['t1'])]
			const partitions = new Map([
				['t1', [0]],
				['t2', [0, 1]], // no one subscribed
			])
			const assignments = stickyAssignor.assign(members, partitions)

			expect(getPartitions(assignments, 'a', 't1')).toEqual([0])
			expect(getPartitions(assignments, 'a', 't2')).toEqual([])
		})

		it('produces deterministic output regardless of member order', () => {
			const members1 = [member('a', ['t']), member('b', ['t']), member('c', ['t'])]
			const members2 = [member('c', ['t']), member('a', ['t']), member('b', ['t'])]
			const partitions = new Map([['t', [0, 1, 2, 3, 4, 5]]])

			const assignments1 = stickyAssignor.assign(members1, partitions)
			const assignments2 = stickyAssignor.assign(members2, partitions)

			expect(getAllPartitions(assignments1, 'a')).toEqual(getAllPartitions(assignments2, 'a'))
			expect(getAllPartitions(assignments1, 'b')).toEqual(getAllPartitions(assignments2, 'b'))
			expect(getAllPartitions(assignments1, 'c')).toEqual(getAllPartitions(assignments2, 'c'))
		})

		it('sorts partitions within assignments', () => {
			const members = [member('a', ['t'])]
			const partitions = new Map([['t', [3, 1, 4, 0, 2]]])
			const assignments = stickyAssignor.assign(members, partitions)

			expect(getPartitions(assignments, 'a', 't')).toEqual([0, 1, 2, 3, 4])
		})

		it('sorts topics within assignments', () => {
			const members = [member('a', ['z', 'a', 'm'])]
			const partitions = new Map([
				['z', [0]],
				['a', [0]],
				['m', [0]],
			])
			const assignments = stickyAssignor.assign(members, partitions)

			const topics = assignments.get('a')!.partitions.map(p => p.topic)
			expect(topics).toEqual(['a', 'm', 'z'])
		})
	})

	describe('multi-topic scenarios', () => {
		it('balances across multiple topics', () => {
			const members = [member('a', ['t1', 't2']), member('b', ['t1', 't2'])]
			const partitions = new Map([
				['t1', [0, 1]],
				['t2', [0, 1]],
			])
			const assignments = stickyAssignor.assign(members, partitions)

			// Each member should get 2 partitions total (balanced)
			const aTotal = getAllPartitions(assignments, 'a').size
			const bTotal = getAllPartitions(assignments, 'b').size
			expect(aTotal).toBe(2)
			expect(bTotal).toBe(2)
		})

		it('relays surplus through an intermediary when heterogeneous subscriptions block a direct move', () => {
			// topics X=[0,1], Y=[0]. a owns both X (2); b owns Y (1); c idle (0).
			// a cannot give to c (c is not subscribed to X) and a->b differs by only 1, so a
			// purely direct balancer is stuck at {2,1,0}. Balancing must relay: a hands an X to
			// b, b hands its Y to c, yielding {1,1,1}.
			const members = [
				member('a', ['X'], [{ topic: 'X', partitions: [0, 1] }]),
				member('b', ['X', 'Y'], [{ topic: 'Y', partitions: [0] }]),
				member('c', ['Y'], []),
			]
			const partitions = new Map([
				['X', [0, 1]],
				['Y', [0]],
			])
			const assignments = stickyAssignor.assign(members, partitions)

			const sizes = ['a', 'b', 'c'].map(id => getAllPartitions(assignments, id).size)
			expect(Math.max(...sizes) - Math.min(...sizes)).toBeLessThanOrEqual(1)
			expect(getAllPartitions(assignments, 'c').size).toBe(1)
			// c only ever holds a Y partition (its sole subscription).
			expect(getPartitions(assignments, 'c', 'Y')).toEqual([0])
		})
	})
})
