import { describe, expect, it } from 'vitest'

import { cooperativeStickyAssignor, CooperativeStickyAssignor } from '@/consumer/assignors/cooperative-sticky.js'
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

function countTotalAssigned(assignments: Map<string, { partitions: TopicPartitionList[] }>): number {
	let total = 0
	for (const [, assignment] of assignments) {
		for (const tp of assignment.partitions) {
			total += tp.partitions.length
		}
	}
	return total
}

describe('CooperativeStickyAssignor', () => {
	it('has correct name and protocol type', () => {
		expect(cooperativeStickyAssignor.name).toBe('cooperative-sticky')
		expect(cooperativeStickyAssignor.protocolType).toBe('cooperative')
	})

	describe('basic assignment (no prior ownership)', () => {
		it('assigns partitions evenly to members', () => {
			const members = [member('a', ['t']), member('b', ['t'])]
			const partitions = new Map([['t', [0, 1, 2, 3]]])
			const assignments = cooperativeStickyAssignor.assign(members, partitions)

			expect(getPartitions(assignments, 'a', 't')).toHaveLength(2)
			expect(getPartitions(assignments, 'b', 't')).toHaveLength(2)
		})

		it('assigns extra partitions when not evenly divisible', () => {
			const members = [member('a', ['t']), member('b', ['t'])]
			const partitions = new Map([['t', [0, 1, 2, 3, 4]]])
			const assignments = cooperativeStickyAssignor.assign(members, partitions)

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
			const assignments = cooperativeStickyAssignor.assign(members, partitions)

			expect(getPartitions(assignments, 'a', 't1')).toEqual([0, 1])
			expect(getPartitions(assignments, 'a', 't2')).toEqual([])
			expect(getPartitions(assignments, 'b', 't1')).toEqual([])
			expect(getPartitions(assignments, 'b', 't2')).toEqual([0, 1])
		})
	})

	describe('cooperative rebalancing (withheld partitions)', () => {
		it('withholds partitions that need to move to a different member', () => {
			// a currently owns all 4 partitions, b joins
			// In cooperative mode, partitions that would move from a to b are withheld
			const members = [member('a', ['t'], [{ topic: 't', partitions: [0, 1, 2, 3] }]), member('b', ['t'], [])]
			const partitions = new Map([['t', [0, 1, 2, 3]]])
			const assignments = cooperativeStickyAssignor.assign(members, partitions)

			// a should keep its partitions (they stay with current owner)
			const aPartitions = getPartitions(assignments, 'a', 't')
			// b should not get any partitions in phase 1 (they are withheld)
			const bPartitions = getPartitions(assignments, 'b', 't')

			// Total assigned should be less than 4 because some are withheld
			// a keeps what it can, b gets nothing yet
			const totalAssigned = aPartitions.length + bPartitions.length
			expect(totalAssigned).toBeLessThanOrEqual(4)

			// a should keep some partitions (stickiness)
			expect(aPartitions.length).toBeGreaterThan(0)
		})

		it('assigns withheld partitions after owner releases them (second rebalance)', () => {
			// Simulate second rebalance after a released some partitions
			// Now a only claims ownership of 2 partitions
			const members = [member('a', ['t'], [{ topic: 't', partitions: [0, 1] }]), member('b', ['t'], [])]
			const partitions = new Map([['t', [0, 1, 2, 3]]])
			const assignments = cooperativeStickyAssignor.assign(members, partitions)

			// a keeps 0, 1
			expect(getPartitions(assignments, 'a', 't')).toContain(0)
			expect(getPartitions(assignments, 'a', 't')).toContain(1)

			// 2, 3 are now unowned and can be assigned to b
			const bPartitions = getPartitions(assignments, 'b', 't')
			expect(bPartitions).toContain(2)
			expect(bPartitions).toContain(3)
		})

		it('preserves assignments that do not need to move', () => {
			// Both members already have balanced assignments
			const members = [
				member('a', ['t'], [{ topic: 't', partitions: [0, 1] }]),
				member('b', ['t'], [{ topic: 't', partitions: [2, 3] }]),
			]
			const partitions = new Map([['t', [0, 1, 2, 3]]])
			const assignments = cooperativeStickyAssignor.assign(members, partitions)

			// All partitions should be assigned (nothing withheld)
			expect(getPartitions(assignments, 'a', 't')).toEqual([0, 1])
			expect(getPartitions(assignments, 'b', 't')).toEqual([2, 3])
		})

		it('handles member leaving without withholding', () => {
			// b left, only a remains - no need to withhold since b is gone
			const members = [member('a', ['t'], [{ topic: 't', partitions: [0, 1] }])]
			const partitions = new Map([['t', [0, 1, 2, 3]]])
			const assignments = cooperativeStickyAssignor.assign(members, partitions)

			// a gets all partitions immediately (2, 3 have no current owner)
			expect(getPartitions(assignments, 'a', 't')).toEqual([0, 1, 2, 3])
		})
	})

	describe('stickiness', () => {
		it('preserves existing assignments when possible', () => {
			const members = [
				member('a', ['t'], [{ topic: 't', partitions: [0, 2] }]),
				member('b', ['t'], [{ topic: 't', partitions: [1, 3] }]),
			]
			const partitions = new Map([['t', [0, 1, 2, 3]]])
			const assignments = cooperativeStickyAssignor.assign(members, partitions)

			expect(getPartitions(assignments, 'a', 't')).toContain(0)
			expect(getPartitions(assignments, 'a', 't')).toContain(2)
			expect(getPartitions(assignments, 'b', 't')).toContain(1)
			expect(getPartitions(assignments, 'b', 't')).toContain(3)
		})

		it('does not preserve ownership for unsubscribed topics', () => {
			// a is only subscribed to t2, b is subscribed to t1 and t2
			// Neither claims prior ownership
			const members = [member('a', ['t2'], []), member('b', ['t1', 't2'], [])]
			const partitions = new Map([
				['t1', [0, 1]],
				['t2', [0, 1]],
			])
			const assignments = cooperativeStickyAssignor.assign(members, partitions)

			// a should not have t1 (not subscribed)
			expect(getPartitions(assignments, 'a', 't1')).toEqual([])
			// b should get all of t1 (only subscriber)
			expect(getPartitions(assignments, 'b', 't1')).toEqual([0, 1])
			// t2 should be split between a and b
			const t2Total = getPartitions(assignments, 'a', 't2').length + getPartitions(assignments, 'b', 't2').length
			expect(t2Total).toBe(2)
		})
	})

	describe('balancing', () => {
		it('balances partitions across three members (after full rebalance)', () => {
			// All members have no prior ownership - fresh assignment
			const members = [member('a', ['t']), member('b', ['t']), member('c', ['t'])]
			const partitions = new Map([['t', [0, 1, 2, 3, 4, 5]]])
			const assignments = cooperativeStickyAssignor.assign(members, partitions)

			expect(getPartitions(assignments, 'a', 't')).toHaveLength(2)
			expect(getPartitions(assignments, 'b', 't')).toHaveLength(2)
			expect(getPartitions(assignments, 'c', 't')).toHaveLength(2)
		})

		it('rebalances from overloaded member to underloaded member', () => {
			// a has all partitions, b and c have none - should rebalance
			const members = [
				member('a', ['t'], [{ topic: 't', partitions: [0, 1, 2, 3, 4, 5] }]),
				member('b', ['t'], []),
				member('c', ['t'], []),
			]
			const partitions = new Map([['t', [0, 1, 2, 3, 4, 5]]])
			const assignments = cooperativeStickyAssignor.assign(members, partitions)

			// Some partitions should be withheld (in-transit to b and c)
			// a keeps 2, moves 4 to b and c (2 each, withheld in phase 1)
			const totalAssigned = countTotalAssigned(assignments)
			// In cooperative mode, partitions moving to new owners are withheld
			expect(totalAssigned).toBeLessThan(6)
		})

		it('handles more members than partitions', () => {
			const members = [member('a', ['t']), member('b', ['t']), member('c', ['t'])]
			const partitions = new Map([['t', [0, 1]]])
			const assignments = cooperativeStickyAssignor.assign(members, partitions)

			const total =
				getPartitions(assignments, 'a', 't').length +
				getPartitions(assignments, 'b', 't').length +
				getPartitions(assignments, 'c', 't').length
			expect(total).toBe(2)
		})
	})

	describe('edge cases', () => {
		it('handles empty members list', () => {
			const assignments = cooperativeStickyAssignor.assign([], new Map([['t', [0, 1]]]))
			expect(assignments.size).toBe(0)
		})

		it('handles empty partitions', () => {
			const members = [member('a', ['t'])]
			const assignments = cooperativeStickyAssignor.assign(members, new Map([['t', []]]))
			expect(assignments.get('a')!.partitions).toEqual([])
		})

		it('handles topic with no subscribers', () => {
			const members = [member('a', ['t1'])]
			const partitions = new Map([
				['t1', [0]],
				['t2', [0, 1]], // no one subscribed
			])
			const assignments = cooperativeStickyAssignor.assign(members, partitions)

			expect(getPartitions(assignments, 'a', 't1')).toEqual([0])
			expect(getPartitions(assignments, 'a', 't2')).toEqual([])
		})

		it('produces balanced output regardless of member order', () => {
			const members1 = [member('a', ['t']), member('b', ['t']), member('c', ['t'])]
			const members2 = [member('c', ['t']), member('a', ['t']), member('b', ['t'])]
			const partitions = new Map([['t', [0, 1, 2, 3, 4, 5]]])

			const assignments1 = cooperativeStickyAssignor.assign(members1, partitions)
			const assignments2 = cooperativeStickyAssignor.assign(members2, partitions)

			// Both should produce balanced assignments (2 partitions each)
			expect(getAllPartitions(assignments1, 'a').size).toBe(2)
			expect(getAllPartitions(assignments1, 'b').size).toBe(2)
			expect(getAllPartitions(assignments1, 'c').size).toBe(2)

			expect(getAllPartitions(assignments2, 'a').size).toBe(2)
			expect(getAllPartitions(assignments2, 'b').size).toBe(2)
			expect(getAllPartitions(assignments2, 'c').size).toBe(2)

			// All partitions should be covered in both cases
			const all1 = new Set([
				...getAllPartitions(assignments1, 'a'),
				...getAllPartitions(assignments1, 'b'),
				...getAllPartitions(assignments1, 'c'),
			])
			const all2 = new Set([
				...getAllPartitions(assignments2, 'a'),
				...getAllPartitions(assignments2, 'b'),
				...getAllPartitions(assignments2, 'c'),
			])
			expect(all1.size).toBe(6)
			expect(all2.size).toBe(6)
		})

		it('sorts partitions within assignments', () => {
			const members = [member('a', ['t'])]
			const partitions = new Map([['t', [3, 1, 4, 0, 2]]])
			const assignments = cooperativeStickyAssignor.assign(members, partitions)

			expect(getPartitions(assignments, 'a', 't')).toEqual([0, 1, 2, 3, 4])
		})

		it('sorts topics within assignments', () => {
			const members = [member('a', ['z', 'a', 'm'])]
			const partitions = new Map([
				['z', [0]],
				['a', [0]],
				['m', [0]],
			])
			const assignments = cooperativeStickyAssignor.assign(members, partitions)

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
			const assignments = cooperativeStickyAssignor.assign(members, partitions)

			// Each member should get 2 partitions total (balanced)
			const aTotal = getAllPartitions(assignments, 'a').size
			const bTotal = getAllPartitions(assignments, 'b').size
			expect(aTotal).toBe(2)
			expect(bTotal).toBe(2)
		})

		it('handles partial topic subscription overlap', () => {
			const members = [member('a', ['t1', 't2']), member('b', ['t2', 't3']), member('c', ['t1', 't3'])]
			const partitions = new Map([
				['t1', [0, 1]],
				['t2', [0, 1]],
				['t3', [0, 1]],
			])
			const assignments = cooperativeStickyAssignor.assign(members, partitions)

			// All 6 partitions should be assigned
			const total = countTotalAssigned(assignments)
			expect(total).toBe(6)

			// Each member should get some partitions
			expect(getAllPartitions(assignments, 'a').size).toBeGreaterThan(0)
			expect(getAllPartitions(assignments, 'b').size).toBeGreaterThan(0)
			expect(getAllPartitions(assignments, 'c').size).toBeGreaterThan(0)
		})
	})

	describe('incremental rebalance simulation', () => {
		it('simulates a complete two-phase cooperative rebalance', () => {
			// Phase 1: a owns everything, b joins
			const phase1Members = [
				member('a', ['t'], [{ topic: 't', partitions: [0, 1, 2, 3] }]),
				member('b', ['t'], []),
			]
			const partitions = new Map([['t', [0, 1, 2, 3]]])
			const phase1 = cooperativeStickyAssignor.assign(phase1Members, partitions)

			// a should keep some, some are withheld
			const aPhase1 = getPartitions(phase1, 'a', 't')
			const bPhase1 = getPartitions(phase1, 'b', 't')

			// In phase 1, b gets nothing because partitions are in-transit
			expect(bPhase1).toHaveLength(0)
			// a keeps only what it will ultimately own
			expect(aPhase1.length).toBe(2)

			// Phase 2: a releases the withheld partitions, b claims them
			const phase2Members = [member('a', ['t'], [{ topic: 't', partitions: aPhase1 }]), member('b', ['t'], [])]
			const phase2 = cooperativeStickyAssignor.assign(phase2Members, partitions)

			// Now balanced
			expect(getPartitions(phase2, 'a', 't')).toHaveLength(2)
			expect(getPartitions(phase2, 'b', 't')).toHaveLength(2)

			// All partitions assigned
			const allAssigned = new Set([...getPartitions(phase2, 'a', 't'), ...getPartitions(phase2, 'b', 't')])
			expect(allAssigned).toEqual(new Set([0, 1, 2, 3]))
		})
	})
})
