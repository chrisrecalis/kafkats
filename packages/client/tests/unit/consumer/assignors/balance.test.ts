import { describe, expect, it } from 'vitest'

import { balanceByRelay } from '@/consumer/assignors/balance.js'

type Counts = Record<string, number>

function setup(
	owned: Record<string, string[]>,
	subs: Record<string, string[]>
): { assignments: Map<string, Set<string>>; subscriptions: Map<string, Set<string>>; ids: string[] } {
	const assignments = new Map<string, Set<string>>()
	const subscriptions = new Map<string, Set<string>>()
	const ids = Object.keys(subs)
	for (const id of ids) {
		assignments.set(id, new Set(owned[id] ?? []))
		subscriptions.set(id, new Set(subs[id]!))
	}
	return { assignments, subscriptions, ids }
}

function counts(assignments: Map<string, Set<string>>): Counts {
	const out: Counts = {}
	for (const [id, set] of assignments) out[id] = set.size
	return out
}

function spread(c: Counts): number {
	const values = Object.values(c)
	return Math.max(...values) - Math.min(...values)
}

describe('balanceByRelay', () => {
	it('balances a uniform overload with direct moves', () => {
		// a owns everything; b, c idle. All subscribe to t.
		const { assignments, subscriptions, ids } = setup(
			{ a: ['t:0', 't:1', 't:2', 't:3', 't:4', 't:5'] },
			{ a: ['t'], b: ['t'], c: ['t'] }
		)
		balanceByRelay(ids, assignments, subscriptions)
		expect(counts(assignments)).toEqual({ a: 2, b: 2, c: 2 })
	})

	it('uses a relay through an intermediary when no direct move is possible', () => {
		// topics X=[0,1], Y=[0].
		// a -> {X} owns X=[0,1] (2); b -> {X,Y} owns Y=[0] (1); c -> {Y} owns nothing (0).
		// a cannot give to c (c lacks X) and the a->b gap is only 1, so a direct balancer is
		// stuck at {2,1,0}. The relay a->b (X) then b->c (Y) yields {1,1,1}.
		const { assignments, subscriptions, ids } = setup(
			{ a: ['X:0', 'X:1'], b: ['Y:0'], c: [] },
			{ a: ['X'], b: ['X', 'Y'], c: ['Y'] }
		)
		balanceByRelay(ids, assignments, subscriptions)
		const c = counts(assignments)
		expect(spread(c)).toBeLessThanOrEqual(1)
		expect(c).toEqual({ a: 1, b: 1, c: 1 })
		// Every assigned partition still goes to a subscribed member.
		for (const [id, set] of assignments) {
			for (const key of set) {
				const topic = key.split(':')[0]!
				expect(subscriptions.get(id)!.has(topic)).toBe(true)
			}
		}
	})

	it('leaves a genuinely unbalanceable split alone (disjoint subscriptions)', () => {
		// a owns 3 of X, b subscribes only to Y. No transfer is possible.
		const { assignments, subscriptions, ids } = setup({ a: ['X:0', 'X:1', 'X:2'], b: [] }, { a: ['X'], b: ['Y'] })
		balanceByRelay(ids, assignments, subscriptions)
		expect(counts(assignments)).toEqual({ a: 3, b: 0 })
	})

	it('is deterministic regardless of member id ordering', () => {
		const run = (ids: string[]) => {
			const { assignments, subscriptions } = setup(
				{ a: ['t:0', 't:1', 't:2', 't:3'], b: [], c: [] },
				{ a: ['t'], b: ['t'], c: ['t'] }
			)
			balanceByRelay(ids, assignments, subscriptions)
			return counts(assignments)
		}
		expect(run(['a', 'b', 'c'])).toEqual(run(['c', 'b', 'a']))
	})

	it('converges with a longer relay chain', () => {
		// Chain: a(X) -> b(X,Y) -> c(Y,Z) -> d(Z). a is overloaded, d is idle; only a relay works.
		// topics X=[0,1,2], Y=[0], Z=[0]  (total 5, 4 members -> spread <= 1)
		const { assignments, subscriptions, ids } = setup(
			{ a: ['X:0', 'X:1', 'X:2'], b: ['Y:0'], c: ['Z:0'], d: [] },
			{ a: ['X'], b: ['X', 'Y'], c: ['Y', 'Z'], d: ['Z'] }
		)
		balanceByRelay(ids, assignments, subscriptions)
		const c = counts(assignments)
		expect(spread(c)).toBeLessThanOrEqual(1)
		expect(Object.values(c).reduce((s, v) => s + v, 0)).toBe(5)
		expect(c.d).toBeGreaterThanOrEqual(1)
	})

	it('terminates and preserves the partition set on random heterogeneous inputs', () => {
		const topics = ['t0', 't1', 't2', 't3']
		let seed = 12345
		const rand = (n: number) => {
			seed = (seed * 1103515245 + 12345) & 0x7fffffff
			return seed % n
		}
		for (let trial = 0; trial < 500; trial++) {
			const owned: Record<string, string[]> = { a: [], b: [], c: [], d: [] }
			const subs: Record<string, string[]> = {
				a: topics.filter(() => rand(2) === 0),
				b: topics.filter(() => rand(2) === 0),
				c: topics.filter(() => rand(2) === 0),
				d: topics.filter(() => rand(2) === 0),
			}
			// Ensure each member subscribes to at least one topic.
			for (const id of Object.keys(subs)) if (subs[id]!.length === 0) subs[id] = [topics[rand(topics.length)]!]

			const all: string[] = []
			for (const t of topics) for (let p = 0; p < 3; p++) all.push(`${t}:${p}`)
			// Assign each partition to a random subscribed member (or leave with a if none).
			for (const key of all) {
				const topic = key.split(':')[0]!
				const eligible = Object.keys(subs).filter(id => subs[id]!.includes(topic))
				owned[eligible[rand(eligible.length)]!]!.push(key)
			}

			const { assignments, subscriptions, ids } = setup(owned, subs)
			const before = new Set<string>()
			for (const set of assignments.values()) for (const k of set) before.add(k)

			balanceByRelay(ids, assignments, subscriptions)

			const after = new Set<string>()
			for (const [id, set] of assignments) {
				for (const k of set) {
					after.add(k)
					expect(subscriptions.get(id)!.has(k.split(':')[0]!)).toBe(true)
				}
			}
			expect(after).toEqual(before)
		}
	})
})
