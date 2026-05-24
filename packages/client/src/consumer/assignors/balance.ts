/**
 * Shared balancing helper for the sticky partition assignors.
 *
 * After the sticky phase keeps owned partitions and a round-robin phase places the rest,
 * the per-member partition counts can still be uneven. This helper redistributes partitions
 * toward an even spread (max-min <= 1) while respecting each member's topic subscriptions.
 *
 * A naive "move one partition directly from the most-loaded to the least-loaded member" loop
 * gets stuck on heterogeneous subscriptions: if the most-loaded member holds only topics the
 * least-loaded member is not subscribed to, no direct move is possible even when a balanced
 * assignment exists. For example:
 *
 *   topics:  X = [0, 1], Y = [0]
 *   a -> {X},      owns X=[0,1]   (2)
 *   b -> {X, Y},   owns Y=[0]     (1)
 *   c -> {Y},      owns nothing   (0)
 *
 * a cannot give to c (c is not subscribed to X) and the a->b gap is only 1, so a direct
 * balancer leaves {2,1,0}. The balanced solution needs a relay: a hands an X partition to b,
 * and b hands its Y partition to c, yielding {1,1,1}.
 *
 * This helper finds those relays with a shortest-path search over a transfer graph (an edge
 * U -> V exists when U holds a partition V is subscribed to). Each relay moves one of every
 * intermediary's own partitions forward by one hop, so intermediaries net zero and only the
 * path's head loses one partition and its tail gains one. Each relay strictly decreases the
 * sum of squared member counts, so the loop terminates, and it stops only when no
 * count-reducing transfer path exists between any over/under-loaded pair — i.e. max-min <= 1
 * within every connected component of the transfer graph (the best achievable; remaining
 * imbalance only spans members with disjoint, non-relayable subscriptions). This mirrors
 * Kafka's AbstractStickyAssignor graph balancing (KIP-54).
 */

import { parseKey } from '@/utils/topic-partition.js'

/**
 * Rebalance partition assignments in place toward an even spread, honoring subscriptions.
 *
 * @param memberIds - all member ids participating in the assignment
 * @param assignments - memberId -> set of owned topic-partition keys (mutated in place)
 * @param subscriptions - memberId -> set of subscribed topic names
 */
export function balanceByRelay(
	memberIds: string[],
	assignments: Map<string, Set<string>>,
	subscriptions: Map<string, Set<string>>
): void {
	const sortedIds = [...memberIds].sort()
	const count = (id: string) => assignments.get(id)!.size

	// The lowest-keyed partition `from` holds that `to` is subscribed to, or null if none.
	// Lowest-keyed keeps the choice deterministic regardless of Set insertion order.
	const transferableKey = (from: string, to: string): string | null => {
		const toTopics = subscriptions.get(to)!
		let best: string | null = null
		for (const key of assignments.get(from)!) {
			const { topic } = parseKey(key)
			if (toTopics.has(topic) && (best === null || key < best)) best = key
		}
		return best
	}

	// Shortest transfer path from `start` to `goal` as a list of member ids, or null if the
	// goal is unreachable. BFS over the transfer graph; members visited in id order so the
	// path is deterministic.
	const findPath = (start: string, goal: string): string[] | null => {
		const prev = new Map<string, string>()
		const visited = new Set<string>([start])
		let frontier = [start]
		while (frontier.length > 0) {
			const next: string[] = []
			for (const u of frontier) {
				for (const v of sortedIds) {
					if (visited.has(v)) continue
					if (transferableKey(u, v) === null) continue
					visited.add(v)
					prev.set(v, u)
					if (v === goal) {
						const path = [v]
						let cur = v
						while (cur !== start) {
							cur = prev.get(cur)!
							path.unshift(cur)
						}
						return path
					}
					next.push(v)
				}
			}
			frontier = next
		}
		return null
	}

	let improved = true
	while (improved) {
		improved = false

		const byCountDesc = [...sortedIds].sort((a, b) => {
			const diff = count(b) - count(a)
			return diff !== 0 ? diff : a.localeCompare(b)
		})
		const byCountAsc = [...byCountDesc].reverse()

		for (const donor of byCountDesc) {
			for (const recipient of byCountAsc) {
				if (count(donor) - count(recipient) < 2) continue

				const path = findPath(donor, recipient)
				if (!path) continue

				// Pick each hop's partition from the pre-relay state so the moves are independent
				// (every hop ships one of its own distinct partitions one step forward).
				const moves: Array<{ key: string; from: string; to: string }> = []
				for (let i = 0; i < path.length - 1; i++) {
					const key = transferableKey(path[i]!, path[i + 1]!)!
					moves.push({ key, from: path[i]!, to: path[i + 1]! })
				}
				for (const move of moves) {
					assignments.get(move.from)!.delete(move.key)
					assignments.get(move.to)!.add(move.key)
				}

				improved = true
				break
			}
			if (improved) break
		}
	}
}
