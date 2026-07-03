import { describe, expect, it } from 'vitest'

import { ConsumerGroup } from '@/consumer/consumer-group.js'
import { decodeSubscriptionMetadata, stickyAssignor } from '@/consumer/assignors/index.js'
import type { MemberSubscription, TopicPartitionList } from '@/consumer/assignors/index.js'
import type { Cluster } from '@/client/cluster.js'

// The 'sticky' assignor reads member.metadata.ownedPartitions to preserve prior
// ownership. That only works if the JoinGroup subscription metadata for the 'sticky'
// protocol is encoded as v1 WITH ownedPartitions — v0 metadata silently disables
// stickiness because ownedPartitions decodes as undefined.
function buildProtocols(
	strategy: 'sticky' | 'cooperative-sticky',
	topics: string[],
	owned: TopicPartitionList[]
): Array<{ name: string; metadata: Buffer }> {
	const cluster = { getLogger: () => null } as unknown as Cluster
	const group = new ConsumerGroup(cluster, { groupId: 'g', partitionAssignmentStrategy: strategy })
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	return (group as any).buildProtocolList(topics, owned)
}

describe('sticky assignor subscription metadata', () => {
	it("encodes ownedPartitions (v1 metadata) for the 'sticky' protocol", () => {
		const owned: TopicPartitionList[] = [{ topic: 't', partitions: [0, 2] }]
		const protocols = buildProtocols('sticky', ['t'], owned)

		const sticky = protocols.find(p => p.name === 'sticky')
		expect(sticky).toBeDefined()

		const metadata = decodeSubscriptionMetadata(sticky!.metadata)
		expect(metadata.topics).toEqual(['t'])
		expect(metadata.ownedPartitions).toEqual(owned)
	})

	it("encodes ownedPartitions for the 'sticky' fallback of the cooperative-sticky strategy", () => {
		const owned: TopicPartitionList[] = [{ topic: 't', partitions: [1] }]
		const protocols = buildProtocols('cooperative-sticky', ['t'], owned)

		const sticky = protocols.find(p => p.name === 'sticky')
		expect(sticky).toBeDefined()

		const metadata = decodeSubscriptionMetadata(sticky!.metadata)
		expect(metadata.ownedPartitions).toEqual(owned)
	})

	it('preserves prior ownership across a rebalance through the encoded metadata', () => {
		// Member a owned [0,1,2] and member b owned [3,4,5]; on rebalance each member
		// reports its ownership via subscription metadata and the sticky assignor must
		// keep the assignments unchanged.
		const protocolsA = buildProtocols('sticky', ['t'], [{ topic: 't', partitions: [0, 1, 2] }])
		const protocolsB = buildProtocols('sticky', ['t'], [{ topic: 't', partitions: [3, 4, 5] }])

		const members: MemberSubscription[] = [
			{
				memberId: 'a',
				metadata: decodeSubscriptionMetadata(protocolsA.find(p => p.name === 'sticky')!.metadata),
			},
			{
				memberId: 'b',
				metadata: decodeSubscriptionMetadata(protocolsB.find(p => p.name === 'sticky')!.metadata),
			},
		]

		const assignments = stickyAssignor.assign(members, new Map([['t', [0, 1, 2, 3, 4, 5]]]))

		const partitionsOf = (memberId: string) =>
			assignments
				.get(memberId)!
				.partitions.find(tp => tp.topic === 't')!
				.partitions.slice()
				.sort((x, y) => x - y)

		expect(partitionsOf('a')).toEqual([0, 1, 2])
		expect(partitionsOf('b')).toEqual([3, 4, 5])
	})
})
