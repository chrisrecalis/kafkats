import { describe, expect, it } from 'vitest'

import { Admin } from '@/admin/admin.js'
import type { Cluster } from '@/client/cluster.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { KafkaProtocolError } from '@/client/errors.js'

interface MockOptions {
	failCoordinatorFor?: string[]
	failBroker?: boolean
}

function mockCluster(options: MockOptions = {}): Cluster {
	const broker = {
		describeGroups: async () => ({
			throttleTimeMs: 0,
			groups: [
				{
					errorCode: ErrorCode.None,
					groupId: 'good-group',
					groupState: 'Stable',
					protocolType: 'consumer',
					protocolData: 'range',
					members: [],
				},
			],
		}),
		deleteGroups: async () => ({
			throttleTimeMs: 0,
			results: [{ groupId: 'good-group', errorCode: ErrorCode.None }],
		}),
	}

	return {
		getLogger: () => undefined,
		getCoordinator: async (_type: string, groupId: string) => {
			if (options.failCoordinatorFor?.includes(groupId)) {
				throw new KafkaProtocolError(ErrorCode.CoordinatorNotAvailable, `group ${groupId}`)
			}
			return { nodeId: 1 }
		},
		getBroker: async () => {
			if (options.failBroker) {
				throw new Error('connection refused')
			}
			return broker
		},
	} as unknown as Cluster
}

describe('Admin group operations surface coordinator failures', () => {
	it('describeGroups returns an errored entry for a group whose coordinator lookup fails', async () => {
		const admin = new Admin(mockCluster({ failCoordinatorFor: ['bad-group'] }))

		const descriptions = await admin.describeGroups(['good-group', 'bad-group'])

		expect(descriptions).toHaveLength(2)

		const good = descriptions.find(d => d.groupId === 'good-group')
		expect(good?.state).toBe('Stable')
		expect(good?.errorCode).toBe(ErrorCode.None)

		const bad = descriptions.find(d => d.groupId === 'bad-group')
		expect(bad).toBeDefined()
		expect(bad?.errorCode).toBe(ErrorCode.CoordinatorNotAvailable)
		expect(bad?.members).toEqual([])
	})

	it('describeGroups returns errored entries when the coordinator request itself fails', async () => {
		const admin = new Admin(mockCluster({ failBroker: true }))

		const descriptions = await admin.describeGroups(['g1', 'g2'])

		expect(descriptions).toHaveLength(2)
		for (const groupId of ['g1', 'g2']) {
			const entry = descriptions.find(d => d.groupId === groupId)
			expect(entry).toBeDefined()
			expect(entry?.errorCode).not.toBe(ErrorCode.None)
		}
	})

	it('deleteGroups returns an errored result for a group whose coordinator lookup fails', async () => {
		const admin = new Admin(mockCluster({ failCoordinatorFor: ['bad-group'] }))

		const results = await admin.deleteGroups(['good-group', 'bad-group'])

		expect(results).toHaveLength(2)
		expect(results.find(r => r.groupId === 'good-group')?.errorCode).toBe(ErrorCode.None)
		expect(results.find(r => r.groupId === 'bad-group')?.errorCode).toBe(ErrorCode.CoordinatorNotAvailable)
	})

	it('deleteGroups returns errored results when the coordinator request itself fails', async () => {
		const admin = new Admin(mockCluster({ failBroker: true }))

		const results = await admin.deleteGroups(['g1', 'g2'])

		expect(results).toHaveLength(2)
		for (const groupId of ['g1', 'g2']) {
			const entry = results.find(r => r.groupId === groupId)
			expect(entry).toBeDefined()
			expect(entry?.errorCode).not.toBe(ErrorCode.None)
		}
	})
})
