import { describe, expect, test } from 'vitest'

import { OffsetManager } from '../../../src/consumer/offset-manager.js'

describe('OffsetManager', () => {
	test('addAssignedPartitions preserves existing assignments (cooperative rebalance)', () => {
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const cluster = {} as any
		const offsetManager = new OffsetManager(cluster, 'g')

		offsetManager.addAssignedPartitions([{ topic: 't', partition: 0 }])
		offsetManager.addAssignedPartitions([{ topic: 't', partition: 1 }])

		expect(offsetManager.isPartitionAssigned('t', 0)).toBe(true)
		expect(offsetManager.isPartitionAssigned('t', 1)).toBe(true)
	})

	test('removeAssignedPartitions only removes specified partitions', () => {
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const cluster = {} as any
		const offsetManager = new OffsetManager(cluster, 'g')

		offsetManager.addAssignedPartitions([
			{ topic: 't', partition: 0 },
			{ topic: 't', partition: 1 },
		])

		offsetManager.removeAssignedPartitions([{ topic: 't', partition: 0 }])

		expect(offsetManager.isPartitionAssigned('t', 0)).toBe(false)
		expect(offsetManager.isPartitionAssigned('t', 1)).toBe(true)
	})
})
