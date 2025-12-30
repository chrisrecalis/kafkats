import { describe, expect, it } from 'vitest'

import { createClient } from '../helpers/kafka.js'

describe.concurrent('Admin - Cluster (integration)', () => {
	it('describes cluster', async () => {
		const client = createClient('admin-describe-cluster')
		await client.connect()

		const admin = client.admin()

		const description = await admin.describeCluster()

		// Should have at least one broker
		expect(description.brokers.length).toBeGreaterThanOrEqual(1)

		// Controller should be one of the brokers
		expect(description.controllerId).toBeGreaterThanOrEqual(0)

		// Verify broker info
		const controller = description.brokers.find(b => b.nodeId === description.controllerId)
		expect(controller).toBeDefined()
		expect(controller!.host).toBeTruthy()
		expect(controller!.port).toBeGreaterThan(0)

		await client.disconnect()
	})
})
