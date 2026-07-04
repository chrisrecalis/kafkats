import { describe, expect, it } from 'vitest'
import { flow, type FlowConfig } from '../../src/index.js'

/**
 * G5 regression: two app instances with identical applicationId + clientId derived the SAME
 * transactionalId, so they mutually fenced each other in a crash-loop. The transactionalId must
 * contain a per-instance unique component. Zombie fencing is preserved by KIP-447-style consumer
 * group generation fencing: offsets ride the transaction via sendOffsets with group metadata.
 */
describe('exactly_once transactionalId', () => {
	const config: FlowConfig = {
		applicationId: 'my-app',
		client: { clientId: 'shared-client', brokers: ['localhost:9092'] },
		processingGuarantee: 'exactly_once',
	}

	type WithTxnId = { buildTransactionalId(): string }

	it('is unique per app instance with identical config', () => {
		const first = (flow(config) as unknown as WithTxnId).buildTransactionalId()
		const second = (flow(config) as unknown as WithTxnId).buildTransactionalId()

		expect(first.startsWith('my-app-shared-client')).toBe(true)
		expect(second.startsWith('my-app-shared-client')).toBe(true)
		expect(first).not.toBe(second)
	})

	it('is stable within one app instance', () => {
		const app = flow(config) as unknown as WithTxnId
		expect(app.buildTransactionalId()).toBe(app.buildTransactionalId())
	})

	it('respects a user-provided producer transactionalId', () => {
		const app = flow({ ...config, producer: { transactionalId: 'user-txn-id' } }) as unknown as {
			buildProducerConfig(workerId: number, threadCount: number): { transactionalId?: string }
		}
		expect(app.buildProducerConfig(0, 1).transactionalId).toBe('user-txn-id')
	})
})
