import { EventEmitter } from 'node:events'
import { describe, expect, it, vi, afterEach } from 'vitest'
import { KafkaClient } from '@kafkats/client'
import { codec, flow } from '../../src/index.js'

/**
 * G4 regression: partitions assigned WHILE the initial restoration is running were paused by the
 * partitionsAssigned handler (restorationComplete still false, so no restore was enqueued) and
 * then blindly resumed by start()'s finally block — processing on unrestored state.
 */

type TestHandler = (message: unknown, ctx: unknown) => Promise<void>

class RebalancingConsumer extends EventEmitter {
	private stopResolve: (() => void) | null = null
	consumerGroup = { currentMemberId: 'member-1', currentGenerationId: 1 }

	constructor(
		private readonly initialAssignment: Array<{ topic: string; partition: number }>,
		private readonly events: string[]
	) {
		super()
	}

	async runEach(_subscription: string[], _handler: TestHandler): Promise<void> {
		this.emit('running')
		this.emit('partitionsAssigned', this.initialAssignment)
		return new Promise(resolve => {
			this.stopResolve = () => {
				this.emit('stopped')
				resolve()
			}
		})
	}

	pause(partitions: Array<{ topic: string; partition: number }>): void {
		for (const tp of partitions) {
			this.events.push(`pause:${tp.topic}:${tp.partition}`)
		}
	}

	resume(partitions: Array<{ topic: string; partition: number }>): void {
		for (const tp of partitions) {
			this.events.push(`resume:${tp.topic}:${tp.partition}`)
		}
	}

	stop(): void {
		this.stopResolve?.()
		this.stopResolve = null
	}
}

class NoopProducer {
	async send(): Promise<void> {}
	async disconnect(): Promise<void> {}
}

afterEach(() => {
	vi.restoreAllMocks()
})

describe('partitions assigned during initial restoration', () => {
	it('are restored before being resumed', async () => {
		const events: string[] = []
		const consumers: RebalancingConsumer[] = []

		const client = new KafkaClient({ clientId: 'test-app', brokers: ['localhost:9092'] })
		vi.spyOn(client, 'connect').mockResolvedValue()
		vi.spyOn(client, 'disconnect').mockResolvedValue()
		vi.spyOn(client, 'consumer').mockImplementation(() => {
			const consumer = new RebalancingConsumer([{ topic: 'orders', partition: 0 }], events)
			consumers.push(consumer)
			return consumer as unknown as never
		})
		vi.spyOn(client, 'producer').mockImplementation(() => new NoopProducer() as unknown as never)
		// Metadata for changelog validation: source topic exists, changelog is created.
		vi.spyOn(client, 'getMetadata').mockResolvedValue({
			topics: new Map([
				[
					'orders',
					{
						partitions: new Map([
							[0, { leaderId: 0 }],
							[1, { leaderId: 0 }],
						]),
					},
				],
			]),
		} as unknown as Awaited<ReturnType<KafkaClient['getMetadata']>>)
		vi.spyOn(client, 'createTopics').mockResolvedValue(undefined as unknown as never)

		const app = flow({ applicationId: 'test-app', client })
		app.stream('orders', { key: codec.string(), value: codec.json<{ n: number }>() })
			.groupByKey()
			.count({ storeName: 'counts' })

		const restoreCalls: Array<Array<{ topic: string; partition: number }>> = []
		const flowAny = app as unknown as {
			restoreFromChangelogs(partitions: Array<{ topic: string; partition: number }>): Promise<void>
		}
		vi.spyOn(flowAny, 'restoreFromChangelogs').mockImplementation(async partitions => {
			restoreCalls.push(partitions)
			for (const tp of partitions) {
				events.push(`restore:${tp.topic}:${tp.partition}`)
			}
			if (restoreCalls.length === 1) {
				// A rebalance assigns a new partition while the initial restore is still running.
				consumers[0]!.emit('partitionsAssigned', [{ topic: 'orders', partition: 1 }])
				await new Promise(resolve => setTimeout(resolve, 10))
			}
		})

		await app.start()

		// The late partition must have been restored...
		const restoredPartitions = restoreCalls.flat().map(tp => `${tp.topic}:${tp.partition}`)
		expect(restoredPartitions).toContain('orders:0')
		expect(restoredPartitions).toContain('orders:1')

		// ...and restored BEFORE it was resumed.
		const restoreIndex = events.indexOf('restore:orders:1')
		const resumeIndex = events.indexOf('resume:orders:1')
		expect(restoreIndex).toBeGreaterThanOrEqual(0)
		expect(resumeIndex).toBeGreaterThanOrEqual(0)
		expect(restoreIndex).toBeLessThan(resumeIndex)

		await app.close()
	})
})
