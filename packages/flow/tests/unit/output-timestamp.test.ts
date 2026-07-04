import { describe, expect, it } from 'vitest'
import { codec } from '../../src/index.js'
import { TestDriver } from '../../src/testing.js'

/**
 * G6 regression: sendToTopic never passed record.timestamp, so every .to()/.through()/repartition
 * write got broker wall-clock produce time — breaking downstream event-time windowing.
 */
describe('output records keep their event timestamp', () => {
	it('.to() propagates the input record timestamp', async () => {
		const driver = new TestDriver()
		driver
			.input('in', { value: codec.json<{ id: string }>() })
			.mapValues(value => ({ ...value, mapped: true }))
			.to('out', { value: codec.json() })

		await driver.run(async ({ send, producer }) => {
			await send('in', { id: 'a' }, { timestamp: 123_456n })

			const messages = producer.messagesFor('out')
			expect(messages).toHaveLength(1)
			expect(messages[0]!.timestamp).toBe(123_456n)
		})
	})

	it('.through() and repartition topics propagate the input record timestamp', async () => {
		const driver = new TestDriver({ applicationId: 'ts-app' })
		driver
			.input('in', { key: codec.string(), value: codec.json<{ user: string }>() })
			.through('audit')
			.groupBy((_key, value) => value!.user, { key: codec.string(), name: 'by-user' })
			.count({ storeName: 'counts', changelog: false })

		await driver.run(async ({ send, producer, consumer }) => {
			await send('in', { user: 'alice' }, { key: Buffer.from('k'), timestamp: 42_000n })

			const audit = producer.messagesFor('audit')
			expect(audit).toHaveLength(1)
			expect(audit[0]!.timestamp).toBe(42_000n)

			// Deliver the through-topic record back (as Kafka would, preserving its timestamp).
			await consumer.sendMessage('audit', audit[0]!.value!, {
				key: audit[0]!.key,
				timestamp: audit[0]!.timestamp,
			})

			const repartitioned = producer.messagesFor('ts-app-by-user-repartition')
			expect(repartitioned).toHaveLength(1)
			expect(repartitioned[0]!.timestamp).toBe(42_000n)
		})
	})
})
