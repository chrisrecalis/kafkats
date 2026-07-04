import { describe, expect, it } from 'vitest'
import { codec, TimeWindows, SessionWindows } from '../../src/index.js'
import { TestDriver } from '../../src/testing.js'

/**
 * G1 regression: changelog writes must be pinned to the task (source) partition.
 *
 * Without an explicit partition the producer's default partitioner hashes the serialized
 * changelog KEY. For windowed/session stores the key embeds window boundaries, so different
 * windows of ONE task scatter across ALL changelog partitions — while restoration only reads
 * the task's source-partition numbers, silently losing most window state on restart.
 */
describe('changelog writes are pinned to the task partition', () => {
	it('windowed aggregation changelog writes carry the source partition and record timestamp', async () => {
		const driver = new TestDriver({ applicationId: 'pin-app' })

		driver
			.input('events', { key: codec.string(), value: codec.json<{ n: number }>() })
			.groupByKey()
			.windowedBy(TimeWindows.of(1_000))
			.count({ storeName: 'wcounts' })

		await driver.run(async ({ send, producer }) => {
			await send('events', { n: 1 }, { key: Buffer.from('user'), partition: 2, timestamp: 1_500n })

			const changelog = producer.messagesFor('pin-app-wcounts-changelog')
			expect(changelog).toHaveLength(1)
			// Restoration for a task processing source partition 2 reads changelog partition 2.
			expect(changelog[0]!.partition).toBe(2)
			// Kafka Streams also stamps the record's event time on changelog writes.
			expect(changelog[0]!.timestamp).toBe(1_500n)
		})
	})

	it('session aggregation changelog writes (including merge tombstones) stay on the source partition', async () => {
		const driver = new TestDriver({ applicationId: 'pin-app' })

		driver
			.input('events', { key: codec.string(), value: codec.json<{ n: number }>() })
			.groupByKey()
			.windowedBy(SessionWindows.withInactivityGap(1_000))
			.count({ storeName: 'scounts' })

		await driver.run(async ({ send, producer }) => {
			await send('events', { n: 1 }, { key: Buffer.from('user'), partition: 1, timestamp: 1_000n })
			// Merges with the first session: deletes the old session (tombstone) and puts the merged one.
			await send('events', { n: 1 }, { key: Buffer.from('user'), partition: 1, timestamp: 1_500n })

			const changelog = producer.messagesFor('pin-app-scounts-changelog')
			expect(changelog.length).toBeGreaterThanOrEqual(3)
			for (const message of changelog) {
				expect(message.partition).toBe(1)
			}
		})
	})

	it('plain key-value store changelog writes are pinned as well', async () => {
		const driver = new TestDriver({ applicationId: 'pin-app' })

		driver
			.input('events', { key: codec.string(), value: codec.json<{ n: number }>() })
			.groupByKey()
			.count({ storeName: 'counts' })

		await driver.run(async ({ send, producer }) => {
			await send('events', { n: 1 }, { key: Buffer.from('user'), partition: 3 })

			const changelog = producer.messagesFor('pin-app-counts-changelog')
			expect(changelog).toHaveLength(1)
			expect(changelog[0]!.partition).toBe(3)
		})
	})
})
