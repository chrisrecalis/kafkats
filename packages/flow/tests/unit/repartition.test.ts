import { describe, expect, it } from 'vitest'
import { codec } from '../../src/index.js'
import { TestDriver } from '../../src/testing.js'

type ChangelogSpec = {
	topicName: string
	sourceTopics: Set<string>
	restrictRestorationToSourcePartitions?: boolean
}

type AppInternals = {
	sourcesByTopic: Map<string, unknown[]>
	changelogTopics: Map<string, ChangelogSpec>
	repartitionTopics: Map<string, { topicName: string; sourceTopics: Set<string> }>
}

function internals(app: unknown): AppInternals {
	return app as AppInternals
}

describe('through() repartitioning', () => {
	it('registers the through topic as a real source instead of forwarding in-process', async () => {
		const driver = new TestDriver()
		driver
			.input('input', { key: codec.string(), value: codec.json<{ id: string }>() })
			.through('rekeyed')
			.to('output')

		// The through topic must be consumed back by the group consumer (real repartitioning).
		expect(internals(driver.flow).sourcesByTopic.has('rekeyed')).toBe(true)

		await driver.run(async ({ send, producer, consumer }) => {
			await send('input', { id: 'a' }, { key: codec.string().encode('k1') })

			// Produced to the through topic but NOT forwarded in-process: downstream only runs once
			// the topic is consumed back, so records adopt the through topic's partitioning.
			expect(producer.messagesFor('rekeyed')).toHaveLength(1)
			expect(producer.messagesFor('output')).toHaveLength(0)

			const msg = producer.messagesFor('rekeyed')[0]!
			await consumer.sendMessage('rekeyed', msg.value!, { key: msg.key })

			const output = producer.messagesFor('output')
			expect(output).toHaveLength(1)
			// Round-trip preserves key and value: produced format equals consumed format.
			expect(codec.string().decode(output[0]!.key!)).toBe('k1')
			expect(JSON.parse(output[0]!.value!.toString())).toEqual({ id: 'a' })
		})
	})
})

describe('groupBy() repartitioning', () => {
	it('routes re-keyed records through an auto-created repartition topic', async () => {
		const driver = new TestDriver({ applicationId: 'gb-app' })
		const results: Array<{ key: string; count: number }> = []

		driver
			.input('events', { key: codec.string(), value: codec.json<{ user: string }>() })
			.groupBy((_key, value) => value!.user, { key: codec.string(), name: 'by-user' })
			.count({ storeName: 'user-counts' })
			.toStream()
			.peek((key, count) => {
				results.push({ key: key!, count: count! })
			})

		const app = internals(driver.flow)
		expect(app.sourcesByTopic.has('gb-app-by-user-repartition')).toBe(true)
		expect(app.repartitionTopics.get('gb-app-by-user-repartition')?.sourceTopics).toEqual(new Set(['events']))

		await driver.run(async ({ send, producer, consumer }) => {
			await send('events', { user: 'alice' }, { key: codec.string().encode('k1') })
			await send('events', { user: 'alice' }, { key: codec.string().encode('k2') })

			const repartitioned = producer.messagesFor('gb-app-by-user-repartition')
			expect(repartitioned).toHaveLength(2)
			// The new key is serialized with the Grouped key codec.
			expect(codec.string().decode(repartitioned[0]!.key!)).toBe('alice')
			// No in-process aggregation: counting happens only after the repartition round-trip.
			expect(results).toHaveLength(0)

			for (const msg of repartitioned) {
				await consumer.sendMessage(msg.topic, msg.value!, { key: msg.key })
			}

			expect(results).toEqual([
				{ key: 'alice', count: 1 },
				{ key: 'alice', count: 2 },
			])
		})
	})

	it('restricts downstream changelog restoration to the repartition topic partitions', () => {
		const driver = new TestDriver({ applicationId: 'gb2-app' })
		driver
			.input('events', { key: codec.string(), value: codec.json<{ user: string }>() })
			.groupBy((_key, value) => value!.user, { key: codec.string(), name: 'by-user' })
			.count({ storeName: 'user-counts' })

		const spec = internals(driver.flow).changelogTopics.get('user-counts')
		expect(spec).toBeDefined()
		// The repartition topic restores key/partition affinity, so restoration is restricted again.
		expect([...spec!.sourceTopics]).toEqual(['gb2-app-by-user-repartition'])
		expect(spec!.restrictRestorationToSourcePartitions).toBe(true)
	})

	it('requires a key codec to serialize the new key', () => {
		const driver = new TestDriver()
		const stream = driver.input('events', { key: codec.string(), value: codec.json<{ user: string }>() })
		expect(() => stream.groupBy((_key, value) => value!.user)).toThrow(/key codec/)
	})
})
