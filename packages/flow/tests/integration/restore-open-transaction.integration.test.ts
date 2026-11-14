import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { KafkaClient } from '@kafkats/client'
import { ChangelogRestorer, InMemoryKeyValueStore, codec } from '../../src/index.js'
import { createTopics, requireKafkaBrokers, uniqueTopicName } from './test-helpers.js'

const stringCodec = codec.string()

describe('changelog restore with a dangling transaction', () => {
	let client: KafkaClient

	beforeAll(async () => {
		client = new KafkaClient({ clientId: `flow-restore-open-tx-${Date.now()}`, brokers: requireKafkaBrokers() })
		await client.connect()
	})

	afterAll(async () => {
		await client.disconnect()
	})

	// A crashed instance leaves its transaction open until the broker times it out. Records
	// committed after it by a successor instance sit beyond the last stable offset. Restoring
	// only up to the last stable offset would silently yield state that is behind the committed
	// input offsets; the restore must wait for the dangling transaction and deliver everything.
	it('restores records committed after a still-open transaction of another producer', async () => {
		const topic = uniqueTopicName('flow-it-restore-open-tx')
		await createTopics(client, [{ name: topic, partitions: 1 }])

		const plain = client.producer({ lingerMs: 0 })
		await plain.send(topic, { key: 'a', value: stringCodec.encode('1'), partition: 0 })

		// "Crashed" instance: transaction left open (aborted by the broker after 4s).
		const crashed = client.producer({
			transactionalId: uniqueTopicName('flow-it-restore-open-tx-crashed'),
			transactionTimeoutMs: 4_000,
			lingerMs: 0,
		})
		let release!: () => void
		const hold = new Promise<void>(resolve => {
			release = resolve
		})
		let sent!: () => void
		const sentPromise = new Promise<void>(resolve => {
			sent = resolve
		})
		const dangling = crashed
			.transaction(async tx => {
				await tx.send(topic, { key: 'a', value: stringCodec.encode('uncommitted'), partition: 0 })
				sent()
				await hold
			})
			.catch(() => {})
		await sentPromise

		// Successor instance: commits newer state beyond the open transaction.
		const successor = client.producer({
			transactionalId: uniqueTopicName('flow-it-restore-open-tx-successor'),
			lingerMs: 0,
		})
		await successor.transaction(async tx => {
			await tx.send(topic, { key: 'a', value: stringCodec.encode('2'), partition: 0 })
			await tx.send(topic, { key: 'b', value: stringCodec.encode('1'), partition: 0 })
		})

		const store = new InMemoryKeyValueStore<string, string>('s', { keyCodec: stringCodec, valueCodec: stringCodec })
		await store.init()
		const restorer = new ChangelogRestorer(topic, stringCodec, stringCodec, store)
		try {
			await restorer.restore(client, { idleTimeoutMs: 1_000, checkIntervalMs: 200, consumerMaxWaitMs: 100 }, [0])

			expect(await store.get('a')).toBe('2')
			expect(await store.get('b')).toBe('1')
		} finally {
			release()
			await dangling
			await plain.disconnect()
			await successor.disconnect()
			await crashed.disconnect().catch(() => {})
		}
	}, 60_000)
})
