import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient } from '@kafkats/client'
import { ChangelogRestorer, InMemoryKeyValueStore, codec, flow } from '../../src/index.js'
import { clientConfig, createTopics, newClient, uniqueTopicName } from './test-helpers.js'

const stringCodec = codec.string()

describe('changelog restore with a dangling transaction', () => {
	let client: KafkaClient

	beforeAll(async () => {
		client = newClient('flow-restore-open-tx')
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

	// Kafka Streams initializes the transactional producer before a task restores (RecordCollector.initialize): its
	// InitProducerId fences the previous incarnation with the same transactional id and aborts the transaction that one
	// left open, whose records otherwise hold every restore of the changelogs it wrote until the broker times it out.
	it('exactly-once: the producer fences the transaction its previous incarnation left open before it restores', async () => {
		const input = uniqueTopicName('flow-it-fence-in')
		const applicationId = uniqueTopicName('flow-it-fence')
		const changelog = `${applicationId}-counts-changelog`
		const transactionalId = `${applicationId}-tx`
		await createTopics(client, [{ name: input }, { name: changelog }])

		// The previous incarnation crashed inside a transaction that wrote the changelog: open for another minute.
		const crashed = client.producer({ transactionalId, transactionTimeoutMs: 60_000, lingerMs: 0 })
		const open = await crashed.beginTransaction()
		await open.send(changelog, { key: 'k', value: stringCodec.encode('uncommitted'), partition: 0 })

		const app = flow({
			applicationId,
			client: clientConfig('flow-it-fence'),
			processingGuarantee: 'exactly_once',
			producer: { transactionalId },
			consumer: { maxWaitMs: 100 },
			changelog: { restoration: { idleTimeoutMs: 1_000, checkIntervalMs: 200, consumerMaxWaitMs: 100 } },
		})
		app.stream(input, { key: stringCodec, value: stringCodec }).groupByKey().count({ storeName: 'counts' })
		const startedAt = Date.now()
		try {
			await app.start()
			// Without the fence the restore waits for the broker to abort the transaction, a minute from now.
			expect(Date.now() - startedAt).toBeLessThan(20_000)
		} finally {
			await app.close()
			await crashed.disconnect().catch(() => {})
		}
	}, 90_000)

	it('exactly-once: a transaction left open after it sent the group offsets is fenced while the consumer joins', async () => {
		const input = uniqueTopicName('flow-it-fence-offsets-in')
		const output = uniqueTopicName('flow-it-fence-offsets-out')
		const applicationId = uniqueTopicName('flow-it-fence-offsets')
		const transactionalId = `${applicationId}-tx`
		await createTopics(client, [{ name: input }, { name: output }])

		// The previous incarnation's commit sent its offsets (TxnOffsetCommit), then lost its transaction coordinator
		// before EndTxn: the group's offsets stay pending (UNSTABLE_OFFSET_COMMIT) for 75 s, past the client's 60 s fetch.
		const crashed = client.producer({ transactionalId, transactionTimeoutMs: 75_000, lingerMs: 0 })
		let offsetsSent!: () => void
		const sent = new Promise<void>(resolve => (offsetsSent = resolve))
		Object.assign(crashed, {
			endTransaction: () => {
				offsetsSent()
				return new Promise(() => {})
			},
		})
		const open = await crashed.beginTransaction()
		await open.send(output, { key: 'k', value: stringCodec.encode('uncommitted'), partition: 0 })
		await open.sendOffsets({ groupId: applicationId, offsets: [{ topic: input, partition: 0, offset: 0n }] })
		void open.commit().catch(() => {})
		await sent

		const app = flow({
			applicationId,
			client: clientConfig('flow-it-fence-offsets'),
			processingGuarantee: 'exactly_once',
			producer: { transactionalId },
			consumer: { maxWaitMs: 100 },
		})
		app.stream(input, { key: stringCodec, value: stringCodec }).to(output, { key: stringCodec, value: stringCodec })
		const startedAt = Date.now()
		try {
			await app.start()
			// Without the fence the assignment's offset fetch waits for the broker and fails after 60 s.
			expect(Date.now() - startedAt).toBeLessThan(20_000)
		} finally {
			await app.close()
			await crashed.disconnect({ timeoutMs: 1000 }).catch(() => {})
		}
	}, 120_000)
})
