import { describe, expect, it } from 'vitest'

import type { Cluster } from '@/client/cluster.js'
import type { KafkaClient } from '@/client/index.js'
import { KafkaError, ProducerFencedError, TransactionAbortedError } from '@/client/errors.js'
import { codec } from '@/codec.js'
import { ConnectionClosedError } from '@/network/errors.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { Producer } from '@/producer/producer.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { sleep, uniqueName } from '../helpers/testkit.js'

/** The values of `topicName` a read_committed consumer sees: waits for `expected` of them, then a little longer. */
async function readCommitted(client: KafkaClient, topicName: string, expected: number): Promise<string[]> {
	const consumer = client.consumer({
		groupId: uniqueName('it-rc'),
		autoOffsetReset: 'earliest',
		isolationLevel: 'read_committed',
	})
	const values: string[] = []
	const controller = new AbortController()
	const run = consumer.runEach(
		topic<string>(topicName, { value: codec.string() }),
		async message => {
			values.push(message.value)
		},
		{ autoCommit: false, signal: controller.signal }
	)
	const deadline = Date.now() + 15_000
	while (values.length < expected && Date.now() < deadline) await sleep(50)
	await sleep(750)
	controller.abort()
	await run
	return values
}

function transactionalProducer(client: KafkaClient, transactionalId: string): Producer {
	return client.producer({ transactionalId, lingerMs: 0, retries: 5, retryBackoffMs: 100, maxRetryBackoffMs: 500 })
}

/** Make the transaction coordinator lose the answer of the next `count` EndTxn requests after applying them. */
async function loseEndTxnAnswers(producer: Producer, transactionalId: string, count: number): Promise<void> {
	const cluster = (producer as unknown as { cluster: Cluster }).cluster
	const coordinator = await cluster.getCoordinator('TRANSACTION', transactionalId)
	const endTxn = coordinator.endTxn.bind(coordinator)
	let lost = 0
	coordinator.endTxn = async request => {
		const response = await endTxn(request)
		if (lost++ < count) throw new ConnectionClosedError('answer lost')
		return response
	}
}

/** The offset of `topicName` partition 0 that `groupId` committed, once no transaction holds it (-1 without one). */
async function committedOffset(producer: Producer, groupId: string, topicName: string): Promise<bigint> {
	const cluster = (producer as unknown as { cluster: Cluster }).cluster
	const coordinator = await cluster.getCoordinator('GROUP', groupId)
	for (;;) {
		const response = await coordinator.offsetFetch({
			groupId,
			topics: [{ name: topicName, partitions: [{ partitionIndex: 0 }] }],
			requireStable: true,
		})
		const partition = response.topics[0]!.partitions[0]!
		if (partition.errorCode !== ErrorCode.UnstableOffsetCommit) return partition.committedOffset
		await sleep(50)
	}
}

describe.concurrent('EOS (integration) - transaction handles', () => {
	it('shows committed handles to read_committed consumers and hides aborted ones', async () => {
		const client = createClient('it-tx-handle')
		await client.connect()
		const topicName = uniqueName('it-tx-handle')
		const testTopic = topic<string>(topicName, { value: codec.string() })
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])
		const producer = transactionalProducer(client, uniqueName('tx'))

		const committed = await producer.beginTransaction()
		await committed.send(testTopic, { value: 'committed' })
		await committed.commit()

		const aborted = await producer.beginTransaction()
		await aborted.send(testTopic, { value: 'aborted' })
		await aborted.abort()

		const controller = new AbortController()
		const cancelled = await producer.beginTransaction({ signal: controller.signal })
		await cancelled.send(testTopic, { value: 'cancelled' })
		controller.abort()

		const last = await producer.beginTransaction()
		await last.send(testTopic, { value: 'last' })
		await last.commit({ timeoutMs: 10_000 })

		expect(await readCommitted(client, topicName, 2)).toEqual(['committed', 'last'])
		expect(producer.transactionState).toBe('idle')

		await producer.disconnect()
		await client.disconnect()
	})

	it("fences the first handle's commit when a second producer with the same id begins", async () => {
		const client = createClient('it-tx-handle-fenced')
		await client.connect()
		const topicName = uniqueName('it-tx-handle-fenced')
		const testTopic = topic<string>(topicName, { value: codec.string() })
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])
		const transactionalId = uniqueName('tx')

		const zombie = transactionalProducer(client, transactionalId)
		const zombieTx = await zombie.beginTransaction()
		await zombieTx.send(testTopic, { value: 'zombie' })

		const successor = transactionalProducer(client, transactionalId)
		const successorTx = await successor.beginTransaction()

		// EndTxn(commit) is answered PRODUCER_FENCED: the zombie can do nothing more (Java parity).
		const error = await zombieTx.commit().catch((e: unknown) => e)
		expect(error).toBeInstanceOf(ProducerFencedError)
		expect(zombie.transactionState).toBe('error')

		await successorTx.send(testTopic, { value: 'successor' })
		await successorTx.commit()

		expect(await readCommitted(client, topicName, 1)).toEqual(['successor'])

		await zombie.disconnect()
		await successor.disconnect()
		await client.disconnect()
	})

	it('a producer fenced by its successor never aborts the transaction the successor has open', async () => {
		const client = createClient('it-tx-handle-zombie')
		await client.connect()
		const topicName = uniqueName('it-tx-handle-zombie')
		const testTopic = topic<string>(topicName, { value: codec.string() })
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])
		const transactionalId = uniqueName('tx')

		const zombie = transactionalProducer(client, transactionalId)
		const zombieTx = await zombie.beginTransaction()
		await zombieTx.send(testTopic, { value: 'zombie' })

		// Initializing, the successor aborts the zombie's transaction under a newer epoch, then opens its own.
		const successor = transactionalProducer(client, transactionalId)
		const successorTx = await successor.beginTransaction()
		await successorTx.send(testTopic, { value: 'successor' })

		// The partition saw the newer epoch (KIP-588: INVALID_PRODUCER_EPOCH). An InitProducerId of the zombie now, with
		// its producer id, would make the coordinator abort the successor's transaction.
		const refused = await zombieTx.send(testTopic, { value: 'zombie-again' }).catch((e: unknown) => e)
		expect(refused).toBeInstanceOf(KafkaError)
		expect([ErrorCode.InvalidProducerEpoch, ErrorCode.ProducerFenced]).toContain((refused as KafkaError).errorCode)
		// Its abort's EndTxn tells the fence: fatal, no epoch bump.
		const fenced = await zombieTx.abort().catch((e: unknown) => e)
		expect(fenced).toBeInstanceOf(ProducerFencedError)
		expect(zombie.transactionState).toBe('error')
		await expect(zombie.beginTransaction()).rejects.toBeInstanceOf(ProducerFencedError)

		await successorTx.commit()
		expect(await readCommitted(client, topicName, 1)).toEqual(['successor'])

		await zombie.disconnect()
		await successor.disconnect()
		await client.disconnect()
	})

	it('resends an EndTxn(commit) whose answer was lost and commits once', async () => {
		const client = createClient('it-tx-lost-answer')
		await client.connect()
		const topicName = uniqueName('it-tx-lost-answer')
		const testTopic = topic<string>(topicName, { value: codec.string() })
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])
		const transactionalId = uniqueName('tx')
		const producer = transactionalProducer(client, transactionalId)

		const tx = await producer.beginTransaction()
		await tx.send(testTopic, { value: 'once' })
		await loseEndTxnAnswers(producer, transactionalId, 1)
		await tx.commit()

		expect(producer.transactionState).toBe('idle')
		expect(await readCommitted(client, topicName, 1)).toEqual(['once'])

		await producer.disconnect()
		await client.disconnect()
	})

	it('reports an unknown outcome when no EndTxn(commit) answer arrives, although Kafka committed', async () => {
		const client = createClient('it-tx-unknown-outcome')
		await client.connect()
		const topicName = uniqueName('it-tx-unknown-outcome')
		const testTopic = topic<string>(topicName, { value: codec.string() })
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])
		const transactionalId = uniqueName('tx')
		const producer = transactionalProducer(client, transactionalId)

		const tx = await producer.beginTransaction()
		await tx.send(testTopic, { value: 'maybe' })
		await loseEndTxnAnswers(producer, transactionalId, Number.POSITIVE_INFINITY)
		const error = await tx.commit().catch((e: unknown) => e)

		expect(error).toBeInstanceOf(TransactionAbortedError)
		expect(error).toMatchObject({ outcome: 'unknown' })
		expect(producer.transactionState).toBe('error')
		// Why the outcome must not be reported as 'aborted': the broker did commit.
		expect(await readCommitted(client, topicName, 1)).toEqual(['maybe'])

		await producer.disconnect()
		await client.disconnect()
	})

	it("an aborted transaction's offsets answered after its abort began are not committed by the next one", async () => {
		const client = createClient('it-tx-offsets-late')
		await client.connect()
		const topicName = uniqueName('it-tx-offsets-late')
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])
		const transactionalId = uniqueName('tx')
		const groupId = uniqueName('it-tx-offsets-group')
		const producer = transactionalProducer(client, transactionalId)
		const offsets = (offset: bigint) => ({ groupId, offsets: [{ topic: topicName, partition: 0, offset }] })

		const aborted = await producer.beginTransaction()
		// Abort once the AddOffsetsToTxn of the first transaction went out, its answer still on the way.
		const cluster = (producer as unknown as { cluster: Cluster }).cluster
		const coordinator = await cluster.getCoordinator('TRANSACTION', transactionalId)
		const addOffsetsToTxn = coordinator.addOffsetsToTxn.bind(coordinator)
		let sent!: () => void
		const requested = new Promise<void>(resolve => (sent = resolve))
		coordinator.addOffsetsToTxn = request => {
			coordinator.addOffsetsToTxn = addOffsetsToTxn
			const answer = addOffsetsToTxn(request)
			sent()
			return answer
		}
		const registering = aborted.sendOffsets(offsets(999n)).catch((e: unknown) => e)
		await requested
		await aborted.abort()

		const next = await producer.beginTransaction()
		await next.sendOffsets(offsets(5n))
		await next.commit()

		expect(await committedOffset(producer, groupId, topicName)).toBe(5n)
		expect(await registering).toBeInstanceOf(TransactionAbortedError)

		await producer.disconnect()
		await client.disconnect()
	})

	it('aborts a transaction left open on disconnect', async () => {
		const client = createClient('it-tx-open-disconnect')
		await client.connect()
		const topicName = uniqueName('it-tx-open-disconnect')
		const testTopic = topic<string>(topicName, { value: codec.string() })
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])
		const transactionalId = uniqueName('tx')

		const producer = transactionalProducer(client, transactionalId)
		const tx = await producer.beginTransaction()
		await tx.send(testTopic, { value: 'left-open' })
		await producer.disconnect({ timeoutMs: 5_000 })
		expect(producer.transactionState).toBe('idle')

		// Aborted rather than left to the broker's transaction timeout: the next producer is not held up by it.
		const next = transactionalProducer(client, transactionalId)
		await next.transaction(async nextTx => {
			await nextTx.send(testTopic, { value: 'next' })
		})

		expect(await readCommitted(client, topicName, 1)).toEqual(['next'])

		await next.disconnect()
		await client.disconnect()
	})
})
