/** The Kafka adapters of the runtime's ports and the internal topic setup against a real broker (WP2.1). */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { noopLogger, string, type KafkaClient, type Message, type TopicPartition } from '@kafkats/client'
import type { FlowConfig } from '../../src/api/app.js'
import { resolveConfig } from '../../src/app/config.js'
import { flow } from '../../src/app/flow.js'
import { internals } from '../../src/app/internal.js'
import type { AssignedPartition, GroupConsumerHooks } from '../../src/runtime/io.js'
import { createKafkaIO, systemClock } from '../../src/runtime/kafka-io.js'
import { prepareTopics } from '../../src/topology/internal-topics.js'
import { collectUnhandledRejections, createTopics, newClient, uniqueTopicName, waitUntil } from './test-helpers.js'

const signal = () => AbortSignal.timeout(20_000)
const tp = (topic: string, partition: number): TopicPartition => ({ topic, partition })

describe('Kafka IO', () => {
	const unhandled = collectUnhandledRejections()
	let client: KafkaClient

	beforeAll(async () => {
		client = newClient('kafka-io')
		await client.connect()
	})

	afterAll(async () => {
		await client.disconnect()
		expect(unhandled).toEqual([])
	})

	/** The topology and IO of an app counting `topic` by value: a repartition and a changelog topic. */
	function io(applicationId: string, topic: string, config: Partial<FlowConfig> = {}) {
		const app = flow({ applicationId, client, ...config })
		const stream = app.stream(topic, { key: string(), value: string() })
		stream.groupBy((_key, value) => value ?? '', { key: string(), value: string() }).count()
		const topology = internals(app).build()
		const settings = { ...resolveConfig({ applicationId, client, ...config }), processId: 'p-1' }
		return { topology, io: createKafkaIO({ client, config, settings, topology }) }
	}

	it('creates repartition and changelog topics with the task count, then finds them valid', async () => {
		const source = uniqueTopicName('kafka-io-source')
		const applicationId = uniqueTopicName('kafka-io-topics')
		await createTopics(client, [{ name: source, partitions: 3 }])
		const { topology, io: ports } = io(applicationId, source)
		const deps = { admin: ports.admin, clock: systemClock, logger: noopLogger }
		const internal = [...topology.internalTopics.keys()]
		expect(internal).toHaveLength(2)

		const counts = await prepareTopics(topology, deps, { topicConfigs: { 'retention.ms': '3600000' } }, signal())
		for (const topic of internal) expect(counts.get(topic)).toBe(3)
		await expect(ports.admin.describeTopics(internal, { signal: signal() })).resolves.toEqual(
			new Map(internal.map(topic => [topic, 3]))
		)
		await expect(prepareTopics(topology, deps, { autoCreate: false }, signal())).resolves.toEqual(counts)
	})

	it('runs a group member: assigned with the reset policy, batches, commits with metadata, read back', async () => {
		const topic = uniqueTopicName('kafka-io-group')
		await createTopics(client, [{ name: topic, partitions: 2 }])
		const sending = io(uniqueTopicName('kafka-io-producer'), topic).io.producer()
		for (const partition of [0, 1]) {
			for (const value of ['a', 'b']) {
				await sending.send(topic, { key: null, value: Buffer.from(value), partition })
			}
		}
		await sending.close({ signal: signal() })

		const applicationId = uniqueTopicName('kafka-io-member')
		const { io: ports } = io(applicationId, topic)
		const assignments: AssignedPartition[][] = []
		const received: Message<Buffer, Buffer>[] = []
		const hooks = (): GroupConsumerHooks => ({
			onPartitionsAssigned: async partitions => void assignments.push([...partitions]),
			onPartitionsRevoked: async () => {},
			onPartitionsLost: async () => {},
		})

		const first = ports.consumer()
		const run = first.run([topic], (_tp, messages) => void received.push(...messages), hooks())
		await waitUntil('4 records', () => received.length === 4, 15_000)
		const assigned = [...assignments[0]!].sort((x, y) => x.partition - y.partition)
		expect(assigned.map(({ partition, offset, committed }) => ({ partition, offset, committed }))).toEqual([
			{ partition: 0, offset: 0n, committed: null },
			{ partition: 1, offset: 0n, committed: null },
		])
		expect(first.groupMetadata()).toMatchObject({ groupId: applicationId })
		expect(first.groupMetadata()!.generationId).toBeGreaterThan(0)
		expect(first.nextOffset(tp(topic, 0))).toBe(2n)
		await waitUntil('lag known', () => first.currentLag(tp(topic, 1)) === 0n, 5_000)

		const offsets = [0, 1].map(partition => ({ topic, partition, offset: 2n, metadata: `meta-${partition}` }))
		await first.commit(offsets, { signal: signal() })
		await expect(first.committed([tp(topic, 0), tp(topic, 1)], { signal: signal() })).resolves.toEqual(
			new Map([
				[`${topic}:0`, { offset: 2n, metadata: 'meta-0' }],
				[`${topic}:1`, { offset: 2n, metadata: 'meta-1' }],
			])
		)
		await first.stop({ signal: signal() })
		await run

		const second = ports.consumer()
		const rerun = second.run([topic], () => {}, hooks())
		await waitUntil('second assignment', () => assignments.length === 2, 15_000)
		const committed = [...assignments[1]!].sort((x, y) => x.partition - y.partition)
		expect(committed.map(({ offset, committed }) => [offset, committed?.metadata])).toEqual([
			[2n, 'meta-0'],
			[2n, 'meta-1'],
		])
		await second.stop({ signal: signal() })
		await rerun
	}, 40_000)

	it('commits exactly-once transactions on a handle; restore consumers read committed records only', async () => {
		const topic = uniqueTopicName('kafka-io-txn')
		await createTopics(client, [{ name: topic }])
		const { io: ports } = io(uniqueTopicName('kafka-io-eos'), topic, { processingGuarantee: 'exactly_once' })
		const producer = ports.producer()
		const committed = await producer.beginTransaction({ signal: signal() })
		await committed.send(topic, { key: Buffer.from('k'), value: Buffer.from('kept'), partition: 0 })
		await committed.commit({ signal: signal() })
		const aborted = await producer.beginTransaction({ signal: signal() })
		await aborted.send(topic, { key: Buffer.from('k'), value: Buffer.from('dropped'), partition: 0 })
		await aborted.abort({ signal: signal() })
		await expect(producer.partitionCount(topic, { signal: signal() })).resolves.toBe(1)
		await producer.close({ signal: signal() })

		const end = await ports.admin.listOffsets([tp(topic, 0)], 'latest', 'read_committed', { signal: signal() })
		const restore = ports.restoreConsumers(uniqueTopicName('kafka-io-restore'))
		restore.assign([{ topic, partition: 0, offset: 0n }])
		const values: string[] = []
		const run = restore.run(
			async (_tp, messages) => void values.push(...messages.map(m => m.value.toString())),
			() => {}
		)
		await waitUntil('caught up', () => (restore.position(tp(topic, 0)) ?? 0n) >= end.get(`${topic}:0`)!, 15_000)
		expect(values).toEqual(['kept'])
		await restore.stop({ signal: signal() })
		await run
	}, 30_000)

	it('reports an out-of-range position of a restore consumer after records were deleted', async () => {
		const topic = uniqueTopicName('kafka-io-deleted')
		await createTopics(client, [{ name: topic }])
		const { io: ports } = io(uniqueTopicName('kafka-io-delete'), topic)
		const producer = ports.producer()
		for (let i = 0; i < 10; i++) {
			await producer.send(topic, { key: null, value: Buffer.from(String(i)), partition: 0 })
		}
		await producer.close({ signal: signal() })
		await ports.admin.deleteRecords([{ topic, partition: 0, offset: 5n }], { signal: signal() })
		await expect(
			ports.admin.listOffsets([tp(topic, 0)], 'earliest', 'read_uncommitted', { signal: signal() })
		).resolves.toEqual(new Map([[`${topic}:0`, 5n]]))

		const restore = ports.restoreConsumers(uniqueTopicName('kafka-io-out-of-range'))
		restore.assign([{ topic, partition: 0, offset: 2n }])
		const outOfRange: [TopicPartition, bigint][] = []
		const run = restore.run(
			async () => {},
			(partition, offset) => void outOfRange.push([partition, offset])
		)
		await waitUntil('out of range', () => outOfRange.length > 0, 15_000)
		expect(outOfRange[0]).toEqual([tp(topic, 0), 2n])
		await restore.stop({ signal: signal() })
		await run
	}, 30_000)
})
