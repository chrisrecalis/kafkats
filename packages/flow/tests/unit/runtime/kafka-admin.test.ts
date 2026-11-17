import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from 'vitest'
import { ErrorCode, KafkaProtocolError, TimeoutError, type Admin } from '@kafkats/client'
import { IllegalStateError } from '@/api/errors.js'
import { KafkaAdmin } from '@/runtime/kafka-admin.js'
import type { InternalTopicSpec } from '@/topology/nodes.js'
import { deferred } from '../support/fake-io.js'

const signal = () => new AbortController().signal

function partitions(count: number) {
	return Array.from({ length: count }, (_, partitionIndex) => ({ partitionIndex }))
}

function stubAdmin() {
	const stub = {
		describeTopics: vi.fn((topics: string[]) =>
			Promise.resolve(
				topics
					.filter(name => name !== 'missing')
					.map(name => ({ name, partitions: partitions(name === 'empty' ? 0 : 3) }))
			)
		),
		createTopics: vi.fn((topics: { name: string }[]) =>
			Promise.resolve(
				topics.map(({ name }) => ({ name, errorCode: ErrorCode.None, errorMessage: null as string | null }))
			)
		),
		fetchTopicOffsets: vi.fn((_topic: string, partitionList: number[], which: string) =>
			Promise.resolve(new Map(partitionList.map(p => [p, BigInt(p) + (which === 'earliest' ? 0n : 100n)])))
		),
		deleteRecords: vi.fn((records: { topic: string; partition: number }[]) =>
			Promise.resolve(records.map(({ topic, partition }) => ({ topic, partition, lowWatermark: 5n })))
		),
	}
	return { stub, port: new KafkaAdmin(stub as unknown as Admin) }
}

function spec(name: string, overrides: Partial<InternalTopicSpec> = {}): InternalTopicSpec {
	return { name, kind: 'changelog', configs: { 'cleanup.policy': 'compact' }, validateOnly: false, ...overrides }
}

let unhandled: Mock<(reason: unknown) => void>
beforeEach(() => {
	unhandled = vi.fn()
	process.on('unhandledRejection', unhandled)
})
afterEach(async () => {
	await new Promise(resolve => setImmediate(resolve))
	process.off('unhandledRejection', unhandled)
	expect(unhandled).not.toHaveBeenCalled()
})

describe('KafkaAdmin', () => {
	it('describes the partition counts of the topics that exist', async () => {
		const { stub, port } = stubAdmin()
		await expect(port.describeTopics(['a', 'missing', 'empty'], { signal: signal() })).resolves.toEqual(
			new Map([['a', 3]])
		)
		await expect(port.describeTopics([], { signal: signal() })).resolves.toEqual(new Map())
		expect(stub.describeTopics).toHaveBeenCalledTimes(1)
	})

	it('stops waiting for a call when its signal aborts, and its later failure is not unhandled', async () => {
		const { stub, port } = stubAdmin()
		const answer = deferred<never>()
		stub.describeTopics.mockReturnValue(answer.promise)
		const controller = new AbortController()
		const described = port.describeTopics(['a'], { signal: controller.signal })
		controller.abort()
		await expect(described).rejects.toMatchObject({ name: 'AbortError' })
		answer.reject(new Error('late'))
	})

	it('creates topics with their partition count, replication factor and configs', async () => {
		const { stub, port } = stubAdmin()
		await port.createTopics(
			[spec('c', { replicationFactor: 3 }), spec('r', { kind: 'repartition', partitions: 6, configs: {} })],
			new Map([
				['c', 4],
				['r', 2],
			]),
			{ signal: signal() }
		)
		expect(stub.createTopics).toHaveBeenCalledWith([
			{ name: 'c', numPartitions: 4, replicationFactor: 3, configs: { 'cleanup.policy': 'compact' } },
			// The explicit count of a repartition topic; the broker's default replication factor.
			{ name: 'r', numPartitions: 6, replicationFactor: -1, configs: {} },
		])
	})

	it('takes a topic that exists already as created and rejects with the code of any other failure', async () => {
		const { stub, port } = stubAdmin()
		const counts = new Map([['c', 1]])
		stub.createTopics.mockResolvedValueOnce([
			{ name: 'c', errorCode: ErrorCode.TopicAlreadyExists, errorMessage: 'exists' },
		])
		await expect(port.createTopics([spec('c')], counts, { signal: signal() })).resolves.toBeUndefined()

		stub.createTopics.mockResolvedValueOnce([
			{ name: 'c', errorCode: ErrorCode.TopicAuthorizationFailed, errorMessage: 'denied' },
		])
		const failure = port.createTopics([spec('c')], counts, { signal: signal() })
		await expect(failure).rejects.toBeInstanceOf(KafkaProtocolError)
		await expect(failure).rejects.toMatchObject({ errorCode: ErrorCode.TopicAuthorizationFailed })
	})

	it('never creates a topic without its partition count', async () => {
		const { stub, port } = stubAdmin()
		await expect(port.createTopics([spec('c')], new Map(), { signal: signal() })).rejects.toThrow(IllegalStateError)
		await port.createTopics([], new Map(), { signal: signal() })
		expect(stub.createTopics).not.toHaveBeenCalled()
	})

	it('lists offsets per topic at the isolation level asked for, by topic:partition', async () => {
		const { stub, port } = stubAdmin()
		const listed = await port.listOffsets(
			[
				{ topic: 'a', partition: 0 },
				{ topic: 'b', partition: 2 },
				{ topic: 'a', partition: 1 },
			],
			'latest',
			'read_committed',
			{ signal: signal() }
		)
		expect(listed).toEqual(
			new Map([
				['a:0', 100n],
				['a:1', 101n],
				['b:2', 102n],
			])
		)
		expect(stub.fetchTopicOffsets.mock.calls).toEqual([
			['a', [0, 1], 'latest', { isolationLevel: 'read_committed' }],
			['b', [2], 'latest', { isolationLevel: 'read_committed' }],
		])
	})

	it('deletes records with the signal of the call and rejects with the first failure of a partition', async () => {
		const { stub, port } = stubAdmin()
		const options = { signal: signal() }
		const records = [
			{ topic: 'r', partition: 0, offset: 5n },
			{ topic: 'r', partition: 1, offset: 7n },
		]
		await port.deleteRecords(records, options)
		expect(stub.deleteRecords).toHaveBeenCalledWith(records, options)

		const failure = new TimeoutError('DeleteRecords for r-1 did not complete', 10)
		stub.deleteRecords.mockResolvedValueOnce([
			{ topic: 'r', partition: 0, lowWatermark: 5n },
			{ topic: 'r', partition: 1, error: failure },
		] as never)
		await expect(port.deleteRecords(records, options)).rejects.toBe(failure)
	})
})
