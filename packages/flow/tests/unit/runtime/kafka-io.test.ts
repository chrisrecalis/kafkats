import { afterEach, describe, expect, it, vi } from 'vitest'
import type { ConsumerConfig, KafkaClient, ProducerConfig } from '@kafkats/client'
import type { FlowConfig } from '@/api/app.js'
import { resolveConfig } from '@/app/config.js'
import type { ResolvedFlowConfig } from '@/runtime/config.js'
import {
	createKafkaIO,
	groupConsumerConfig,
	manualConsumerConfig,
	producerConfig,
	systemClock,
	type KafkaIOOptions,
} from '@/runtime/kafka-io.js'
import { builder, chain, repartition, repartitionTopic } from '../topology/support.js'

type Config = Omit<FlowConfig, 'client' | 'applicationId'>

function settings(config: Config = {}): ResolvedFlowConfig {
	return {
		...resolveConfig({ applicationId: 'app', client: { clientId: 'app', brokers: [] }, ...config }),
		processId: 'p-1',
	}
}

/** `events` reads 'latest', `orders` the app default, and a repartition topic fed by `orders`. */
function topology() {
	const b = builder()
	b.addSource({ name: 'events', topics: ['events'], internal: false, offsetReset: 'latest' })
	chain(b, 'orders', ['orders'], 'p')
	repartition(b, 'p', 'by-key')
	return b.build()
}

function options(config: Config = {}, client = stubClient()): KafkaIOOptions {
	return { client, config, settings: settings(config), topology: topology() }
}

/** A client whose consumers, producers and admins are the configs they were created with. */
function stubClient() {
	return {
		consumer: vi.fn((config: ConsumerConfig) => ({ config })),
		producer: vi.fn((config?: ProducerConfig) => ({ config })),
		admin: vi.fn(() => ({})),
	} as unknown as Pick<KafkaClient, 'consumer' | 'producer' | 'admin'> & {
		consumer: ReturnType<typeof vi.fn>
		producer: ReturnType<typeof vi.fn>
		admin: ReturnType<typeof vi.fn>
	}
}

describe('groupConsumerConfig', () => {
	it('joins the application group by range, waits at most 500 ms per fetch and keeps the user settings', () => {
		const config = groupConsumerConfig(
			options({
				consumer: { sessionTimeoutMs: 10_000, groupInstanceId: 'instance-1', rebalanceTimeoutMs: 20_000 },
			})
		)
		expect(config).toMatchObject({
			groupId: 'app',
			partitionAssignmentStrategy: 'range',
			maxWaitMs: 500,
			sessionTimeoutMs: 10_000,
			groupInstanceId: 'instance-1',
			rebalanceTimeoutMs: 20_000,
		})
		expect(config).not.toHaveProperty('onPartitionsRevoked')
	})

	it("resets each partition by its source's policy, repartition topics from the earliest offset", () => {
		const reset = groupConsumerConfig(options({ consumer: { autoOffsetReset: 'none' } })).autoOffsetReset
		expect(reset).toBeTypeOf('function')
		const policy = reset as (topic: string, partition: number) => string
		expect(policy('events', 0)).toBe('latest')
		expect(policy('orders', 3)).toBe('none')
		expect(policy(repartitionTopic('by-key'), 0)).toBe('earliest')
		expect((groupConsumerConfig(options()).autoOffsetReset as typeof policy)('orders', 0)).toBe('earliest')
	})

	it('reads committed records only under exactly_once, and as configured under at_least_once', () => {
		const consumer = { isolationLevel: 'read_uncommitted', maxWaitMs: 100 } as const
		expect(groupConsumerConfig(options({ consumer }))).toMatchObject({
			isolationLevel: 'read_uncommitted',
			maxWaitMs: 100,
		})
		expect(groupConsumerConfig(options({ consumer, processingGuarantee: 'exactly_once' }))).toMatchObject({
			isolationLevel: 'read_committed',
		})
	})
})

describe('producerConfig', () => {
	it('is the user config under at_least_once', () => {
		const producer = { lingerMs: 3, retries: 2 }
		expect(producerConfig(options({ producer }))).toEqual(producer)
	})

	it('is transactional under exactly_once with the id <applicationId>-<processId>', () => {
		expect(producerConfig(options({ processingGuarantee: 'exactly_once', producer: { lingerMs: 3 } }))).toEqual({
			lingerMs: 3,
			transactionalId: 'app-p-1',
			transactionConcurrency: 1,
			idempotent: true,
			retries: 10,
			transactionTimeoutMs: 10_000,
		})
	})

	it("keeps the user's transactional id, retries and transaction timeout", () => {
		const producer = {
			transactionalId: 'mine',
			retries: 3,
			transactionTimeoutMs: 30_000,
			transactionConcurrency: 4,
		}
		expect(producerConfig(options({ processingGuarantee: 'exactly_once', producer }))).toMatchObject({
			transactionalId: 'mine',
			retries: 3,
			transactionTimeoutMs: 30_000,
			transactionConcurrency: 1,
		})
	})
})

describe('manualConsumerConfig', () => {
	it('reads committed records from explicit offsets, never resets, and takes the fetch settings', () => {
		const consumer = { maxBytesPerPartition: 1024, minBytes: 2, checkCrcs: false, sessionTimeoutMs: 9000 }
		expect(manualConsumerConfig(options({ consumer }), 'app-restore-0_1-3')).toEqual({
			groupId: 'app-restore-0_1-3',
			maxBytesPerPartition: 1024,
			minBytes: 2,
			checkCrcs: false,
			maxWaitMs: 500,
			isolationLevel: 'read_committed',
			autoOffsetReset: 'none',
		})
		expect(manualConsumerConfig(options({ consumer: { maxWaitMs: 50 } }), 'g').maxWaitMs).toBe(50)
		expect(manualConsumerConfig(options({ consumer: { maxWaitMs: 50 } }), 'g', 20).maxWaitMs).toBe(20)
	})
})

describe('createKafkaIO', () => {
	it('creates a group consumer and a producer per call, the admin once', () => {
		const client = stubClient()
		const io = createKafkaIO(options({ processingGuarantee: 'exactly_once' }, client))
		expect(client.admin).toHaveBeenCalledTimes(1)
		io.consumer()
		io.consumer()
		io.producer()
		io.producer()
		expect(client.consumer).toHaveBeenCalledTimes(2)
		expect(client.consumer.mock.calls[0]![0]).toMatchObject({
			groupId: 'app',
			partitionAssignmentStrategy: 'range',
		})
		expect(client.producer).toHaveBeenCalledTimes(2)
		for (const [config] of client.producer.mock.calls) expect(config).toMatchObject({ transactionalId: 'app-p-1' })
		expect(io.clock).toBe(systemClock)
	})

	it('restore consumers wait restoration.consumerMaxWaitMs per fetch, global-table consumers do not', () => {
		const client = stubClient()
		const io = createKafkaIO(options({ changelog: { restoration: { consumerMaxWaitMs: 25 } } }, client))
		io.restoreConsumers('app-restore-0_0-1')
		io.globalConsumers('app-global-lookup-p-1')
		expect(client.consumer.mock.calls.map(([config]) => [config.groupId, config.maxWaitMs])).toEqual([
			['app-restore-0_0-1', 25],
			['app-global-lookup-p-1', 500],
		])
	})
})

describe('systemClock', () => {
	afterEach(() => {
		vi.useRealTimers()
	})

	it('calls a timer at its time, and not once cancelled', () => {
		vi.useFakeTimers({ now: 1000 })
		const fired = vi.fn()
		const cancelled = vi.fn()
		systemClock.setTimer(1500, fired)
		systemClock.setTimer(1500, cancelled)()
		vi.advanceTimersByTime(499)
		expect(fired).not.toHaveBeenCalled()
		vi.advanceTimersByTime(1)
		expect(fired).toHaveBeenCalledTimes(1)
		expect(cancelled).not.toHaveBeenCalled()
		expect(systemClock.now()).toBe(1500)
	})

	it("calls a timer past setTimeout's range at its time, and a past one at once", () => {
		vi.useFakeTimers({ now: 0 })
		const far = vi.fn()
		const past = vi.fn()
		const at = 2 ** 31 + 10_000
		systemClock.setTimer(at, far)
		systemClock.setTimer(-5, past)
		vi.advanceTimersByTime(0)
		expect(past).toHaveBeenCalledTimes(1)
		vi.advanceTimersByTime(at - 1)
		expect(far).not.toHaveBeenCalled()
		vi.advanceTimersByTime(1)
		expect(far).toHaveBeenCalledTimes(1)
	})
})
