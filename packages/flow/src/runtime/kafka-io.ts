/**
 * The runtime's IO over @kafkats/client (3.6): the client configs flow derives from FlowConfig, and the factory of
 * the Kafka adapters. Every Run gets a new group consumer and producer (with the same transactional id under
 * exactly-once); restore and global-table consumers are created per job.
 */
import type { ConsumerConfig, KafkaClient, ProducerConfig } from '@kafkats/client'
import type { FlowConfig } from '@/api/app.js'
import type { ResolvedFlowConfig } from '@/runtime/config.js'
import type { Clock, RuntimeIO } from '@/runtime/io.js'
import { KafkaAdmin } from '@/runtime/kafka-admin.js'
import { KafkaGroupConsumer, KafkaRestoreConsumer, type GroupConsumerConfig } from '@/runtime/kafka-consumers.js'
import { KafkaProducer } from '@/runtime/kafka-producer.js'
import type { BuiltTopology } from '@/topology/built-topology.js'

/** Kafka Streams' `fetch.max.wait.ms`: an idle partition reports its end soon, so idle tasks learn their lag (R3). */
export const DEFAULT_MAX_WAIT_MS = 500

/** Retries of an exactly-once producer unless the user sets them (3.15). */
const DEFAULT_EOS_RETRIES = 10

export interface KafkaIOOptions {
	readonly client: Pick<KafkaClient, 'consumer' | 'producer' | 'admin'>
	readonly config: Pick<FlowConfig, 'consumer' | 'producer'>
	/** The start's resolved config: application and process id, guarantee, reset policy, restoration options. */
	readonly settings: ResolvedFlowConfig
	readonly topology: Pick<BuiltTopology, 'resetPolicy'>
}

export function createKafkaIO(options: KafkaIOOptions): RuntimeIO {
	const { client, settings } = options
	const group = groupConsumerConfig(options)
	const producer = producerConfig(options)
	const manual = (groupId: string, maxWaitMs?: number) =>
		new KafkaRestoreConsumer(client, manualConsumerConfig(options, groupId, maxWaitMs))
	return {
		consumer: () => new KafkaGroupConsumer(client, group),
		producer: () => new KafkaProducer(client.producer(producer)),
		admin: new KafkaAdmin(client.admin()),
		restoreConsumers: groupId => manual(groupId, settings.restoration.consumerMaxWaitMs),
		globalConsumers: groupId => manual(groupId),
		clock: systemClock,
	}
}

/**
 * The group consumer: the application id is the group, partitions are assigned by `range` (co-partitioned topics
 * together), a partition without a committed offset or out of range goes where its source's reset policy says
 * (repartition and through topics: 'earliest', KF16), exactly-once reads committed records only.
 */
export function groupConsumerConfig({ config, settings, topology }: KafkaIOOptions): GroupConsumerConfig {
	const consumer = config.consumer ?? {}
	return {
		...consumer,
		groupId: settings.applicationId,
		partitionAssignmentStrategy: 'range',
		rebalanceTimeoutMs: settings.rebalanceTimeoutMs,
		maxWaitMs: consumer.maxWaitMs ?? DEFAULT_MAX_WAIT_MS,
		isolationLevel: settings.eos ? 'read_committed' : consumer.isolationLevel,
		autoOffsetReset: topic => topology.resetPolicy(topic, settings.defaultResetPolicy),
	}
}

/**
 * At-least-once: the user's producer config. Exactly-once: transactional with the user's id verbatim or
 * `<applicationId>-<processId>` (D11), one transaction at a time (more would use other ids).
 */
export function producerConfig({ config, settings }: KafkaIOOptions): ProducerConfig {
	const producer = config.producer ?? {}
	if (!settings.eos) {
		return producer
	}
	return {
		...producer,
		transactionalId: producer.transactionalId || `${settings.applicationId}-${settings.processId}`,
		transactionConcurrency: 1,
		idempotent: true,
		retries: producer.retries ?? DEFAULT_EOS_RETRIES,
		transactionTimeoutMs: settings.transactionTimeoutMs,
	}
}

/**
 * Restore and global-table consumers: the fetch settings of the group consumer, read_committed, reset policy 'none'
 * (out of range is reported, never reset), started from explicit offsets of a manual assignment.
 */
export function manualConsumerConfig({ config }: KafkaIOOptions, groupId: string, maxWaitMs?: number): ConsumerConfig {
	const { maxBytesPerPartition, minBytes, checkCrcs } = config.consumer ?? {}
	return {
		groupId,
		maxBytesPerPartition,
		minBytes,
		checkCrcs,
		maxWaitMs: maxWaitMs ?? config.consumer?.maxWaitMs ?? DEFAULT_MAX_WAIT_MS,
		isolationLevel: 'read_committed',
		autoOffsetReset: 'none',
	}
}

/** setTimeout's longest delay (2^31 - 1 ms); a timer due later is re-armed until it is due. */
const MAX_TIMER_DELAY_MS = 2 ** 31 - 1

/** Wall-clock time (Kafka Streams' `Time.SYSTEM`): commit intervals, wall-clock punctuation, idle deadlines. */
export const systemClock: Clock = {
	now: () => Date.now(),
	setTimer(at, fn) {
		let timer: NodeJS.Timeout
		const arm = (): void => {
			const delay = at - Date.now()
			timer =
				delay > MAX_TIMER_DELAY_MS ? setTimeout(arm, MAX_TIMER_DELAY_MS) : setTimeout(fn, Math.max(0, delay))
		}
		arm()
		return () => clearTimeout(timer)
	},
}
