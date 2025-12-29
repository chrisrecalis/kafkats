import type { KafkaClient, Producer } from '@kafkats/client'
import type { SendResult } from '@kafkats/client'
import type { Codec } from './codec.js'
import type { ChangelogCheckpointStore, KeyValueStore, WindowedKey } from './state.js'

/**
 * Configuration for changelog topics backing state stores.
 *
 * Note: Partition count is always inferred from source topic(s) and cannot be overridden.
 * This ensures state locality - Task N processing partition N writes to changelog partition N.
 */
export interface ChangelogConfig {
	/** Whether changelog is enabled. Default: true */
	enabled?: boolean
	/** Custom topic name. Default: {appId}-{storeName}-changelog */
	topicName?: string
	/** Replication factor. Default: broker default */
	replicationFactor?: number
	/** Custom topic configs. Merged with defaults */
	topicConfigs?: Record<string, string>
	/** Skip state restoration on startup. Default: false */
	skipRestoration?: boolean
}

export interface ChangelogRestorationOptions {
	/**
	 * Abort restoration if no progress is made for this many milliseconds while restoration is incomplete.
	 * Default: 5000ms.
	 */
	idleTimeoutMs?: number
	/**
	 * Abort restoration if no progress is made for this many milliseconds BEFORE the first message is received,
	 * while restoration is incomplete.
	 *
	 * This can be set higher than idleTimeoutMs to tolerate slow consumer startup
	 * while still aborting quickly if restoration stalls.
	 *
	 * Default: idleTimeoutMs.
	 */
	initialIdleTimeoutMs?: number
	/**
	 * How often to check for the progress timeout.
	 * Default: 1000ms.
	 */
	checkIntervalMs?: number
	/**
	 * Override the consumer's Fetch maxWaitMs during restoration to make stop() responsive.
	 * Default: unset (uses consumer defaults).
	 */
	consumerMaxWaitMs?: number
}

/**
 * Error thrown when an existing changelog topic has the wrong number of partitions.
 * This indicates data corruption risk and requires manual intervention.
 */
export class ChangelogPartitionMismatchError extends Error {
	override readonly name = 'ChangelogPartitionMismatchError'

	constructor(
		public readonly changelogTopic: string,
		public readonly expectedPartitions: number,
		public readonly actualPartitions: number,
		public readonly sourceTopics: string[]
	) {
		super(
			`Changelog topic "${changelogTopic}" has ${actualPartitions} partitions ` +
				`but source topics [${sourceTopics.join(', ')}] require ${expectedPartitions}. ` +
				`Delete the changelog topic and restart, or manually recreate with ${expectedPartitions} partitions.`
		)
	}
}

/**
 * Error thrown when a source topic does not exist and partition count cannot be inferred.
 */
export class SourceTopicNotFoundError extends Error {
	override readonly name = 'SourceTopicNotFoundError'

	constructor(
		public readonly topic: string,
		public readonly storeName: string
	) {
		super(
			`Source topic "${topic}" does not exist. Cannot determine partition count for ` +
				`changelog topic of store "${storeName}". Create the source topic first or ` +
				`explicitly set changelog.partitions in the Materialized options.`
		)
	}
}

/**
 * Default topic configs for changelog topics.
 */
export const DEFAULT_CHANGELOG_CONFIGS: Record<string, string> = {
	'cleanup.policy': 'compact',
	'retention.ms': '-1',
	'segment.bytes': '52428800',
}

/**
 * Builds the changelog topic name for a state store.
 */
export function buildChangelogTopicName(appId: string, storeName: string): string {
	return `${appId}-${storeName}-changelog`
}

/**
 * Returns the default topic configs for changelog topics.
 */
export function getDefaultTopicConfigs(overrides?: Record<string, string>): Record<string, string> {
	return { ...DEFAULT_CHANGELOG_CONFIGS, ...overrides }
}

/**
 * Resolves changelog config from various input types.
 */
export function resolveChangelogConfig(changelog: boolean | ChangelogConfig | undefined): ChangelogConfig {
	if (changelog === false) {
		return { enabled: false }
	}
	if (changelog === undefined || changelog === true) {
		return { enabled: true }
	}
	return { enabled: changelog.enabled !== false, ...changelog }
}

/**
 * Spec for a changelog topic to be created.
 */
export interface ChangelogTopicSpec {
	storeName: string
	topicName: string
	replicationFactor?: number
	configs: Record<string, string>
	keyCodec: Codec<unknown>
	valueCodec: Codec<unknown>
	skipRestoration: boolean
	/** Source topics that feed into this state store. Used to infer partition count. */
	sourceTopics: Set<string>
	/** If true, only validate existing topics, don't create new ones. */
	validateOnly: boolean
}

/**
 * Transaction-like interface for sending messages.
 * Matches the send signature of both Producer and ActiveTransaction.
 */
interface TransactionLike {
	send(
		topic: string,
		messages: { key?: Buffer | null; value: Buffer | null; headers?: Record<string, Buffer>; partition?: number }
	): Promise<SendResult>
}

/**
 * Writes state mutations to a changelog topic.
 */
export class ChangelogWriter<K, V> {
	constructor(
		private readonly topicName: string,
		private readonly keyCodec: Codec<K>,
		private readonly valueCodec: Codec<V>,
		private readonly getProducer: () => Producer,
		private readonly getTransaction: () => TransactionLike | null,
		private readonly onWriteAck?: (result: SendResult) => void | Promise<void>
	) {}

	/**
	 * Write a key-value pair to the changelog.
	 */
	async write(key: K, value: V): Promise<void> {
		const keyBuffer = this.keyCodec.encode(key)
		const valueBuffer = this.valueCodec.encode(value)
		const transaction = this.getTransaction()

		if (transaction) {
			const result = await transaction.send(this.topicName, {
				key: keyBuffer,
				value: valueBuffer,
			})
			await this.onWriteAck?.(result)
		} else {
			const result = await this.getProducer().send(this.topicName, {
				key: keyBuffer,
				value: valueBuffer,
			})
			await this.onWriteAck?.(result)
		}
	}

	/**
	 * Write a tombstone (null value) to the changelog.
	 */
	async writeTombstone(key: K): Promise<void> {
		const keyBuffer = this.keyCodec.encode(key)
		const transaction = this.getTransaction()

		if (transaction) {
			const result = await transaction.send(this.topicName, { key: keyBuffer, value: null })
			await this.onWriteAck?.(result)
		} else {
			const result = await this.getProducer().send(this.topicName, { key: keyBuffer, value: null })
			await this.onWriteAck?.(result)
		}
	}
}

/**
 * Restores state from a changelog topic.
 */
export class ChangelogRestorer<K, V> {
	constructor(
		private readonly topicName: string,
		private readonly keyCodec: Codec<K>,
		private readonly valueCodec: Codec<V>,
		private readonly store: KeyValueStore<K, V>
	) {}

	/**
	 * Restore state by consuming the changelog topic from the beginning.
	 * @returns Number of records restored
	 */
	async restore(
		client: KafkaClient,
		options?: ChangelogRestorationOptions,
		partitions?: number[],
		checkpointStore?: ChangelogCheckpointStore
	): Promise<number> {
		const admin = client.admin()
		const [desc] = await admin.describeTopics([this.topicName])
		if (!desc) {
			return 0
		}

		const topicPartitions = partitions ?? desc.partitions.map(p => p.partitionIndex)

		if (topicPartitions.length === 0) {
			return 0
		}

		const uniqueGroupId = `${this.topicName}-restorer-${Date.now()}-${Math.random().toString(36).slice(2)}`

		const consumer = client.consumer({
			groupId: uniqueGroupId,
			autoOffsetReset: 'earliest',
			...(options?.consumerMaxWaitMs !== undefined ? { maxWaitMs: options.consumerMaxWaitMs } : {}),
		})

		const [startOffsets, endOffsets] = await Promise.all([
			admin.fetchTopicOffsets(this.topicName, topicPartitions, 'earliest', { isolationLevel: 'read_committed' }),
			admin.fetchTopicOffsets(this.topicName, topicPartitions, 'latest', { isolationLevel: 'read_committed' }),
		])

		const assigned = [...new Set(topicPartitions)].sort((a, b) => a - b)
		const checkpointOffsets = checkpointStore
			? new Map<number, bigint>(
					(
						await Promise.all(
							assigned.map(async partition => {
								const offset = await checkpointStore.get(this.topicName, partition)
								return offset !== undefined ? ([partition, offset] as const) : null
							})
						)
					).filter((pair): pair is readonly [number, bigint] => pair !== null)
				)
			: new Map<number, bigint>()

		const pendingPartitions = new Set<number>()
		const assignment = assigned.flatMap(partition => {
			const earliestOffset = startOffsets.get(partition)
			const endOffset = endOffsets.get(partition)
			const checkpointOffset = checkpointOffsets.get(partition)
			const startOffset =
				checkpointOffset !== undefined && earliestOffset !== undefined && checkpointOffset > earliestOffset
					? checkpointOffset
					: earliestOffset

			if (startOffset === undefined || endOffset === undefined) {
				throw new Error(`Missing offset information for ${this.topicName}-${partition}`)
			}

			if (startOffset >= endOffset) {
				return []
			}

			pendingPartitions.add(partition)
			return [{ topic: this.topicName, partition, offset: startOffset }]
		})

		if (pendingPartitions.size === 0) {
			return 0
		}

		let restoredCount = 0

		const abortController = new AbortController()

		let lastProgressTime = Date.now()
		let isRunning = false
		let hasConsumedMessage = false
		const idleTimeoutMs = options?.idleTimeoutMs ?? 5000
		const initialIdleTimeoutMs = options?.initialIdleTimeoutMs ?? idleTimeoutMs
		const checkIntervalMs = options?.checkIntervalMs ?? 1000
		const restoreCompleteReason = Symbol('changelogRestoreComplete')

		consumer.once('running', () => {
			isRunning = true
			lastProgressTime = Date.now()
		})

		const checkIdle = setInterval(() => {
			if (!isRunning) return
			if (abortController.signal.aborted) return
			if (pendingPartitions.size === 0) return

			const timeoutMs = hasConsumedMessage ? idleTimeoutMs : initialIdleTimeoutMs
			if (Date.now() - lastProgressTime > timeoutMs) {
				abortController.abort(
					new Error(
						`Timed out restoring changelog "${this.topicName}" (pending partitions: ${[
							...pendingPartitions,
						].join(', ')})`
					)
				)
			}
		}, checkIntervalMs)

		const pausedPartitions = new Set<number>()
		try {
			await consumer.runEach(
				[this.topicName],
				async message => {
					const endOffset = endOffsets.get(message.partition)
					if (endOffset === undefined) {
						return
					}

					// Ignore any records that arrive beyond the captured end offset (e.g. concurrent writers).
					if (message.offset >= endOffset) {
						pendingPartitions.delete(message.partition)
						if (!pausedPartitions.has(message.partition)) {
							pausedPartitions.add(message.partition)
							consumer.pause([{ topic: message.topic, partition: message.partition }])
						}
						if (pendingPartitions.size === 0) {
							abortController.abort(restoreCompleteReason)
						}
						return
					}

					lastProgressTime = Date.now()
					hasConsumedMessage = true

					if (message.key !== null) {
						const key = this.keyCodec.decode(message.key)

						if (message.value === null) {
							// Tombstone - delete from store
							await this.store.delete(key)
						} else {
							const value = this.valueCodec.decode(message.value)
							await this.store.put(key, value)
							restoredCount++
						}
					}

					if (message.offset + 1n >= endOffset) {
						pendingPartitions.delete(message.partition)
						if (!pausedPartitions.has(message.partition)) {
							pausedPartitions.add(message.partition)
							consumer.pause([{ topic: message.topic, partition: message.partition }])
						}
						if (pendingPartitions.size === 0) {
							abortController.abort(restoreCompleteReason)
						}
					}
				},
				{
					commitOffsets: false,
					assignment,
					signal: abortController.signal,
				}
			)
		} finally {
			clearInterval(checkIdle)
		}

		const abortReason: unknown = abortController.signal.reason
		if (abortReason !== restoreCompleteReason) {
			if (abortReason instanceof Error) {
				throw abortReason
			}
			if (pendingPartitions.size > 0) {
				throw new Error(`Aborted restoring changelog "${this.topicName}"`, { cause: abortReason })
			}
		}

		if (checkpointStore) {
			await Promise.all(
				assigned.map(async partition => {
					const endOffset = endOffsets.get(partition)
					if (endOffset === undefined) {
						return
					}
					await checkpointStore.set(this.topicName, partition, endOffset)
				})
			).catch(() => {})
		}

		return restoredCount
	}
}

/**
 * Creates a windowed key codec by encoding key + window boundaries.
 */
export function windowedKeyCodec<K>(keyCodec: Codec<K>): Codec<WindowedKey<K>> {
	return {
		encode(windowed: WindowedKey<K>): Buffer {
			const keyBuffer = keyCodec.encode(windowed.key)
			const windowBuffer = Buffer.alloc(16)
			windowBuffer.writeBigInt64BE(BigInt(windowed.windowStart), 0)
			windowBuffer.writeBigInt64BE(BigInt(windowed.windowEnd), 8)
			return Buffer.concat([keyBuffer, windowBuffer])
		},
		decode(buffer: Buffer): WindowedKey<K> {
			const keyBuffer = buffer.subarray(0, buffer.length - 16)
			const windowBuffer = buffer.subarray(buffer.length - 16)
			return {
				key: keyCodec.decode(keyBuffer),
				windowStart: Number(windowBuffer.readBigInt64BE(0)),
				windowEnd: Number(windowBuffer.readBigInt64BE(8)),
			}
		},
	}
}
