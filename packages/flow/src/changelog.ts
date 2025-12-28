import type { KafkaClient, Producer } from '@kafkats/client'
import type { Codec } from './codec.js'
import type { KeyValueStore, WindowedKey } from './state.js'

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
	 * Stop restoration after this many milliseconds without consuming any messages.
	 * Default: 5000ms.
	 */
	idleTimeoutMs?: number
	/**
	 * Stop restoration after this many milliseconds without consuming any messages
	 * BEFORE the first message is received.
	 *
	 * This can be set higher than idleTimeoutMs to tolerate slow consumer startup
	 * while still stopping quickly after reaching the end of the changelog.
	 *
	 * Default: idleTimeoutMs.
	 */
	initialIdleTimeoutMs?: number
	/**
	 * How often to check for the idle timeout.
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
	): Promise<unknown>
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
		private readonly getTransaction: () => TransactionLike | null
	) {}

	/**
	 * Write a key-value pair to the changelog.
	 */
	async write(key: K, value: V): Promise<void> {
		const keyBuffer = this.keyCodec.encode(key)
		const valueBuffer = this.valueCodec.encode(value)
		const transaction = this.getTransaction()

		if (transaction) {
			await transaction.send(this.topicName, {
				key: keyBuffer,
				value: valueBuffer,
			})
		} else {
			await this.getProducer().send(this.topicName, {
				key: keyBuffer,
				value: valueBuffer,
			})
		}
	}

	/**
	 * Write a tombstone (null value) to the changelog.
	 */
	async writeTombstone(key: K): Promise<void> {
		const keyBuffer = this.keyCodec.encode(key)
		const transaction = this.getTransaction()

		// Kafka supports null values for tombstones, but types may not reflect this
		// Use type assertion to work around the type restriction
		const tombstoneMessage = {
			key: keyBuffer,
			value: null as unknown as Buffer,
		}

		if (transaction) {
			await transaction.send(this.topicName, tombstoneMessage)
		} else {
			await this.getProducer().send(this.topicName, tombstoneMessage)
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
	async restore(client: KafkaClient, options?: ChangelogRestorationOptions): Promise<number> {
		const uniqueGroupId = `${this.topicName}-restorer-${Date.now()}-${Math.random().toString(36).slice(2)}`

		const consumer = client.consumer({
			groupId: uniqueGroupId,
			autoOffsetReset: 'earliest',
			...(options?.consumerMaxWaitMs !== undefined ? { maxWaitMs: options.consumerMaxWaitMs } : {}),
		})

		let restoredCount = 0
		let stopRequested = false

		// We'll use a timeout to detect when we've consumed all available messages
		// This is a simpler approach than tracking end offsets
		let lastMessageTime = Date.now()
		let isRunning = false
		let hasConsumedMessage = false
		const idleTimeoutMs = options?.idleTimeoutMs ?? 5000 // Stop after N ms of no messages
		const initialIdleTimeoutMs = options?.initialIdleTimeoutMs ?? idleTimeoutMs
		const checkIntervalMs = options?.checkIntervalMs ?? 1000

		consumer.once('running', () => {
			isRunning = true
			lastMessageTime = Date.now()
		})

		await consumer.runEach(
			[this.topicName],
			async message => {
				lastMessageTime = Date.now()
				hasConsumedMessage = true

				if (message.key === null) {
					return
				}

				const key = this.keyCodec.decode(message.key)

				if (message.value === null) {
					// Tombstone - delete from store
					await this.store.delete(key)
				} else {
					const value = this.valueCodec.decode(message.value)
					await this.store.put(key, value)
					restoredCount++
				}
			},
			{
				commitOffsets: false,
				signal: (() => {
					// Create an abort controller that will abort after idle timeout
					const controller = new AbortController()

					// Check periodically if we should stop
					const checkIdle = setInterval(() => {
						if (!isRunning) return
						if (stopRequested) {
							clearInterval(checkIdle)
							controller.abort()
							return
						}
						const timeoutMs = hasConsumedMessage ? idleTimeoutMs : initialIdleTimeoutMs
						if (Date.now() - lastMessageTime > timeoutMs) {
							stopRequested = true
							clearInterval(checkIdle)
							controller.abort()
						}
					}, checkIntervalMs)

					return controller.signal
				})(),
			}
		)

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
