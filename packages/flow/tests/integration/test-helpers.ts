/** Shared helpers for the integration tests (real broker, see kafka.global-setup.ts). */
import { afterAll, beforeAll } from 'vitest'
import { ErrorCode, KafkaClient, KafkaError, type Consumer, type Message } from '@kafkats/client'
import type {
	ChangelogCheckpointStore,
	Codec,
	KeyValueStore,
	KeyValueStoreOptions,
	SessionStore,
	SessionStoreOptions,
	StateStoreProvider,
	WindowStore,
	WindowStoreOptions,
} from '../../src/index.js'
import { InMemoryStateStoreProvider } from '../../src/state/memory.js'

// ============================================================================
// Basics
// ============================================================================

const RUN_ID = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
const randomSuffix = () => Math.random().toString(36).slice(2, 8)

export const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms))

export function requireKafkaBrokers(): string[] {
	const raw = process.env.KAFKA_BROKERS
	if (!raw) {
		throw new Error('Missing env var KAFKA_BROKERS (set by tests/integration/kafka.global-setup.ts)')
	}
	return raw.split(',').map(b => b.trim())
}

/** A name that is unique per test run and call; also used for group and application ids. */
export function uniqueTopicName(prefix: string): string {
	return `${prefix.replace(/[^a-zA-Z0-9._-]/g, '-')}-${RUN_ID}-${randomSuffix()}`
}

/** A new (not yet connected) client for the test broker. */
export function newClient(prefix: string): KafkaClient {
	return new KafkaClient({ clientId: `${prefix}-${Date.now()}-${randomSuffix()}`, brokers: requireKafkaBrokers() })
}

/** Rejects with `Timed out after <ms>ms: <label>` when `promise` does not settle in time. */
export async function withTimeout<T>(label: string, promise: Promise<T>, timeoutMs: number): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined
	try {
		return await Promise.race([
			promise,
			new Promise<T>((_resolve, reject) => {
				timer = setTimeout(() => reject(new Error(`Timed out after ${timeoutMs}ms: ${label}`)), timeoutMs)
			}),
		])
	} finally {
		clearTimeout(timer)
	}
}

/** Poll `cond` until it holds; throws `Timed out waiting for: <label>` after `timeoutMs`. */
export async function waitUntil(label: string, cond: () => boolean, timeoutMs = 10_000): Promise<void> {
	const deadline = Date.now() + timeoutMs
	while (Date.now() < deadline) {
		if (cond()) return
		await sleep(50)
	}
	throw new Error(`Timed out waiting for: ${label}`)
}

/** Deterministic pseudo-random numbers in [0, 1) (linear congruential generator). */
export function lcg(seed: number): () => number {
	let s = seed >>> 0
	return () => {
		s = (Math.imul(s, 1664525) + 1013904223) >>> 0
		return s / 2 ** 32
	}
}

/** 8-byte little-endian double codec. */
export const float64Codec: Codec<number> = {
	encode: (v: number) => {
		const buf = Buffer.alloc(8)
		buf.writeDoubleLE(v, 0)
		return buf
	},
	decode: (b: Buffer) => b.readDoubleLE(0),
}

/** Collect the process's unhandled rejections while the enclosing suite runs. */
export function collectUnhandledRejections(): unknown[] {
	const rejections: unknown[] = []
	const onRejection = (reason: unknown) => rejections.push(reason)
	beforeAll(() => {
		process.on('unhandledRejection', onRejection)
	})
	afterAll(() => {
		process.off('unhandledRejection', onRejection)
	})
	return rejections
}

// ============================================================================
// Topics
// ============================================================================

/** Create topics and wait until every partition has a leader. */
export async function createTopics(
	client: KafkaClient,
	topics: Array<{ name: string; partitions?: number; replicationFactor?: number }>
): Promise<void> {
	const specs = topics.map(t => ({
		name: t.name,
		numPartitions: t.partitions ?? 1,
		replicationFactor: t.replicationFactor ?? 1,
	}))
	await client.createTopics(specs)

	const deadline = Date.now() + 20_000
	while (Date.now() < deadline) {
		const meta = await client.getMetadata(specs.map(t => t.name))
		const ready = specs.every(spec => {
			const topic = meta.topics.get(spec.name)
			return (
				topic?.partitions.size === spec.numPartitions &&
				[...topic.partitions.values()].every(p => p.leaderId >= 0)
			)
		})
		if (ready) return
		await sleep(100)
	}
	throw new Error(`Timed out waiting for topics to become ready: ${specs.map(s => s.name).join(', ')}`)
}

export type CommittedRecord = {
	key: Buffer | null
	value: Buffer | null
	partition: number
	offset: bigint
	timestamp: bigint
	headers: Record<string, Buffer>
}

/** Read everything committed on a topic (read_committed) until it has been quiet for `quietMs`. */
export async function readAllCommitted(
	client: KafkaClient,
	topic: string,
	quietMs = 1_500
): Promise<CommittedRecord[]> {
	const consumer = client.consumer({
		groupId: uniqueTopicName('reader'),
		autoOffsetReset: 'earliest',
		isolationLevel: 'read_committed',
		maxWaitMs: 100,
	})
	const out: CommittedRecord[] = []
	let last = Date.now()
	const running = new Promise<void>(resolve => consumer.once('running', () => resolve()))
	const run = consumer.runEach(topic, async m => {
		out.push({
			key: m.key,
			value: m.value,
			partition: m.partition,
			offset: m.offset,
			timestamp: m.timestamp,
			headers: m.headers,
		})
		last = Date.now()
	})
	await running
	await sleep(700)
	while (Date.now() - last < quietMs) await sleep(100)
	consumer.stop()
	await run
	return out
}

/** The committed contents of a table topic: the latest value per (string) key, tombstones removing the key. */
export async function readCommittedTable<V>(
	client: KafkaClient,
	topic: string,
	valueCodec: Codec<V>,
	quietMs?: number
): Promise<Record<string, V>> {
	const table: Record<string, V> = {}
	for (const m of await readAllCommitted(client, topic, quietMs)) {
		const key = m.key!.toString()
		if (m.value === null) delete table[key]
		else table[key] = valueCodec.decode(m.value)
	}
	return table
}

/**
 * A group's committed offset (-1 without one) and its metadata for one partition; pending transactional offset
 * commits are waited for.
 */
export async function fetchCommitted(
	client: KafkaClient,
	groupId: string,
	topic: string,
	partition = 0
): Promise<{ offset: bigint; metadata: string | null }> {
	const coordinator = await client.cluster.getCoordinator('GROUP', groupId)
	const response = await coordinator.offsetFetch({
		groupId,
		topics: [{ name: topic, partitions: [{ partitionIndex: partition }] }],
		requireStable: true,
	})
	const { committedOffset, metadata } = response.topics[0]!.partitions[0]!
	return { offset: committedOffset, metadata }
}

// ============================================================================
// Fault injection and state providers
// ============================================================================

const injectedFailure = () => new KafkaError('injected commit failure', ErrorCode.RequestTimedOut)

/**
 * Make `client.producer()` return producers whose transactions (`beginTransaction()`, which `transaction()` uses too)
 * fail `sendOffsets` with `error()` whenever `shouldFail()` returns true (a retriable Kafka error by default): the
 * fault point of an exactly-once commit after its outputs and changelog records were sent.
 */
export function injectCommitFailures(
	client: KafkaClient,
	shouldFail: () => boolean,
	error: () => Error = injectedFailure
): void {
	const original = client.producer.bind(client)
	client.producer = config => {
		const producer = original(config)
		const begin = producer.beginTransaction.bind(producer)
		producer.beginTransaction = async options => {
			const transaction = await begin(options)
			const { sendOffsets } = transaction
			transaction.sendOffsets = (async (...args: unknown[]) => {
				if (shouldFail()) throw error()
				return Reflect.apply(sendOffsets, transaction, args)
			}) as typeof sendOffsets
			return transaction
		}
		return producer
	}
}

/**
 * Make `client.consumer()` return consumers whose `commit()` (at-least-once offset commits outside handlers) fails
 * with `error()` whenever `shouldFail()` returns true (a retriable Kafka error by default).
 */
export function injectConsumerCommitFailures(
	client: KafkaClient,
	shouldFail: () => boolean,
	error: () => Error = injectedFailure
): void {
	const original = client.consumer.bind(client)
	client.consumer = config => {
		const consumer = original(config)
		const commit = consumer.commit.bind(consumer)
		consumer.commit = async (offsets, options) => {
			if (shouldFail()) throw error()
			return commit(offsets, options)
		}
		return consumer
	}
}

/** Wrap a store so that every `put()` first awaits `beforePut()`. */
export function delayPuts<K, V>(store: KeyValueStore<K, V>, beforePut: () => Promise<void>): KeyValueStore<K, V> {
	return new Proxy(store, {
		get(target, prop) {
			const value = Reflect.get(target, prop, target) as unknown
			if (prop === 'put') {
				return async (key: K, v: V) => {
					await beforePut()
					return target.put(key, v)
				}
			}
			return typeof value === 'function' ? (value as (...a: unknown[]) => unknown).bind(target) : value
		},
	})
}

/**
 * Provider whose stores and changelog checkpoints survive across flow() apps (like LMDB with a
 * stable stateDir): close() does not drop state.
 */
export class DurableMemoryProvider implements StateStoreProvider {
	readonly name = 'durable-memory'
	private readonly inner = new InMemoryStateStoreProvider()
	private readonly stores = new Map<string, unknown>()
	private readonly checkpoints = new Map<string, bigint>()
	readonly checkpointStore: ChangelogCheckpointStore = {
		get: async (topic, partition) => this.checkpoints.get(`${topic}:${partition}`),
		set: async (topic, partition, offset) => {
			this.checkpoints.set(`${topic}:${partition}`, offset)
		},
		flush: async () => {},
	}

	getChangelogCheckpointStore() {
		return this.checkpointStore
	}
	private cached<T>(name: string, make: () => T): T {
		if (!this.stores.has(name)) this.stores.set(name, make())
		return this.stores.get(name) as T
	}
	createKeyValueStore<K, V>(name: string, o: KeyValueStoreOptions<K, V>): KeyValueStore<K, V> {
		return this.cached(name, () => this.inner.createKeyValueStore(name, o))
	}
	createWindowStore<K, V>(name: string, o: WindowStoreOptions<K, V>): WindowStore<K, V> {
		return this.cached(name, () => this.inner.createWindowStore(name, o))
	}
	createSessionStore<K, V>(name: string, o: SessionStoreOptions<K, V>): SessionStore<K, V> {
		return this.cached(name, () => this.inner.createSessionStore(name, o))
	}
	async close(): Promise<void> {}
}

// ============================================================================
// Output collector
// ============================================================================

export type CollectedMessage<K, V> = Message<V, K> & { value: V }

/** Consumes a set of topics in the background and lets tests wait for (matching) messages. */
export class MultiTopicCollector {
	readonly messages: CollectedMessage<unknown, unknown>[] = []
	private readonly messagesByTopic = new Map<string, CollectedMessage<unknown, unknown>[]>()
	private readonly onMessage = new Set<(topic: string) => void>()

	private readonly consumer: Consumer
	private readonly subscription: Array<{
		topic: string
		decoder: (buf: Buffer) => unknown
		keyDecoder?: (buf: Buffer) => unknown
	}>
	private runPromise: Promise<void> | null = null

	constructor(params: {
		client: KafkaClient
		groupId: string
		topics: Array<{ topic: string; keyCodec?: Codec<unknown>; valueCodec?: Codec<unknown> }>
		maxWaitMs?: number
	}) {
		this.consumer = params.client.consumer({
			groupId: params.groupId,
			autoOffsetReset: 'earliest',
			maxWaitMs: params.maxWaitMs ?? 100,
		})
		this.subscription = params.topics.map(({ topic, keyCodec, valueCodec }) => ({
			topic,
			decoder: (buf: Buffer) => (valueCodec ? valueCodec.decode(buf) : buf),
			keyDecoder: keyCodec ? (buf: Buffer) => keyCodec.decode(buf) : undefined,
		}))
	}

	async start(): Promise<void> {
		if (this.runPromise) return

		const ready = new Promise<void>(resolve => this.consumer.once('running', () => resolve()))
		this.runPromise = this.consumer.runEach(this.subscription, async message => {
			const collected = message as CollectedMessage<unknown, unknown>
			this.messages.push(collected)
			const list = this.messagesByTopic.get(collected.topic) ?? []
			list.push(collected)
			this.messagesByTopic.set(collected.topic, list)
			for (const listener of this.onMessage) listener(collected.topic)
		})
		await ready
	}

	async stop(): Promise<void> {
		this.consumer.stop()
		await this.runPromise
		this.runPromise = null
	}

	getTopicMessages<K, V>(topic: string): CollectedMessage<K, V>[] {
		return (this.messagesByTopic.get(topic) ?? []) as CollectedMessage<K, V>[]
	}

	/** The first message on `topic` matching `predicate`, waiting up to `timeoutMs` for it. */
	async waitFor<K, V>(
		topic: string,
		predicate: (msg: CollectedMessage<K, V>) => boolean,
		timeoutMs: number = 5_000
	): Promise<CollectedMessage<K, V>> {
		const [first] = await this.waitForCount(topic, predicate, 1, timeoutMs)
		return first!
	}

	/** The first `count` messages on `topic` matching `predicate`, waiting up to `timeoutMs` for them. */
	waitForCount<K, V>(
		topic: string,
		predicate: (msg: CollectedMessage<K, V>) => boolean,
		count: number,
		timeoutMs: number = 5_000
	): Promise<CollectedMessage<K, V>[]> {
		const matches = () => this.getTopicMessages<K, V>(topic).filter(predicate)
		return new Promise((resolve, reject) => {
			const check = (messageTopic: string) => {
				if (messageTopic !== topic) return
				const found = matches()
				if (found.length < count) return
				cleanup()
				resolve(found.slice(0, count))
			}
			const timer = setTimeout(() => {
				cleanup()
				reject(
					new Error(
						`Timed out after ${timeoutMs}ms waiting for ${count} messages on ${topic} (got ${matches().length})`
					)
				)
			}, timeoutMs)
			const cleanup = () => {
				clearTimeout(timer)
				this.onMessage.delete(check)
			}
			this.onMessage.add(check)
			check(topic)
		})
	}
}
