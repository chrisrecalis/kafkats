import { ErrorCode, KafkaClient, KafkaError } from '@kafkats/client'
import type {
	ChangelogCheckpointStore,
	KeyValueStore,
	KeyValueStoreOptions,
	SessionStore,
	SessionStoreOptions,
	StateStoreProvider,
	WindowStore,
	WindowStoreOptions,
} from '../../src/index.js'
import { InMemoryStateStoreProvider } from '../../src/state/memory.js'
import { requireKafkaBrokers } from './test-helpers.js'

export const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms))

export function newClient(prefix: string): KafkaClient {
	return new KafkaClient({
		clientId: `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
		brokers: requireKafkaBrokers(),
	})
}

export type Out = {
	key: Buffer | null
	value: Buffer | null
	partition: number
	offset: bigint
	timestamp: bigint
	headers: Record<string, Buffer>
}

/** Read everything committed on a topic (read_committed) until it has been quiet for `quietMs`. */
export async function readAllCommitted(client: KafkaClient, topic: string, quietMs = 1_500): Promise<Out[]> {
	const consumer = client.consumer({
		groupId: `r2-reader-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
		autoOffsetReset: 'earliest',
		isolationLevel: 'read_committed',
		maxWaitMs: 100,
	})
	const out: Out[] = []
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

/** Make `client.producer()` return producers whose transactions fail commit (sendOffsets) while `shouldFail()` is true. */
export function injectCommitFailures(client: KafkaClient, shouldFail: () => boolean, code = ErrorCode.RequestTimedOut) {
	const original = client.producer.bind(client)
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	;(client as any).producer = (config: any) => {
		const producer = original(config)
		const originalTransaction = producer.transaction.bind(producer)
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(producer as any).transaction = (fn: any, ...rest: any[]) =>
			originalTransaction(async (tx: any) => {
				const sendOffsets = tx.sendOffsets.bind(tx)
				tx.sendOffsets = async (...args: any[]) => {
					if (shouldFail()) throw new KafkaError('injected commit failure', code)
					return sendOffsets(...args)
				}
				return fn(tx)
			}, ...rest)
		return producer
	}
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
		let s = this.stores.get(name)
		if (!s) {
			s = make()
			this.stores.set(name, s)
		}
		return s as T
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
