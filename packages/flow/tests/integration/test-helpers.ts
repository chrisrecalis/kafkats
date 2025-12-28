import { EventEmitter } from 'node:events'
import { KafkaClient, type Consumer, type Message } from '@kafkats/client'
import type { Codec } from '../../src/index.js'

const RUN_ID = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`

export function requireKafkaBrokers(): string[] {
	const raw = process.env.KAFKA_BROKERS
	if (!raw) {
		throw new Error('Missing env var KAFKA_BROKERS (set by tests/integration/kafka.global-setup.ts)')
	}
	return raw.split(',').map(b => b.trim())
}

export function uniqueTopicName(prefix: string): string {
	const safePrefix = prefix.replace(/[^a-zA-Z0-9._-]/g, '-')
	return `${safePrefix}-${RUN_ID}-${Math.random().toString(36).slice(2, 8)}`
}

export async function createTopics(
	client: KafkaClient,
	topics: Array<{ name: string; partitions?: number; replicationFactor?: number }>
): Promise<void> {
	const specs = topics.map(t => ({
		name: t.name,
		partitions: t.partitions ?? 1,
		replicationFactor: t.replicationFactor ?? 1,
	}))

	await client.createTopics(
		topics.map(t => ({
			name: t.name,
			numPartitions: t.partitions ?? 1,
			replicationFactor: t.replicationFactor ?? 1,
		}))
	)

	const deadline = Date.now() + 20_000
	while (Date.now() < deadline) {
		const meta = await client.getMetadata(specs.map(t => t.name))

		const allReady = specs.every(spec => {
			const topic = meta.topics.get(spec.name)
			if (!topic) return false
			if (topic.partitions.size !== spec.partitions) return false
			for (const partition of topic.partitions.values()) {
				if (partition.leaderId < 0) return false
			}
			return true
		})

		if (allReady) {
			return
		}

		await new Promise(resolve => setTimeout(resolve, 100))
	}

	throw new Error(`Timed out waiting for topics to become ready: ${specs.map(s => s.name).join(', ')}`)
}

export type CollectedMessage<K, V> = Message<V, K> & { value: V }

export class TopicCollector<K = Buffer, V = Buffer> extends EventEmitter {
	readonly topic: string
	readonly messages: CollectedMessage<K, V>[] = []

	private readonly consumer: Consumer
	private readonly subscription: Array<{
		topic: string
		decoder: (buf: Buffer) => V
		keyDecoder?: (buf: Buffer) => K
	}>
	private runPromise: Promise<void> | null = null

	constructor(params: {
		client: KafkaClient
		groupId: string
		topic: string
		keyCodec?: Codec<K>
		valueCodec?: Codec<V>
		autoOffsetReset?: 'earliest' | 'latest'
		maxWaitMs?: number
	}) {
		super()
		this.topic = params.topic

		this.consumer = params.client.consumer({
			groupId: params.groupId,
			autoOffsetReset: params.autoOffsetReset ?? 'earliest',
			maxWaitMs: params.maxWaitMs ?? 100,
		})

		this.subscription = [
			{
				topic: params.topic,
				decoder: (buf: Buffer) => (params.valueCodec ? params.valueCodec.decode(buf) : (buf as unknown as V)),
				keyDecoder: params.keyCodec ? (buf: Buffer) => params.keyCodec!.decode(buf) : undefined,
			},
		]
	}

	async start(): Promise<void> {
		if (this.runPromise) {
			return
		}

		const ready = new Promise<void>(resolve => {
			this.consumer.once('running', () => resolve())
		})

		this.runPromise = this.consumer.runEach(this.subscription, async message => {
			const collected = message as CollectedMessage<K, V>
			this.messages.push(collected)
			this.emit('message', collected)
		})

		await ready
	}

	async stop(): Promise<void> {
		this.consumer.stop()
		await this.runPromise
		this.runPromise = null
	}

	waitFor(predicate: (msg: CollectedMessage<K, V>) => boolean, timeoutMs: number = 5_000): Promise<CollectedMessage<K, V>> {
		const existing = this.messages.find(predicate)
		if (existing) {
			return Promise.resolve(existing)
		}

		return new Promise((resolve, reject) => {
			const timeout = setTimeout(() => {
				cleanup()
				reject(new Error(`Timed out after ${timeoutMs}ms waiting for message on ${this.topic}`))
			}, timeoutMs)

			const onMessage = (msg: CollectedMessage<K, V>) => {
				if (!predicate(msg)) return
				cleanup()
				resolve(msg)
			}

			const cleanup = () => {
				clearTimeout(timeout)
				this.off('message', onMessage)
			}

			this.on('message', onMessage)
		})
	}

	async waitForCount(
		predicate: (msg: CollectedMessage<K, V>) => boolean,
		count: number,
		timeoutMs: number = 5_000
	): Promise<CollectedMessage<K, V>[]> {
		const existing = this.messages.filter(predicate)
		if (existing.length >= count) {
			return existing.slice(0, count)
		}

		return new Promise((resolve, reject) => {
			const timeout = setTimeout(() => {
				cleanup()
				reject(
					new Error(
						`Timed out after ${timeoutMs}ms waiting for ${count} messages on ${this.topic} (got ${currentCount()})`
					)
				)
			}, timeoutMs)

			const currentCount = () => this.messages.filter(predicate).length

			const onMessage = () => {
				if (currentCount() < count) return
				const results = this.messages.filter(predicate).slice(0, count)
				cleanup()
				resolve(results)
			}

			const cleanup = () => {
				clearTimeout(timeout)
				this.off('message', onMessage)
			}

			this.on('message', onMessage)
		})
	}
}

export class MultiTopicCollector extends EventEmitter {
	readonly messages: CollectedMessage<unknown, unknown>[] = []
	private readonly messagesByTopic = new Map<string, CollectedMessage<unknown, unknown>[]>()

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
		autoOffsetReset?: 'earliest' | 'latest'
		maxWaitMs?: number
	}) {
		super()

		this.consumer = params.client.consumer({
			groupId: params.groupId,
			autoOffsetReset: params.autoOffsetReset ?? 'earliest',
			maxWaitMs: params.maxWaitMs ?? 100,
		})

		this.subscription = params.topics.map(t => ({
			topic: t.topic,
			decoder: (buf: Buffer) => (t.valueCodec ? t.valueCodec.decode(buf) : buf),
			keyDecoder: t.keyCodec ? (buf: Buffer) => t.keyCodec!.decode(buf) : undefined,
		}))
	}

	async start(): Promise<void> {
		if (this.runPromise) {
			return
		}

		const ready = new Promise<void>(resolve => {
			this.consumer.once('running', () => resolve())
		})

		this.runPromise = this.consumer.runEach(this.subscription, async message => {
			const collected = message as CollectedMessage<unknown, unknown>
			this.messages.push(collected)

			const list = this.messagesByTopic.get(collected.topic) ?? []
			list.push(collected)
			this.messagesByTopic.set(collected.topic, list)

			this.emit('message', collected)
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

	waitFor<K, V>(
		topic: string,
		predicate: (msg: CollectedMessage<K, V>) => boolean,
		timeoutMs: number = 5_000
	): Promise<CollectedMessage<K, V>> {
		const existing = this.getTopicMessages<K, V>(topic).find(predicate)
		if (existing) {
			return Promise.resolve(existing)
		}

		return new Promise((resolve, reject) => {
			const timeout = setTimeout(() => {
				cleanup()
				reject(new Error(`Timed out after ${timeoutMs}ms waiting for message on ${topic}`))
			}, timeoutMs)

			const onMessage = (msg: CollectedMessage<unknown, unknown>) => {
				if (msg.topic !== topic) return
				const typed = msg as CollectedMessage<K, V>
				if (!predicate(typed)) return
				cleanup()
				resolve(typed)
			}

			const cleanup = () => {
				clearTimeout(timeout)
				this.off('message', onMessage)
			}

			this.on('message', onMessage)
		})
	}

	waitForCount<K, V>(
		topic: string,
		predicate: (msg: CollectedMessage<K, V>) => boolean,
		count: number,
		timeoutMs: number = 5_000
	): Promise<CollectedMessage<K, V>[]> {
		const existing = this.getTopicMessages<K, V>(topic).filter(predicate)
		if (existing.length >= count) {
			return Promise.resolve(existing.slice(0, count))
		}

		return new Promise((resolve, reject) => {
			const timeout = setTimeout(() => {
				cleanup()
				reject(
					new Error(
						`Timed out after ${timeoutMs}ms waiting for ${count} messages on ${topic} (got ${currentCount()})`
					)
				)
			}, timeoutMs)

			const currentCount = () => this.getTopicMessages<K, V>(topic).filter(predicate).length

			const onMessage = (msg: CollectedMessage<unknown, unknown>) => {
				if (msg.topic !== topic) return
				if (currentCount() < count) return
				const results = this.getTopicMessages<K, V>(topic).filter(predicate).slice(0, count)
				cleanup()
				resolve(results)
			}

			const cleanup = () => {
				clearTimeout(timeout)
				this.off('message', onMessage)
			}

			this.on('message', onMessage)
		})
	}
}
