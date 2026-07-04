import { EventEmitter } from 'node:events'
import { describe, expect, it } from 'vitest'
import type { KafkaClient } from '@kafkats/client'
import { ChangelogRestorer } from '../../src/changelog.js'
import { InMemoryKeyValueStore } from '../../src/state/memory.js'
import { codec } from '../../src/codec.js'
import type { ChangelogCheckpointStore } from '../../src/state.js'

/**
 * G2 regression: when restoration ends via the idle-timeout escape (e.g. the consumer stalls or a
 * read_committed changelog's LSO sits past the last user record), the checkpoint must only advance
 * to the last offset actually applied — never unconditionally to endOffset, which would silently
 * skip the unrestored range forever on the next restart.
 */

const TOPIC = 'idle-checkpoint-changelog'
const stringCodec = codec.string()
const numberCodec = codec.json<number>()

type StubRecord = { offset: bigint; key: Buffer | null; value: Buffer | null }
type StubMessage = {
	topic: string
	partition: number
	offset: bigint
	timestamp: bigint
	key: Buffer | null
	value: Buffer | null
	headers: Record<string, Buffer>
}
type RunEachOptions = {
	assignment: Array<{ topic: string; partition: number; offset: bigint }>
	signal: AbortSignal
}

class StubConsumer extends EventEmitter {
	assignment: Array<{ topic: string; partition: number; offset: bigint }> = []

	constructor(private readonly records: StubRecord[]) {
		super()
	}

	async runEach(
		_topics: unknown,
		handler: (message: StubMessage) => Promise<void>,
		options: RunEachOptions
	): Promise<void> {
		this.assignment = options.assignment
		this.emit('running')

		const startOffset = options.assignment[0]?.offset ?? 0n
		for (const record of this.records) {
			if (record.offset < startOffset) continue
			if (options.signal.aborted) break
			await handler({
				topic: TOPIC,
				partition: 0,
				offset: record.offset,
				timestamp: 0n,
				key: record.key,
				value: record.value,
				headers: {},
			})
		}

		if (!options.signal.aborted) {
			await new Promise<void>(resolve =>
				options.signal.addEventListener('abort', () => resolve(), { once: true })
			)
		}
	}

	pause(): void {}
	stop(): void {}
}

function stubClient(records: StubRecord[], endOffset: bigint): { client: KafkaClient; consumers: StubConsumer[] } {
	const consumers: StubConsumer[] = []
	const client = {
		admin: () => ({
			describeTopics: async () => [{ partitions: [{ partitionIndex: 0 }] }],
			fetchTopicOffsets: async (_topic: string, _partitions: number[], type: 'earliest' | 'latest') =>
				new Map([[0, type === 'earliest' ? 0n : endOffset]]),
		}),
		consumer: () => {
			const consumer = new StubConsumer(records)
			consumers.push(consumer)
			return consumer
		},
		cluster: { getLogger: () => ({ error: () => {} }) },
	} as unknown as KafkaClient
	return { client, consumers }
}

function memoryCheckpointStore(
	initial?: Map<string, bigint>
): ChangelogCheckpointStore & { offsets: Map<string, bigint> } {
	const offsets = initial ?? new Map<string, bigint>()
	return {
		offsets,
		async get(topic, partition) {
			return offsets.get(`${topic}:${partition}`)
		},
		async set(topic, partition, offset) {
			offsets.set(`${topic}:${partition}`, offset)
		},
	}
}

function record(offset: bigint, value: number): StubRecord {
	return { offset, key: stringCodec.encode('a'), value: numberCodec.encode(value) }
}

const fastOptions = { idleTimeoutMs: 40, initialIdleTimeoutMs: 40, checkIntervalMs: 10 }

describe('restore idle-timeout checkpointing', () => {
	it('checkpoints lastRestored+1, not endOffset, when the idle escape fires before endOffset', async () => {
		// 5 records (offsets 0..4) but endOffset 10: the consumer goes idle far below endOffset.
		const { client } = stubClient(
			[0n, 1n, 2n, 3n, 4n].map(o => record(o, Number(o) + 1)),
			10n
		)
		const store = new InMemoryKeyValueStore<string, number>('s', { keyCodec: stringCodec, valueCodec: numberCodec })
		const checkpoints = memoryCheckpointStore()

		const restorer = new ChangelogRestorer(TOPIC, stringCodec, numberCodec, store)
		const restored = await restorer.restore(client, fastOptions, [0], checkpoints)

		expect(restored).toBe(5)
		// Last applied offset was 4, so the next restore must resume at 5 — NOT skip to 10.
		expect(await checkpoints.get(TOPIC, 0)).toBe(5n)
	})

	it('the next restore resumes from the bounded checkpoint', async () => {
		const checkpoints = memoryCheckpointStore(new Map([[`${TOPIC}:0`, 5n]]))
		const { client, consumers } = stubClient([record(5n, 6), record(6n, 7)], 10n)
		const store = new InMemoryKeyValueStore<string, number>('s', { keyCodec: stringCodec, valueCodec: numberCodec })

		const restorer = new ChangelogRestorer(TOPIC, stringCodec, numberCodec, store)
		const restored = await restorer.restore(client, fastOptions, [0], checkpoints)

		expect(consumers[0]!.assignment).toEqual([{ topic: TOPIC, partition: 0, offset: 5n }])
		expect(restored).toBe(2)
		expect(await store.get('a')).toBe(7)
		expect(await checkpoints.get(TOPIC, 0)).toBe(7n)
	})

	it('completes gracefully (without moving the checkpoint) when a checkpoint-resumed partition yields nothing', async () => {
		// The F7b/LSO edge: the checkpoint sits at the control-marker tail of a transactional
		// changelog, so no user records exist between checkpoint and endOffset. This must not be
		// treated as a restore failure, and the checkpoint must not move.
		const checkpoints = memoryCheckpointStore(new Map([[`${TOPIC}:0`, 5n]]))
		const { client } = stubClient([], 10n)
		const store = new InMemoryKeyValueStore<string, number>('s', { keyCodec: stringCodec, valueCodec: numberCodec })

		const restorer = new ChangelogRestorer(TOPIC, stringCodec, numberCodec, store)
		const restored = await restorer.restore(client, fastOptions, [0], checkpoints)

		expect(restored).toBe(0)
		expect(await checkpoints.get(TOPIC, 0)).toBe(5n)
	})
})
