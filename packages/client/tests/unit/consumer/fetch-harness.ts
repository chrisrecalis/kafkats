import { onTestFinished, vi } from 'vitest'

import type { Cluster } from '@/client/cluster.js'
import { FetchManager, type FetchManagerConfig } from '@/consumer/fetch-manager.js'
import type { OffsetManager } from '@/consumer/offset-manager.js'
import type { AutoOffsetReset } from '@/consumer/types.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { createRecordBatch, encodeRecordBatchSync } from '@/protocol/records/index.js'

/** Topic of every partition in these tests. */
export const TOPIC = 't'

/** One encoded record batch with `count` records from `baseOffset` on. */
export function recordBatch(baseOffset: bigint, count: number): Buffer {
	const messages = Array.from({ length: count }, (_, index) => ({ value: `v${baseOffset + BigInt(index)}` }))
	return encodeRecordBatchSync(createRecordBatch(messages, baseOffset, 0n))
}

/** One encoded control batch (a transaction marker) at `offset`. */
export function controlBatch(offset: bigint): Buffer {
	return encodeRecordBatchSync(createRecordBatch([{ value: 'marker' }], offset, 0n), { isControlBatch: true })
}

export interface PartitionResponse {
	partition: number
	records?: Buffer
	errorCode?: ErrorCode
	highWatermark?: bigint
	/** Defaults to the high watermark. */
	lastStableOffset?: bigint
}

/** A Fetch response for partitions of {@link TOPIC}. */
export function fetchResponse(...partitions: PartitionResponse[]) {
	return {
		throttleTimeMs: 0,
		errorCode: ErrorCode.None,
		sessionId: 0,
		topics: [
			{
				topic: TOPIC,
				partitions: partitions.map(response => ({
					partitionIndex: response.partition,
					errorCode: response.errorCode ?? ErrorCode.None,
					highWatermark: response.highWatermark ?? 0n,
					lastStableOffset: response.lastStableOffset ?? response.highWatermark ?? 0n,
					logStartOffset: 0n,
					abortedTransactions: [],
					preferredReadReplica: -1,
					recordsData: response.records ?? null,
				})),
			},
		],
	}
}

/** The FetchManager internals the harness drives. */
interface Internals {
	partitionStates: Map<string, unknown>
	fetchFromBrokerToBuffer(broker: unknown, partitions: unknown[], maxBytes: number): Promise<void>
}

export interface HarnessOptions {
	autoOffsetReset?: AutoOffsetReset | ((topic: string, partition: number) => AutoOffsetReset)
	offsetManager?: Partial<Pick<OffsetManager, 'getEarliestOffset' | 'getLatestOffset'>>
	config?: Partial<FetchManagerConfig>
}

/**
 * A FetchManager whose background loop never fetches (its leader lookup never resolves). A test feeds it
 * Fetch responses one at a time with `fetch()`, so every step is deterministic. Stopped when the test ends.
 */
export async function fetchManagerHarness(options: HarnessOptions = {}) {
	const broker = { nodeId: 1, fetch: vi.fn(), closeFetchConnection: vi.fn().mockResolvedValue(undefined) }
	const cluster = { getLeaderForPartition: () => new Promise(() => {}), getLogger: () => null }
	const fm = new FetchManager(
		cluster as unknown as Cluster,
		(options.offsetManager ?? {}) as OffsetManager,
		options.autoOffsetReset ?? 'earliest',
		{
			maxBytesPerPartition: 1 << 20,
			minBytes: 1,
			maxWaitMs: 1,
			partitionConcurrency: 1,
			isolationLevel: 'read_uncommitted',
			...options.config,
		}
	)
	onTestFinished(() => fm.stop())
	// Creates the fetch buffer and starts the background loop, which then waits on the leader lookup.
	await fm.poll()
	const internals = fm as unknown as Internals

	return {
		fm,
		/** Run one Fetch of the given partitions, answered with `response`. */
		async fetch(response: ReturnType<typeof fetchResponse>, ...partitions: number[]): Promise<void> {
			broker.fetch.mockResolvedValueOnce(response)
			const states = partitions.map(partition => internals.partitionStates.get(`${TOPIC}:${partition}`))
			await internals.fetchFromBrokerToBuffer(broker, states, 1 << 20)
		},
		/** The offset the latest Fetch asked for, for a partition. */
		requestedOffset(partition: number): bigint | undefined {
			const request = broker.fetch.mock.calls.at(-1)?.[0] as
				| { topics: Array<{ partitions: Array<{ partitionIndex: number; fetchOffset: bigint }> }> }
				| undefined
			return request?.topics[0]?.partitions.find(entry => entry.partitionIndex === partition)?.fetchOffset
		},
	}
}

/** Offsets of the records of a batch. */
export function offsetsOf(batch: { records: Array<{ offset: bigint }> }): bigint[] {
	return batch.records.map(record => record.offset)
}

/** A topic-partition of {@link TOPIC}. */
export function tp(partition: number) {
	return { topic: TOPIC, partition }
}
