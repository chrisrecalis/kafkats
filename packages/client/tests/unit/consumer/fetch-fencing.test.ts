import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

const FETCH_RESPONSE = {
	topics: [
		{
			topic: 't',
			partitions: [
				{
					partitionIndex: 0,
					errorCode: ErrorCode.None,
					highWatermark: 50n,
					lastStableOffset: 50n,
					logStartOffset: 0n,
					abortedTransactions: null,
					preferredReadReplica: -1,
					recordsData: Buffer.from('non-empty'),
				},
			],
		},
	],
	throttleTimeMs: 0,
	errorCode: ErrorCode.None,
	sessionId: 0,
}

describe('FetchManager response fencing', () => {
	it('drops records from a stale fetch when the PartitionState was replaced', async () => {
		let resolveFetch: (resp: unknown) => void = () => {}
		const fetchResponse = new Promise(resolve => {
			resolveFetch = resolve
		})

		const broker = {
			nodeId: 1,
			fetch: vi.fn().mockReturnValue(fetchResponse),
		}

		const cluster = {
			getLeaderForPartition: vi.fn().mockResolvedValue(broker),
			getLogger: () => null,
		}

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const offsetManager = new OffsetManager(cluster as any, 'g1')
		const fm = new FetchManager(
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			cluster as any,
			offsetManager,
			'earliest',
			{
				maxBytesPerPartition: 1024,
				minBytes: 1,
				maxWaitMs: 50,
				partitionConcurrency: 1,
				isolationLevel: 'read_uncommitted',
			}
		)
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const fmAny = fm as any

		const addedToBuffer: Array<{ topic: string; partition: number }> = []
		fmAny.fetchBuffer = {
			add: vi.fn((entry: { topic: string; partition: number }) => {
				addedToBuffer.push({ topic: entry.topic, partition: entry.partition })
			}),
			removePartitions: vi.fn(),
			removePartitionsReturningEarliest: vi.fn(() => new Map()),
			isFull: () => false,
		}

		// Stale records (offsets 12-13) that would clobber the fresh state's offset of 100n without the fence.
		fmAny.decodeRecords = vi.fn().mockReturnValue({
			records: [
				{ offset: 12n, key: null, value: Buffer.from('a'), headers: {}, timestamp: 0n },
				{ offset: 13n, key: null, value: Buffer.from('b'), headers: {}, timestamp: 0n },
			],
			nextOffset: 14n,
		})

		fm.addPartitions([{ topic: 't', partition: 0, offset: 10n }])
		const oldState = fmAny.partitionStates.get('t:0')

		const inflight: Promise<void> = fmAny.fetchFromBrokerToBuffer(broker, [oldState])

		fm.removePartitions([{ topic: 't', partition: 0 }])
		fm.addPartitions([{ topic: 't', partition: 0, offset: 100n }])
		const newState = fmAny.partitionStates.get('t:0')
		expect(newState).not.toBe(oldState)
		expect(newState.offset).toBe(100n)

		resolveFetch(FETCH_RESPONSE)

		await inflight

		expect(newState.offset).toBe(100n)
		expect(addedToBuffer).toEqual([])
	})

	it('drops records from an in-flight fetch when the partition is paused mid-flight', async () => {
		let resolveFetch: (resp: unknown) => void = () => {}
		const fetchResponse = new Promise(resolve => {
			resolveFetch = resolve
		})

		const broker = {
			nodeId: 1,
			fetch: vi.fn().mockReturnValue(fetchResponse),
		}
		const cluster = {
			getLeaderForPartition: vi.fn().mockResolvedValue(broker),
			getLogger: () => null,
		}

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const offsetManager = new OffsetManager(cluster as any, 'g1')
		const fm = new FetchManager(
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			cluster as any,
			offsetManager,
			'earliest',
			{
				maxBytesPerPartition: 1024,
				minBytes: 1,
				maxWaitMs: 50,
				partitionConcurrency: 1,
				isolationLevel: 'read_uncommitted',
			}
		)
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const fmAny = fm as any

		const addedToBuffer: Array<{ topic: string; partition: number }> = []
		fmAny.fetchBuffer = {
			add: vi.fn((entry: { topic: string; partition: number }) => {
				addedToBuffer.push({ topic: entry.topic, partition: entry.partition })
			}),
			removePartitions: vi.fn(),
			removePartitionsReturningEarliest: vi.fn(() => new Map()),
			isFull: () => false,
		}

		fmAny.decodeRecords = vi.fn().mockReturnValue({
			records: [
				{ offset: 10n, key: null, value: Buffer.from('a'), headers: {}, timestamp: 0n },
				{ offset: 11n, key: null, value: Buffer.from('b'), headers: {}, timestamp: 0n },
			],
			nextOffset: 12n,
		})

		fm.addPartitions([{ topic: 't', partition: 0, offset: 10n }])
		const state = fmAny.partitionStates.get('t:0')

		const inflight: Promise<void> = fmAny.fetchFromBrokerToBuffer(broker, [state])

		// Pause the partition while its fetch is still in flight.
		fm.pausePartitions([{ topic: 't', partition: 0 }])

		resolveFetch(FETCH_RESPONSE)

		await inflight

		// Paused mid-flight: the records must not be delivered and the fetch offset must
		// not advance, so they are re-fetched after resume.
		expect(addedToBuffer).toEqual([])
		expect(state.offset).toBe(10n)
	})

	it('drops a stale in-flight fetch and keeps the new position when seek lands mid-flight', async () => {
		let resolveFetch: (resp: unknown) => void = () => {}
		const fetchResponse = new Promise(resolve => {
			resolveFetch = resolve
		})

		const broker = {
			nodeId: 1,
			fetch: vi.fn().mockReturnValue(fetchResponse),
		}
		const cluster = {
			getLeaderForPartition: vi.fn().mockResolvedValue(broker),
			getLogger: () => null,
		}

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const offsetManager = new OffsetManager(cluster as any, 'g1')
		const fm = new FetchManager(
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			cluster as any,
			offsetManager,
			'earliest',
			{
				maxBytesPerPartition: 1024,
				minBytes: 1,
				maxWaitMs: 50,
				partitionConcurrency: 1,
				isolationLevel: 'read_uncommitted',
			}
		)
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const fmAny = fm as any

		const addedToBuffer: Array<{ topic: string; partition: number }> = []
		const removePartitionsSpy = vi.fn()
		fmAny.fetchBuffer = {
			add: vi.fn((entry: { topic: string; partition: number }) => {
				addedToBuffer.push({ topic: entry.topic, partition: entry.partition })
			}),
			removePartitions: removePartitionsSpy,
			isFull: () => false,
		}

		fmAny.decodeRecords = vi.fn().mockReturnValue({
			records: [
				{ offset: 10n, key: null, value: Buffer.from('a'), headers: {}, timestamp: 0n },
				{ offset: 11n, key: null, value: Buffer.from('b'), headers: {}, timestamp: 0n },
			],
			nextOffset: 12n,
		})

		fm.addPartitions([{ topic: 't', partition: 0, offset: 10n }])
		const state = fmAny.partitionStates.get('t:0')

		const inflight: Promise<void> = fmAny.fetchFromBrokerToBuffer(broker, [state])

		// Seek (backward) while the fetch issued at offset 10 is in flight.
		fm.seekPartition('t', 0, 5n)

		resolveFetch(FETCH_RESPONSE)

		await inflight

		// The stale response must not clobber the seeked position or buffer its records.
		expect(state.offset).toBe(5n)
		expect(addedToBuffer).toEqual([])
		// seek() also drops any records buffered before the seek.
		expect(removePartitionsSpy).toHaveBeenCalledWith([{ topic: 't', partition: 0 }])
	})

	it('does not let an OffsetOutOfRange reset clobber a seek that lands during the reset lookup', async () => {
		const broker = { nodeId: 1, fetch: vi.fn() }
		const cluster = {
			getLeaderForPartition: vi.fn().mockResolvedValue(broker),
			getLogger: () => null,
		}

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const offsetManager = new OffsetManager(cluster as any, 'g1')
		let resolveReset: (offset: bigint) => void = () => {}
		const resetLookup = new Promise<bigint>(resolve => {
			resolveReset = resolve
		})
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(offsetManager as any).getEarliestOffset = vi.fn().mockReturnValue(resetLookup)

		const fm = new FetchManager(
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			cluster as any,
			offsetManager,
			'earliest',
			{
				maxBytesPerPartition: 1024,
				minBytes: 1,
				maxWaitMs: 50,
				partitionConcurrency: 1,
				isolationLevel: 'read_uncommitted',
			}
		)
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const fmAny = fm as any

		fm.addPartitions([{ topic: 't', partition: 0, offset: 100n }])
		const state = fmAny.partitionStates.get('t:0')

		// Begin handling OffsetOutOfRange (awaits the earliest-offset lookup).
		const handling: Promise<void> = fmAny.handleFetchError(state, ErrorCode.OffsetOutOfRange)

		// Seek to 5 while the reset lookup is in flight.
		fm.seekPartition('t', 0, 5n)

		// Reset would move to 0, but the seek must win.
		resolveReset(0n)
		await handling

		expect(state.offset).toBe(5n)
	})
})
