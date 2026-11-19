import { describe, expect, it, vi } from 'vitest'

import { OffsetOutOfRangeError } from '@/client/errors.js'
import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import type { AutoOffsetReset, PartitionBatch } from '@/consumer/types.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { fetchManagerHarness, fetchResponse, offsetsOf, recordBatch, TOPIC, tp } from './fetch-harness.js'

const OUT_OF_RANGE = { errorCode: ErrorCode.OffsetOutOfRange }

describe('FetchManager OffsetOutOfRange handling', () => {
	it("surfaces OffsetOutOfRangeError from poll() when the policy is 'none' and nothing handles it", async () => {
		const { fm, fetch } = await fetchManagerHarness({ autoOffsetReset: 'none' })
		fm.addPartitions([{ ...tp(0), offset: 100n }])

		await fetch(fetchResponse({ partition: 0, ...OUT_OF_RANGE }), 0)

		const error = await fm.poll().catch((cause: unknown) => cause)
		expect(error).toBeInstanceOf(OffsetOutOfRangeError)
		expect(error).toMatchObject({ topic: TOPIC, partition: 0, offset: 100n })
	})

	it("surfaces OffsetOutOfRangeError from poll() under 'none' when the callback reports nobody listens", async () => {
		const onOffsetOutOfRange = vi.fn(() => false)
		const { fm, fetch } = await fetchManagerHarness({ autoOffsetReset: 'none', config: { onOffsetOutOfRange } })
		fm.addPartitions([{ ...tp(0), offset: 100n }])

		await fetch(fetchResponse({ partition: 0, ...OUT_OF_RANGE }), 0)

		expect(onOffsetOutOfRange).toHaveBeenCalledExactlyOnceWith({ topic: TOPIC, partition: 0, offset: 100n })
		await expect(fm.poll()).rejects.toBeInstanceOf(OffsetOutOfRangeError)
	})

	it("pauses the partition and reports it under 'none', and the run goes on", async () => {
		const pending: PartitionBatch[] = []
		const onOffsetOutOfRange = vi.fn(() => true)
		const { fm, fetch } = await fetchManagerHarness({
			autoOffsetReset: 'none',
			config: { onOffsetOutOfRange, undelivered: pending, maxBytesPerPartition: 150 },
		})
		fm.addPartitions([
			{ ...tp(0), offset: 10n },
			{ ...tp(1), offset: 0n },
		])
		await fetch(fetchResponse({ partition: 0, records: recordBatch(10n, 4), highWatermark: 14n }), 0)
		await fm.poll()

		// The log was truncated below the fetch position (14) while records 10-13 wait for delivery.
		await fetch(fetchResponse({ partition: 0, ...OUT_OF_RANGE }), 0)

		expect(onOffsetOutOfRange).toHaveBeenCalledExactlyOnceWith({ topic: TOPIC, partition: 0, offset: 14n })
		const [metrics] = fm.partitionMetrics()
		expect(metrics).toMatchObject({ paused: true, bufferedRecords: 0, position: 10n, nextOffset: 10n })
		expect(fm.isBatchAssigned(pending[0]!)).toBe(false)

		// Other partitions are still delivered.
		await fetch(fetchResponse({ partition: 1, records: recordBatch(0n, 1), highWatermark: 1n }), 1)
		const batches = await fm.poll()
		expect(batches.map(batch => [batch.partition, offsetsOf(batch)])).toEqual([[1, [0n]]])
	})

	it("resets to the earliest offset under 'earliest', discards the buffered records and reports it", async () => {
		const onOffsetReset = vi.fn()
		const { fm, fetch } = await fetchManagerHarness({
			autoOffsetReset: 'earliest',
			// Retention has advanced the log start to 200.
			offsetManager: { getEarliestOffset: vi.fn().mockResolvedValue(200n) },
			config: { onOffsetReset },
		})
		fm.addPartitions([{ ...tp(0), offset: 100n }])
		await fetch(fetchResponse({ partition: 0, records: recordBatch(100n, 2), highWatermark: 300n }), 0)

		await fetch(fetchResponse({ partition: 0, ...OUT_OF_RANGE }), 0)

		expect(fm.partitionMetrics()[0]).toMatchObject({ position: 200n, nextOffset: 200n, bufferedRecords: 0 })
		expect(onOffsetReset).toHaveBeenCalledExactlyOnceWith({
			topic: TOPIC,
			partition: 0,
			from: 102n,
			to: 200n,
			reason: 'out-of-range',
		})
	})

	it("resets to the latest offset under 'latest'", async () => {
		const onOffsetReset = vi.fn()
		const getLatestOffset = vi.fn().mockResolvedValue(50n)
		const { fm, fetch } = await fetchManagerHarness({
			autoOffsetReset: 'latest',
			offsetManager: { getLatestOffset },
			config: { onOffsetReset },
		})
		fm.addPartitions([{ ...tp(3), offset: 900n }])

		await fetch(fetchResponse({ partition: 3, ...OUT_OF_RANGE }), 3)

		expect(getLatestOffset).toHaveBeenCalledWith(TOPIC, 3)
		expect(fm.getFetchPosition(TOPIC, 3)).toBe(50n)
		expect(onOffsetReset).toHaveBeenCalledWith(expect.objectContaining({ from: 900n, to: 50n }))
	})

	it('evaluates a policy function for each partition', async () => {
		const policy = vi.fn((_topic: string, partition: number): AutoOffsetReset =>
			partition === 0 ? 'earliest' : 'none'
		)
		const onOffsetOutOfRange = vi.fn(() => true)
		const onOffsetReset = vi.fn()
		const { fm, fetch } = await fetchManagerHarness({
			autoOffsetReset: policy,
			offsetManager: { getEarliestOffset: vi.fn().mockResolvedValue(7n) },
			config: { onOffsetOutOfRange, onOffsetReset },
		})
		fm.addPartitions([
			{ ...tp(0), offset: 1n },
			{ ...tp(1), offset: 2n },
		])

		await fetch(fetchResponse({ partition: 0, ...OUT_OF_RANGE }, { partition: 1, ...OUT_OF_RANGE }), 0, 1)

		expect(policy.mock.calls).toEqual([
			[TOPIC, 0],
			[TOPIC, 1],
		])
		expect(onOffsetReset).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ partition: 0, to: 7n }))
		expect(onOffsetOutOfRange).toHaveBeenCalledExactlyOnceWith({ topic: TOPIC, partition: 1, offset: 2n })
		expect(fm.partitionMetrics().map(p => p.paused)).toEqual([false, true])
	})

	it('ends the run with the error of a policy function that throws', async () => {
		const failure = new Error('no policy for t')
		const { fm, fetch } = await fetchManagerHarness({
			autoOffsetReset: () => {
				throw failure
			},
		})
		fm.addPartitions([{ ...tp(0), offset: 1n }])

		await fetch(fetchResponse({ partition: 0, ...OUT_OF_RANGE }), 0)

		await expect(fm.poll()).rejects.toBe(failure)
	})

	it('neither resets nor reports a partition sought or revoked while the reset offset is looked up', async () => {
		let resolveLookup: (offset: bigint) => void = () => {}
		const getEarliestOffset = vi.fn(
			() =>
				new Promise<bigint>(resolve => {
					resolveLookup = resolve
				})
		)
		const onOffsetReset = vi.fn()
		const { fm, fetch } = await fetchManagerHarness({
			offsetManager: { getEarliestOffset },
			config: { onOffsetReset },
		})
		fm.addPartitions([
			{ ...tp(0), offset: 100n },
			{ ...tp(1), offset: 100n },
		])

		const sought = fetch(fetchResponse({ partition: 0, ...OUT_OF_RANGE }), 0)
		await vi.waitFor(() => expect(getEarliestOffset).toHaveBeenCalledTimes(1))
		fm.seekPartition(TOPIC, 0, 5n)
		resolveLookup(0n)
		await sought

		const revoked = fetch(fetchResponse({ partition: 1, ...OUT_OF_RANGE }), 1)
		await vi.waitFor(() => expect(getEarliestOffset).toHaveBeenCalledTimes(2))
		fm.removePartitions([tp(1)])
		resolveLookup(0n)
		await revoked

		expect(fm.getFetchPosition(TOPIC, 0)).toBe(5n)
		expect(onOffsetReset).not.toHaveBeenCalled()
	})

	it('discards buffered records BEFORE the listOffsets round-trip (no stale delivery window)', async () => {
		// The buffer must be cleared synchronously, before awaiting the offset lookup —
		// otherwise a concurrent poll() could drain the stale records while we await, and a
		// transient lookup failure would skip the clear entirely.
		const broker = {
			nodeId: 1,
			fetch: vi.fn().mockResolvedValue({
				topics: [
					{
						topic: 't',
						partitions: [{ partitionIndex: 0, errorCode: ErrorCode.OffsetOutOfRange, recordsData: null }],
					},
				],
			}),
		}

		const cluster = {
			getLeaderForPartition: vi.fn().mockResolvedValue(broker),
			getLogger: () => null,
		}

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const offsetManager = new OffsetManager(cluster as any, 'g1')

		// A slow listOffsets round-trip: resolve it only after we have observed the clear.
		let resolveOffset: (o: bigint) => void = () => {}
		const offsetLookup = new Promise<bigint>(resolve => {
			resolveOffset = resolve
		})
		let lookupStarted = false
		vi.spyOn(offsetManager, 'getEarliestOffset').mockImplementation(() => {
			lookupStarted = true
			return offsetLookup
		})

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

		let clearedDuringLookup = false
		fmAny.fetchBuffer = {
			add: vi.fn(),
			removePartitions: vi.fn(() => {
				// The clear must run before the (still-pending) offset lookup resolves.
				if (!lookupStarted) clearedDuringLookup = true
			}),
			isFull: () => false,
		}

		fm.addPartitions([{ topic: 't', partition: 0, offset: 100n }])
		const state = fmAny.partitionStates.get('t:0')

		const inflight: Promise<void> = fmAny.fetchFromBrokerToBuffer(broker, [state], 1048576)

		// Give the microtask queue a chance to reach the await on the offset lookup.
		await new Promise(resolve => setTimeout(resolve, 0))

		// Buffer was cleared before the lookup resolved (i.e. before the await).
		expect(clearedDuringLookup).toBe(true)
		expect(fmAny.fetchBuffer.removePartitions).toHaveBeenCalledWith([{ topic: 't', partition: 0 }])

		// Let the lookup complete and the handler finish.
		resolveOffset(200n)
		await inflight
		expect(state.offset).toBe(200n)
	})
})
