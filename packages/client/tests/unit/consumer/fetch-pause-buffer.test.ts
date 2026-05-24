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

describe('FetchManager pause with already-buffered records', () => {
	it('rewinds the fetch offset to the earliest buffered record when a partition is paused', async () => {
		const broker = { nodeId: 1, fetch: vi.fn().mockResolvedValue(FETCH_RESPONSE) }
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

		// Minimal in-memory stand-in for FetchBuffer that mirrors removePartitionsReturningEarliest.
		const added: Array<{ topic: string; partition: number }> = []
		const queue: Array<{ topic: string; partition: number; records: Array<{ offset: bigint }> }> = []
		fmAny.fetchBuffer = {
			add: vi.fn((entry: { topic: string; partition: number; records: Array<{ offset: bigint }> }) => {
				queue.push(entry)
				added.push({ topic: entry.topic, partition: entry.partition })
			}),
			removePartitions: vi.fn(),
			removePartitionsReturningEarliest: vi.fn((parts: Array<{ topic: string; partition: number }>) => {
				const keys = new Set(parts.map(p => `${p.topic}:${p.partition}`))
				const earliest = new Map<string, bigint>()
				for (let i = queue.length - 1; i >= 0; i--) {
					const f = queue[i]!
					const key = `${f.topic}:${f.partition}`
					if (!keys.has(key)) continue
					for (const r of f.records) {
						const cur = earliest.get(key)
						if (cur === undefined || r.offset < cur) earliest.set(key, r.offset)
					}
					queue.splice(i, 1)
				}
				return earliest
			}),
			isFull: () => false,
		}

		// Records buffered at offsets 10 and 11; the fetch offset advances to 12 when buffered.
		fmAny.decodeRecords = vi.fn().mockReturnValue([
			{ offset: 10n, key: null, value: Buffer.from('a'), headers: {}, timestamp: 0n },
			{ offset: 11n, key: null, value: Buffer.from('b'), headers: {}, timestamp: 0n },
		])

		fm.addPartitions([{ topic: 't', partition: 0, offset: 10n }])
		const state = fmAny.partitionStates.get('t:0')

		await fmAny.fetchFromBrokerToBuffer(broker, [state])
		expect(state.offset).toBe(12n) // advanced past the buffered records
		expect(added).toEqual([{ topic: 't', partition: 0 }])

		// Pausing must not lose the buffered-but-undelivered records: rewind so they are
		// re-fetched (and re-delivered) on resume.
		fm.pausePartitions([{ topic: 't', partition: 0 }])
		expect(state.paused).toBe(true)
		expect(state.offset).toBe(10n)
	})

	it('fences a fetch issued before the pause-rewind so it cannot clobber the rewound offset after resume', async () => {
		let resolveFetch: (resp: unknown) => void = () => {}
		const fetchResponse = new Promise(resolve => {
			resolveFetch = resolve
		})
		const broker = { nodeId: 1, fetch: vi.fn().mockReturnValue(fetchResponse) }
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
			// Records 10,11 are already buffered, so pause rewinds state.offset 12 -> 10.
			removePartitionsReturningEarliest: vi.fn(() => new Map([['t:0', 10n]])),
			isFull: () => false,
		}

		// The in-flight fetch (issued at offset 12) would return offsets 12,13.
		fmAny.decodeRecords = vi.fn().mockReturnValue([
			{ offset: 12n, key: null, value: Buffer.from('a'), headers: {}, timestamp: 0n },
			{ offset: 13n, key: null, value: Buffer.from('b'), headers: {}, timestamp: 0n },
		])

		fm.addPartitions([{ topic: 't', partition: 0, offset: 12n }])
		const state = fmAny.partitionStates.get('t:0')

		// Fetch issued while state.offset === 12 (mirrors a fetch in flight when pause lands).
		const inflight: Promise<void> = fmAny.fetchFromBrokerToBuffer(broker, [state])

		// Pause rewinds to 10 (records 10,11 were buffered-but-undelivered), then resume reopens.
		fm.pausePartitions([{ topic: 't', partition: 0 }])
		expect(state.offset).toBe(10n)
		fm.resumePartitions([{ topic: 't', partition: 0 }])

		// The pre-rewind fetch (issued at 12) now resolves. It must be fenced: if buffered, it would
		// advance state.offset to 14 and silently drop the rewound records 10,11.
		resolveFetch(FETCH_RESPONSE)
		await inflight

		expect(addedToBuffer).toEqual([])
		expect(state.offset).toBe(10n)
	})
})
