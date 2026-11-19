import { afterEach, describe, expect, it, test, vi } from 'vitest'

import type { Broker } from '@/client/broker.js'
import type { Cluster } from '@/client/cluster.js'
import {
	CommitFailedError,
	KafkaProtocolError,
	NoOffsetForPartitionError,
	RebalanceInProgressError,
	TimeoutError,
} from '@/client/errors.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import type { IsolationLevel } from '@/consumer/types.js'
import { diagnostics, type CommitDiagnostics } from '@/diagnostics.js'
import { ConnectionClosedError } from '@/network/errors.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { encodeOffsetCommitRequest, type OffsetCommitRequest } from '@/protocol/messages/requests/offset-commit.js'
import type { OffsetFetchPartitionResponse } from '@/protocol/messages/responses/offset-fetch.js'
import { Decoder } from '@/protocol/primitives/decoder.js'
import { Encoder } from '@/protocol/primitives/encoder.js'

/** How a fake coordinator answers an OffsetCommit: an error code for every partition, a thrown error, or never. */
type CommitAnswer = ErrorCode | Error | 'hang'

/** A coordinator answering the n-th OffsetCommit with `answers[n]` (the last answer repeats). */
function fakeCoordinator(...answers: CommitAnswer[]) {
	const requests: OffsetCommitRequest[] = []
	const offsetCommit = vi.fn(async (request: OffsetCommitRequest) => {
		requests.push(request)
		const answer = answers[Math.min(requests.length, answers.length) - 1] ?? ErrorCode.None
		if (answer === 'hang') return new Promise<never>(() => {})
		if (answer instanceof Error) throw answer
		return {
			throttleTimeMs: 0,
			topics: request.topics.map(topic => ({
				name: topic.name,
				partitions: topic.partitions.map(p => ({ partitionIndex: p.partitionIndex, errorCode: answer })),
			})),
		}
	})
	return { offsetCommit, requests, offsetFetch: vi.fn() }
}

type FakeCoordinator = ReturnType<typeof fakeCoordinator>

/** A manager of member `member-1`, generation 7, with t-0 and t-1 assigned; re-discovery finds `rediscovered`. */
function makeManager(coordinator: FakeCoordinator, rediscovered: FakeCoordinator = coordinator) {
	const cluster = {
		clientId: 'client-1',
		invalidateCoordinator: vi.fn(),
		getCoordinator: vi.fn().mockResolvedValue(rediscovered),
	}
	const manager = new OffsetManager(cluster as unknown as Cluster, 'g', 'instance-1')
	manager.updateGroupState('member-1', 7, coordinator as unknown as Broker)
	manager.addAssignedPartitions([
		{ topic: 't', partition: 0 },
		{ topic: 't', partition: 1 },
	])
	return { manager, cluster }
}

const tp0 = { topic: 't', partition: 0 }

afterEach(() => {
	vi.useRealTimers()
})

describe('OffsetManager', () => {
	test('addAssignedPartitions preserves existing assignments (cooperative rebalance)', () => {
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const cluster = {} as any
		const offsetManager = new OffsetManager(cluster, 'g')

		offsetManager.addAssignedPartitions([{ topic: 't', partition: 0 }])
		offsetManager.addAssignedPartitions([{ topic: 't', partition: 1 }])

		expect(offsetManager.isPartitionAssigned('t', 0)).toBe(true)
		expect(offsetManager.isPartitionAssigned('t', 1)).toBe(true)
	})

	test('removeAssignedPartitions only removes specified partitions', () => {
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const cluster = {} as any
		const offsetManager = new OffsetManager(cluster, 'g')

		offsetManager.addAssignedPartitions([
			{ topic: 't', partition: 0 },
			{ topic: 't', partition: 1 },
		])

		offsetManager.removeAssignedPartitions([{ topic: 't', partition: 0 }])

		expect(offsetManager.isPartitionAssigned('t', 0)).toBe(false)
		expect(offsetManager.isPartitionAssigned('t', 1)).toBe(true)
	})
})

describe('OffsetManager.commitExplicit', () => {
	it('sends one OffsetCommit with metadata and leader epochs under the member generation', async () => {
		const coordinator = fakeCoordinator(ErrorCode.None)
		const { manager } = makeManager(coordinator)

		await manager.commitExplicit([
			{ topic: 't', partition: 0, offset: 10n, metadata: 'AgAAAAAAAAAB', leaderEpoch: 3 },
			{ topic: 't', partition: 1, offset: 20n },
		])

		expect(coordinator.requests).toEqual([
			{
				groupId: 'g',
				generationId: 7,
				memberId: 'member-1',
				groupInstanceId: 'instance-1',
				topics: [
					{
						name: 't',
						partitions: [
							{
								partitionIndex: 0,
								committedOffset: 10n,
								committedLeaderEpoch: 3,
								committedMetadata: 'AgAAAAAAAAAB',
							},
							{
								partitionIndex: 1,
								committedOffset: 20n,
								committedLeaderEpoch: undefined,
								committedMetadata: null,
							},
						],
					},
				],
			},
		])

		// On the wire (OffsetCommit v7): offset, leader epoch (-1: none) and metadata (null: none) per partition.
		const encoder = new Encoder()
		encodeOffsetCommitRequest(encoder, 7, coordinator.requests[0]!)
		const decoder = new Decoder(encoder.toBuffer())
		expect([decoder.readString(), decoder.readInt32(), decoder.readString(), decoder.readNullableString()]).toEqual(
			['g', 7, 'member-1', 'instance-1']
		)
		const topics = decoder.readArray(topic => ({
			name: topic.readString(),
			partitions: topic.readArray(p => [p.readInt32(), p.readInt64(), p.readInt32(), p.readNullableString()]),
		}))
		expect(topics).toEqual([
			{
				name: 't',
				partitions: [
					[0, 10n, 3, 'AgAAAAAAAAAB'],
					[1, 20n, -1, null],
				],
			},
		])
	})

	it('commits generation -1 and no member without a group', async () => {
		const coordinator = fakeCoordinator(ErrorCode.None)
		const cluster = { clientId: 'client-1' }
		const manager = new OffsetManager(cluster as unknown as Cluster, 'g')
		manager.updateGroupState('', -1, coordinator as unknown as Broker)
		manager.addAssignedPartitions([{ topic: 't', partition: 0 }])

		await manager.commitExplicit([{ topic: 't', partition: 0, offset: 1n }])

		expect(coordinator.requests[0]).toMatchObject({ generationId: -1, memberId: '', groupInstanceId: null })
	})

	it('skips partitions not assigned to this member, and sends nothing when none is', async () => {
		const coordinator = fakeCoordinator(ErrorCode.None)
		const { manager } = makeManager(coordinator)

		await manager.commitExplicit([{ topic: 'other', partition: 0, offset: 5n }])
		expect(coordinator.offsetCommit).not.toHaveBeenCalled()

		await manager.commitExplicit([
			{ topic: 't', partition: 2, offset: 5n },
			{ topic: 't', partition: 1, offset: 6n },
		])
		expect(coordinator.requests[0]!.topics).toEqual([
			{
				name: 't',
				partitions: [
					{
						partitionIndex: 1,
						committedOffset: 6n,
						committedLeaderEpoch: undefined,
						committedMetadata: null,
					},
				],
			},
		])
	})

	it('commits the last of several offsets given for one partition', async () => {
		const coordinator = fakeCoordinator(ErrorCode.None)
		const { manager } = makeManager(coordinator)

		await manager.commitExplicit([
			{ topic: 't', partition: 0, offset: 5n, metadata: 'first' },
			{ topic: 't', partition: 0, offset: 5n, metadata: 'last' },
		])

		expect(coordinator.requests[0]!.topics[0]!.partitions).toEqual([
			{ partitionIndex: 0, committedOffset: 5n, committedLeaderEpoch: undefined, committedMetadata: 'last' },
		])
	})

	it('throws when no group coordinator is known yet', async () => {
		const manager = new OffsetManager({} as Cluster, 'g')
		manager.addAssignedPartitions([{ topic: 't', partition: 0 }])

		await expect(manager.commitExplicit([{ topic: 't', partition: 0, offset: 1n }])).rejects.toThrow(
			/group coordinator/
		)
	})

	it('retries a moved coordinator, then commits to the one it re-discovered', async () => {
		const moved = fakeCoordinator(ErrorCode.NotCoordinator)
		const current = fakeCoordinator(ErrorCode.None)
		const { manager, cluster } = makeManager(moved, current)

		await manager.commitExplicit([{ topic: 't', partition: 0, offset: 10n }])

		expect(moved.offsetCommit).toHaveBeenCalledTimes(1)
		expect(cluster.invalidateCoordinator).toHaveBeenCalledWith('GROUP', 'g')
		expect(current.requests).toHaveLength(1)
		expect(current.requests[0]!.topics[0]!.partitions[0]!.committedOffset).toBe(10n)
	})

	it.each([
		['a connection failure', new ConnectionClosedError()],
		['CoordinatorLoadInProgress', ErrorCode.CoordinatorLoadInProgress],
		['CoordinatorNotAvailable', ErrorCode.CoordinatorNotAvailable],
		['RequestTimedOut', ErrorCode.RequestTimedOut],
	] as const)('retries %s with backoff', async (_, failure) => {
		vi.useFakeTimers()
		const coordinator = fakeCoordinator(failure, failure, ErrorCode.None)
		const { manager } = makeManager(coordinator)

		const commit = manager.commitExplicit([{ topic: 't', partition: 0, offset: 10n }])
		await vi.advanceTimersByTimeAsync(99)
		expect(coordinator.offsetCommit).toHaveBeenCalledTimes(1)
		await vi.advanceTimersByTimeAsync(1)
		expect(coordinator.offsetCommit).toHaveBeenCalledTimes(2)
		await vi.advanceTimersByTimeAsync(200) // the backoff doubles
		await commit

		expect(coordinator.offsetCommit).toHaveBeenCalledTimes(3)
	})

	it.each([
		[
			'the member rejoined',
			(manager: OffsetManager, broker: Broker) => manager.updateGroupState('member-1', 8, broker),
		],
		['the partition was revoked or lost', (manager: OffsetManager) => manager.removeAssignedPartitions([tp0])],
	])('stops retrying with CommitFailedError once %s, sending nothing more', async (_, change) => {
		vi.useFakeTimers()
		const coordinator = fakeCoordinator(ErrorCode.NotCoordinator, ErrorCode.None)
		const { manager } = makeManager(coordinator)

		const commit = manager.commitExplicit([{ topic: 't', partition: 0, offset: 100n, metadata: 'm' }])
		const settled = commit.catch(e => e)
		await vi.advanceTimersByTimeAsync(0)
		expect(coordinator.requests).toHaveLength(1)
		// While it backs off, the group rebalances: the partition may belong to another member now.
		change(manager, coordinator as unknown as Broker)
		await vi.advanceTimersByTimeAsync(100)

		const error = await settled
		expect(error).toBeInstanceOf(CommitFailedError)
		expect(error.errorCode).toBe(ErrorCode.IllegalGeneration)
		expect(coordinator.requests).toHaveLength(1)
	})

	it('rejects RebalanceInProgress with RebalanceInProgressError without retrying', async () => {
		const coordinator = fakeCoordinator(ErrorCode.RebalanceInProgress)
		const { manager } = makeManager(coordinator)

		const error = await manager.commitExplicit([{ topic: 't', partition: 0, offset: 10n }]).catch(e => e)

		expect(error).toBeInstanceOf(RebalanceInProgressError)
		expect(error.groupId).toBe('g')
		expect(coordinator.offsetCommit).toHaveBeenCalledTimes(1)
	})

	it.each([ErrorCode.IllegalGeneration, ErrorCode.UnknownMemberId, ErrorCode.FencedInstanceId])(
		'rejects error code %i (generation gone) with CommitFailedError without retrying',
		async code => {
			const coordinator = fakeCoordinator(code)
			const { manager } = makeManager(coordinator)

			const error = await manager.commitExplicit([{ topic: 't', partition: 0, offset: 10n }]).catch(e => e)

			expect(error).toBeInstanceOf(CommitFailedError)
			expect(error.errorCode).toBe(code)
			expect(error.cause).toBeInstanceOf(KafkaProtocolError)
			expect(error.cause.errorCode).toBe(code)
			expect(coordinator.offsetCommit).toHaveBeenCalledTimes(1)
		}
	)

	it.each([ErrorCode.OffsetMetadataTooLarge, ErrorCode.GroupAuthorizationFailed])(
		'rejects other error codes (%i) with the KafkaProtocolError without retrying',
		async code => {
			const coordinator = fakeCoordinator(code)
			const { manager } = makeManager(coordinator)

			const error = await manager.commitExplicit([{ topic: 't', partition: 0, offset: 10n }]).catch(e => e)

			expect(error.constructor).toBe(KafkaProtocolError)
			expect(error.errorCode).toBe(code)
			expect(coordinator.offsetCommit).toHaveBeenCalledTimes(1)
		}
	)

	it('rejects with a TimeoutError carrying the last failure once timeoutMs passed', async () => {
		vi.useFakeTimers()
		const coordinator = fakeCoordinator(ErrorCode.CoordinatorLoadInProgress)
		const { manager } = makeManager(coordinator)

		const commit = manager.commitExplicit([{ topic: 't', partition: 0, offset: 10n }], { timeoutMs: 2_000 })
		const settled = commit.catch(e => e)
		await vi.advanceTimersByTimeAsync(1_999)
		expect(coordinator.offsetCommit.mock.calls.length).toBeGreaterThan(2)
		await vi.advanceTimersByTimeAsync(1)
		const error = await settled

		expect(error).toBeInstanceOf(TimeoutError)
		expect(error.timeoutMs).toBe(2_000)
		expect(error.cause).toBeInstanceOf(KafkaProtocolError)
		expect(error.cause.errorCode).toBe(ErrorCode.CoordinatorLoadInProgress)
	})

	it('bounds a request the coordinator does not answer by timeoutMs', async () => {
		vi.useFakeTimers()
		const coordinator = fakeCoordinator('hang')
		const { manager } = makeManager(coordinator)

		const settled = manager
			.commitExplicit([{ topic: 't', partition: 0, offset: 10n }], { timeoutMs: 500 })
			.catch(e => e)
		await vi.advanceTimersByTimeAsync(500)
		const error = await settled

		expect(error).toBeInstanceOf(TimeoutError)
		expect(error.cause).toBeUndefined()
	})

	it('defaults the budget to defaultApiTimeoutMs', async () => {
		vi.useFakeTimers()
		const coordinator = fakeCoordinator('hang')
		const cluster = { clientId: 'client-1' }
		const manager = new OffsetManager(cluster as unknown as Cluster, 'g', undefined, undefined, 3_000)
		manager.updateGroupState('member-1', 7, coordinator as unknown as Broker)
		manager.addAssignedPartitions([{ topic: 't', partition: 0 }])

		const settled = manager.commitExplicit([{ topic: 't', partition: 0, offset: 10n }]).catch(e => e)
		await vi.advanceTimersByTimeAsync(2_999)
		await vi.advanceTimersByTimeAsync(1)

		expect(await settled).toMatchObject({ name: 'TimeoutError', timeoutMs: 3_000 })
	})

	it('rejects with the reason of an aborted signal and stops retrying', async () => {
		vi.useFakeTimers()
		const coordinator = fakeCoordinator(ErrorCode.NotCoordinator)
		const { manager } = makeManager(coordinator)
		const controller = new AbortController()

		const settled = manager
			.commitExplicit([{ topic: 't', partition: 0, offset: 10n }], { signal: controller.signal })
			.catch(e => e)
		await vi.advanceTimersByTimeAsync(150)
		controller.abort()
		const error = await settled
		const attempts = coordinator.offsetCommit.mock.calls.length
		await vi.advanceTimersByTimeAsync(5_000)

		expect(error).toBe(controller.signal.reason)
		expect(error.name).toBe('AbortError')
		expect(coordinator.offsetCommit).toHaveBeenCalledTimes(attempts)
	})

	it('stops waiting for a request in flight when the signal aborts', async () => {
		const coordinator = fakeCoordinator('hang')
		const { manager } = makeManager(coordinator)
		const controller = new AbortController()

		const settled = manager
			.commitExplicit([{ topic: 't', partition: 0, offset: 10n }], { signal: controller.signal })
			.catch(e => e)
		await vi.waitFor(() => expect(coordinator.offsetCommit).toHaveBeenCalled())
		controller.abort(new Error('closing'))

		expect(await settled).toEqual(new Error('closing'))
	})

	it('sends nothing when the signal is already aborted', async () => {
		const coordinator = fakeCoordinator(ErrorCode.None)
		const { manager } = makeManager(coordinator)

		await expect(
			manager.commitExplicit([{ topic: 't', partition: 0, offset: 10n }], { signal: AbortSignal.abort() })
		).rejects.toMatchObject({ name: 'AbortError' })
		expect(coordinator.offsetCommit).not.toHaveBeenCalled()
	})

	it('drops the consumed offsets the commit covers, and keeps those past it', async () => {
		const coordinator = fakeCoordinator(ErrorCode.None)
		const { manager } = makeManager(coordinator)
		manager.markConsumed('t', 0, 9n) // would commit 10
		manager.markConsumed('t', 1, 30n) // would commit 31

		await manager.commitExplicit([
			{ topic: 't', partition: 0, offset: 10n },
			{ topic: 't', partition: 1, offset: 20n },
		])
		expect(manager.pendingCommitCount).toBe(1)

		await manager.commitPendingOffsets()
		expect(coordinator.requests[1]!.topics[0]!.partitions).toEqual([
			{ partitionIndex: 1, committedOffset: 31n, committedLeaderEpoch: undefined, committedMetadata: null },
		])
	})

	it('is traced on diagnostics.commit', async () => {
		const coordinator = fakeCoordinator(ErrorCode.None)
		const { manager } = makeManager(coordinator)
		const traced: CommitDiagnostics[] = []
		const handlers = { start: (context: CommitDiagnostics) => void traced.push(context) }
		// Node calls the handlers a subscriber has; its types ask for all of them.
		const channel: { subscribe(handlers: object): void; unsubscribe(handlers: object): void } = diagnostics.commit
		channel.subscribe(handlers)
		try {
			await manager.commitExplicit([{ topic: 't', partition: 0, offset: 10n, metadata: 'm' }])
		} finally {
			channel.unsubscribe(handlers)
		}

		expect(traced).toEqual([
			{ clientId: 'client-1', groupId: 'g', offsets: [{ topic: 't', partition: 0, offset: 10n }] },
		])
	})
})

describe('OffsetManager.commitPendingOffsets', () => {
	it('keeps a consumed offset that moved past the offset of a commit in flight', async () => {
		let answer!: () => void
		const coordinator = fakeCoordinator(ErrorCode.None)
		const respond = coordinator.offsetCommit.getMockImplementation()!
		coordinator.offsetCommit.mockImplementationOnce(
			request => new Promise(resolve => (answer = () => resolve(respond(request))))
		)
		const { manager } = makeManager(coordinator)

		manager.markConsumed('t', 0, 4n)
		const commit = manager.commitPendingOffsets()
		manager.markConsumed('t', 0, 8n)
		answer()
		await commit

		expect(manager.pendingCommitCount).toBe(1)
		await manager.commitPendingOffsets()
		expect(coordinator.requests.map(r => r.topics[0]!.partitions[0]!.committedOffset)).toEqual([5n, 9n])
	})
})

describe('OffsetManager.fetchCommittedOffsets', () => {
	function fetchResponse(partitions: Omit<OffsetFetchPartitionResponse, 'errorCode'>[], errorCode = ErrorCode.None) {
		return {
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			topics: [{ name: 't', partitions: partitions.map(p => ({ ...p, errorCode })) }],
		}
	}

	it('returns offset, metadata and leader epoch, and omits partitions without a commit', async () => {
		const coordinator = fakeCoordinator()
		coordinator.offsetFetch.mockResolvedValue(
			fetchResponse([
				{ partitionIndex: 0, committedOffset: 42n, committedLeaderEpoch: 5, metadata: 'meta' },
				// The coordinator stores a commit without metadata as ''.
				{ partitionIndex: 1, committedOffset: 7n, committedLeaderEpoch: -1, metadata: '' },
				{ partitionIndex: 2, committedOffset: -1n, committedLeaderEpoch: -1, metadata: null },
			])
		)
		const { manager } = makeManager(coordinator)

		const committed = await manager.fetchCommittedOffsets([0, 1, 2].map(partition => ({ topic: 't', partition })))

		expect(committed).toStrictEqual(
			new Map([
				['t:0', { offset: 42n, metadata: 'meta', leaderEpoch: 5 }],
				['t:1', { offset: 7n, metadata: null }],
			])
		)
		expect(coordinator.offsetFetch).toHaveBeenCalledWith({
			groupId: 'g',
			requireStable: true,
			topics: [{ name: 't', partitions: [{ partitionIndex: 0 }, { partitionIndex: 1 }, { partitionIndex: 2 }] }],
		})
	})

	it('retries unstable offsets for up to timeoutMs, then rejects with a TimeoutError carrying the last failure', async () => {
		vi.useFakeTimers()
		const coordinator = fakeCoordinator()
		coordinator.offsetFetch.mockResolvedValue(
			fetchResponse(
				[{ partitionIndex: 0, committedOffset: -1n, committedLeaderEpoch: -1, metadata: null }],
				ErrorCode.UnstableOffsetCommit
			)
		)
		const { manager } = makeManager(coordinator)

		const settled = manager
			.fetchCommittedOffsets([{ topic: 't', partition: 0 }], { timeoutMs: 1_000 })
			.catch(e => e)
		await vi.advanceTimersByTimeAsync(1_000)
		const error = await settled
		const attempts = coordinator.offsetFetch.mock.calls.length
		await vi.advanceTimersByTimeAsync(5_000)

		expect(error).toBeInstanceOf(TimeoutError)
		expect(error.timeoutMs).toBe(1_000)
		expect(error.cause).toBeInstanceOf(KafkaProtocolError)
		expect(error.cause.errorCode).toBe(ErrorCode.UnstableOffsetCommit)
		expect(attempts).toBeGreaterThan(1)
		expect(coordinator.offsetFetch).toHaveBeenCalledTimes(attempts)
	})

	it('bounds a fetch the coordinator does not answer by timeoutMs', async () => {
		vi.useFakeTimers()
		const coordinator = fakeCoordinator()
		coordinator.offsetFetch.mockReturnValue(new Promise(() => {}))
		const { manager } = makeManager(coordinator)

		const settled = manager.fetchCommittedOffsets([{ topic: 't', partition: 0 }], { timeoutMs: 10 }).catch(e => e)
		await vi.advanceTimersByTimeAsync(1_000)

		const error = await Promise.race([settled, 'still pending'])
		expect(error).toBeInstanceOf(TimeoutError)
		expect(error.timeoutMs).toBe(10)
		expect(error.cause).toBeUndefined()
	})

	it('bounds a coordinator rediscovery that does not answer by timeoutMs, and sends nothing after it', async () => {
		vi.useFakeTimers()
		const coordinator = fakeCoordinator()
		coordinator.offsetFetch.mockResolvedValue({
			throttleTimeMs: 0,
			errorCode: ErrorCode.NotCoordinator,
			topics: [],
		})
		const { manager, cluster } = makeManager(coordinator)
		let lookedUp!: (found: FakeCoordinator) => void
		cluster.getCoordinator.mockReturnValueOnce(new Promise(resolve => (lookedUp = resolve)))

		const settled = manager.fetchCommittedOffsets([{ topic: 't', partition: 0 }], { timeoutMs: 500 }).catch(e => e)
		await vi.advanceTimersByTimeAsync(1_000)
		const error = await Promise.race([settled, 'still pending'])
		lookedUp(coordinator)
		await vi.advanceTimersByTimeAsync(5_000)

		expect(error).toBeInstanceOf(TimeoutError)
		expect(error.cause).toMatchObject({ errorCode: ErrorCode.NotCoordinator })
		expect(coordinator.offsetFetch).toHaveBeenCalledTimes(1)
	})

	it('rejects with the reason of an aborted signal and stops retrying', async () => {
		vi.useFakeTimers()
		const coordinator = fakeCoordinator()
		coordinator.offsetFetch.mockResolvedValue(
			fetchResponse(
				[{ partitionIndex: 0, committedOffset: -1n, committedLeaderEpoch: -1, metadata: null }],
				ErrorCode.UnstableOffsetCommit
			)
		)
		const { manager } = makeManager(coordinator)
		const controller = new AbortController()

		const settled = manager
			.fetchCommittedOffsets([{ topic: 't', partition: 0 }], { signal: controller.signal })
			.catch(e => e)
		await vi.advanceTimersByTimeAsync(150)
		controller.abort()
		const error = await settled
		const attempts = coordinator.offsetFetch.mock.calls.length
		await vi.advanceTimersByTimeAsync(5_000)

		expect(error.name).toBe('AbortError')
		expect(coordinator.offsetFetch).toHaveBeenCalledTimes(attempts)
	})
})

describe('OffsetManager.resolveStartingOffset', () => {
	it('starts at the committed offset', async () => {
		const manager = new OffsetManager({} as Cluster, 'g')
		const committed = new Map([['t:0', { offset: 42n, metadata: 'm' }]])

		await expect(manager.resolveStartingOffset('t', 0, 'none', committed)).resolves.toBe(42n)
	})

	it("throws NoOffsetForPartitionError without a commit under 'none'", async () => {
		const manager = new OffsetManager({} as Cluster, 'g')

		const error = await manager.resolveStartingOffset('t', 3, 'none', new Map()).catch(e => e)

		expect(error).toBeInstanceOf(NoOffsetForPartitionError)
		expect(error).toMatchObject({ topic: 't', partition: 3 })
	})
})

describe('OffsetManager offset lookups', () => {
	function makeLookupManager(isolationLevel?: IsolationLevel) {
		const listOffsets = vi.fn().mockResolvedValue({
			throttleTimeMs: 0,
			topics: [
				{
					name: 't',
					partitions: [{ partitionIndex: 0, errorCode: 0, timestamp: -1n, offset: 17n, leaderEpoch: 0 }],
				},
			],
		})
		const cluster = { getLeaderForPartition: vi.fn().mockResolvedValue({ listOffsets }) }
		const manager = new OffsetManager(
			cluster as unknown as Cluster,
			'g',
			undefined,
			undefined,
			60_000,
			isolationLevel
		)
		return { manager, listOffsets }
	}

	it.each([
		[undefined, 0],
		['read_uncommitted', 0],
		['read_committed', 1],
	] as const)('lists offsets at the isolation level of the consumer (%s: %i on the wire)', async (level, wire) => {
		// Under read_committed (1), the broker answers 'latest' with the last stable offset.
		const { manager, listOffsets } = makeLookupManager(level)

		await expect(manager.getLatestOffset('t', 0)).resolves.toBe(17n)
		await expect(manager.getEarliestOffset('t', 0)).resolves.toBe(17n)

		expect(listOffsets).toHaveBeenCalledTimes(2)
		for (const [request] of listOffsets.mock.calls) {
			expect(request.isolationLevel).toBe(wire)
		}
	})
})
