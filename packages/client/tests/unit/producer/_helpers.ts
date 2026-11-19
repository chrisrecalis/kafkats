import { mockDeep, type DeepMockProxy } from 'vitest-mock-extended'

import type { Cluster } from '@/client/cluster.js'
import type { Broker } from '@/client/broker.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { ProduceResponse } from '@/protocol/messages/responses/produce.js'
import { decodeRecordBatch } from '@/protocol/records/record-batch.js'

export type MockCluster = DeepMockProxy<Cluster>
export type MockBroker = DeepMockProxy<Broker>

export function createMockCluster(opts: { partitionCount?: number } = {}): MockCluster {
	const partitionCount = opts.partitionCount ?? 1
	const partitions = new Map(
		Array.from({ length: partitionCount }, (_, i) => [
			i,
			{ partitionIndex: i, leaderId: 1, leaderEpoch: 0, replicaNodes: [1], isrNodes: [1], offlineReplicas: [] },
		])
	)

	const cluster = mockDeep<Cluster>()
	cluster.refreshMetadata.mockResolvedValue({
		clusterId: 'test',
		controllerId: 1,
		brokers: new Map([[1, { nodeId: 1, host: 'localhost', port: 9092, rack: null }]]),
		topics: new Map([['test-topic', { name: 'test-topic', topicId: 'test-id', isInternal: false, partitions }]]),
		updatedAt: Date.now(),
	})
	return cluster
}

export function createMockBroker(nodeId: number): MockBroker {
	const broker = mockDeep<Broker>()
	Object.assign(broker, {
		nodeId,
		host: `broker-${nodeId}.test`,
		port: 9092,
		isConnected: true,
	})
	broker.getApiVersion.mockReturnValue(9)
	return broker
}

/**
 * A cluster whose transaction coordinator, group coordinator and partition leader answer every transactional request
 * of `topic` partition 0 with success (producer id 1, epoch 0).
 */
export function createTransactionalMocks(topic = 'test-topic', options: { partitionCount?: number } = {}) {
	const cluster = createMockCluster(options)
	const txnCoordinator = createMockBroker(1)
	const groupCoordinator = createMockBroker(2)
	const leader = createMockBroker(3)

	cluster.getCoordinator.mockImplementation(async type =>
		type === 'TRANSACTION' ? txnCoordinator : groupCoordinator
	)
	cluster.getAnyBroker.mockResolvedValue(txnCoordinator)
	cluster.getLeaderForPartition.mockResolvedValue(leader)

	txnCoordinator.initProducerId.mockResolvedValue({
		throttleTimeMs: 0,
		errorCode: ErrorCode.None,
		producerId: 1n,
		producerEpoch: 0,
	})
	txnCoordinator.addPartitionsToTxn.mockResolvedValue({
		throttleTimeMs: 0,
		errorCode: ErrorCode.None,
		results: [{ name: topic, resultsByPartition: [{ partitionIndex: 0, errorCode: ErrorCode.None }] }],
	})
	txnCoordinator.addOffsetsToTxn.mockResolvedValue({ throttleTimeMs: 0, errorCode: ErrorCode.None })
	txnCoordinator.endTxn.mockResolvedValue({ throttleTimeMs: 0, errorCode: ErrorCode.None })
	groupCoordinator.txnOffsetCommit.mockResolvedValue({
		throttleTimeMs: 0,
		topics: [{ name: topic, partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None }] }],
	})
	leader.produce.mockResolvedValue(
		buildProduceResponse([{ name: topic, partitions: [{ partitionIndex: 0, errorCode: ErrorCode.None }] }])
	)

	return { cluster, txnCoordinator, groupCoordinator, leader }
}

type TransactionalMocks = ReturnType<typeof createTransactionalMocks>
type TransactionStatus = 'Empty' | 'Ongoing' | 'PrepareCommit' | 'PrepareAbort' | 'CompleteCommit' | 'CompleteAbort'

/**
 * The transaction coordinator of one transactional id and the partition leader, after Kafka 3.9
 * (TransactionCoordinator.handleInitProducerId, prepareInitProducerIdTransit, endTransaction,
 * handleAddPartitionsToTransaction; TransactionMetadata.prepareFenceProducerEpoch, prepareIncrementProducerEpoch),
 * installed on the mocks:
 *
 * - The first InitProducerId creates the transactional id's metadata with a new producer id (1, then 2, ...) and no
 *   epoch yet; so does one sent once the id expired (`expire()`).
 * - InitProducerId with a producer id other than the metadata's is PRODUCER_FENCED, unless the metadata has no epoch
 *   yet (it was just created: Kafka's `isValidProducerId`).
 * - InitProducerId during an ongoing transaction fences its epoch: the coordinator aborts the transaction under
 *   epoch + 1 and forgets the last epoch, answering CONCURRENT_TRANSACTIONS, so a retry with the epoch of before is
 *   PRODUCER_FENCED. It only checks the producer id, so a fenced producer's InitProducerId aborts its successor's
 *   ongoing transaction (`fenceAborts` counts those aborts).
 * - InitProducerId with the current epoch bumps it, with the last one (a retry) answers the current one, with another
 *   one is PRODUCER_FENCED; without an identity (a new producer) it bumps the epoch.
 * - EndTxn, AddPartitionsToTxn and AddOffsetsToTxn require the metadata's producer id (else, or once the id expired,
 *   INVALID_PRODUCER_ID_MAPPING) and the current epoch (else PRODUCER_FENCED). EndTxn of a transaction that was never
 *   ongoing is INVALID_TXN_STATE (unless it repeats the outcome it completed with).
 * - Markers are written lazily: the first request after EndTxn or a fence (whose epoch is current) is answered
 *   CONCURRENT_TRANSACTIONS and completes them.
 * - The leader rejects a batch whose epoch is below that of the last markers of its producer id with
 *   INVALID_PRODUCER_EPOCH (KIP-588); answers queued with `mockResolvedValueOnce` take precedence.
 *
 * `successor()` is a new producer with the same transactional id initializing, `successorBegins()` its first
 * AddPartitionsToTxn, `successorEndTxn()` its EndTxn. `requests` lists what this producer sent, with the answers.
 * `staleEpochError` is the coordinator's answer to a stale epoch: PRODUCER_FENCED, or INVALID_PRODUCER_EPOCH from a
 * coordinator older than KIP-588.
 */
export function modelTransactions({ txnCoordinator, leader }: Pick<TransactionalMocks, 'txnCoordinator' | 'leader'>) {
	const model = {
		/** The producer id of the transactional id's metadata; null before the first InitProducerId and once expired. */
		producerId: null as bigint | null,
		epoch: -1,
		lastEpoch: -1,
		status: 'Empty' as TransactionStatus,
		staleEpochError: ErrorCode.ProducerFenced as ErrorCode,
		/** Producer id and epoch of the last transaction markers written. */
		markerProducerId: -1n,
		markerEpoch: -1,
		/** Ongoing transactions aborted by an InitProducerId that fenced their epoch, or by their timeout. */
		fenceAborts: 0,
		requests: [] as string[],
		/** The epoch the last `successor()` initialized with. */
		successorEpoch: -1,
		successor(): number {
			create()
			completeMarkers()
			if (model.status === 'Ongoing') fenceOngoing()
			completeMarkers()
			bump()
			model.successorEpoch = model.epoch
			return model.epoch
		},
		successorBegins(): void {
			if (add(model.producerId!, model.successorEpoch) !== ErrorCode.None) {
				throw new Error('the successor could not begin')
			}
		},
		successorEndTxn(committed: boolean): ErrorCode {
			return endTxn(model.producerId!, model.successorEpoch, committed)
		},
		/**
		 * The transactional id expires (`transactional.id.expiration.ms` without a transaction): the coordinator forgets
		 * its metadata. A transaction still ongoing timed out before (aborted under epoch + 1, like a fence).
		 */
		expire(): void {
			completeMarkers()
			if (model.status === 'Ongoing') fenceOngoing()
			completeMarkers()
			model.producerId = null
		},
	}

	let nextProducerId = 1n
	/** Metadata for a transactional id that has none: a new producer id, no epoch yet. */
	const create = (): void => {
		if (model.producerId !== null) return
		model.producerId = nextProducerId++
		model.epoch = -1
		model.lastEpoch = -1
		model.status = 'Empty'
	}
	const completeMarkers = (): boolean => {
		if (model.status !== 'PrepareCommit' && model.status !== 'PrepareAbort') return false
		model.status = model.status === 'PrepareCommit' ? 'CompleteCommit' : 'CompleteAbort'
		model.markerProducerId = model.producerId!
		model.markerEpoch = model.epoch
		return true
	}
	const fenceOngoing = (): void => {
		model.epoch += 1
		model.lastEpoch = -1
		model.status = 'PrepareAbort'
		model.fenceAborts += 1
	}
	const bump = (expected?: number): void => {
		model.lastEpoch = expected === undefined ? -1 : model.epoch
		model.epoch += 1
		model.status = 'Empty'
	}
	const endTxn = (producerId: bigint, epoch: number, committed: boolean): ErrorCode => {
		if (producerId !== model.producerId) return ErrorCode.InvalidProducerIdMapping
		if (epoch !== model.epoch) return model.staleEpochError
		if (completeMarkers()) return ErrorCode.ConcurrentTransactions
		if (model.status === 'Ongoing') {
			model.status = committed ? 'PrepareCommit' : 'PrepareAbort'
			return ErrorCode.None
		}
		return model.status === (committed ? 'CompleteCommit' : 'CompleteAbort')
			? ErrorCode.None
			: ErrorCode.InvalidTxnState
	}
	/** AddPartitionsToTxn and AddOffsetsToTxn. */
	const add = (producerId: bigint, epoch: number): ErrorCode => {
		if (producerId !== model.producerId) return ErrorCode.InvalidProducerIdMapping
		if (epoch !== model.epoch) return model.staleEpochError
		if (completeMarkers()) return ErrorCode.ConcurrentTransactions
		model.status = 'Ongoing'
		return ErrorCode.None
	}
	const record = (request: string, errorCode: ErrorCode): ErrorCode => {
		model.requests.push(`${request}${errorCode === ErrorCode.None ? '' : `: ${ErrorCode[errorCode]}`}`)
		return errorCode
	}

	txnCoordinator.initProducerId.mockImplementation(async ({ producerId, producerEpoch }) => {
		const identity = producerId === undefined ? '' : `${producerId}, ${producerEpoch}`
		const answer = (errorCode: ErrorCode) => ({
			throttleTimeMs: 0,
			errorCode: record(`InitProducerId(${identity})`, errorCode),
			producerId: errorCode === ErrorCode.None ? model.producerId! : -1n,
			producerEpoch: errorCode === ErrorCode.None ? model.epoch : -1,
		})
		create()
		if (producerId !== undefined && producerId !== model.producerId && model.epoch !== -1) {
			return answer(ErrorCode.ProducerFenced)
		}
		if (completeMarkers()) return answer(ErrorCode.ConcurrentTransactions)
		if (model.status === 'Ongoing') {
			fenceOngoing()
			return answer(ErrorCode.ConcurrentTransactions)
		}
		if (producerEpoch === undefined || model.epoch === -1 || producerEpoch === model.epoch) {
			bump(producerEpoch)
		} else if (producerEpoch !== model.lastEpoch) {
			return answer(model.staleEpochError)
		}
		return answer(ErrorCode.None)
	})
	txnCoordinator.endTxn.mockImplementation(async ({ producerId, producerEpoch, committed }) => ({
		throttleTimeMs: 0,
		errorCode: record(
			`EndTxn(${committed ? 'commit' : 'abort'}, ${producerEpoch})`,
			endTxn(producerId, producerEpoch, committed)
		),
	}))
	txnCoordinator.addPartitionsToTxn.mockImplementation(async ({ producerId, producerEpoch, topics }) => {
		const errorCode = record(`AddPartitionsToTxn(${producerEpoch})`, add(producerId, producerEpoch))
		return {
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			results: topics.map(({ name, partitions }) => ({
				name,
				resultsByPartition: partitions.map(partitionIndex => ({ partitionIndex, errorCode })),
			})),
		}
	})
	txnCoordinator.addOffsetsToTxn.mockImplementation(async ({ producerId, producerEpoch }) => ({
		throttleTimeMs: 0,
		errorCode: record(`AddOffsetsToTxn(${producerEpoch})`, add(producerId, producerEpoch)),
	}))
	leader.produce.mockImplementation(async ({ topics }) => {
		const answers = await Promise.all(
			topics.map(async ({ name, partitions }) => ({
				name,
				partitions: await Promise.all(
					partitions.map(async ({ partitionIndex, records }) => {
						const { producerId, producerEpoch } = await decodeRecordBatch(records)
						const superseded = producerId === model.markerProducerId && producerEpoch < model.markerEpoch
						return {
							partitionIndex,
							errorCode: superseded ? ErrorCode.InvalidProducerEpoch : ErrorCode.None,
						}
					})
				),
			}))
		)
		return buildProduceResponse(answers)
	})
	return model
}

export function buildProduceResponse(
	topics: Array<{
		name: string
		partitions: Array<{
			partitionIndex: number
			errorCode: ErrorCode
			baseOffset?: bigint
			logAppendTimeMs?: bigint
		}>
	}>
): ProduceResponse {
	return {
		topics: topics.map(t => ({
			name: t.name,
			partitions: t.partitions.map(p => ({
				partitionIndex: p.partitionIndex,
				errorCode: p.errorCode,
				baseOffset: p.baseOffset ?? 0n,
				logAppendTimeMs: p.logAppendTimeMs ?? BigInt(Date.now()),
				logStartOffset: 0n,
				recordErrors: [],
				errorMessage: null,
			})),
		})),
		throttleTimeMs: 0,
	}
}
