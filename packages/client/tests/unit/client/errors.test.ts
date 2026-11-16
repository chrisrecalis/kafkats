import { describe, expect, it } from 'vitest'

import {
	CommitFailedError,
	GroupError,
	InvalidTxnStateError,
	KafkaError,
	KafkaProtocolError,
	LeaderNotAvailableError,
	NoOffsetForPartitionError,
	NotCoordinatorError,
	OffsetOutOfRangeError,
	ProducerFencedError,
	RebalanceInProgressError,
	TimeoutError,
	TransactionAbortedError,
	UnsupportedVersionError,
} from '@/client/errors.js'
import * as root from '@/index.js'
import { ApiKey } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

describe('client errors', () => {
	it('KafkaError defaults retriable based on error code', () => {
		const error = new KafkaError('oops', ErrorCode.LeaderNotAvailable)
		expect(error.retriable).toBe(true)
		const nonRetriable = new KafkaError('no', ErrorCode.InvalidTopicException)
		expect(nonRetriable.retriable).toBe(false)
	})

	it('KafkaProtocolError formats message with context', () => {
		const error = new KafkaProtocolError(ErrorCode.BrokerNotAvailable, 'fetch')
		expect(error.message).toContain('BrokerNotAvailable')
		expect(error.message).toContain('fetch')
	})

	it('UnsupportedVersionError includes range', () => {
		const error = new UnsupportedVersionError(ApiKey.Metadata, 9, { min: 0, max: 3 })
		expect(error.message).toContain('broker supports 0-3')
		expect(error.apiKey).toBe(ApiKey.Metadata)
	})

	it('specific error subclasses expose metadata', () => {
		const leader = new LeaderNotAvailableError('topic', 1)
		expect(leader.topic).toBe('topic')
		expect(leader.partition).toBe(1)
		const notCoordinator = new NotCoordinatorError('GROUP', 'group')
		expect(notCoordinator.coordinatorType).toBe('GROUP')
		expect(notCoordinator.key).toBe('group')
	})

	describe('CommitFailedError', () => {
		it.each([ErrorCode.IllegalGeneration, ErrorCode.UnknownMemberId, ErrorCode.FencedInstanceId])(
			'is a non-retriable group error carrying the broker error %s',
			code => {
				const cause = new KafkaProtocolError(code, 'OffsetCommit')
				const error = new CommitFailedError('group-a', cause)
				expect(error).toBeInstanceOf(GroupError)
				expect(error).toBeInstanceOf(KafkaError)
				expect(error.name).toBe('CommitFailedError')
				expect(error.groupId).toBe('group-a')
				expect(error.cause).toBe(cause)
				expect(error.errorCode).toBe(code)
				expect(error.retriable).toBe(false)
				expect(error.message).toContain('group-a')
				expect(error.message).toContain(cause.message)
			}
		)
	})

	describe('TransactionAbortedError', () => {
		it('defaults to outcome aborted', () => {
			const cause = new Error('boom')
			const error = new TransactionAbortedError('tx-1', cause)
			expect(error.outcome).toBe('aborted')
			expect(error.transactionalId).toBe('tx-1')
			expect(error.cause).toBe(cause)
			expect(error.retriable).toBe(false)
			expect(error.message).toBe('Transaction aborted for tx-1: boom')
		})

		it('reports an unknown outcome when the commit may have reached the broker', () => {
			const error = new TransactionAbortedError('tx-1', new Error('connection lost'), 'unknown')
			expect(error.outcome).toBe('unknown')
			expect(error.name).toBe('TransactionAbortedError')
			expect(error.message).toBe('Transaction commit outcome unknown for tx-1: connection lost')
		})
	})

	it('OffsetOutOfRangeError carries the partition and offset', () => {
		const error = new OffsetOutOfRangeError('orders', 3, 42n)
		expect(error).toBeInstanceOf(KafkaError)
		expect(error.name).toBe('OffsetOutOfRangeError')
		expect(error.topic).toBe('orders')
		expect(error.partition).toBe(3)
		expect(error.offset).toBe(42n)
		expect(error.errorCode).toBe(ErrorCode.OffsetOutOfRange)
		expect(error.retriable).toBe(false)
		expect(error.message).toBe('Offset 42 is out of range for orders-3')
	})

	it('NoOffsetForPartitionError carries the partition', () => {
		const error = new NoOffsetForPartitionError('orders', 1)
		expect(error).toBeInstanceOf(KafkaError)
		expect(error.name).toBe('NoOffsetForPartitionError')
		expect(error.topic).toBe('orders')
		expect(error.partition).toBe(1)
		expect(error.errorCode).toBe(ErrorCode.None)
		expect(error.retriable).toBe(false)
		expect(error.message).toContain('orders-1')
	})

	it('exports the typed errors from the package root', () => {
		expect(root).toMatchObject({
			CommitFailedError,
			TransactionAbortedError,
			OffsetOutOfRangeError,
			NoOffsetForPartitionError,
			ProducerFencedError,
			InvalidTxnStateError,
			RebalanceInProgressError,
			TimeoutError,
			KafkaProtocolError,
		})
	})
})
