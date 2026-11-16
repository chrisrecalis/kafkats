import { describe, expect, it } from 'vitest'
import {
	CommitFailedError,
	ConnectionError,
	ErrorCode,
	KafkaError,
	KafkaProtocolError,
	NoOffsetForPartitionError,
	ProducerFencedError,
	RebalanceInProgressError,
	RecordTooLargeError,
	TimeoutError,
	TransactionAbortedError,
} from '@kafkats/client'
import {
	AssignmentError,
	FailedProcessingError,
	IllegalStateError,
	InvalidTimestampError,
	NoOffsetResetError,
	StreamsError,
	TaskCorruptedError,
	TaskMigratedError,
	TopologyError,
} from '@/api/errors.js'
import { ChangelogPartitionMismatchError, CoPartitioningError } from '@/changelog/errors.js'
import {
	classify,
	isFatalKafkaError,
	isFatalSendError,
	isFencedError,
	isRetriableError,
	type FailureSite,
} from '@/runtime/errors.js'

const SITES: readonly FailureSite[] = [
	'processing',
	'punctuation',
	'commit',
	'restore',
	'assignment',
	'consumer-run',
	'global',
	'purge',
]

describe('classify', () => {
	const fatal: [string, unknown][] = [
		['a fatal Kafka code', new KafkaProtocolError(ErrorCode.TopicAuthorizationFailed)],
		['a too large message', new KafkaProtocolError(ErrorCode.MessageTooLarge)],
		['too large commit metadata', new KafkaProtocolError(ErrorCode.OffsetMetadataTooLarge)],
		[
			'a commit fenced by a static member',
			new CommitFailedError('g', new KafkaProtocolError(ErrorCode.FencedInstanceId)),
		],
		['AssignmentError', new AssignmentError('x')],
		['ChangelogPartitionMismatchError', new ChangelogPartitionMismatchError('c', 2, 3, ['t'])],
		['CoPartitioningError', new CoPartitioningError(new Map([['a', 1]]))],
		['TopologyError', new TopologyError('x')],
		['FailedProcessingError', new FailedProcessingError('x')],
		['InvalidTimestampError', new InvalidTimestampError('x')],
		['NoOffsetResetError', new NoOffsetResetError('x')],
		["the client's NoOffsetForPartitionError", new NoOffsetForPartitionError('t', 0)],
		['IllegalStateError', new IllegalStateError('x')],
		['a StreamsError around user code', new StreamsError('failed to initialize processor')],
	]
	for (const [name, error] of fatal) {
		it(`${name} is fatal at every site`, () => {
			for (const site of SITES) expect(classify(error, site)).toBe('fatal')
		})
	}

	const migrate: [string, unknown][] = [
		['TaskMigratedError', new TaskMigratedError('x')],
		['a fenced producer', new ProducerFencedError(1n, 0)],
		['an invalid producer epoch', new KafkaProtocolError(ErrorCode.InvalidProducerEpoch)],
		['an out of order sequence', new KafkaProtocolError(ErrorCode.OutOfOrderSequenceNumber)],
		['an aborted transaction', new TransactionAbortedError('tx')],
		['a transaction with an unknown outcome', new TransactionAbortedError('tx', undefined, 'unknown')],
		['a failed commit', new CommitFailedError('g', new KafkaProtocolError(ErrorCode.IllegalGeneration))],
		['a timeout', new TimeoutError('commit', 10)],
		['a rebalance', new RebalanceInProgressError('g')],
		['a connection failure', new ConnectionError('localhost', 9092)],
		['any other Kafka error', new KafkaError('unknown')],
	]
	for (const [name, error] of migrate) {
		it(`${name} migrates at every site`, () => {
			for (const site of SITES) expect(classify(error, site)).toBe('migrate')
		})
	}

	it('TaskCorruptedError revives the tasks in place', () => {
		for (const site of SITES) expect(classify(new TaskCorruptedError('x', []), site)).toBe('corrupted')
	})

	it('any other error is fatal where user code runs and migrates where none does', () => {
		for (const error of [new Error('boom'), new TypeError('x'), 'thrown string']) {
			expect(classify(error, 'processing')).toBe('fatal')
			expect(classify(error, 'punctuation')).toBe('fatal')
			for (const site of ['commit', 'restore', 'assignment', 'consumer-run', 'global', 'purge'] as const) {
				expect(classify(error, site)).toBe('migrate')
			}
		}
	})
})

describe('send failure classes', () => {
	it('fenced codes', () => {
		expect(isFencedError(new ProducerFencedError(1n, 0))).toBe(true)
		expect(isFencedError(new KafkaProtocolError(ErrorCode.InvalidProducerIdMapping))).toBe(true)
		expect(isFencedError(new KafkaProtocolError(ErrorCode.MessageTooLarge))).toBe(false)
	})

	it('fatal codes', () => {
		expect(isFatalKafkaError(new KafkaProtocolError(ErrorCode.InvalidTopicException))).toBe(true)
		expect(isFatalKafkaError(new ProducerFencedError(1n, 0))).toBe(false)
		expect(isFatalKafkaError(new Error('x'))).toBe(false)
	})

	it('fatal whatever the production handler says (KS isFatalException): security, invalid topic', () => {
		expect(isFatalSendError(new KafkaProtocolError(ErrorCode.ClusterAuthorizationFailed))).toBe(true)
		expect(isFatalSendError(new KafkaProtocolError(ErrorCode.SecurityDisabled))).toBe(true)
		expect(isFatalSendError(new KafkaProtocolError(ErrorCode.InvalidTopicException))).toBe(true)
		// The production handler's classic case: skipping records that are too large.
		expect(isFatalSendError(new RecordTooLargeError('t', 0, 2, 1))).toBe(false)
		expect(isFatalSendError(new Error('x'))).toBe(false)
	})

	it('retriable: connections, timeouts and retriable Kafka errors', () => {
		expect(isRetriableError(new ConnectionError('h', 1))).toBe(true)
		expect(isRetriableError(new TimeoutError('send', 1))).toBe(true)
		expect(isRetriableError(new KafkaProtocolError(ErrorCode.NotLeaderOrFollower))).toBe(true)
		expect(isRetriableError(new KafkaProtocolError(ErrorCode.MessageTooLarge))).toBe(false)
		expect(isRetriableError(new Error('x'))).toBe(false)
	})
})
