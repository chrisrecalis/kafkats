import { describe, expect, it } from 'vitest'

import {
	KafkaError,
	KafkaProtocolError,
	LeaderNotAvailableError,
	NotCoordinatorError,
	UnsupportedVersionError,
} from '@/client/errors.js'
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
})
