import { describe, expect, it } from 'vitest'

import {
	createError,
	ErrorCode,
	getErrorMessage,
	getErrorName,
	isRetriableError,
} from '@/protocol/messages/error-codes.js'

describe('error code helpers', () => {
	it('reports retriable errors', () => {
		expect(isRetriableError(ErrorCode.LeaderNotAvailable)).toBe(true)
		expect(isRetriableError(ErrorCode.InvalidTopicException)).toBe(false)
	})

	it('returns friendly error names and messages', () => {
		expect(getErrorName(ErrorCode.UnknownTopicOrPartition)).toBe('UnknownTopicOrPartition')
		expect(getErrorMessage(ErrorCode.UnknownTopicOrPartition)).toContain('does not exist')
	})

	it('handles unknown error codes gracefully', () => {
		expect(getErrorName(12345 as ErrorCode)).toBe('Unknown(12345)')
		expect(getErrorMessage(12345 as ErrorCode)).toBe('Unknown error code: 12345')
	})

	it('creates errors with context', () => {
		const error = createError(ErrorCode.BrokerNotAvailable, 'during test')
		expect(error.message).toContain('BrokerNotAvailable')
		expect(error.message).toContain('during test')
	})

	it('creates errors without context', () => {
		const error = createError(ErrorCode.None)
		expect(error.message).toContain('No error')
	})
})
