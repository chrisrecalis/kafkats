import { describe, expect, it, vi } from 'vitest'

import { createLogger, noopLogger } from '@/logger.js'

describe('logger', () => {
	it('writes info logs as JSON', () => {
		const spy = vi.spyOn(console, 'log').mockImplementation(() => {})
		const logger = createLogger('info', { service: 'test' })
		logger.info('hello', { value: 1 })
		expect(spy).toHaveBeenCalledTimes(1)
		const payload = JSON.parse(spy.mock.calls[0]![0] as string)
		expect(payload.message).toBe('hello')
		expect(payload.service).toBe('test')
		expect(payload.value).toBe(1)
		spy.mockRestore()
	})

	it('routes error logs to console.error', () => {
		const spy = vi.spyOn(console, 'error').mockImplementation(() => {})
		const logger = createLogger('error')
		logger.error('boom')
		expect(spy).toHaveBeenCalledTimes(1)
		spy.mockRestore()
	})

	it('filters debug logs when level is info', () => {
		const spy = vi.spyOn(console, 'log').mockImplementation(() => {})
		const logger = createLogger('info')
		logger.debug('hidden')
		expect(spy).not.toHaveBeenCalled()
		spy.mockRestore()
	})

	it('child logger merges context', () => {
		const spy = vi.spyOn(console, 'log').mockImplementation(() => {})
		const logger = createLogger('info', { a: 1 })
		const child = logger.child({ b: 2 })
		child.info('child')
		const payload = JSON.parse(spy.mock.calls[0]![0] as string)
		expect(payload.a).toBe(1)
		expect(payload.b).toBe(2)
		spy.mockRestore()
	})

	it('noopLogger never logs', () => {
		const spy = vi.spyOn(console, 'log').mockImplementation(() => {})
		noopLogger.info('nope')
		noopLogger.debug('nope')
		expect(spy).not.toHaveBeenCalled()
		spy.mockRestore()
	})
})
