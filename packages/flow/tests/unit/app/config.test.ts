import { describe, expect, it } from 'vitest'
import { FlowConfigError } from '@/api/errors.js'
import type { FlowConfig } from '@/api/app.js'
import type { ErrorHandlerContext } from '@/api/handlers.js'
import { resolveConfig, resolveHandlers } from '@/app/config.js'
import { TimestampExtractors } from '@/processor/timestamps.js'

const client = { clientId: 'app', brokers: ['localhost:9092'] }
const base: FlowConfig = { applicationId: 'app', client }
const resolve = (config: Partial<FlowConfig> | Record<string, unknown>) =>
	resolveConfig({ ...base, ...config } as FlowConfig)
const ctx = {} as ErrorHandlerContext

describe('resolveConfig', () => {
	it('fills the defaults', () => {
		const settings = resolve({})
		expect(settings).toMatchObject({
			applicationId: 'app',
			eos: false,
			commitIntervalMs: 5000,
			taskTimeoutMs: 300_000,
			startTimeoutMs: 120_000,
			maxTaskIdleMs: 0,
			bufferedRecordsPerPartition: 1000,
			emitIntervalMs: 1000,
			repartitionPurgeIntervalMs: 30_000,
			transactionTimeoutMs: 10_000,
			rebalanceTimeoutMs: 60_000,
			defaultTimestampExtractor: TimestampExtractors.failOnInvalid,
			defaultResetPolicy: 'earliest',
			restoration: {
				idleTimeoutMs: 30_000,
				initialIdleTimeoutMs: 30_000,
				checkIntervalMs: 1000,
				maxConcurrentTasks: 4,
			},
		})
		expect(settings.handlers.deserialization(ctx, {} as never, new Error())).toBe('fail')
		expect(settings.handlers.processing(ctx, null, new Error())).toBe('fail')
		expect(settings.handlers.production(ctx, {} as never, new Error(), 'send')).toBe('fail')
		expect(settings.handlers.uncaught(new Error())).toBe('shutdown')
		expect(settings).not.toHaveProperty('processId')
	})

	it('commits every 100 ms by default under exactly_once', () => {
		expect(resolve({ processingGuarantee: 'exactly_once' })).toMatchObject({ eos: true, commitIntervalMs: 100 })
	})

	it('takes the given values', () => {
		const extractor = TimestampExtractors.wallClock
		const settings = resolve({
			commitIntervalMs: 250,
			maxTaskIdleMs: -1,
			defaultTimestampExtractor: extractor,
			consumer: { autoOffsetReset: 'latest', rebalanceTimeoutMs: 20_000 },
			producer: { transactionTimeoutMs: 30_000 },
			changelog: { restoration: { idleTimeoutMs: 5000, maxConcurrentTasks: 2 } },
		})
		expect(settings).toMatchObject({
			commitIntervalMs: 250,
			maxTaskIdleMs: -1,
			defaultTimestampExtractor: extractor,
			defaultResetPolicy: 'latest',
			rebalanceTimeoutMs: 20_000,
			transactionTimeoutMs: 30_000,
			restoration: { idleTimeoutMs: 5000, initialIdleTimeoutMs: 5000, maxConcurrentTasks: 2 },
		})
	})

	it.each([
		['numStreamThreads', 2, 'run more instances'],
		['restartOnError', false, 'uncaughtExceptionHandler'],
		['consumerRun', {}, 'commitIntervalMs'],
		['runEach', {}, 'commitIntervalMs'],
		['stateDir', '/tmp', 'lmdb({ stateDir })'],
	])('rejects the removed key %s, naming its replacement', (key, value, replacement) => {
		expect(() => resolve({ [key]: value })).toThrow(FlowConfigError)
		expect(() => resolve({ [key]: value })).toThrow(`FlowConfig.${key} was removed`)
		expect(() => resolve({ [key]: value })).toThrow(replacement)
	})

	it.each(['groupId', 'onBeforeRebalance', 'onPartitionsAssigned', 'onPartitionsRevoked', 'onPartitionsLost'])(
		'rejects consumer.%s: flow owns the group and its rebalances',
		key => {
			expect(() => resolve({ consumer: { [key]: 'x' } })).toThrow(`consumer.${key} cannot be set`)
		}
	)

	it("accepts only the 'range' assignor", () => {
		expect(() => resolve({ consumer: { partitionAssignmentStrategy: 'range' } as never })).not.toThrow()
		expect(() => resolve({ consumer: { partitionAssignmentStrategy: 'roundrobin' } as never })).toThrow(
			"consumer.partitionAssignmentStrategy must be 'range'"
		)
	})

	it('rejects a transaction timeout below the commit interval under exactly_once only', () => {
		const config = { commitIntervalMs: 20_000, producer: { transactionTimeoutMs: 10_000 } }
		expect(() => resolve({ ...config, processingGuarantee: 'exactly_once' })).toThrow(
			'producer.transactionTimeoutMs (10000) must not be below commitIntervalMs (20000)'
		)
		expect(() => resolve(config)).not.toThrow()
	})

	it.each([
		[{ applicationId: '' }, 'applicationId must be a non-empty string'],
		[{ commitIntervalMs: 0 }, 'commitIntervalMs must be an integer >= 1'],
		[{ maxTaskIdleMs: -2 }, 'maxTaskIdleMs must be an integer >= -1'],
		[{ bufferedRecordsPerPartition: 1.5 }, 'bufferedRecordsPerPartition must be an integer >= 1'],
		[{ repartitionPurgeIntervalMs: -1 }, 'repartitionPurgeIntervalMs must be an integer >= 0'],
		[{ processingGuarantee: 'twice' }, "processingGuarantee must be 'at_least_once' or 'exactly_once'"],
		[
			{ consumer: { autoOffsetReset: 'middle' } },
			"consumer.autoOffsetReset must be 'earliest', 'latest' or 'none'",
		],
		[
			{ processingExceptionHandler: 'retry' },
			"processingExceptionHandler must be 'fail', 'continue' or a function",
		],
		[
			{ uncaughtExceptionHandler: 'ignore' },
			"uncaughtExceptionHandler must be 'shutdown', 'restart' or a function",
		],
	])('rejects %j', (config, message) => {
		expect(() => resolve(config)).toThrow(FlowConfigError)
		expect(() => resolve(config)).toThrow(message)
	})
})

describe('resolveHandlers', () => {
	it('turns answers into functions and keeps functions', () => {
		const production = () => 'continue' as const
		const handlers = resolveHandlers({
			deserializationExceptionHandler: 'continue',
			processingExceptionHandler: 'fail',
			productionExceptionHandler: production,
			uncaughtExceptionHandler: 'restart',
		})
		expect(handlers.deserialization(ctx, {} as never, new Error())).toBe('continue')
		expect(handlers.processing(ctx, null, new Error())).toBe('fail')
		expect(handlers.production).toBe(production)
		expect(handlers.uncaught(new Error())).toBe('restart')
	})
})
