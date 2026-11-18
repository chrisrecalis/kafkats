/**
 * FlowConfig validation, defaults and handler resolution (3.15). `flow()` resolves the config once, so a removed or
 * invalid key throws FlowConfigError there; each start adds its process id (`ResolvedFlowConfig`, 3.17).
 */
import { FlowConfigError } from '@/api/errors.js'
import type { FlowConfig } from '@/api/app.js'
import type { HandlerResponse, UncaughtExceptionResponse } from '@/api/handlers.js'
import { TimestampExtractors } from '@/processor/timestamps.js'
import type { ResolvedFlowConfig, ResolvedHandlers } from '@/runtime/config.js'
import type { ResetPolicy } from '@/topology/nodes.js'

/** The resolved config without the process id, which is new for every start. */
export type AppSettings = Omit<ResolvedFlowConfig, 'processId'>

/** Keys of earlier versions, with what replaces them. */
const REMOVED_KEYS: Readonly<Record<string, string>> = {
	numStreamThreads: 'run more instances of the application: tasks of one instance already process concurrently',
	restartOnError: "use uncaughtExceptionHandler ('shutdown' is the default; 'restart' rejoins after a fatal error)",
	consumerRun: 'use commitIntervalMs; tasks of one instance process concurrently',
	runEach: 'use commitIntervalMs; tasks of one instance process concurrently',
	stateDir: 'pass the directory to the state store provider, e.g. lmdb({ stateDir })',
}

/** Consumer settings flow owns: the group is the application, and flow handles every rebalance itself. */
const OWNED_CONSUMER_KEYS = [
	'groupId',
	'onBeforeRebalance',
	'onPartitionsAssigned',
	'onPartitionsRevoked',
	'onPartitionsLost',
] as const

/** The client's default (ConsumerConfig.rebalanceTimeoutMs). */
const DEFAULT_REBALANCE_TIMEOUT_MS = 60_000
/** Exactly-once commits at this interval by default (KS); at-least-once at the slower one. */
const DEFAULT_COMMIT_INTERVAL_MS = { eos: 100, alos: 5000 }
const DEFAULT_TRANSACTION_TIMEOUT_MS = 10_000
/** Internal throttle of emit-on-window-close and outer-join scans (wall clock); the TestDriver sets its own. */
const EMIT_INTERVAL_MS = 1000

export function resolveConfig(config: FlowConfig): AppSettings {
	if (typeof config.applicationId !== 'string' || config.applicationId.length === 0) {
		throw new FlowConfigError('applicationId must be a non-empty string')
	}
	for (const [key, replacement] of Object.entries(REMOVED_KEYS)) {
		if (key in config) {
			throw new FlowConfigError(`FlowConfig.${key} was removed: ${replacement}`)
		}
	}
	const consumer = config.consumer ?? {}
	for (const key of OWNED_CONSUMER_KEYS) {
		if (key in consumer) {
			throw new FlowConfigError(
				`consumer.${key} cannot be set: flow uses the application id as the group and handles rebalances`
			)
		}
	}
	if (consumer.partitionAssignmentStrategy !== undefined && consumer.partitionAssignmentStrategy !== 'range') {
		throw new FlowConfigError(
			`consumer.partitionAssignmentStrategy must be 'range' (co-partitioned topics are assigned together), got '${consumer.partitionAssignmentStrategy}'`
		)
	}
	const eos = guarantee(config.processingGuarantee) === 'exactly_once'
	const commitIntervalMs = integer(
		'commitIntervalMs',
		config.commitIntervalMs ?? (eos ? DEFAULT_COMMIT_INTERVAL_MS.eos : DEFAULT_COMMIT_INTERVAL_MS.alos),
		1
	)
	const transactionTimeoutMs = integer(
		'producer.transactionTimeoutMs',
		config.producer?.transactionTimeoutMs ?? DEFAULT_TRANSACTION_TIMEOUT_MS,
		1
	)
	if (eos && transactionTimeoutMs < commitIntervalMs) {
		throw new FlowConfigError(
			`producer.transactionTimeoutMs (${transactionTimeoutMs}) must not be below commitIntervalMs (${commitIntervalMs}): a transaction stays open for a commit interval`
		)
	}
	const restoration = config.changelog?.restoration ?? {}
	const idleTimeoutMs = integer('changelog.restoration.idleTimeoutMs', restoration.idleTimeoutMs ?? 30_000, 1)
	return {
		applicationId: config.applicationId,
		eos,
		commitIntervalMs,
		taskTimeoutMs: integer('taskTimeoutMs', config.taskTimeoutMs ?? 300_000, 0),
		startTimeoutMs: integer('startTimeoutMs', config.startTimeoutMs ?? 120_000, 1),
		maxTaskIdleMs: integer('maxTaskIdleMs', config.maxTaskIdleMs ?? 0, -1),
		bufferedRecordsPerPartition: integer(
			'bufferedRecordsPerPartition',
			config.bufferedRecordsPerPartition ?? 1000,
			1
		),
		emitIntervalMs: EMIT_INTERVAL_MS,
		repartitionPurgeIntervalMs: integer(
			'repartitionPurgeIntervalMs',
			config.repartitionPurgeIntervalMs ?? 30_000,
			0
		),
		transactionTimeoutMs,
		rebalanceTimeoutMs: integer(
			'consumer.rebalanceTimeoutMs',
			consumer.rebalanceTimeoutMs ?? DEFAULT_REBALANCE_TIMEOUT_MS,
			1
		),
		defaultTimestampExtractor: config.defaultTimestampExtractor ?? TimestampExtractors.failOnInvalid,
		defaultResetPolicy: resetPolicy(consumer.autoOffsetReset),
		handlers: resolveHandlers(config),
		restoration: {
			idleTimeoutMs,
			initialIdleTimeoutMs: integer(
				'changelog.restoration.initialIdleTimeoutMs',
				restoration.initialIdleTimeoutMs ?? idleTimeoutMs,
				1
			),
			checkIntervalMs: integer('changelog.restoration.checkIntervalMs', restoration.checkIntervalMs ?? 1000, 1),
			consumerMaxWaitMs: restoration.consumerMaxWaitMs,
			maxConcurrentTasks: integer(
				'changelog.restoration.maxConcurrentTasks',
				restoration.maxConcurrentTasks ?? 4,
				1
			),
		},
	}
}

/** The four handler options as functions ('fail' | 'continue', 'shutdown' | 'restart', or the user's function). */
export function resolveHandlers(
	config: Pick<
		FlowConfig,
		| 'deserializationExceptionHandler'
		| 'processingExceptionHandler'
		| 'productionExceptionHandler'
		| 'uncaughtExceptionHandler'
	>
): ResolvedHandlers {
	return {
		deserialization: handler('deserializationExceptionHandler', config.deserializationExceptionHandler),
		processing: handler('processingExceptionHandler', config.processingExceptionHandler),
		production: handler('productionExceptionHandler', config.productionExceptionHandler),
		uncaught: uncaughtHandler(config.uncaughtExceptionHandler),
	}
}

function handler<F extends (...args: never[]) => HandlerResponse>(
	key: string,
	option: HandlerResponse | F | undefined
): F | (() => HandlerResponse) {
	if (typeof option === 'function') {
		return option
	}
	const response = option ?? 'fail'
	if (response !== 'fail' && response !== 'continue') {
		throw new FlowConfigError(`${key} must be 'fail', 'continue' or a function, got ${String(response)}`)
	}
	return () => response
}

function uncaughtHandler(option: FlowConfig['uncaughtExceptionHandler']): ResolvedHandlers['uncaught'] {
	if (typeof option === 'function') {
		return option
	}
	const response: UncaughtExceptionResponse = option ?? 'shutdown'
	if (response !== 'shutdown' && response !== 'restart') {
		throw new FlowConfigError(
			`uncaughtExceptionHandler must be 'shutdown', 'restart' or a function, got ${String(response)}`
		)
	}
	return () => response
}

function guarantee(option: FlowConfig['processingGuarantee']): 'at_least_once' | 'exactly_once' {
	const value = option ?? 'at_least_once'
	if (value !== 'at_least_once' && value !== 'exactly_once') {
		throw new FlowConfigError(`processingGuarantee must be 'at_least_once' or 'exactly_once', got ${String(value)}`)
	}
	return value
}

/** The app's default reset policy (KS: 'earliest'). */
function resetPolicy(option: unknown): ResetPolicy {
	const value = option ?? 'earliest'
	if (value !== 'earliest' && value !== 'latest' && value !== 'none') {
		throw new FlowConfigError(
			`consumer.autoOffsetReset must be 'earliest', 'latest' or 'none', got ${String(value)}`
		)
	}
	return value
}

function integer(key: string, value: number, min: number): number {
	if (!Number.isSafeInteger(value) || value < min) {
		throw new FlowConfigError(`${key} must be an integer >= ${min}, got ${value}`)
	}
	return value
}
