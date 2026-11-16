import type { ResolvedChangelogConfig } from '@/topology/nodes.js'

/**
 * Configuration for changelog topics backing state stores.
 *
 * Note: Partition count is always inferred from source topic(s) and cannot be overridden.
 * This ensures state locality - Task N processing partition N writes to changelog partition N.
 */
export interface ChangelogConfig {
	/** Whether changelog is enabled. Default: true */
	enabled?: boolean
	/** Custom topic name. Default: {appId}-{storeName}-changelog */
	topicName?: string
	/** Replication factor. Default: broker default */
	replicationFactor?: number
	/** Custom topic configs. Merged with defaults */
	topicConfigs?: Record<string, string>
	/** Skip state restoration on startup. Default: false */
	skipRestoration?: boolean
}

export interface ChangelogRestorationOptions {
	/**
	 * Fail restoration if no progress is made for this many milliseconds while restoration is incomplete.
	 * A stalled restore is always an error; it is never treated as complete.
	 * Default: 30000ms.
	 */
	idleTimeoutMs?: number
	/**
	 * Abort restoration if no progress is made for this many milliseconds BEFORE the first message is received,
	 * while restoration is incomplete.
	 *
	 * This can be set higher than idleTimeoutMs to tolerate slow consumer startup
	 * while still aborting quickly if restoration stalls.
	 *
	 * Default: idleTimeoutMs.
	 */
	initialIdleTimeoutMs?: number
	/**
	 * How often to check for the progress timeout.
	 * Default: 1000ms.
	 */
	checkIntervalMs?: number
	/**
	 * Override the consumer's Fetch maxWaitMs during restoration to make stop() responsive.
	 * Default: unset (uses consumer defaults).
	 */
	consumerMaxWaitMs?: number
	/** How many tasks restore at once (one restore consumer each). Default: 4. */
	maxConcurrentTasks?: number
}

/** Key-value changelogs: compacted, kept forever. */
export const DEFAULT_CHANGELOG_CONFIGS: Record<string, string> = {
	'cleanup.policy': 'compact',
	'retention.ms': '-1',
	'segment.bytes': '52428800',
}

/**
 * Repartition topics (Kafka Streams' RepartitionTopicConfig): never compacted, not expired by time; the records below
 * the committed offset are purged instead.
 */
export const DEFAULT_REPARTITION_CONFIGS: Readonly<Record<string, string>> = {
	'cleanup.policy': 'delete',
	'retention.ms': '-1',
	'segment.bytes': '52428800',
}

export function buildChangelogTopicName(appId: string, storeName: string): string {
	return `${appId}-${storeName}-changelog`
}

export function getDefaultTopicConfigs(overrides?: Record<string, string>): Record<string, string> {
	return { ...DEFAULT_CHANGELOG_CONFIGS, ...overrides }
}

/** Normalizes the `changelog` option of a store (`true`/`false`/config/unset) to a config. */
export function resolveChangelogConfig(changelog: boolean | ChangelogConfig | undefined): ChangelogConfig {
	if (changelog === false) {
		return { enabled: false }
	}
	if (changelog === undefined || changelog === true) {
		return { enabled: true }
	}
	return { enabled: changelog.enabled !== false, ...changelog }
}

/** What the changelog of every store of an application is resolved against. */
export interface ChangelogDefaults {
	readonly applicationId: string
	/** `FlowConfig.changelog.topicConfigs`: applied over the defaults, below a store's own topic configs. */
	readonly topicConfigs?: Readonly<Record<string, string>>
}

/**
 * The changelog of a store: its topic and the configs it is created with (the defaults, then the application's, then
 * the store's own). Window and session stores (those with a `retentionMs`) default to `delete,compact` with the store's
 * retention, so the broker drops the records of expired windows instead of keeping every window forever.
 */
export function resolveStoreChangelog(
	defaults: ChangelogDefaults,
	store: { readonly name: string; readonly retentionMs?: number },
	option: boolean | ChangelogConfig | undefined
): ResolvedChangelogConfig {
	const config = resolveChangelogConfig(option)
	if (!config.enabled) {
		return { enabled: false }
	}
	const windowed: Record<string, string> =
		store.retentionMs === undefined
			? {}
			: { 'cleanup.policy': 'delete,compact', 'retention.ms': String(store.retentionMs) }
	return {
		enabled: true,
		topic: config.topicName ?? buildChangelogTopicName(defaults.applicationId, store.name),
		configs: getDefaultTopicConfigs({ ...windowed, ...defaults.topicConfigs, ...config.topicConfigs }),
		replicationFactor: config.replicationFactor,
		skipRestoration: config.skipRestoration ?? false,
	}
}
