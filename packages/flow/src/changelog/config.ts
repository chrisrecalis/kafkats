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
	/** Replication factor. Default: FlowConfig.changelog.replicationFactor, else the broker's default. */
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
	 * Override the consumer's Fetch maxWaitMs during restoration to make stop() responsive. Default, in
	 * FlowConfig.changelog.restoration: FlowConfig.consumer.maxWaitMs, else 500. For ChangelogRestorer.restore(): the
	 * client consumer's default.
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
 * How much longer a window or session changelog keeps records than its store keeps windows: a day, the default of Kafka
 * Streams' `windowstore.changelog.additional.retention.ms`. The broker deletes by its own clock, so the margin keeps
 * windows the store still holds when stream time lags behind it.
 */
export const WINDOWED_CHANGELOG_ADDITIONAL_RETENTION_MS = 86_400_000

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
 * retention plus a day (5.4), so the broker drops the records of expired windows instead of keeping every window
 * forever; their expiry writes no tombstones (`state/logging.ts`). That retention is the store's own, set over the
 * application's configs (KS `WindowedChangelogTopicConfig`): an application-wide `retention.ms`, meant for repartition
 * topics, must not cut the changelog of a window below the store's retention; only the store's own configs replace it.
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
	const { retentionMs } = store
	const policy: Record<string, string> = retentionMs === undefined ? {} : { 'cleanup.policy': 'delete,compact' }
	const retention: Record<string, string> =
		retentionMs === undefined
			? {}
			: { 'retention.ms': String(retentionMs + WINDOWED_CHANGELOG_ADDITIONAL_RETENTION_MS) }
	return {
		enabled: true,
		topic: config.topicName ?? buildChangelogTopicName(defaults.applicationId, store.name),
		configs: getDefaultTopicConfigs({ ...policy, ...defaults.topicConfigs, ...retention, ...config.topicConfigs }),
		replicationFactor: config.replicationFactor,
		skipRestoration: config.skipRestoration ?? false,
	}
}
