import type { Codec } from '@/codec.js'

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
}

/**
 * Spec for a changelog topic to be created.
 */
export interface ChangelogTopicSpec {
	storeName: string
	topicName: string
	replicationFactor?: number
	configs: Record<string, string>
	keyCodec: Codec<unknown>
	valueCodec: Codec<unknown>
	skipRestoration: boolean
	/** Source topics that feed into this state store. Used to infer partition count. */
	sourceTopics: Set<string>
	/**
	 * If true, restoration is restricted to partitions currently assigned from source topics.
	 * Use false for stores that are re-keyed and no longer have source-partition affinity.
	 */
	restrictRestorationToSourcePartitions?: boolean
	/** If true, only validate existing topics, don't create new ones. */
	validateOnly: boolean
}

export const DEFAULT_CHANGELOG_CONFIGS: Record<string, string> = {
	'cleanup.policy': 'compact',
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
