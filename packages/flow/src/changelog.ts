// Stable import path for the changelog layer.
export {
	type ChangelogConfig,
	type ChangelogRestorationOptions,
	DEFAULT_CHANGELOG_CONFIGS,
	DEFAULT_REPARTITION_CONFIGS,
	buildChangelogTopicName,
	getDefaultTopicConfigs,
	resolveChangelogConfig,
} from '@/changelog/config.js'
export { ChangelogPartitionMismatchError, CoPartitioningError, SourceTopicNotFoundError } from '@/changelog/errors.js'
export { ChangelogWriter, windowedKeyCodec } from '@/changelog/writer.js'
export { ChangelogRestorer, type RestoreTarget } from '@/changelog/restorer.js'
