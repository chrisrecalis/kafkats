// Stable import path for the changelog layer; see src/changelog/.
export {
	type ChangelogConfig,
	type ChangelogRestorationOptions,
	type ChangelogTopicSpec,
	DEFAULT_CHANGELOG_CONFIGS,
	buildChangelogTopicName,
	getDefaultTopicConfigs,
	resolveChangelogConfig,
} from '@/changelog/config.js'
export { ChangelogPartitionMismatchError, CoPartitioningError, SourceTopicNotFoundError } from '@/changelog/errors.js'
export { ChangelogWriter, windowedKeyCodec } from '@/changelog/writer.js'
export { ChangelogRestorer } from '@/changelog/restorer.js'
