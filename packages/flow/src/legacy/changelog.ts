// The legacy runtime's changelog layer: its own copies of changelog/{config,writer}.ts, the shared errors and restorer.
export {
	type ChangelogConfig,
	type ChangelogRestorationOptions,
	type ChangelogTopicSpec,
	DEFAULT_CHANGELOG_CONFIGS,
	buildChangelogTopicName,
	getDefaultTopicConfigs,
	resolveChangelogConfig,
} from '@/legacy/changelog/config.js'
export { ChangelogPartitionMismatchError, CoPartitioningError, SourceTopicNotFoundError } from '@/changelog/errors.js'
export { ChangelogWriter, windowedKeyCodec } from '@/legacy/changelog/writer.js'
export { ChangelogRestorer } from '@/changelog/restorer.js'
