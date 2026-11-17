/**
 * Error thrown when an existing changelog topic has the wrong number of partitions.
 * This indicates data corruption risk and requires manual intervention.
 */
export class ChangelogPartitionMismatchError extends Error {
	override readonly name = 'ChangelogPartitionMismatchError'

	constructor(
		public readonly changelogTopic: string,
		public readonly expectedPartitions: number,
		public readonly actualPartitions: number,
		public readonly sourceTopics: string[]
	) {
		super(
			`Changelog topic "${changelogTopic}" has ${actualPartitions} partitions ` +
				`but source topics [${sourceTopics.join(', ')}] require ${expectedPartitions}. ` +
				`Delete the changelog topic and restart, or manually recreate with ${expectedPartitions} partitions.`
		)
	}
}

/**
 * Error thrown when a source topic does not exist and partition count cannot be inferred.
 */
export class SourceTopicNotFoundError extends Error {
	override readonly name = 'SourceTopicNotFoundError'

	constructor(
		public readonly topic: string,
		public readonly storeName: string
	) {
		super(
			`Source topic "${topic}" does not exist. Cannot determine partition count for ` +
				`changelog topic of store "${storeName}". Create the source topic first.`
		)
	}
}

/**
 * Error thrown when topics that must be co-partitioned have different partition counts.
 *
 * State is partitioned by source partition, so records with the same key from every topic feeding a
 * stateful operation (join sides, merged streams) must land in the same partition index.
 */
export class CoPartitioningError extends Error {
	override readonly name = 'CoPartitioningError'

	constructor(public readonly partitionCounts: Map<string, number>) {
		super(
			`Topics [${[...partitionCounts].map(([topic, count]) => `${topic} (${count})`).join(', ')}] feed the same ` +
				'stateful operation and must have the same number of partitions. ' +
				'Repartition one side with through(), or use a globalTable() for lookups.'
		)
	}
}

/**
 * Error thrown when a changelog record cannot be decoded with the store's codecs while restoring. The changelog was
 * written with other codecs (a changed topology, or another application's topic): restarting cannot fix it.
 */
export class ChangelogDecodeError extends Error {
	override readonly name = 'ChangelogDecodeError'

	constructor(
		public readonly changelogTopic: string,
		public readonly partition: number,
		options: { cause: unknown }
	) {
		super(
			`A record of changelog topic "${changelogTopic}" partition ${partition} cannot be decoded with the codecs of ` +
				'its store. Reset the application state if the store changed its key or value format.',
			options
		)
	}
}
