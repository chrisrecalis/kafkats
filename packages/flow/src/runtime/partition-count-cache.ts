/**
 * Partition counts for sinks with a partitioner (3.17, B13): one cache per Run, read synchronously on the hot path. An
 * entry older than REFRESH_AFTER_MS is refreshed in the background through the producer's metadata (one refresh per
 * topic at a time); a failed refresh is logged and keeps the old count.
 */
import type { Logger } from '@kafkats/client'
import type { PartitionCountCache } from '@/runtime/config.js'
import { errorMessage } from '@/runtime/errors.js'
import type { Clock, ProducerPort } from '@/runtime/io.js'
import { cap } from '@/runtime/signals.js'

const REFRESH_AFTER_MS = 60_000
const REFRESH_TIMEOUT_MS = 10_000

export interface PartitionCountCacheDeps {
	/** The Run's current producer. */
	readonly producer: () => Pick<ProducerPort, 'partitionCount'>
	readonly clock: Clock
	/** Ends the background refreshes (the Run's hard stop). */
	readonly signal: AbortSignal
	readonly logger: Logger
}

export class SinkPartitionCounts implements PartitionCountCache {
	private readonly entries = new Map<string, { readonly count: number; readonly at: number }>()
	private readonly refreshing = new Set<string>()

	constructor(private readonly deps: PartitionCountCacheDeps) {}

	get(topic: string): number | undefined {
		const entry = this.entries.get(topic)
		if (entry !== undefined && this.deps.clock.now() - entry.at >= REFRESH_AFTER_MS) {
			this.refresh(topic)
		}
		return entry?.count
	}

	async load(topic: string, signal: AbortSignal): Promise<number> {
		const count = await this.deps.producer().partitionCount(topic, { signal })
		this.entries.set(topic, { count, at: this.deps.clock.now() })
		return count
	}

	private refresh(topic: string): void {
		if (this.refreshing.has(topic) || this.deps.signal.aborted) {
			return
		}
		this.refreshing.add(topic)
		this.load(topic, cap(this.deps.signal, REFRESH_TIMEOUT_MS))
			.catch((error: unknown) => {
				this.deps.logger.warn('refreshing a partition count failed: the last one is kept', {
					topic,
					error: errorMessage(error),
				})
			})
			.finally(() => this.refreshing.delete(topic))
	}
}
