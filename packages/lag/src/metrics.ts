import { Counter, Gauge, Registry } from 'prom-client'
import type { LagSnapshot } from './types.js'

export interface LagMetricsOptions {
	/** Adds a `cluster_name` label to every series */
	clusterName?: string
}

/**
 * Names sit in the kafka_exporter namespace with its `consumergroup`/`topic`/`partition` labels, but
 * deliberately do not duplicate any series kafka_exporter already emits, so both can run side by side.
 */
export const LAG_METRICS = {
	lagSeconds: 'kafka_consumergroup_lag_seconds',
	maxLagSeconds: 'kafka_consumergroup_max_lag_seconds',
	collectionSuccess: 'kafkats_lag_collection_success',
	collectionDurationSeconds: 'kafkats_lag_collection_duration_seconds',
	lastSuccessUnixTime: 'kafkats_lag_last_success_unixtime',
	partitions: 'kafkats_lag_partitions',
	fetchRequests: 'kafkats_lag_fetch_requests',
	fetchBytes: 'kafkats_lag_fetch_bytes',
	fetchRequestsTotal: 'kafkats_lag_fetch_requests_total',
	fetchBytesTotal: 'kafkats_lag_fetch_bytes_total',
} as const

const PARTITION_LABELS = ['consumergroup', 'topic', 'partition', 'consumer_id', 'client_id', 'member_host'] as const

/**
 * Prometheus metrics for lag snapshots. Every `update` clears all gauge series first, so a partition that
 * moved to another member, a group that was deleted, or a flag that flipped simply stops being
 * reported instead of lingering at its last value.
 */
export class LagMetrics {
	readonly registry = new Registry()
	private readonly lagSeconds: Gauge<string>
	private readonly maxLagSeconds: Gauge<string>
	private readonly collectionSuccess: Gauge<string>
	private readonly collectionDuration: Gauge<string>
	private readonly lastSuccess: Gauge<string>
	private readonly partitions: Gauge<string>
	private readonly fetchRequests: Gauge<string>
	private readonly fetchBytes: Gauge<string>
	private readonly fetchRequestsTotal: Counter<string>
	private readonly fetchBytesTotal: Counter<string>
	/** Reset before every update; the counters are not */
	private readonly perSnapshot: Gauge<string>[]

	constructor(options: LagMetricsOptions = {}) {
		if (options.clusterName) this.registry.setDefaultLabels({ cluster_name: options.clusterName })
		const gauge = (name: string, help: string, labelNames: readonly string[] = []) =>
			new Gauge({ name, help, labelNames: [...labelNames], registers: [this.registry] })

		this.lagSeconds = gauge(
			LAG_METRICS.lagSeconds,
			'Age in seconds of the oldest record not yet consumed by the group (0 when caught up)',
			PARTITION_LABELS
		)
		this.maxLagSeconds = gauge(LAG_METRICS.maxLagSeconds, 'Maximum partition time lag of the group in seconds', [
			'consumergroup',
		])
		this.collectionSuccess = gauge(
			LAG_METRICS.collectionSuccess,
			'Whether the most recent Kafka lag collection completed without errors'
		)
		this.collectionDuration = gauge(
			LAG_METRICS.collectionDurationSeconds,
			'Duration of the most recent Kafka lag collection in seconds'
		)
		this.lastSuccess = gauge(
			LAG_METRICS.lastSuccessUnixTime,
			'Unix time when the most recent successful Kafka lag collection completed'
		)
		this.partitions = gauge(
			LAG_METRICS.partitions,
			'Partitions in the most recent collection by how their lag was obtained: caught_up, fetched (record read from the broker), cached (earlier fetch reused), estimated (produce rate), unknown',
			['source']
		)
		this.fetchRequests = gauge(
			LAG_METRICS.fetchRequests,
			'Fetch requests sent to brokers for record timestamps during the most recent collection'
		)
		this.fetchBytes = gauge(
			LAG_METRICS.fetchBytes,
			'Bytes of record data read from brokers during the most recent collection'
		)
		const counter = (name: string, help: string) => new Counter({ name, help, registers: [this.registry] })
		this.fetchRequestsTotal = counter(
			LAG_METRICS.fetchRequestsTotal,
			'Fetch requests sent to brokers for record timestamps since the exporter started'
		)
		this.fetchBytesTotal = counter(
			LAG_METRICS.fetchBytesTotal,
			'Bytes of record data read from brokers since the exporter started'
		)
		this.perSnapshot = [
			this.lagSeconds,
			this.maxLagSeconds,
			this.collectionSuccess,
			this.collectionDuration,
			this.lastSuccess,
			this.partitions,
			this.fetchRequests,
			this.fetchBytes,
		]
	}

	update(snapshot: LagSnapshot, lastSuccessAt: number | null): void {
		for (const gauge of this.perSnapshot) gauge.reset()

		this.collectionSuccess.set(snapshot.errors.length === 0 ? 1 : 0)
		this.collectionDuration.set(snapshot.durationMs / 1000)
		if (lastSuccessAt !== null) this.lastSuccess.set(lastSuccessAt / 1000)

		const { stats } = snapshot
		for (const [source, count] of Object.entries(stats.partitions)) this.partitions.set({ source }, count)
		this.fetchRequests.set(stats.fetchRequests)
		this.fetchBytes.set(stats.fetchBytes)
		this.fetchRequestsTotal.inc(stats.fetchRequests)
		this.fetchBytesTotal.inc(stats.fetchBytes)

		for (const group of snapshot.groups) {
			if (group.maxTimeLagSeconds !== null) {
				this.maxLagSeconds.set({ consumergroup: group.groupId }, group.maxTimeLagSeconds)
			}
			for (const p of group.partitions) {
				const labels: Record<string, string> = {
					consumergroup: group.groupId,
					topic: p.topic,
					partition: String(p.partition),
				}
				if (p.member) {
					labels.consumer_id = p.member.memberId
					labels.client_id = p.member.clientId
					labels.member_host = p.member.clientHost
				}
				if (p.timeLagSeconds !== null) this.lagSeconds.set(labels, p.timeLagSeconds)
			}
		}
	}
}
