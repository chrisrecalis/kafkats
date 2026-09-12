import { describe, expect, it } from 'vitest'
import { LAG_METRICS, LagMetrics } from '../../src/metrics.js'
import { emptyStats, type LagSnapshot, type PartitionLag } from '../../src/types.js'

function partition(overrides: Partial<PartitionLag> = {}): PartitionLag {
	return {
		topic: 'events',
		partition: 0,
		committedOffset: 4n,
		earliestOffset: 0n,
		latestOffset: 10n,
		timeLagSeconds: 300,
		member: { memberId: 'm-A', clientId: 'orders-svc', clientHost: '/10.0.0.1' },
		...overrides,
	}
}

function snapshot(partitions: PartitionLag[], errors: LagSnapshot['errors'] = []): LagSnapshot {
	return {
		collectedAt: 400_000,
		durationMs: 250,
		errors,
		stats: {
			...emptyStats(),
			fetchRequests: 3,
			fetchBytes: 4096,
			partitions: { ...emptyStats().partitions, fetched: 1 },
		},
		groups: [
			{
				groupId: 'orders',
				partitions,
				maxTimeLagSeconds: Math.max(...partitions.map(p => p.timeLagSeconds ?? 0)),
			},
		],
	}
}

describe('lag metrics', () => {
	it('emits only series kafka_exporter lacks, under its label conventions', async () => {
		const metrics = new LagMetrics({ clusterName: 'local' })
		metrics.update(snapshot([partition()]), 400_250)

		const text = await metrics.registry.metrics()
		const names = (await metrics.registry.getMetricsAsJSON()).map(m => m.name).sort()
		expect(names).toEqual(Object.values(LAG_METRICS).sort())
		expect(text).not.toContain('kafka_consumergroup_lag{')
		expect(text).not.toContain('kafka_consumergroup_current_offset')

		expect(text).toContain(
			'kafka_consumergroup_lag_seconds{consumergroup="orders",topic="events",partition="0",consumer_id="m-A",client_id="orders-svc",member_host="/10.0.0.1",cluster_name="local"} 300'
		)
		expect(text).toContain('kafkats_lag_collection_success{cluster_name="local"} 1')
		expect(text).toContain('kafkats_lag_last_success_unixtime{cluster_name="local"} 400.25')
		expect(text).toContain('kafkats_lag_partitions{source="fetched",cluster_name="local"} 1')
		expect(text).toContain('kafkats_lag_fetch_requests{cluster_name="local"} 3')
		expect(text).toContain('kafkats_lag_fetch_bytes{cluster_name="local"} 4096')
	})

	it('accumulates fetch totals across updates so rate() works on them', async () => {
		const metrics = new LagMetrics()
		metrics.update(snapshot([partition()]), null)
		metrics.update(snapshot([partition()]), null)
		const text = await metrics.registry.metrics()
		expect(text).toContain('kafkats_lag_fetch_requests 3\n')
		expect(text).toContain('kafkats_lag_fetch_requests_total 6\n')
		expect(text).toContain('kafkats_lag_fetch_bytes_total 8192\n')
	})

	it('drops series that are no longer observed instead of freezing them', async () => {
		const metrics = new LagMetrics()
		metrics.update(snapshot([partition()]), null)
		expect(await metrics.registry.metrics()).toContain('consumer_id="m-A"')

		// Rebalance: same partition, different owner. The old owner's series must vanish.
		metrics.update(
			snapshot([partition({ member: { memberId: 'm-B', clientId: 'orders-svc', clientHost: '/10.0.0.2' } })]),
			null
		)
		const text = await metrics.registry.metrics()
		expect(text).toContain('consumer_id="m-B"')
		expect(text).not.toContain('consumer_id="m-A"')

		// Group gone entirely: no lag series at all, but health still reported.
		metrics.update(
			{
				collectedAt: 1,
				durationMs: 1,
				errors: [{ scope: 'groups', message: 'x' }],
				groups: [],
				stats: emptyStats(),
			},
			500_000
		)
		const empty = await metrics.registry.metrics()
		expect(empty).not.toContain('consumergroup="orders"')
		expect(empty).toContain('kafkats_lag_collection_success 0')
	})
})
