import type { Attributes, Meter, Observable } from '@opentelemetry/api'
import { ErrorCode, diagnostics, type InstanceDiagnostics } from '@kafkats/client'

import { ATTR, KAFKATS_METRIC, METRIC, SYSTEM_KAFKA, errorType } from './attributes.js'
import { onOperation, secondsSince } from './operations.js'

const failure = (error: unknown): Attributes => (error === undefined ? {} : { [ATTR.errorType]: errorType(error) })

/**
 * Metrics of the client's own work: the four the messaging conventions define, and a few more
 * for what only this client can know (batching, backlog, the cost of a rebalance, how far its
 * fetching is behind). Consumer group lag, topic offsets and broker request rates are not here:
 * a broker exporter measures those for every client at once.
 */
export function measureClients(meter: Meter): () => void {
	const duration = { unit: 's' }
	const sentMessages = meter.createCounter(METRIC.sentMessages, { unit: '{message}' })
	const consumedMessages = meter.createCounter(METRIC.consumedMessages, { unit: '{message}' })
	const operationDuration = meter.createHistogram(METRIC.operationDuration, duration)
	const processDuration = meter.createHistogram(METRIC.processDuration, duration)
	const requestDuration = meter.createHistogram(KAFKATS_METRIC.requestDuration, duration)
	const requestRecords = meter.createHistogram(KAFKATS_METRIC.requestRecords, { unit: '{record}' })
	const rebalanceDuration = meter.createHistogram(KAFKATS_METRIC.rebalanceDuration, duration)

	const timed = { started: () => performance.now() }
	const stop = [
		onOperation(diagnostics.send, {
			...timed,
			finished(send, startedAt, error) {
				const attributes = {
					[ATTR.system]: SYSTEM_KAFKA,
					[ATTR.topic]: send.topic,
					[ATTR.operationName]: 'send',
					...failure(error),
				}
				sentMessages.add(send.messages.length, attributes)
				operationDuration.record(secondsSince(startedAt), attributes)
			},
		}),
		onOperation(diagnostics.produceRequest, {
			...timed,
			finished(request, startedAt, error) {
				// A request the broker answered can still have had partitions rejected.
				const rejected = request.rejected?.[0]
				const attributes = {
					[ATTR.brokerId]: request.brokerId,
					...(rejected ? { [ATTR.errorType]: ErrorCode[rejected.errorCode] ?? 'rejected' } : failure(error)),
				}
				requestDuration.record(secondsSince(startedAt), attributes)
				requestRecords.record(request.records, attributes)
			},
		}),
		onOperation(diagnostics.process, {
			...timed,
			finished(processing, startedAt, error) {
				const attributes = {
					[ATTR.system]: SYSTEM_KAFKA,
					[ATTR.topic]: processing.topic,
					[ATTR.partition]: String(processing.partition),
					[ATTR.consumerGroup]: processing.groupId,
					[ATTR.operationName]: 'process',
					...failure(error),
				}
				consumedMessages.add(processing.messages.length, attributes)
				processDuration.record(secondsSince(startedAt), attributes)
			},
		}),
		onOperation(diagnostics.commit, {
			...timed,
			finished(commit, startedAt, error) {
				operationDuration.record(secondsSince(startedAt), {
					[ATTR.system]: SYSTEM_KAFKA,
					[ATTR.consumerGroup]: commit.groupId,
					[ATTR.operationName]: 'commit',
					...failure(error),
				})
			},
		}),
		onOperation(diagnostics.rebalance, {
			...timed,
			finished(rebalance, startedAt, error) {
				rebalanceDuration.record(secondsSince(startedAt), {
					[ATTR.consumerGroup]: rebalance.groupId,
					...failure(error),
				})
			},
		}),
		observeInstances(meter),
	]
	return () => {
		for (const stopOne of stop) stopOne()
	}
}

/**
 * Gauges read from `metrics()` of every producer and consumer created from now on, each time the
 * meter collects. The instances are held weakly: one that is no longer used stops being reported.
 */
function observeInstances(meter: Meter): () => void {
	/** Each returns its producer or consumer for as long as the application still holds it. */
	const instances = new Set<() => InstanceDiagnostics | undefined>()
	const created = (message: unknown): void => {
		const { instance, ...identity } = message as InstanceDiagnostics
		const reference = new WeakRef(instance)
		instances.add(() => {
			const live = reference.deref()
			return live && ({ ...identity, instance: live } as InstanceDiagnostics)
		})
	}

	const gauge = (name: string, unit: string) => meter.createObservableGauge(name, { unit })
	const queuedRecords = gauge(KAFKATS_METRIC.queuedRecords, '{record}')
	const queuedBytes = gauge(KAFKATS_METRIC.queuedBytes, 'By')
	const inFlightRequests = gauge(KAFKATS_METRIC.inFlightRequests, '{request}')
	const unacknowledgedSends = gauge(KAFKATS_METRIC.unacknowledgedSends, '{send}')
	const positionLag = gauge(KAFKATS_METRIC.positionLag, '{record}')
	const bufferedBytes = gauge(KAFKATS_METRIC.bufferedBytes, 'By')
	const assignedPartitions = gauge(KAFKATS_METRIC.assignedPartitions, '{partition}')
	const pausedPartitions = gauge(KAFKATS_METRIC.pausedPartitions, '{partition}')
	const gauges: Observable[] = [
		queuedRecords,
		queuedBytes,
		inFlightRequests,
		unacknowledgedSends,
		positionLag,
		bufferedBytes,
		assignedPartitions,
		pausedPartitions,
	]

	const collect: Parameters<Meter['addBatchObservableCallback']>[0] = result => {
		// Instances that share their attributes (two producers of one client, the members of a group
		// in one process) are one series: their values add up instead of the last one winning.
		const totals = new Map<Observable, Map<string, { attributes: Attributes; value: number }>>()
		const add = (gauge: Observable, value: number, attributes: Attributes): void => {
			const series = totals.get(gauge) ?? new Map<string, { attributes: Attributes; value: number }>()
			totals.set(gauge, series)
			const key = JSON.stringify(attributes)
			const total = series.get(key)
			if (total) total.value += value
			else series.set(key, { attributes, value })
		}

		for (const find of instances) {
			const created = find()
			if (!created) {
				instances.delete(find)
				continue
			}
			if (created.type === 'producer') {
				const client = { [ATTR.clientId]: created.clientId }
				const metrics = created.instance.metrics()
				add(queuedRecords, metrics.queuedRecords, client)
				add(queuedBytes, metrics.queuedBytes, client)
				add(inFlightRequests, metrics.inFlightRequests, client)
				add(unacknowledgedSends, metrics.unacknowledgedSends, client)
				continue
			}
			const group = { [ATTR.consumerGroup]: created.groupId, [ATTR.clientId]: created.clientId }
			const { partitions } = created.instance.metrics()
			add(assignedPartitions, partitions.length, group)
			add(pausedPartitions, partitions.filter(p => p.paused).length, group)
			for (const partition of partitions) {
				const attributes = {
					...group,
					[ATTR.topic]: partition.topic,
					[ATTR.partition]: String(partition.partition),
				}
				add(bufferedBytes, partition.bufferedBytes, attributes)
				// Unknown before the first fetch and while the partition is paused.
				if (partition.endOffset !== undefined) {
					add(positionLag, Number(partition.endOffset - partition.position), attributes)
				}
			}
		}

		for (const [gauge, series] of totals) {
			for (const { attributes, value } of series.values()) result.observe(gauge, value, attributes)
		}
	}

	diagnostics.instances.subscribe(created)
	meter.addBatchObservableCallback(collect, gauges)
	return () => {
		diagnostics.instances.unsubscribe(created)
		meter.removeBatchObservableCallback(collect, gauges)
	}
}
