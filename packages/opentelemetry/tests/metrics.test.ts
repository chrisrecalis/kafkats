import {
	ErrorCode,
	KafkaClient,
	diagnostics,
	type CommitDiagnostics,
	type ConsumerMetrics,
	type ProcessDiagnostics,
	type ProduceRequestDiagnostics,
	type ProducerMetrics,
	type RebalanceDiagnostics,
	type SendDiagnostics,
} from '@kafkats/client'
import type { Histogram } from '@opentelemetry/sdk-metrics'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { ATTR, KAFKATS_METRIC, METRIC, instrumentKafka } from '../src/index.js'
import { metricRecorder } from './setup.js'

const settle = () => new Promise(resolve => setTimeout(resolve, 5))
const unique = (prefix: string) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`

describe('metrics', () => {
	let recorder: ReturnType<typeof metricRecorder>
	let stop: () => void

	beforeEach(() => {
		recorder = metricRecorder()
		stop = instrumentKafka({ meterProvider: recorder.meterProvider, traces: false })
	})
	afterEach(() => stop())

	it('counts sent messages and times sends, by topic', async () => {
		const sending: SendDiagnostics = {
			clientId: 'app',
			topic: 'orders',
			transactional: false,
			messages: [{ value: null }, { value: null }],
			addHeaders: () => {},
		}
		await diagnostics.send.tracePromise(async () => {
			await settle()
			return [] as never
		}, sending)

		const metrics = await recorder.collect()
		const [sent] = metrics.get(METRIC.sentMessages)!
		expect(sent!.value).toBe(2)
		expect(sent!.attributes).toEqual({
			[ATTR.system]: 'kafka',
			[ATTR.topic]: 'orders',
			[ATTR.operationName]: 'send',
		})
		const [duration] = metrics.get(METRIC.operationDuration)!
		const histogram = duration!.value as Histogram
		expect(histogram.count).toBe(1)
		// Seconds, not milliseconds.
		expect(histogram.sum).toBeGreaterThan(0.004)
		expect(histogram.sum).toBeLessThan(1)
	})

	it('counts consumed messages and times the handler, marking failures', async () => {
		const processing = (): ProcessDiagnostics => ({
			clientId: 'app',
			groupId: 'billing',
			topic: 'orders',
			partition: 3,
			messages: [
				{ topic: 'orders', partition: 3, offset: 1n, timestamp: 0n, key: null, value: null, headers: {} },
			],
			batch: false,
			runHandlerIn: () => {},
		})
		await diagnostics.process.tracePromise(async () => {}, processing())
		await diagnostics.process
			.tracePromise(async () => {
				throw new RangeError('bad record')
			}, processing())
			.catch(() => {})

		const metrics = await recorder.collect()
		const consumed = metrics.get(METRIC.consumedMessages)!
		expect(consumed.map(point => [point.value, point.attributes[ATTR.errorType]])).toEqual([
			[1, undefined],
			[1, 'RangeError'],
		])
		expect(consumed[0]!.attributes).toMatchObject({
			[ATTR.topic]: 'orders',
			[ATTR.partition]: '3',
			[ATTR.consumerGroup]: 'billing',
		})
		expect(metrics.get(METRIC.processDuration)).toHaveLength(2)
	})

	it('times commits and rebalances, and records what a produce request carried', async () => {
		const commit: CommitDiagnostics = { clientId: 'app', groupId: 'billing', offsets: [] }
		const rebalance: RebalanceDiagnostics = { clientId: 'app', groupId: 'billing' }
		const request: ProduceRequestDiagnostics = {
			clientId: 'app',
			brokerId: 2,
			partitions: 1,
			records: 120,
			bytes: 9000,
		}
		await diagnostics.commit.tracePromise(async () => {}, commit)
		await diagnostics.rebalance.tracePromise(async () => {}, rebalance)
		await diagnostics.produceRequest.tracePromise(async () => ({}) as never, request)

		const metrics = await recorder.collect()
		const commitDuration = metrics
			.get(METRIC.operationDuration)!
			.find(point => point.attributes[ATTR.operationName] === 'commit')!
		expect(commitDuration.attributes[ATTR.consumerGroup]).toBe('billing')
		expect((metrics.get(KAFKATS_METRIC.rebalanceDuration)![0]!.value as Histogram).count).toBe(1)
		const records = metrics.get(KAFKATS_METRIC.requestRecords)![0]!
		expect((records.value as Histogram).sum).toBe(120)
		expect(records.attributes[ATTR.brokerId]).toBe(2)
		expect(records.attributes[ATTR.errorType]).toBeUndefined()
	})

	it('marks a produce request whose partitions the broker rejected, though the request itself succeeded', async () => {
		const request: ProduceRequestDiagnostics = {
			clientId: 'app',
			brokerId: 2,
			partitions: 1,
			records: 3,
			bytes: 90,
		}
		await diagnostics.produceRequest.tracePromise(async () => {
			request.rejected = [{ topic: 'orders', partition: 0, errorCode: ErrorCode.NotLeaderOrFollower }]
			return {} as never
		}, request)

		const [duration] = (await recorder.collect()).get(KAFKATS_METRIC.requestDuration)!
		expect(duration!.attributes[ATTR.errorType]).toBe('NotLeaderOrFollower')
	})

	it('reports the backlog of producers and the position of consumers when metrics are collected', async () => {
		const producerMetrics: ProducerMetrics = {
			queuedBatches: 2,
			queuedRecords: 30,
			queuedBytes: 4000,
			inFlightRequests: 1,
			unacknowledgedSends: 5,
		}
		const consumerMetrics: ConsumerMetrics = {
			partitions: [
				{
					topic: 'orders',
					partition: 0,
					position: 90n,
					endOffset: 100n,
					bufferedBytes: 512,
					bufferedRecords: 4,
					paused: false,
				},
				{ topic: 'orders', partition: 1, position: 7n, bufferedBytes: 0, bufferedRecords: 0, paused: true },
			],
		}
		// Created before or after instrumentKafka(): both are found.
		const clientId = unique('app')
		const client = new KafkaClient({ brokers: ['localhost:1'], clientId, logLevel: 'silent' })
		const producer = client.producer()
		producer.metrics = () => producerMetrics
		const consumer = client.consumer({ groupId: 'billing' })
		consumer.metrics = () => consumerMetrics

		const mine = async (name: string) =>
			(await recorder.collect()).get(name)!.filter(point => point.attributes[ATTR.clientId] === clientId)
		expect((await mine(KAFKATS_METRIC.queuedRecords))[0]!.value).toBe(30)
		expect((await mine(KAFKATS_METRIC.queuedBytes))[0]!.value).toBe(4000)
		expect((await mine(KAFKATS_METRIC.inFlightRequests))[0]!.value).toBe(1)
		expect((await mine(KAFKATS_METRIC.unacknowledgedSends))[0]!.value).toBe(5)
		expect((await mine(KAFKATS_METRIC.assignedPartitions))[0]!.value).toBe(2)
		expect((await mine(KAFKATS_METRIC.pausedPartitions))[0]!.value).toBe(1)
		// Only where it is known how far the partition can be read.
		const lag = await mine(KAFKATS_METRIC.positionLag)
		expect(lag.map(point => [point.attributes[ATTR.partition], point.value])).toEqual([['0', 10]])
		expect((await mine(KAFKATS_METRIC.bufferedBytes)).map(point => point.value)).toEqual([512, 0])

		// The numbers are read again each time.
		producerMetrics.queuedRecords = 0
		expect((await mine(KAFKATS_METRIC.queuedRecords))[0]!.value).toBe(0)
	})

	it('adds up instances that share their attributes instead of reporting only one of them', async () => {
		const backlog = (queuedRecords: number): ProducerMetrics => ({
			queuedBatches: 1,
			queuedRecords,
			queuedBytes: 0,
			inFlightRequests: 1,
			unacknowledgedSends: 0,
		})
		// Two producers of one client, as with transaction lanes.
		const clientId = unique('app')
		const client = new KafkaClient({ brokers: ['localhost:1'], clientId, logLevel: 'silent' })
		const producers = [client.producer(), client.producer()]
		producers[0]!.metrics = () => backlog(40)
		producers[1]!.metrics = () => backlog(2)

		const queued = (await recorder.collect())
			.get(KAFKATS_METRIC.queuedRecords)!
			.filter(point => point.attributes[ATTR.clientId] === clientId)
		expect(queued.map(point => point.value)).toEqual([42])
		expect(producers).toHaveLength(2)
	})
})
