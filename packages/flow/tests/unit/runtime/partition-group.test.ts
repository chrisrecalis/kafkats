import { describe, expect, it } from 'vitest'
import type { TopicPartition } from '@kafkats/client'
import { IllegalStateError } from '@/api/errors.js'
import { string } from '@/codec.js'
import { TimestampExtractors } from '@/processor/timestamps.js'
import { PartitionGroup } from '@/runtime/partition-group.js'
import { isSkipMarker, RecordQueue } from '@/runtime/record-queue.js'
import { TaskIdRegistry, tpKey } from '@/runtime/task-id.js'
import { message } from '../support/fake-io.js'

const TASK = new TaskIdRegistry().get(0, 0)
const A = { topic: 'a', partition: 0 }
const B = { topic: 'b', partition: 0 }

function group(maxTaskIdleMs: number, partitions: readonly TopicPartition[] = [A, B]) {
	const queues = partitions.map(
		tp =>
			new RecordQueue(
				tp,
				[
					{
						kind: 'source',
						name: `src-${tp.topic}`,
						topics: [tp.topic],
						valueCodec: string(),
						internal: false,
					},
				],
				TimestampExtractors.logAndSkipOnInvalid,
				() => 'fail',
				() => {},
				TASK
			)
	)
	const offsets = new Map<string, bigint>()
	const lags = new Map<string, bigint | undefined>()
	const g = new PartitionGroup(queues, maxTaskIdleMs)
	return {
		g,
		lags,
		lag: (tp: TopicPartition) => lags.get(tpKey(tp)),
		add(tp: TopicPartition, ...timestamps: number[]) {
			let offset = offsets.get(tpKey(tp)) ?? 0n
			const messages = timestamps.map(timestamp =>
				message(tp.topic, tp.partition, offset++, { value: 'v', timestamp })
			)
			offsets.set(tpKey(tp), offset)
			return g.addRawRecords(tp, messages)
		},
	}
}

function polled(g: PartitionGroup): [string, number][] {
	const order: [string, number][] = []
	for (let record = g.nextRecord(); record !== null; record = g.nextRecord()) {
		order.push([record.queue.tp.topic, isSkipMarker(record) ? -1 : record.timestamp])
	}
	return order
}

describe('PartitionGroup readiness (KIP-695)', () => {
	it('idling disabled (-1): ready iff anything is buffered, whatever the lags', () => {
		const { g, add, lag } = group(-1)
		expect(g.readyToProcess(0, lag)).toBe(false)
		add(A, 1)
		expect(g.readyToProcess(0, lag)).toBe(true)
	})

	it('one input partition: ready iff its queue is not empty', () => {
		const { g, add, lag, lags } = group(1000, [A])
		lags.set(tpKey(A), 0n)
		expect(g.readyToProcess(0, lag)).toBe(false)
		expect(g.nextIdleDeadline()).toBeUndefined()
		add(A, 1)
		lags.set(tpKey(A), undefined)
		expect(g.readyToProcess(0, lag)).toBe(true)
	})

	it('every partition buffered: ready', () => {
		const { g, add, lag } = group(1000)
		add(A, 1)
		add(B, 2)
		expect(g.readyToProcess(0, lag)).toBe(true)
		expect(g.nextIdleDeadline()).toBeUndefined()
	})

	it('an empty partition with an unknown lag: waiting to fetch', () => {
		const { g, add, lag } = group(0)
		add(A, 1)
		expect(g.readyToProcess(0, lag)).toBe(false)
		expect(g.nextIdleDeadline()).toBeUndefined()
	})

	it('an empty partition with records on the broker: waiting to buffer them', () => {
		const { g, add, lag, lags } = group(0)
		add(A, 1)
		lags.set(tpKey(B), 3n)
		expect(g.readyToProcess(0, lag)).toBe(false)
	})

	it('an empty partition with lag 0: idles until the deadline, then processes the others (enforced)', () => {
		const { g, add, lag, lags } = group(100)
		add(A, 1)
		lags.set(tpKey(B), 0n)
		expect(g.readyToProcess(1000, lag)).toBe(false)
		expect(g.nextIdleDeadline()).toBe(1100)
		// The deadline is not moved by later checks.
		expect(g.readyToProcess(1050, lag)).toBe(false)
		expect(g.nextIdleDeadline()).toBe(1100)
		expect(g.readyToProcess(1100, lag)).toBe(true)
		expect(g.nextIdleDeadline()).toBeUndefined()
	})

	it('maxTaskIdleMs 0: an empty partition with lag 0 does not hold the others back', () => {
		const { g, add, lag, lags } = group(0)
		add(A, 1)
		lags.set(tpKey(B), 0n)
		expect(g.readyToProcess(0, lag)).toBe(true)
	})

	it('enforced processing with nothing buffered: not ready, no deadline to wait for', () => {
		const { g, lag, lags } = group(0)
		lags.set(tpKey(A), 0n)
		lags.set(tpKey(B), 0n)
		expect(g.readyToProcess(0, lag)).toBe(false)
		expect(g.nextIdleDeadline()).toBeUndefined()
	})

	it('a partition that gets records, or lag, starts a new idle period later', () => {
		const { g, add, lag, lags } = group(100)
		add(A, 1, 2)
		lags.set(tpKey(B), 0n)
		expect(g.readyToProcess(0, lag)).toBe(false)
		expect(g.nextIdleDeadline()).toBe(100)
		add(B, 3)
		expect(g.readyToProcess(10, lag)).toBe(true)
		g.nextRecord()
		g.nextRecord()
		g.nextRecord()
		add(A, 4)
		expect(g.readyToProcess(500, lag)).toBe(false)
		expect(g.nextIdleDeadline()).toBe(600)
		lags.set(tpKey(B), 1n)
		expect(g.readyToProcess(700, lag)).toBe(false)
		lags.set(tpKey(B), 0n)
		expect(g.readyToProcess(700, lag)).toBe(false)
		expect(g.nextIdleDeadline()).toBe(800)
	})

	it('a partition whose records arrive and drain between two readiness checks idles again afresh', () => {
		const { g, add, lag, lags } = group(1000)
		add(A, 10, 20, 30)
		lags.set(tpKey(B), 0n)
		expect(g.readyToProcess(0, lag)).toBe(false)
		expect(g.readyToProcess(1000, lag)).toBe(true)
		// Within one slice (readiness is not checked again until a queue drains): B gets a record and drains.
		add(B, 5)
		expect(g.nextRecord()).toMatchObject({ queue: { tp: B }, timestamp: 5 })
		// B's deadline of the last idle period is gone: it idles for maxTaskIdleMs again.
		expect(g.readyToProcess(1001, lag)).toBe(false)
		expect(g.nextIdleDeadline()).toBe(2001)
	})
})

describe('PartitionGroup selection and time', () => {
	it('picks the smallest head timestamp; ties go to the earlier input partition', () => {
		const { g, add } = group(-1)
		add(A, 5, 7, 9)
		add(B, 5, 6, 10)
		expect(polled(g)).toEqual([
			['a', 5],
			['b', 5],
			['b', 6],
			['a', 7],
			['a', 9],
			['b', 10],
		])
	})

	it('stream time is the max timestamp polled; partition times per partition', () => {
		const { g, add } = group(-1)
		expect(g.streamTime).toBe(-1)
		add(A, 8)
		add(B, 3)
		g.nextRecord()
		expect(g.streamTime).toBe(3)
		g.nextRecord()
		expect(g.streamTime).toBe(8)
		expect(g.partitionTime(A)).toBe(8)
		expect(g.partitionTime(B)).toBe(3)
		add(B, 1)
		g.nextRecord()
		expect(g.streamTime).toBe(8)
		expect(g.partitionTime(B)).toBe(3)
	})

	it('a SkipMarker is polled first and does not move time', () => {
		const { g, add } = group(-1)
		add(A, 4)
		add(B, -1)
		expect(polled(g)).toEqual([
			['b', -1],
			['a', 4],
		])
		expect(g.partitionTime(B)).toBe(-1)
	})

	it('seeded partition times raise the stream time', () => {
		const { g } = group(-1)
		g.setPartitionTime(A, 50)
		g.setPartitionTime(B, 20)
		expect(g.partitionTime(A)).toBe(50)
		expect(g.streamTime).toBe(50)
	})

	it('counts buffered records; clear() drops records, times and deadlines', () => {
		const { g, add, lag, lags } = group(100)
		add(A, 1, 2)
		expect(add(B, 3)).toBe(1)
		expect(g.numBuffered).toBe(3)
		g.nextRecord()
		expect(g.numBuffered).toBe(2)
		lags.set(tpKey(B), 0n)
		g.nextRecord()
		g.readyToProcess(0, lag)
		g.clear()
		expect(g.numBuffered).toBe(0)
		expect(g.streamTime).toBe(-1)
		expect(g.partitionTime(A)).toBe(-1)
		expect(g.nextIdleDeadline()).toBeUndefined()
		add(A, 1)
		expect(g.readyToProcess(500, lag)).toBe(false)
		expect(g.nextIdleDeadline()).toBe(600)
	})

	it('an unknown partition is a bug', () => {
		const { g } = group(-1)
		expect(() => g.queue({ topic: 'c', partition: 0 })).toThrow(IllegalStateError)
	})
})
