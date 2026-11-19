import { describe, expect, it } from 'vitest'

import type { Consumer } from '@/consumer/consumer.js'
import type { Message } from '@/consumer/types.js'
import { deferred, fakeBroker, groupConsumer, sleep, TOPIC, tp, until } from './group-harness.js'

const label = (message: Message) => `${message.partition}:${message.offset}`
const offsetsOf = (delivered: string[], partition: number) =>
	delivered.filter(entry => entry.startsWith(`${partition}:`)).map(entry => entry.split(':')[1])

/** The batches the consumer's run loop drained and has not handed to a handler yet. */
const waiting = (consumer: Consumer) =>
	(consumer as unknown as { undelivered: Array<{ partition: number }> }).undelivered.map(batch => batch.partition)

/**
 * Two partitions with three records each, fetched in one response, so that while the handler of the first runs
 * (one at a time), the batch of the other waits for its turn. `act` runs once, in the first handler, with the
 * partition that waits.
 */
async function twoPartitionsOneSlot(act: (consumer: Consumer, other: number) => void) {
	const broker = fakeBroker()
	broker.produce(0, 3)
	broker.produce(1, 3)
	const consumer = groupConsumer(broker)
	const delivered: string[] = []
	let first: number | undefined
	const run = consumer.runBatch(
		TOPIC,
		async messages => {
			delivered.push(...messages.map(label))
			if (first !== undefined) return
			first = messages[0]!.partition
			act(consumer, 1 - first)
		},
		{ commitOffsets: false }
	)
	await until(() => first !== undefined, 'the first batch')
	return { broker, consumer, run, delivered, first: first!, other: 1 - first! }
}

describe('Consumer delivery around pause, seek and the next offset', () => {
	it('withholds the waiting batch of a partition paused by another handler, and delivers it once after resume', async () => {
		const observed: Record<string, unknown> = {}
		const { consumer, run, delivered, other } = await twoPartitionsOneSlot((consumer, other) => {
			observed.waiting = waiting(consumer)
			observed.nextOffset = consumer.nextOffset(TOPIC, other)
			observed.lag = consumer.currentLag(TOPIC, other)
			consumer.pause([tp(other)])
			observed.nextOffsetPaused = consumer.nextOffset(TOPIC, other)
			observed.lagPaused = consumer.currentLag(TOPIC, other)
		})

		// The waiting batch counts as not delivered; the pause rewinds over it and stops following the end.
		expect(observed).toEqual({
			waiting: [other],
			nextOffset: 0n,
			lag: 3n,
			nextOffsetPaused: 0n,
			lagPaused: undefined,
		})
		await sleep(50)
		expect(offsetsOf(delivered, other)).toEqual([])

		consumer.resume([tp(other)])
		await until(() => delivered.length === 6, 'the resumed partition')
		await sleep(30)
		expect(offsetsOf(delivered, other)).toEqual(['0', '1', '2'])
		expect(delivered).toHaveLength(6)

		consumer.stop()
		await run
	})

	it('does not deliver again the batch of a handler that pauses its own partition', async () => {
		const broker = fakeBroker()
		broker.produce(0, 3)
		const consumer = groupConsumer(broker)
		const delivered: string[] = []
		let paused = false
		const run = consumer.runBatch(
			TOPIC,
			async messages => {
				delivered.push(...messages.map(label))
				// Backpressure: the records were taken over; stop fetching until they are processed.
				if (!paused) {
					paused = true
					consumer.pause([tp(0)])
					expect(consumer.nextOffset(TOPIC, 0)).toBe(3n)
				}
			},
			{ commitOffsets: false }
		)
		await until(() => paused, 'the batch')

		consumer.resume([tp(0)])
		broker.produce(0, 1)
		await until(() => delivered.length === 4, 'the next record')
		await sleep(30)
		expect(delivered).toEqual(['0:0', '0:1', '0:2', '0:3'])

		consumer.stop()
		await run
	})

	it('drops the waiting batch of a partition sought by another handler, and delivers from the seek offset', async () => {
		const { consumer, run, delivered, other } = await twoPartitionsOneSlot((consumer, other) => {
			consumer.seek(TOPIC, other, 2n)
		})

		await until(() => offsetsOf(delivered, other).length > 0, 'the sought partition')
		await sleep(30)
		expect(offsetsOf(delivered, other)).toEqual(['2'])

		consumer.stop()
		await run
	})

	it('is at the fetch position once everything fetched is delivered, with no lag when caught up', async () => {
		const { broker, consumer, run, delivered, first } = await twoPartitionsOneSlot(() => {})
		await until(() => delivered.length === 6, 'every record')

		expect(consumer.nextOffset(TOPIC, first)).toBe(3n)
		await until(() => consumer.currentLag(TOPIC, first) === 0n, 'the end of the partition')

		broker.produce(first, 2)
		await until(() => delivered.length === 8, 'the new records')
		expect(consumer.nextOffset(TOPIC, first)).toBe(5n)
		expect(consumer.nextOffset(TOPIC, 9)).toBeUndefined()
		expect(consumer.currentLag(TOPIC, 9)).toBeUndefined()

		consumer.stop()
		await run
	})

	it('counts a batch as undelivered from the moment it leaves the buffer, so a pause right then withholds it', async () => {
		const broker = fakeBroker({ partitions: 3 })
		broker.produce(0, 1)
		broker.produce(1, 1)
		const consumer = groupConsumer(broker)
		const gate = deferred()
		const delivered: string[] = []
		const started = new Set<number>()
		const nextOffsets: Array<bigint | undefined> = []
		let deliveredAtPause: boolean | undefined
		const deliveredOf2 = () => offsetsOf(delivered, 2).length > 0
		const bufferedOf2 = () => consumer.metrics().partitions.find(p => p.partition === 2)?.bufferedRecords

		const run = consumer.runBatch(
			TOPIC,
			async messages => {
				const { partition } = messages[0]!
				delivered.push(...messages.map(label))
				if (partition === 2) return
				started.add(partition)
				await gate.promise
				if (partition === 0) return
				// Runs between the run loop's steps, as handlers of other partitions do: partition 2's batch leaves
				// the buffer meanwhile, and is handed to a handler a few steps later.
				for (let hop = 0; hop < 50; hop++) {
					await Promise.resolve()
					nextOffsets.push(consumer.nextOffset(TOPIC, 2))
					if (deliveredAtPause === undefined && bufferedOf2() === 0) {
						deliveredAtPause = deliveredOf2()
						consumer.pause([tp(2)])
					}
				}
			},
			{ commitOffsets: false, partitionConcurrency: 2 }
		)
		await until(() => started.size === 2, 'both slots busy')
		broker.produce(2, 3)
		await until(() => bufferedOf2() === 3, 'the records of partition 2 buffered')

		gate.resolve()
		await until(() => nextOffsets.length === 50, 'the spinning handler')
		await sleep(30)

		// Never delivered while paused; until then, its first record is the next offset wherever the batch is.
		expect(deliveredAtPause).toBe(false)
		expect(offsetsOf(delivered, 2)).toEqual([])
		expect(new Set(nextOffsets)).toEqual(new Set([0n]))

		consumer.resume([tp(2)])
		await until(() => offsetsOf(delivered, 2).length === 3, 'the resumed partition')
		await sleep(30)
		expect(offsetsOf(delivered, 2)).toEqual(['0', '1', '2'])

		consumer.stop()
		await run
	})

	it('stream() skips the batch of a partition paused before its turn, and yields it once after resume', async () => {
		const broker = fakeBroker()
		broker.produce(0, 3)
		broker.produce(1, 3)
		const consumer = groupConsumer(broker)
		const delivered: string[] = []
		/** For each record of the paused partition: whether it was yielded after the resume. */
		const yieldedAfterResume: boolean[] = []
		let other: number | undefined
		let resumed = false

		for await (const { message } of consumer.stream(TOPIC, { commitOffsets: false })) {
			delivered.push(label(message))
			if (message.partition === other) yieldedAfterResume.push(resumed)
			if (other === undefined) {
				other = 1 - message.partition
				consumer.pause([tp(other)])
				setTimeout(() => {
					resumed = true
					consumer.resume([tp(other!)])
				}, 50)
			}
			if (delivered.length === 6) break
		}

		// Its batch, drained before the pause, was not yielded while paused, and only once after the resume.
		expect(yieldedAfterResume).toEqual([true, true, true])
		expect(offsetsOf(delivered, other!)).toEqual(['0', '1', '2'])
		expect(offsetsOf(delivered, 1 - other!)).toEqual(['0', '1', '2'])
	})
})
