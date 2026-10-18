import { describe, expect, it } from 'vitest'

import { string } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { sleep, uniqueName } from '../helpers/testkit.js'

const maxBytesPerPartition = 16 * 1024

// The consumer estimates each record as key + value + 64 bytes when sizing batches.
function batchBytes(values: string[]): number {
	return values.reduce((sum, value) => sum + Buffer.byteLength(value) + 64, 0)
}

describe.concurrent('Consumer (integration) - maxBytesPerPartition', () => {
	it('caps each batch when prefetched fetch responses pile up behind a busy handler', async () => {
		const client = createClient('it-max-bytes-prefetch')
		await client.connect()

		const topicName = uniqueName('it-max-bytes-prefetch')
		const testTopic = topic<string>(topicName, { value: string() })
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		// Each send becomes one ~15KB record batch, so every Fetch response carries roughly one
		// maxBytesPerPartition worth of records.
		const producer = client.producer({ lingerMs: 0 })
		const total = 180
		const expected = Array.from({ length: total }, (_, index) => String(index).padEnd(1000, 'x'))
		for (let start = 0; start < total; start += 15) {
			await producer.send(
				testTopic,
				expected.slice(start, start + 15).map(value => ({ value }))
			)
		}
		await producer.flush()

		const consumer = client.consumer({
			groupId: uniqueName('it-group'),
			autoOffsetReset: 'earliest',
			maxBytesPerPartition,
		})
		let fetchResponses = 0
		consumer.on('fetchPosition', () => {
			fetchResponses++
		})

		const batches: string[][] = []
		let fetchResponsesAtRelease = 0
		const safetyTimer = setTimeout(() => consumer.stop(), 20_000)
		await consumer.runBatch(
			testTopic,
			async messages => {
				if (batches.length === 0) {
					// Hold the first batch until at least two more Fetch responses for this partition sit
					// in the internal buffer, the way a slow read-transform-write loop lets prefetch run ahead.
					const deadline = Date.now() + 10_000
					while (fetchResponses < 3 && Date.now() < deadline) await sleep(10)
					fetchResponsesAtRelease = fetchResponses
				}
				batches.push(messages.map(message => message.value))
				if (batches.flat().length >= total) consumer.stop()
			},
			{ autoCommit: false }
		)
		clearTimeout(safetyTimer)

		expect(fetchResponsesAtRelease).toBeGreaterThanOrEqual(3)
		expect(batches.flat()).toEqual(expected)
		for (const batch of batches) {
			expect(batchBytes(batch)).toBeLessThanOrEqual(maxBytesPerPartition)
		}
		// The buffered backlog is still delivered as full batches, not one record at a time.
		expect(batchBytes(batches[1]!)).toBeGreaterThan(maxBytesPerPartition / 2)

		await producer.disconnect()
		await client.disconnect()
	})

	it('delivers a record larger than maxBytesPerPartition in a batch of its own', async () => {
		const client = createClient('it-max-bytes-oversized')
		await client.connect()

		const topicName = uniqueName('it-max-bytes-oversized')
		const testTopic = topic<string>(topicName, { value: string() })
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		const big = 'big'.padEnd(4 * maxBytesPerPartition, 'x')
		await producer.send(testTopic, [{ value: big }, { value: 'small-1' }, { value: 'small-2' }])
		await producer.flush()

		const consumer = client.consumer({
			groupId: uniqueName('it-group'),
			autoOffsetReset: 'earliest',
			maxBytesPerPartition,
		})
		const batches: string[][] = []
		const safetyTimer = setTimeout(() => consumer.stop(), 20_000)
		await consumer.runBatch(
			testTopic,
			async messages => {
				batches.push(messages.map(message => message.value))
				if (batches.flat().length >= 3) consumer.stop()
			},
			{ autoCommit: false }
		)
		clearTimeout(safetyTimer)

		expect(batches[0]).toEqual([big])
		expect(batches.flat()).toEqual([big, 'small-1', 'small-2'])

		await producer.disconnect()
		await client.disconnect()
	})
})
