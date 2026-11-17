/**
 * The task runtime against a real broker (WP2.3, the 2A walking skeleton): a stateless and a counting topology under
 * both guarantees, start and close. Topics are fresh, so changelogs start empty (this build restores nothing).
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { string, type Consumer, type KafkaClient, type Producer } from '@kafkats/client'
import type { FlowApp, FlowConfig } from '../../src/api/app.js'
import { SourceTopicNotFoundError } from '../../src/changelog/errors.js'
import { flow } from '../../src/app/flow.js'
import { internals } from '../../src/app/internal.js'
import { COUNT_CODEC } from '../../src/dsl/ops/aggregate.js'
import { valueAndTimestampCodec } from '../../src/state/timestamped.js'
import {
	collectUnhandledRejections,
	createTopics,
	newClient,
	readCommittedTable,
	sleep,
	uniqueTopicName,
	waitUntil,
} from './test-helpers.js'

const strings = { key: string(), value: string() }
const GUARANTEES = ['at_least_once', 'exactly_once'] as const

let client: KafkaClient
let producer: Producer

/** What a read_committed consumer of `topic` saw so far. */
class Output {
	readonly records: { key: string; value: Buffer; partition: number }[] = []
	private readonly consumer: Consumer
	private running: Promise<void> | undefined

	private constructor(private readonly topic: string) {
		this.consumer = client.consumer({
			groupId: uniqueTopicName('skeleton-reader'),
			autoOffsetReset: 'earliest',
			isolationLevel: 'read_committed',
			maxWaitMs: 100,
		})
	}

	static async start(topic: string): Promise<Output> {
		const output = new Output(topic)
		const ready = new Promise<void>(resolve => output.consumer.once('running', () => resolve()))
		output.running = output.consumer.runEach(output.topic, async m => {
			output.records.push({ key: m.key!.toString(), value: m.value, partition: m.partition })
		})
		await ready
		return output
	}

	values(): string[] {
		return this.records.map(r => r.value.toString())
	}

	async stop(): Promise<void> {
		this.consumer.stop()
		await this.running
	}
}

function app(applicationId: string, config: Partial<FlowConfig> = {}): FlowApp {
	return flow({ applicationId, client, consumer: { maxWaitMs: 100 }, ...config })
}

async function produce(topic: string, records: readonly { key: string; value: string }[]): Promise<void> {
	await producer.send(
		topic,
		records.map(({ key, value }) => ({ key: Buffer.from(key), value: Buffer.from(value) }))
	)
}

const range = (from: number, to: number) => Array.from({ length: to - from }, (_, i) => from + i)

describe('walking skeleton', () => {
	const unhandled = collectUnhandledRejections()

	beforeAll(async () => {
		client = newClient('walking-skeleton')
		await client.connect()
		producer = client.producer()
	})

	afterAll(async () => {
		await producer.disconnect()
		await client.disconnect()
		expect(unhandled).toEqual([])
	})

	describe.each(GUARANTEES)('%s', processingGuarantee => {
		it('a stateless topology processes records written before and after start; close commits, a restart resumes', async () => {
			const input = uniqueTopicName('skeleton-stateless-in')
			const outputTopic = uniqueTopicName('skeleton-stateless-out')
			const applicationId = uniqueTopicName('skeleton-stateless')
			await createTopics(client, [
				{ name: input, partitions: 2 },
				{ name: outputTopic, partitions: 2 },
			])
			const output = await Output.start(outputTopic)
			const build = () => {
				const instance = app(applicationId, { processingGuarantee })
				instance
					.stream(input, strings)
					.filter((_key, value) => value !== 'skip')
					.mapValues(value => value?.toUpperCase() ?? null)
					.to(outputTopic, strings)
				return instance
			}
			const record = (i: number) => ({ key: `k${i}`, value: `v${i}` })

			await produce(input, range(0, 10).map(record))
			const first = build()
			expect(first.state()).toBe('CREATED')
			await first.start()
			expect(first.state()).toBe('RUNNING')
			await produce(input, [...range(10, 20).map(record), { key: 'k', value: 'skip' }])
			await waitUntil('20 outputs', () => output.records.length >= 20, 30_000)
			await first.close()
			expect(first.state()).toBe('STOPPED')
			expect(first.getError()).toBeNull()

			// Everything processed was committed at close: the next start reads only what came after.
			const second = build()
			await second.start()
			await produce(input, range(20, 25).map(record))
			await waitUntil('25 outputs', () => output.records.length >= 25, 30_000)
			await sleep(1000)
			await second.close()
			await output.stop()
			expect(output.values().sort()).toEqual(
				range(0, 25)
					.map(i => `V${i}`)
					.sort()
			)
			for (const { key, value } of output.records) expect(value.toString()).toBe(key.replace('k', 'V'))
		}, 120_000)

		it('a count through a repartition topic counts every record once; its changelog holds the counts', async () => {
			const input = uniqueTopicName('skeleton-count-in')
			const outputTopic = uniqueTopicName('skeleton-count-out')
			const applicationId = uniqueTopicName('skeleton-count')
			await createTopics(client, [
				{ name: input, partitions: 2 },
				{ name: outputTopic, partitions: 2 },
			])
			const output = await Output.start(outputTopic)
			const instance = app(applicationId, { processingGuarantee })
			instance
				.stream(input, strings)
				.groupBy((_key, value) => value, strings)
				.count()
				.toStream()
				.to(outputTopic, { key: string(), value: COUNT_CODEC })
			const internal = [...internals(instance).build().internalTopics.values()]
			expect(internal.map(spec => spec.kind).sort()).toEqual(['changelog', 'repartition'])

			const words = ['apple', 'banana', 'cherry']
			await produce(
				input,
				range(0, 15).map(i => ({ key: `k${i}`, value: words[i % 3]! }))
			)
			await instance.start()
			await produce(
				input,
				range(15, 30).map(i => ({ key: `k${i}`, value: words[i % 3]! }))
			)
			await waitUntil('30 counts', () => output.records.length >= 30, 30_000)
			await sleep(1000)
			await instance.close()
			await output.stop()

			// A word's counts are written in order to one partition: 1..10, each exactly once.
			for (const word of words) {
				const counts = output.records.filter(r => r.key === word).map(r => COUNT_CODEC.decode(r.value))
				expect(counts).toEqual(range(1, 11))
			}
			// Its store is timestamped: values are `[timestamp][count]`.
			const changelog = internal.find(spec => spec.kind === 'changelog')!.name
			const table = await readCommittedTable(client, changelog, valueAndTimestampCodec(COUNT_CODEC))
			expect(Object.fromEntries(Object.entries(table).map(([word, { value }]) => [word, value]))).toEqual({
				apple: 10,
				banana: 10,
				cherry: 10,
			})
		}, 120_000)
	})

	describe('start and close', () => {
		it('a custom partitioner reads the partition count of its topic', async () => {
			const input = uniqueTopicName('skeleton-partitioner-in')
			const outputTopic = uniqueTopicName('skeleton-partitioner-out')
			await createTopics(client, [
				{ name: input, partitions: 1 },
				{ name: outputTopic, partitions: 3 },
			])
			const output = await Output.start(outputTopic)
			const instance = app(uniqueTopicName('skeleton-partitioner'))
			instance
				.stream(input, strings)
				.to(outputTopic, { ...strings, partitioner: (_key, _value, count) => count - 1 })
			await instance.start()
			await produce(input, [
				{ key: 'a', value: '1' },
				{ key: 'b', value: '2' },
			])
			await waitUntil('2 outputs', () => output.records.length >= 2, 30_000)
			await instance.close()
			await output.stop()
			expect(output.records.map(r => r.partition)).toEqual([2, 2])
		}, 60_000)

		it('close() during start fails the start and stops the app; a later start runs', async () => {
			const input = uniqueTopicName('skeleton-close-in')
			const outputTopic = uniqueTopicName('skeleton-close-out')
			await createTopics(client, [{ name: input }, { name: outputTopic }])
			const instance = app(uniqueTopicName('skeleton-close'))
			instance.stream(input, strings).to(outputTopic, strings)

			const starting = instance.start()
			const closing = instance.close()
			expect(instance.close()).toBe(closing)
			await expect(starting).rejects.toThrow()
			await closing
			expect(instance.state()).toBe('STOPPED')

			const output = await Output.start(outputTopic)
			await instance.start()
			expect(instance.state()).toBe('RUNNING')
			await produce(input, [{ key: 'a', value: '1' }])
			await waitUntil('1 output', () => output.records.length >= 1, 30_000)
			const closedAt = Date.now()
			await instance.close({ timeoutMs: 10_000 })
			expect(Date.now() - closedAt).toBeLessThan(10_000)
			expect(instance.state()).toBe('STOPPED')
			await output.stop()
		}, 60_000)

		it('start() rejects when a source topic does not exist; close() stops the app', async () => {
			const instance = app(uniqueTopicName('skeleton-missing'))
			instance.stream(uniqueTopicName('skeleton-missing-in'), strings).to(uniqueTopicName('skeleton-x'), strings)
			await expect(instance.start()).rejects.toThrow(SourceTopicNotFoundError)
			expect(instance.state()).toBe('ERROR')
			expect(instance.getError()).toBeInstanceOf(SourceTopicNotFoundError)
			await instance.close()
			expect(instance.state()).toBe('STOPPED')
		}, 30_000)
	})
})
