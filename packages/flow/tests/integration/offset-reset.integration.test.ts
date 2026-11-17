/**
 * Where partitions without a usable committed offset start (KF16, CR7, 4.7.4): the default policy 'earliest',
 * per-source policies, 'latest' start positions committed and resumed after a restart, repartition topics always
 * from the beginning, and 'none' failing the app when there is no committed offset or its records are gone.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { NoOffsetForPartitionError, string, type KafkaClient, type Producer } from '@kafkats/client'
import type { FlowApp, FlowConfig } from '../../src/api/app.js'
import { NoOffsetResetError } from '../../src/api/errors.js'
import { flow } from '../../src/app/flow.js'
import { internals } from '../../src/app/internal.js'
import { COUNT_CODEC } from '../../src/dsl/ops/aggregate.js'
import {
	collectUnhandledRejections,
	createTopics,
	MultiTopicCollector,
	newClient,
	sleep,
	uniqueTopicName,
	waitUntil,
} from './test-helpers.js'

const strings = { key: string(), value: string() }
const GUARANTEES = ['at_least_once', 'exactly_once'] as const

let client: KafkaClient
let producer: Producer

function app(applicationId: string, config: Partial<FlowConfig> = {}): FlowApp {
	return flow({ applicationId, client, consumer: { maxWaitMs: 100 }, ...config })
}

async function produce(topic: string, ...values: string[]): Promise<void> {
	await producer.send(
		topic,
		values.map(value => ({ key: Buffer.from(value), value: Buffer.from(value) }))
	)
}

async function collector(...topics: string[]): Promise<MultiTopicCollector> {
	const output = new MultiTopicCollector({
		client,
		groupId: uniqueTopicName('offset-reset-reader'),
		topics: topics.map(topic => ({ topic, keyCodec: string(), valueCodec: string() })),
	})
	await output.start()
	return output
}

function values(output: MultiTopicCollector, topic: string): string[] {
	return output.getTopicMessages<string, string>(topic).map(m => m.value)
}

/** The group's committed offset of partition 0 of `topic` (none: undefined). */
async function committed(groupId: string, topic: string): Promise<bigint | undefined> {
	const coordinator = await client.cluster.getCoordinator('GROUP', groupId)
	const response = await coordinator.offsetFetch({
		groupId,
		topics: [{ name: topic, partitions: [{ partitionIndex: 0 }] }],
		requireStable: true,
	})
	const offset = response.topics[0]?.partitions[0]?.committedOffset ?? -1n
	return offset < 0n ? undefined : offset
}

describe('offset reset', () => {
	const unhandled = collectUnhandledRejections()

	beforeAll(async () => {
		client = newClient('offset-reset')
		await client.connect()
		producer = client.producer({ lingerMs: 0 })
	})

	afterAll(async () => {
		await producer.disconnect()
		await client.disconnect()
		expect(unhandled).toEqual([])
	})

	describe.each(GUARANTEES)('%s', processingGuarantee => {
		it("reads from 'earliest' by default; Consumed.offsetReset 'latest' skips what its topic held before the start", async () => {
			const early = uniqueTopicName('reset-default-in')
			const late = uniqueTopicName('reset-latest-in')
			const out = uniqueTopicName('reset-default-out')
			const applicationId = uniqueTopicName('reset-default')
			await createTopics(client, [{ name: early }, { name: late }, { name: out }])
			await produce(early, 'e1', 'e2')
			await produce(late, 'l1', 'l2')
			const output = await collector(out)
			const instance = app(applicationId)
			instance.stream(early, strings).to(out, strings)
			instance.stream(late, { ...strings, offsetReset: 'latest' }).to(out, strings)
			try {
				await instance.start()
				// The start position of the 'latest' source is committed before its task runs.
				expect(await committed(applicationId, late)).toBe(2n)
				await produce(late, 'l3')
				await output.waitForCount(out, () => true, 3, 15_000)
				await sleep(500)
				expect(values(output, out).sort()).toEqual(['e1', 'e2', 'l3'])
			} finally {
				await instance.close()
				await output.stop()
			}
		}, 60_000)

		it("'latest': a restart before the first commit resumes from the committed start position", async () => {
			const input = uniqueTopicName('reset-pin-in')
			const out = uniqueTopicName('reset-pin-out')
			const applicationId = uniqueTopicName('reset-pin')
			await createTopics(client, [{ name: input }, { name: out }])
			await produce(input, 'before')
			const output = await collector(out)
			const eos = processingGuarantee === 'exactly_once'
			let processed = 0
			const build = (poisoned: boolean) => {
				// The first instance does not commit before its failure (a transaction stays open up to the interval).
				const commitIntervalMs = poisoned ? 10_000 : undefined
				const instance = app(applicationId, { processingGuarantee, commitIntervalMs })
				instance
					.stream(input, { ...strings, offsetReset: 'latest' })
					.mapValues(value => {
						if (poisoned && value === 'boom') throw new Error('boom')
						processed++
						return value?.toUpperCase() ?? null
					})
					.to(out, strings)
				return instance
			}
			try {
				// The first instance stops on a failure before it ever commits what it processed (exactly-once: its
				// output is aborted, the reader sees only committed records).
				const first = build(true)
				await first.start()
				await produce(input, 'a', 'b')
				await waitUntil('a and b processed', () => processed === 2, 15_000)
				if (!eos) await output.waitForCount(out, () => true, 2, 15_000)
				await produce(input, 'boom')
				await waitUntil('the first instance stopped', () => first.state() === 'ERROR', 15_000)
				await first.close()
				expect(await committed(applicationId, input)).toBe(1n)

				// Not from the end of the log: from where the first instance started.
				const expected = [...(eos ? [] : ['A', 'B']), 'A', 'B', 'BOOM']
				const second = build(false)
				await second.start()
				await output.waitForCount(out, () => true, expected.length, 15_000)
				await second.close()
				expect(values(output, out)).toEqual(expected)
				expect(await committed(applicationId, input)).toBe(4n)
			} finally {
				await output.stop()
			}
		}, 90_000)
	})

	it("repartition topics are read from the beginning even when the app's policy is 'latest'", async () => {
		const input = uniqueTopicName('reset-repartition-in')
		const out = uniqueTopicName('reset-repartition-out')
		const applicationId = uniqueTopicName('reset-repartition')
		await createTopics(client, [{ name: input }, { name: out }])
		const instance = app(applicationId, { consumer: { maxWaitMs: 100, autoOffsetReset: 'latest' } })
		instance
			.stream(input, strings)
			.groupBy((_key, value) => value, { ...strings, name: 'by-value' })
			.count()
			.toStream()
			.to(out, { key: string(), value: COUNT_CODEC })
		const repartition = [...internals(instance).build().internalTopics.values()].find(
			spec => spec.kind === 'repartition'
		)!.name
		// What an upstream task of an earlier start wrote and no downstream task committed yet.
		await createTopics(client, [{ name: repartition }])
		await produce(repartition, 'x', 'x')
		const output = new MultiTopicCollector({
			client,
			groupId: uniqueTopicName('offset-reset-reader'),
			topics: [{ topic: out, keyCodec: string(), valueCodec: COUNT_CODEC }],
		})
		await output.start()
		try {
			await instance.start()
			await produce(input, 'x')
			const counts = await output.waitForCount<string, number>(out, () => true, 3, 15_000)
			expect(counts.map(m => m.value)).toEqual([1, 2, 3])
		} finally {
			await instance.close()
			await output.stop()
		}
	}, 60_000)

	it("'none' without a committed offset fails the start", async () => {
		const input = uniqueTopicName('reset-none-in')
		await createTopics(client, [{ name: input }])
		const instance = app(uniqueTopicName('reset-none'))
		instance.stream(input, { ...strings, offsetReset: 'none' }).to(uniqueTopicName('reset-none-out'), strings)
		await expect(instance.start()).rejects.toThrow(NoOffsetForPartitionError)
		expect(instance.state()).toBe('ERROR')
		await instance.close()
		expect(instance.state()).toBe('STOPPED')
	}, 60_000)

	it("a committed offset whose records were deleted: 'none' stops the app, 'earliest' goes to the log start", async () => {
		const input = uniqueTopicName('reset-range-in')
		const out = uniqueTopicName('reset-range-out')
		const applicationId = uniqueTopicName('reset-range')
		await createTopics(client, [{ name: input }, { name: out }])
		const output = await collector(out)
		const build = (offsetReset?: 'none') => {
			const instance = app(applicationId)
			instance.stream(input, { ...strings, offsetReset }).to(out, strings)
			return instance
		}
		try {
			await produce(input, 'r0', 'r1')
			const first = build()
			await first.start()
			await output.waitForCount(out, () => true, 2, 15_000)
			await first.close()
			expect(await committed(applicationId, input)).toBe(2n)

			await produce(input, 'r2', 'r3', 'r4', 'r5')
			await client.admin().deleteRecords([{ topic: input, partition: 0, offset: 4n }])

			// Its task runs, and its first fetch finds the offset out of range.
			const strict = build('none')
			await strict.start().catch(() => {})
			await waitUntil('ERROR', () => strict.state() === 'ERROR', 10_000)
			expect(strict.getError()).toBeInstanceOf(NoOffsetResetError)
			await strict.close()
			expect(values(output, out)).toEqual(['r0', 'r1'])

			const lenient = build()
			await lenient.start()
			await output.waitForCount(out, () => true, 4, 15_000)
			await lenient.close()
			expect(values(output, out)).toEqual(['r0', 'r1', 'r4', 'r5'])
		} finally {
			await output.stop()
		}
	}, 90_000)
})
