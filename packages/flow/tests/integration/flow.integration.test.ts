import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { KafkaClient, type Producer } from '@kafkats/client'
import {
	ChangelogPartitionMismatchError,
	SourceTopicNotFoundError,
	TimeWindows,
	buildChangelogTopicName,
	codec,
	flow,
} from '../../src/index.js'
import { MultiTopicCollector, createTopics, requireKafkaBrokers, uniqueTopicName } from './test-helpers.js'

type InputEvent = { n: number; type: string }

const stringCodec = codec.string()
const jsonCodec = codec.json<InputEvent>()

const numberCodec = {
	encode: (v: number) => {
		const buf = Buffer.alloc(8)
		buf.writeDoubleLE(v, 0)
		return buf
	},
	decode: (b: Buffer) => b.readDoubleLE(0),
}

let client: KafkaClient
let producer: Producer

beforeAll(async () => {
	client = new KafkaClient({
		clientId: `flow-integration-${Date.now()}`,
		brokers: requireKafkaBrokers(),
	})
	await client.connect()
	producer = client.producer()
})

afterAll(async () => {
	await producer.disconnect()
	await client.disconnect()
})

async function withTimeout<T>(label: string, promise: Promise<T>, timeoutMs: number): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined
	try {
		return await Promise.race([
			promise,
			new Promise<T>((_resolve, reject) => {
				timer = setTimeout(() => reject(new Error(`Timed out after ${timeoutMs}ms: ${label}`)), timeoutMs)
			}),
		])
	} finally {
		if (timer) clearTimeout(timer)
	}
}

async function waitForTopicPartitionCount(topic: string, expected: number, timeoutMs: number = 5_000): Promise<void> {
	const start = Date.now()
	while (Date.now() - start < timeoutMs) {
		const meta = await client.getMetadata([topic])
		const count = meta.topics.get(topic)?.partitions.size
		if (count === expected) {
			return
		}
		await new Promise(resolve => setTimeout(resolve, 100))
	}
	throw new Error(`Timed out waiting for ${topic} to have ${expected} partitions`)
}

describe('KStream operators', () => {
	const inputA = uniqueTopicName('flow-it-ops-input-a')
	const inputB = uniqueTopicName('flow-it-ops-input-b')

	const outMap = uniqueTopicName('flow-it-ops-out-map')
	const outMapValues = uniqueTopicName('flow-it-ops-out-map-values')
	const outFlat = uniqueTopicName('flow-it-ops-out-flat')
	const outFilter = uniqueTopicName('flow-it-ops-out-filter')
	const outSelectKey = uniqueTopicName('flow-it-ops-out-select-key')
	const outBranchA = uniqueTopicName('flow-it-ops-out-branch-a')
	const outBranchB = uniqueTopicName('flow-it-ops-out-branch-b')
	const throughTopic = uniqueTopicName('flow-it-ops-through')
	const outThrough = uniqueTopicName('flow-it-ops-out-through')
	const outMerged = uniqueTopicName('flow-it-ops-out-merged')
	const outPartitioned = uniqueTopicName('flow-it-ops-out-partitioned')
	const outPeek = uniqueTopicName('flow-it-ops-out-peek')
	const outNullKey = uniqueTopicName('flow-it-ops-out-null-key')

	const mapCodec = codec.json<{ key: string; n: number; mapped: true }>()
	const mapValuesCodec = codec.json<{ nPlus1: number; type: string }>()
	const throughCodec = codec.json<{ n: number; type: string; through: true }>()
	const mergedCodec = codec.json<{ source: 'a' | 'b'; n: number }>()

	const peeked: Array<{ key: string | null; value: InputEvent }> = []

	const appId = `flow-it-ops-${Date.now()}`
	let app: ReturnType<typeof flow>
	let output: MultiTopicCollector

	beforeAll(async () => {
		app = flow({
			applicationId: appId,
			client,
			numStreamThreads: 1,
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 100 },
		})

		const streamA = app.stream(inputA, { key: stringCodec, value: jsonCodec })
		const streamB = app.stream(inputB, { key: stringCodec, value: jsonCodec })

		// map
		streamA
			.map((key, value) => [`${key}`.toUpperCase(), { key: `${key}`, n: value.n, mapped: true }])
			.to(outMap, {
				key: stringCodec,
				value: mapCodec,
			})

		// mapValues
		streamA.mapValues(v => ({ nPlus1: v.n + 1, type: v.type })).to(outMapValues, { value: mapValuesCodec })

		// flatMapValues
		streamA.flatMapValues(v => [v.n, v.n + 1]).to(outFlat, { value: numberCodec })

		// filter
		streamA.filter((_k, v) => v.n > 0).to(outFilter)

		// selectKey
		streamA.selectKey((v, _k) => v.type).to(outSelectKey, { key: stringCodec })

		// branch
		const [branchA, branchB] = streamA.branch((_k, v) => v.type === 'a', (_k, v) => v.type === 'b')
		branchA.to(outBranchA)
		branchB.to(outBranchB)

		// through
		streamA
			.through(throughTopic)
			.mapValues(v => ({ ...v, through: true as const }))
			.to(outThrough, { value: throughCodec })

		// merge
		streamA
			.mapValues(v => ({ source: 'a' as const, n: v.n }))
			.merge(streamB.mapValues(v => ({ source: 'b' as const, n: v.n })))
			.to(outMerged, { value: mergedCodec })

		// custom partitioner
		streamA.to(outPartitioned, {
			partitioner: (_k, v, partitionCount) => (partitionCount === 0 ? 0 : v.n % partitionCount),
		})

		// peek
		streamA.peek((k, v) => peeked.push({ key: k, value: v })).to(outPeek)

		// null key propagation
		streamA.mapValues(v => v.n).to(outNullKey, { value: numberCodec })

		output = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector`,
			topics: [
				{ topic: outMap, keyCodec: stringCodec, valueCodec: mapCodec },
				{ topic: outMapValues, keyCodec: stringCodec, valueCodec: mapValuesCodec },
				{ topic: outFlat, keyCodec: stringCodec, valueCodec: numberCodec },
				{ topic: outFilter, keyCodec: stringCodec, valueCodec: jsonCodec },
				{ topic: outSelectKey, keyCodec: stringCodec, valueCodec: jsonCodec },
				{ topic: outBranchA, keyCodec: stringCodec, valueCodec: jsonCodec },
				{ topic: outBranchB, keyCodec: stringCodec, valueCodec: jsonCodec },
				{ topic: throughTopic, keyCodec: stringCodec, valueCodec: jsonCodec },
				{ topic: outThrough, keyCodec: stringCodec, valueCodec: throughCodec },
				{ topic: outMerged, keyCodec: stringCodec, valueCodec: mergedCodec },
				{ topic: outPartitioned, keyCodec: stringCodec, valueCodec: jsonCodec },
				{ topic: outPeek, keyCodec: stringCodec, valueCodec: jsonCodec },
				{ topic: outNullKey, keyCodec: stringCodec, valueCodec: numberCodec },
			],
		})

		await createTopics(client, [
			{ name: inputA },
			{ name: inputB },
			{ name: outMap },
			{ name: outMapValues },
			{ name: outFlat },
			{ name: outFilter },
			{ name: outSelectKey },
			{ name: outBranchA },
			{ name: outBranchB },
			{ name: throughTopic },
			{ name: outThrough },
			{ name: outMerged },
			{ name: outPartitioned, partitions: 2 },
			{ name: outPeek },
			{ name: outNullKey },
		])

		await output.start()
		await app.start()

		// Warm up the app so tests don't race consumer startup.
		await producer.send(inputA, { key: '__warmup__', value: jsonCodec.encode({ n: 1, type: 'warmup' }) })
		await output.waitFor(outFilter, m => m.key === '__warmup__', 15_000)
	})

	afterAll(async () => {
		await app.close()
		await output.stop()
	})

	it('map transforms key and value', async () => {
		await producer.send(inputA, { key: 'k1', value: jsonCodec.encode({ n: 10, type: 'a' }) })
		const msg = await output.waitFor(outMap, m => m.key === 'K1')
		expect(msg.value).toEqual({ key: 'k1', n: 10, mapped: true })
	})

	it('mapValues transforms values and preserves keys', async () => {
		await producer.send(inputA, { key: 'k2', value: jsonCodec.encode({ n: 2, type: 'x' }) })
		const msg = await output.waitFor(outMapValues, m => m.key === 'k2')
		expect(msg.value).toEqual({ nPlus1: 3, type: 'x' })
	})

	it('flatMapValues emits multiple records per input', async () => {
		await producer.send(inputA, { key: 'k3', value: jsonCodec.encode({ n: 5, type: 'x' }) })
		const msgs = await output.waitForCount(outFlat, m => m.key === 'k3', 2)
		const values = msgs.map(m => m.value).sort((a, b) => a - b)
		expect(values).toEqual([5, 6])
	})

	it('filter drops records that do not match predicate', async () => {
		await producer.send(inputA, { key: 'k4', value: jsonCodec.encode({ n: 0, type: 'x' }) })
		await expect(output.waitFor(outFilter, m => m.key === 'k4', 300)).rejects.toThrow('Timed out')
	})

	it('selectKey re-keys records based on value', async () => {
		await producer.send(inputA, { key: 'orig', value: jsonCodec.encode({ n: 1, type: 'new-key' }) })
		const msg = await output.waitFor(outSelectKey, m => m.key === 'new-key')
		expect(msg.value).toEqual({ n: 1, type: 'new-key' })
	})

	it('branch routes records to the first matching predicate', async () => {
		await producer.send(inputA, { key: 'b1', value: jsonCodec.encode({ n: 1, type: 'a' }) })
		await output.waitFor(outBranchA, m => m.key === 'b1')
		await expect(output.waitFor(outBranchB, m => m.key === 'b1', 300)).rejects.toThrow('Timed out')
	})

	it('branch routes records to later predicates when earlier ones do not match', async () => {
		await producer.send(inputA, { key: 'b2', value: jsonCodec.encode({ n: 1, type: 'b' }) })
		await output.waitFor(outBranchB, m => m.key === 'b2')
		await expect(output.waitFor(outBranchA, m => m.key === 'b2', 300)).rejects.toThrow('Timed out')
	})

	it('branch drops records when no predicates match', async () => {
		await producer.send(inputA, { key: 'b3', value: jsonCodec.encode({ n: 1, type: 'c' }) })
		await expect(output.waitFor(outBranchA, m => m.key === 'b3', 300)).rejects.toThrow('Timed out')
		await expect(output.waitFor(outBranchB, m => m.key === 'b3', 300)).rejects.toThrow('Timed out')
	})

	it('through writes to an intermediate topic and continues processing', async () => {
		await producer.send(inputA, { key: 't1', value: jsonCodec.encode({ n: 7, type: 'x' }) })
		await output.waitFor(throughTopic, m => m.key === 't1')
		const msg = await output.waitFor(outThrough, m => m.key === 't1')
		expect(msg.value).toEqual({ n: 7, type: 'x', through: true })
	})

	it('merge combines multiple source streams into a single stream', async () => {
		await producer.send(inputA, { key: 'm1', value: jsonCodec.encode({ n: 1, type: 'x' }) })
		await producer.send(inputB, { key: 'm2', value: jsonCodec.encode({ n: 2, type: 'x' }) })

		const a = await output.waitFor(outMerged, m => m.key === 'm1')
		const b = await output.waitFor(outMerged, m => m.key === 'm2')
		expect(a.value).toEqual({ source: 'a', n: 1 })
		expect(b.value).toEqual({ source: 'b', n: 2 })
	})

	it('to(partitioner) can control output partition selection', async () => {
		await producer.send(inputA, { key: 'p1', value: jsonCodec.encode({ n: 1, type: 'x' }) })
		await producer.send(inputA, { key: 'p2', value: jsonCodec.encode({ n: 2, type: 'x' }) })

		const p1 = await output.waitFor(outPartitioned, m => m.key === 'p1')
		const p2 = await output.waitFor(outPartitioned, m => m.key === 'p2')

		expect(p1.partition).toBe(1)
		expect(p2.partition).toBe(0)
	})

	it('peek runs side effects without changing the record', async () => {
		await producer.send(inputA, { key: 'pk', value: jsonCodec.encode({ n: 9, type: 'x' }) })
		await output.waitFor(outPeek, m => m.key === 'pk')

		expect(peeked.some(r => r.key === 'pk' && r.value.n === 9)).toBe(true)
	})

	it('null keys can flow through stateless operations', async () => {
		await producer.send(inputA, { key: null, value: jsonCodec.encode({ n: 42, type: 'x' }) })
		const msg = await output.waitFor(outNullKey, m => m.key === null && m.value === 42)
		expect(msg.key).toBe(null)
	})
})

describe('offsetReset policies', () => {
	it(
		'offsetReset=earliest consumes messages produced before app.start()',
			async () => {
				const input = uniqueTopicName('flow-it-offset-earliest-input')
				const outputTopic = uniqueTopicName('flow-it-offset-earliest-out')
				const appId = `flow-it-offset-earliest-${Date.now()}`

				await createTopics(client, [{ name: input }, { name: outputTopic }])

				await producer.send(input, { key: 'pre', value: jsonCodec.encode({ n: 1, type: 'x' }) })

				const app = flow({
					applicationId: appId,
					client,
					numStreamThreads: 1,
					consumer: { autoOffsetReset: 'earliest', maxWaitMs: 100 },
				})
				app.stream(input, { key: stringCodec, value: jsonCodec, offsetReset: 'earliest' }).to(outputTopic)

				const out = new MultiTopicCollector({
					client,
					groupId: `${appId}-collector`,
					topics: [{ topic: outputTopic, keyCodec: stringCodec, valueCodec: jsonCodec }],
				})

				await out.start()
				await app.start()

				await out.waitFor(outputTopic, m => m.key === 'pre')

				await app.close()
				await out.stop()
			},
			30_000
		)

	it(
		'offsetReset=latest ignores pre-existing messages but consumes new ones',
			async () => {
				const input = uniqueTopicName('flow-it-offset-latest-input')
				const outputTopic = uniqueTopicName('flow-it-offset-latest-out')
				const appId = `flow-it-offset-latest-${Date.now()}`

				await createTopics(client, [{ name: input }, { name: outputTopic }])

				await producer.send(input, { key: 'pre', value: jsonCodec.encode({ n: 1, type: 'x' }) })

				const app = flow({
					applicationId: appId,
					client,
					numStreamThreads: 1,
					consumer: { autoOffsetReset: 'earliest', maxWaitMs: 100 },
				})
				app.stream(input, { key: stringCodec, value: jsonCodec, offsetReset: 'latest' }).to(outputTopic)

				const out = new MultiTopicCollector({
					client,
					groupId: `${appId}-collector`,
					topics: [{ topic: outputTopic, keyCodec: stringCodec, valueCodec: jsonCodec }],
				})

				await out.start()
				await app.start()

				// pre-existing should be ignored
				await expect(out.waitFor(outputTopic, m => m.key === 'pre', 500)).rejects.toThrow('Timed out')

				// app.start() waits for the consumer to be running, so this message is guaranteed
				// to be produced after partitions are assigned.
				await producer.send(input, { key: 'post', value: jsonCodec.encode({ n: 1, type: 'x' }) })
				await out.waitFor(outputTopic, m => m.key === 'post', 10_000)

				await app.close()
				await out.stop()
			},
			30_000
		)
	})

describe('grouping and aggregations', () => {
	const input = uniqueTopicName('flow-it-agg-input')
	const parityInput = uniqueTopicName('flow-it-agg-parity-input')
	const outCount = uniqueTopicName('flow-it-agg-out-count')
	const outSum = uniqueTopicName('flow-it-agg-out-sum')
	const outAgg = uniqueTopicName('flow-it-agg-out-agg')
	const outGroupCount = uniqueTopicName('flow-it-agg-out-group-count')

	const aggCodec = codec.json<{ sum: number }>()

	const appId = `flow-it-agg-${Date.now()}`
	let app: ReturnType<typeof flow>
	let output: MultiTopicCollector

	beforeAll(async () => {
		app = flow({
			applicationId: appId,
			client,
			numStreamThreads: 1,
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 100 },
		})

		const stream = app.stream(input, { key: stringCodec, value: numberCodec })
		const grouped = stream.groupByKey()

		grouped
			.count({ storeName: 'counts', key: stringCodec, value: numberCodec, changelog: false })
			.toStream()
			.to(outCount)

		grouped
			.reduce((a, b) => a + b, { storeName: 'sums', key: stringCodec, value: numberCodec, changelog: false })
			.toStream()
			.to(outSum)

		grouped
			.aggregate(() => ({ sum: 0 }), (_k, v, agg) => ({ sum: agg.sum + v }), {
				storeName: 'aggs',
				key: stringCodec,
				value: aggCodec,
				changelog: false,
			})
			.toStream()
			.to(outAgg)

		const parityStream = app.stream(parityInput, { key: stringCodec, value: numberCodec })
		parityStream
			.groupBy((_k, v) => (v % 2 === 0 ? 'even' : 'odd'), { key: stringCodec, value: numberCodec })
			.count({ storeName: 'parity', key: stringCodec, value: numberCodec, changelog: false })
			.toStream()
			.to(outGroupCount)

		output = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector`,
			topics: [
				{ topic: outCount, keyCodec: stringCodec, valueCodec: numberCodec },
				{ topic: outSum, keyCodec: stringCodec, valueCodec: numberCodec },
				{ topic: outAgg, keyCodec: stringCodec, valueCodec: aggCodec },
				{ topic: outGroupCount, keyCodec: stringCodec, valueCodec: numberCodec },
			],
		})

		await createTopics(client, [
			{ name: input },
			{ name: parityInput },
			{ name: outCount },
			{ name: outSum },
			{ name: outAgg },
			{ name: outGroupCount },
		])

		await output.start()
		await app.start()

		await producer.send(input, { key: '__warmup__', value: numberCodec.encode(1) })
		await output.waitFor(outCount, m => m.key === '__warmup__', 15_000)
	})

	afterAll(async () => {
		await app.close()
		await output.stop()
	})

	it('count increments per key', async () => {
		await producer.send(input, { key: 'a', value: numberCodec.encode(1) })
		await output.waitFor(outCount, m => m.key === 'a' && m.value === 1)
	})

	it('count continues incrementing for the same key', async () => {
		await producer.send(input, { key: 'a', value: numberCodec.encode(1) })
		await output.waitFor(outCount, m => m.key === 'a' && m.value === 2)
	})

	it('count is tracked independently across keys', async () => {
		await producer.send(input, { key: 'b', value: numberCodec.encode(1) })
		await output.waitFor(outCount, m => m.key === 'b' && m.value === 1)
	})

	it('reduce aggregates values (sum)', async () => {
		await producer.send(input, { key: 's', value: numberCodec.encode(5) })
		await producer.send(input, { key: 's', value: numberCodec.encode(7) })
		await output.waitFor(outSum, m => m.key === 's' && m.value === 12)
	})

	it('aggregate can build custom aggregate objects', async () => {
		await producer.send(input, { key: 'x', value: numberCodec.encode(2) })
		await producer.send(input, { key: 'x', value: numberCodec.encode(3) })
		await output.waitFor(outAgg, m => m.key === 'x' && m.value.sum === 5)
	})

	it('aggregations skip null keys', async () => {
		await producer.send(input, { key: null, value: numberCodec.encode(1) })
		await expect(output.waitFor(outCount, m => m.key === null, 400)).rejects.toThrow('Timed out')
	})

	it('groupBy can re-key based on the value', async () => {
		await producer.send(parityInput, { key: 'k1', value: numberCodec.encode(2) })
		await producer.send(parityInput, { key: 'k2', value: numberCodec.encode(3) })
		await output.waitFor(outGroupCount, m => m.key === 'even' && m.value === 1)
		await output.waitFor(outGroupCount, m => m.key === 'odd' && m.value === 1)
	})
})

describe('stream-table joins', () => {
	type User = { name: string }
	type Event = { action: string }
	type Enriched = { action: string; user: string | null }

	const userCodec = codec.json<User>()
	const eventCodec = codec.json<Event>()
	const enrichedCodec = codec.json<Enriched>()

	const usersTopic = uniqueTopicName('flow-it-join-users')
	const eventsTopic = uniqueTopicName('flow-it-join-events')
	const outUsers = uniqueTopicName('flow-it-join-users-out')
	const outInner = uniqueTopicName('flow-it-join-inner-out')
	const outLeft = uniqueTopicName('flow-it-join-left-out')

	const appId = `flow-it-join-st-${Date.now()}`
	let app: ReturnType<typeof flow>
	let output: MultiTopicCollector

	beforeAll(async () => {
		app = flow({
			applicationId: appId,
			client,
			numStreamThreads: 1,
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 100 },
		})

		const users = app.table(usersTopic, { key: stringCodec, value: userCodec, materialized: { changelog: false } })
		const events = app.stream(eventsTopic, { key: stringCodec, value: eventCodec })

		users.toStream().to(outUsers)

		events
			.join(users, (event, user) => ({ action: event.action, user: user.name }))
			.to(outInner, { value: enrichedCodec })
		events
			.leftJoin(users, (event, user) => ({ action: event.action, user: user?.name ?? null }))
			.to(outLeft, { value: enrichedCodec })

		output = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector`,
			topics: [
				{ topic: outUsers, keyCodec: stringCodec, valueCodec: userCodec },
				{ topic: outInner, keyCodec: stringCodec, valueCodec: enrichedCodec },
				{ topic: outLeft, keyCodec: stringCodec, valueCodec: enrichedCodec },
			],
		})

		await createTopics(client, [
			{ name: usersTopic },
			{ name: eventsTopic },
			{ name: outUsers },
			{ name: outInner },
			{ name: outLeft },
		])

		await output.start()
		await app.start()

		await producer.send(usersTopic, { key: '__warmup__', value: userCodec.encode({ name: 'warmup' }) })
		await output.waitFor(outUsers, m => m.key === '__warmup__', 15_000)
	})

	afterAll(async () => {
		await app.close()
		await output.stop()
	})

	it('inner join emits when the table has a matching key', async () => {
		await producer.send(usersTopic, { key: 'u1', value: userCodec.encode({ name: 'Alice' }) })
		await output.waitFor(outUsers, m => m.key === 'u1' && m.value.name === 'Alice')

		await producer.send(eventsTopic, { key: 'u1', value: eventCodec.encode({ action: 'click' }) })
		const msg = await output.waitFor(outInner, m => m.key === 'u1' && m.value.action === 'click')
		expect(msg.value).toEqual({ action: 'click', user: 'Alice' })
	})

	it('inner join skips stream records without a table match', async () => {
		await producer.send(eventsTopic, { key: 'nope1', value: eventCodec.encode({ action: 'view' }) })
		await expect(output.waitFor(outInner, m => m.key === 'nope1', 400)).rejects.toThrow('Timed out')
	})

	it('left join emits with null when there is no table match', async () => {
		await producer.send(eventsTopic, { key: 'nope2', value: eventCodec.encode({ action: 'view' }) })
		const msg = await output.waitFor(outLeft, m => m.key === 'nope2')
		expect(msg.value).toEqual({ action: 'view', user: null })
	})

	it('joins observe updated table values', async () => {
		await producer.send(usersTopic, { key: 'u2', value: userCodec.encode({ name: 'Bob' }) })
		await output.waitFor(outUsers, m => m.key === 'u2' && m.value.name === 'Bob')

		await producer.send(usersTopic, { key: 'u2', value: userCodec.encode({ name: 'Robert' }) })
		await output.waitFor(outUsers, m => m.key === 'u2' && m.value.name === 'Robert')

		await producer.send(eventsTopic, { key: 'u2', value: eventCodec.encode({ action: 'purchase' }) })
		const msg = await output.waitFor(outInner, m => m.key === 'u2' && m.value.action === 'purchase')
		expect(msg.value.user).toBe('Robert')
	})

	it('null keys are skipped by join processors', async () => {
		await producer.send(eventsTopic, { key: null, value: eventCodec.encode({ action: 'click' }) })
		await expect(output.waitFor(outLeft, m => m.key === null, 400)).rejects.toThrow('Timed out')
	})
})

describe('stream-stream joins', () => {
	const leftTopic = uniqueTopicName('flow-it-ss-left')
	const rightTopic = uniqueTopicName('flow-it-ss-right')
	const outInner = uniqueTopicName('flow-it-ss-out-inner')
	const outLeft = uniqueTopicName('flow-it-ss-out-left')
	const outOuter = uniqueTopicName('flow-it-ss-out-outer')

	const appId = `flow-it-join-ss-${Date.now()}`
	let app: ReturnType<typeof flow>
	let output: MultiTopicCollector

	beforeAll(async () => {
		app = flow({
			applicationId: appId,
			client,
			numStreamThreads: 1,
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 100 },
		})

		const left = app.stream(leftTopic, { key: stringCodec, value: numberCodec })
		const right = app.stream(rightTopic, { key: stringCodec, value: numberCodec })

		left.join(right, (l, r) => l + r, { within: TimeWindows.of(1_000) }).to(outInner, { value: numberCodec })
		left
			.leftJoin(right, (l, r) => (r === null ? l : l + r), { within: TimeWindows.of(1_000) })
			.to(outLeft, {
				value: numberCodec,
			})
		left.outerJoin(right, (l, r) => (l ?? 0) + (r ?? 0), { within: TimeWindows.of(1_000) }).to(outOuter, {
			value: numberCodec,
		})

		output = new MultiTopicCollector({
			client,
			groupId: `${appId}-collector`,
			topics: [
				{ topic: outInner, keyCodec: stringCodec, valueCodec: numberCodec },
				{ topic: outLeft, keyCodec: stringCodec, valueCodec: numberCodec },
				{ topic: outOuter, keyCodec: stringCodec, valueCodec: numberCodec },
			],
		})

		await createTopics(client, [
			{ name: leftTopic },
			{ name: rightTopic },
			{ name: outInner },
			{ name: outLeft },
			{ name: outOuter },
		])

		await output.start()
		await app.start()

		await producer.send(leftTopic, { key: '__warmup__', value: numberCodec.encode(1), timestamp: new Date(1_000) })
		await output.waitFor(outLeft, m => m.key === '__warmup__', 15_000)
	})

	afterAll(async () => {
		await app.close()
		await output.stop()
	})

	it('inner join emits when right arrives within the window after left', async () => {
		await producer.send(leftTopic, { key: 'k1', value: numberCodec.encode(1), timestamp: new Date(10_000) })
		await producer.send(rightTopic, { key: 'k1', value: numberCodec.encode(2), timestamp: new Date(10_500) })
		const msg = await output.waitFor(outInner, m => m.key === 'k1' && m.value === 3)
		expect(msg.value).toBe(3)
	})

	it('inner join emits when left arrives within the window after right', async () => {
		await producer.send(rightTopic, { key: 'k2', value: numberCodec.encode(2), timestamp: new Date(20_000) })
		await producer.send(leftTopic, { key: 'k2', value: numberCodec.encode(1), timestamp: new Date(20_200) })
		await output.waitFor(outInner, m => m.key === 'k2' && m.value === 3)
	})

	it('inner join does not emit when records are outside the window', async () => {
		await producer.send(leftTopic, { key: 'k3', value: numberCodec.encode(1), timestamp: new Date(30_000) })
		await producer.send(rightTopic, { key: 'k3', value: numberCodec.encode(2), timestamp: new Date(40_000) })
		await expect(output.waitFor(outInner, m => m.key === 'k3', 400)).rejects.toThrow('Timed out')
	})

	it('leftJoin emits with null when no match exists', async () => {
		await producer.send(leftTopic, { key: 'k4', value: numberCodec.encode(9), timestamp: new Date(50_000) })
		await output.waitFor(outLeft, m => m.key === 'k4' && m.value === 9)
	})

	it('leftJoin emits joined results when a match exists', async () => {
		await producer.send(rightTopic, { key: 'k5', value: numberCodec.encode(2), timestamp: new Date(60_000) })
		await producer.send(leftTopic, { key: 'k5', value: numberCodec.encode(1), timestamp: new Date(60_100) })
		await output.waitFor(outLeft, m => m.key === 'k5' && m.value === 3)
	})

	it('outerJoin emits when left record has no match', async () => {
		await producer.send(leftTopic, { key: 'k6', value: numberCodec.encode(7), timestamp: new Date(70_000) })
		await output.waitFor(outOuter, m => m.key === 'k6' && m.value === 7)
	})

	it('outerJoin emits when right record has no match', async () => {
		await producer.send(rightTopic, { key: 'k7', value: numberCodec.encode(8), timestamp: new Date(80_000) })
		await output.waitFor(outOuter, m => m.key === 'k7' && m.value === 8)
	})

	it('null keys are skipped by stream-stream joins', async () => {
		await producer.send(leftTopic, { key: null, value: numberCodec.encode(1), timestamp: new Date(90_000) })
		await expect(output.waitFor(outLeft, m => m.key === null, 400)).rejects.toThrow('Timed out')
	})
})

describe('changelog topics and restoration', () => {
	it('creates changelog topics with the same partition count as the source topic', async () => {
		const input = uniqueTopicName('flow-it-changelog-src')
		const outputTopic = uniqueTopicName('flow-it-changelog-out')
		const appId = `flow-it-changelog-create-${Date.now()}`
		const storeName = 'counts'
		const changelogTopic = buildChangelogTopicName(appId, storeName)

		await createTopics(client, [{ name: input, partitions: 3 }, { name: outputTopic }])

		const app = flow({ applicationId: appId, client, numStreamThreads: 1, consumer: { maxWaitMs: 100 } })
		app.stream(input, { key: stringCodec, value: numberCodec })
			.groupByKey()
			.count({ storeName, key: stringCodec, value: numberCodec, changelog: { skipRestoration: true } })
			.toStream()
			.to(outputTopic)

		await app.start()
		await waitForTopicPartitionCount(changelogTopic, 3)

		await app.close()
	})

	it('infers changelog partitions from the maximum partition count of merged sources', async () => {
		const a = uniqueTopicName('flow-it-changelog-merge-a')
		const b = uniqueTopicName('flow-it-changelog-merge-b')
		const outputTopic = uniqueTopicName('flow-it-changelog-merge-out')
		const appId = `flow-it-changelog-merge-${Date.now()}`
		const storeName = 'counts'
		const changelogTopic = buildChangelogTopicName(appId, storeName)

		await createTopics(client, [
			{ name: a, partitions: 1 },
			{ name: b, partitions: 4 },
			{ name: outputTopic },
		])

		const app = flow({ applicationId: appId, client, numStreamThreads: 1, consumer: { maxWaitMs: 100 } })
		const s1 = app.stream(a, { key: stringCodec, value: numberCodec })
		const s2 = app.stream(b, { key: stringCodec, value: numberCodec })

		s1.merge(s2)
			.groupByKey()
			.count({ storeName, key: stringCodec, value: numberCodec, changelog: { skipRestoration: true } })
			.toStream()
			.to(outputTopic)

		await app.start()
		await waitForTopicPartitionCount(changelogTopic, 4)

		await app.close()
	})

	it('throws when an existing changelog topic has the wrong partition count', async () => {
		const input = uniqueTopicName('flow-it-changelog-mismatch-src')
		const outputTopic = uniqueTopicName('flow-it-changelog-mismatch-out')
		const appId = `flow-it-changelog-mismatch-${Date.now()}`
		const storeName = 'counts'
		const changelogTopic = buildChangelogTopicName(appId, storeName)

		await createTopics(client, [
			{ name: input, partitions: 2 },
			{ name: outputTopic },
			{ name: changelogTopic, partitions: 1 },
		])
		await waitForTopicPartitionCount(changelogTopic, 1)

		const app = flow({ applicationId: appId, client, numStreamThreads: 1, consumer: { maxWaitMs: 100 } })
		app.stream(input, { key: stringCodec, value: numberCodec })
			.groupByKey()
			.count({ storeName, key: stringCodec, value: numberCodec, changelog: { skipRestoration: true } })
			.toStream()
			.to(outputTopic)

		await expect(app.start()).rejects.toBeInstanceOf(ChangelogPartitionMismatchError)
		await app.close()
	})

	it('throws when a source topic does not exist for partition inference', async () => {
		const missing = uniqueTopicName('flow-it-changelog-missing-src')
		const outputTopic = uniqueTopicName('flow-it-changelog-missing-out')
		const appId = `flow-it-changelog-missing-${Date.now()}`

		await createTopics(client, [{ name: outputTopic }])

		const app = flow({ applicationId: appId, client, numStreamThreads: 1, consumer: { maxWaitMs: 100 } })
		app.stream(missing, { key: stringCodec, value: numberCodec })
			.groupByKey()
			.count({ storeName: 'counts', key: stringCodec, value: numberCodec, changelog: { skipRestoration: true } })
			.toStream()
			.to(outputTopic)

		await expect(app.start()).rejects.toBeInstanceOf(SourceTopicNotFoundError)
		await app.close()
	})

		it(
			'restores state from changelog topics after restart',
			async () => {
			const input = uniqueTopicName('flow-it-restore-src')
			const outputTopic = uniqueTopicName('flow-it-restore-out')
			const appId = `flow-it-restore-${Date.now()}`
			const storeName = 'counts'

				await createTopics(client, [{ name: input }, { name: outputTopic }])

				const buildApp = (skipRestoration: boolean) => {
					const app = flow({
						applicationId: appId,
						client,
						numStreamThreads: 1,
						consumer: { maxWaitMs: 100 },
						changelog: {
							restoration: {
								idleTimeoutMs: 2_000,
								initialIdleTimeoutMs: 10_000,
								checkIntervalMs: 200,
								consumerMaxWaitMs: 200,
							},
						},
					})
					app.stream(input, { key: stringCodec, value: numberCodec })
						.groupByKey()
						.count({ storeName, key: stringCodec, value: numberCodec, changelog: { skipRestoration } })
						.toStream()
						.to(outputTopic)
					return app
				}

			const out = new MultiTopicCollector({
				client,
					groupId: `${appId}-collector-${Date.now()}`,
					topics: [{ topic: outputTopic, keyCodec: stringCodec, valueCodec: numberCodec }],
				})
				await withTimeout('output collector start', out.start(), 15_000)

				const app1 = buildApp(true)
				await withTimeout('app1.start', app1.start(), 20_000)

				await withTimeout(
					'produce input (1)',
					producer.send(input, { key: 'a', value: numberCodec.encode(1) }),
					15_000
				)
				await withTimeout(
					'produce input (2)',
					producer.send(input, { key: 'a', value: numberCodec.encode(1) }),
					15_000
				)
				await out.waitFor(outputTopic, m => m.key === 'a' && m.value === 2)

				await withTimeout('app1.close', app1.close(), 20_000)

				const app2 = buildApp(false)
				await withTimeout('app2.start', app2.start(), 20_000)

				await withTimeout(
					'produce input (3)',
					producer.send(input, { key: 'a', value: numberCodec.encode(1) }),
					15_000
				)
				try {
					await out.waitFor(outputTopic, m => m.key === 'a' && m.value === 3)
				} catch (error) {
					const seen = out.getTopicMessages<string, number>(outputTopic).filter(m => m.key === 'a').map(m => m.value)
					throw new Error(`Expected restored count to reach 3, but saw values: [${seen.join(', ')}]`, {
						cause: error instanceof Error ? error : new Error(String(error)),
					})
				}

				await withTimeout('app2.close', app2.close(), 20_000)
				await withTimeout('output collector stop', out.stop(), 15_000)
			},
			60_000
		)
	})
