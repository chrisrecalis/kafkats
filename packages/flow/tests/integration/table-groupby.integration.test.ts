import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow, type FlowApp } from '../../src/index.js'
import {
	createTopics,
	injectCommitFailures,
	newClient,
	readAllCommitted,
	readCommittedTable,
	sleep,
	uniqueTopicName,
} from './test-helpers.js'

const stringCodec = codec.string()
const numberCodec = codec.json<number>()
const USERS = 24
const GROUPS = 4

async function eventually(admin: KafkaClient, topic: string, expected: Record<string, number>, timeoutMs: number) {
	const deadline = Date.now() + timeoutMs
	let last: Record<string, number> = {}
	while (Date.now() < deadline) {
		last = await readCommittedTable(admin, topic, numberCodec, 1_200)
		if (JSON.stringify(Object.entries(last).sort()) === JSON.stringify(Object.entries(expected).sort())) return last
		await sleep(500)
	}
	return last
}

describe('KTable.groupBy aggregates across several consumers', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('r2-tg-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	async function waitAssign(groupId: string, members: number, topic: string, partitions: number) {
		const a = admin.admin()
		const deadline = Date.now() + 30_000
		while (Date.now() < deadline) {
			const [desc] = await a.describeGroups([groupId])
			if (desc?.state === 'Stable' && desc.members.length === members) {
				const covered = new Set(
					desc.members.flatMap(m => m.assignment.filter(tp => tp.topic === topic).map(tp => tp.partition))
				)
				if (covered.size === partitions && desc.members.every(m => m.assignment.length > 0)) return
			}
			await sleep(200)
		}
		throw new Error('group did not stabilise')
	}

	const buildApp = (
		applicationId: string,
		input: string,
		output: string,
		mode: 'at_least_once' | 'exactly_once',
		client = newClient('r2-tg-app')
	): FlowApp => {
		const app = flow({
			applicationId,
			client,
			processingGuarantee: mode,
			commitIntervalMs: 100,
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			producer: { lingerMs: 0 },
			changelog: { restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 5_000, checkIntervalMs: 50 } },
		})
		app.table(input, { key: stringCodec, value: stringCodec })
			.groupBy((_user, group) => [group, group] as const, { key: stringCodec, value: stringCodec })
			.count({ storeName: 'group-counts', key: stringCodec, value: numberCodec })
			.toStream()
			.to(output, { key: stringCodec, value: numberCodec })
		return app
	}

	// BUG (F5): KTable.groupBy() keeps its mapping store and aggregate store app-scoped (one local copy per
	// instance, restored once at startup) and never repartitions by the new key. Every instance therefore only
	// counts the rows of the partitions it owns, and writes that partial count to the same output key.
	for (const mode of ['at_least_once', 'exactly_once'] as const) {
		it(`${mode}: count by group is exact with 2 instances on a 4-partition table topic`, async () => {
			const input = uniqueTopicName('r2-tg-in')
			const output = uniqueTopicName('r2-tg-out')
			const applicationId = uniqueTopicName('r2-tg-app')
			await createTopics(admin, [{ name: input, partitions: 4 }, { name: output }])
			const a1 = buildApp(applicationId, input, output, mode)
			const a2 = buildApp(applicationId, input, output, mode)
			try {
				await Promise.all([a1.start(), a2.start()])
				await waitAssign(applicationId, 2, input, 4)
				await producer.send(
					input,
					Array.from({ length: USERS }, (_, i) => ({ key: `u${i}`, value: Buffer.from(`g${i % GROUPS}`) }))
				)
				const want = Object.fromEntries(Array.from({ length: GROUPS }, (_, g) => [`g${g}`, USERS / GROUPS]))
				expect(await eventually(admin, output, want, 15_000)).toEqual(want)

				// move every user to the next group: totals stay the same, but only if retractions are applied to the right counts
				await producer.send(
					input,
					Array.from({ length: USERS }, (_, i) => ({
						key: `u${i}`,
						value: Buffer.from(`g${(i + 1) % GROUPS}`),
					}))
				)
				expect(await eventually(admin, output, want, 15_000)).toEqual(want)
			} finally {
				await a1.close().catch(() => {})
				await a2.close().catch(() => {})
			}
		}, 80_000)
	}

	// Same structure inside one process: two instances, each with its own consumer, producer and tasks (D1).
	it('exactly_once: count by group is exact with 2 instances in one process', async () => {
		const input = uniqueTopicName('r2-tg2-in')
		const output = uniqueTopicName('r2-tg2-out')
		const applicationId = uniqueTopicName('r2-tg2-app')
		await createTopics(admin, [{ name: input, partitions: 4 }, { name: output }])
		const apps = [0, 1].map(() => buildApp(applicationId, input, output, 'exactly_once'))
		try {
			await Promise.all(apps.map(app => app.start()))
			await sleep(2_000)
			const batches = 6
			for (let b = 0; b < batches; b += 1) {
				await producer.send(
					input,
					Array.from({ length: USERS }, (_, i) => ({
						key: `u${i}`,
						value: Buffer.from(`g${(i + b) % GROUPS}`),
					}))
				)
				await sleep(120)
			}
			// after the last batch user i sits in group (i + batches - 1) % GROUPS => still 6 per group
			const want = Object.fromEntries(Array.from({ length: GROUPS }, (_, g) => [`g${g}`, USERS / GROUPS]))
			expect(await eventually(admin, output, want, 15_000)).toEqual(want)
		} finally {
			await Promise.all(apps.map(app => app.close().catch(() => {})))
		}
	}, 80_000)

	// F4 end-to-end: a failed commit aborts the failing instance's transaction and drops its buffered store writes
	// only; the other instance's transactions and state are its own, so later counts are computed from committed state.
	it('exactly_once: count by group stays exact with 2 instances in one process whose commits fail', async () => {
		const input = uniqueTopicName('r2-tg3-in')
		const output = uniqueTopicName('r2-tg3-out')
		const applicationId = uniqueTopicName('r2-tg3-app')
		await createTopics(admin, [{ name: input, partitions: 4 }, { name: output }])
		let commits = 0
		const apps = [0, 1].map(() => {
			const client = newClient('r2-tg3-app')
			injectCommitFailures(client, () => {
				commits += 1
				return commits % 3 === 0 && commits <= 18
			})
			return buildApp(applicationId, input, output, 'exactly_once', client)
		})
		try {
			await Promise.all(apps.map(app => app.start()))
			await sleep(2_000)
			const batches = 8
			for (let b = 0; b < batches; b += 1) {
				await producer.send(
					input,
					Array.from({ length: USERS }, (_, i) => ({
						key: `u${i}`,
						value: Buffer.from(`g${(i + b) % GROUPS}`),
					}))
				)
				await sleep(150)
			}
			const want = Object.fromEntries(Array.from({ length: GROUPS }, (_, g) => [`g${g}`, USERS / GROUPS]))
			expect(await eventually(admin, output, want, 30_000)).toEqual(want)
			expect(commits).toBeGreaterThan(3) // failures were actually injected
		} finally {
			await Promise.all(apps.map(app => app.close().catch(() => {})))
		}
	}, 85_000)

	// Kafka Streams semantics (6.15): a group whose last row left keeps its aggregate, so the count ends at 0 instead
	// of a tombstone (BC11), and a row updated within its group is one change of that group, not a subtraction and an
	// addition (KIP-904).
	it('exactly_once: an emptied group stays at 0 and an update within a group is one change', async () => {
		const input = uniqueTopicName('r2-tg0-in')
		const output = uniqueTopicName('r2-tg0-out')
		const applicationId = uniqueTopicName('r2-tg0-app')
		await createTopics(admin, [{ name: input }, { name: output }])
		const app = flow({
			applicationId,
			client: newClient('r2-tg0-app'),
			processingGuarantee: 'exactly_once',
			commitIntervalMs: 100,
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			producer: { lingerMs: 0 },
		})
		// A row's value is `<group>:<version>`.
		app.table(input, { key: stringCodec, value: stringCodec })
			.groupBy((_user, value) => [value.split(':')[0]!, value] as const, { key: stringCodec, value: stringCodec })
			.count({ storeName: 'group-counts', key: stringCodec, value: numberCodec })
			.toStream()
			.to(output, { key: stringCodec, value: numberCodec })
		const rows: Array<[string, string | null]> = [
			['u1', 'gA:1'],
			['u2', 'gA:1'],
			['u1', 'gA:2'], // within gA: one change, gA stays 2
			['u1', 'gB:1'],
			['u2', 'gB:1'], // gA empties
			['u1', null], // a deleted row leaves gB
		]
		try {
			await app.start()
			for (const [key, value] of rows) {
				await producer.send(input, { key, value: value === null ? null : Buffer.from(value) })
			}
			const want = ['gA=1', 'gA=2', 'gA=2', 'gA=1', 'gB=1', 'gA=0', 'gB=2', 'gB=1']
			let got: string[] = []
			for (const deadline = Date.now() + 20_000; got.length < want.length && Date.now() < deadline;) {
				await sleep(300)
				got = (await readAllCommitted(admin, output, 800)).map(
					m => `${m.key!.toString()}=${m.value === null ? 'null' : numberCodec.decode(m.value)}`
				)
			}
			expect(got).toEqual(want)
		} finally {
			await app.close().catch(() => {})
		}
	}, 60_000)
})
