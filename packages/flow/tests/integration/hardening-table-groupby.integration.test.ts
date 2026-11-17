/**
 * Round 3 / attack 2: KTable.groupBy count/reduce/aggregate against a ground truth computed from the final table
 * contents, with 4 instances (formerly 2 instances x 2 threads), random row moves / tombstones / re-adds, instance
 * churn and commit failures.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow, type FlowApp } from '../../src/index.js'
import {
	createTopics,
	injectCommitFailures,
	lcg,
	newClient,
	readCommittedTable,
	sleep,
	uniqueTopicName,
} from './test-helpers.js'

const str = codec.string()
const num = codec.json<number>()
const USERS = 20
const GROUPS = 4

describe('hardening: KTable.groupBy vs ground truth', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('r3-tg-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0, idempotent: true })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	const build = (
		applicationId: string,
		input: string,
		outs: { count: string; sum: string; agg: string },
		guarantee: 'at_least_once' | 'exactly_once',
		client: KafkaClient
	): FlowApp => {
		const app = flow({
			applicationId,
			client,
			processingGuarantee: guarantee,
			commitIntervalMs: 100,
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			producer: { lingerMs: 0 },
			changelog: { restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 5_000, checkIntervalMs: 50 } },
		})
		const grouped = (name: string) =>
			app
				.table(input, { key: str, value: num })
				.groupBy((_u, n) => [`g${n % GROUPS}`, n] as const, { key: str, value: num, name })
		grouped('by-g-count')
			.count({ storeName: 'cnt', key: str, value: num })
			.toStream()
			.to(outs.count, { key: str, value: num })
		grouped('by-g-sum')
			.reduce(
				(a, v) => a + v,
				(a, v) => a - v,
				{ storeName: 'sum', key: str, value: num }
			)
			.toStream()
			.to(outs.sum, { key: str, value: num })
		grouped('by-g-agg')
			.aggregate(
				() => 0,
				(_k, v, a) => a + v * 2,
				(_k, v, a) => a - v * 2,
				{ storeName: 'agg', key: str, value: num }
			)
			.toStream()
			.to(outs.agg, { key: str, value: num })
		return app
	}

	async function scenario(opts: {
		guarantee: 'at_least_once' | 'exactly_once'
		failCommits: boolean
		churn: boolean
		instances: number
		seed: number
	}) {
		const input = uniqueTopicName('r3-tg-in')
		const outs = {
			count: uniqueTopicName('r3-tg-cnt'),
			sum: uniqueTopicName('r3-tg-sum'),
			agg: uniqueTopicName('r3-tg-agg'),
		}
		const applicationId = uniqueTopicName('r3-tg-app')
		await createTopics(admin, [
			{ name: input, partitions: 4 },
			{ name: outs.count },
			{ name: outs.sum },
			{ name: outs.agg },
		])
		let commits = 0
		const mk = (withFailures: boolean) => {
			const c = newClient('r3-tg-app')
			if (withFailures)
				injectCommitFailures(c, () => {
					commits += 1
					return commits % 7 === 0 && commits <= 70
				})
			return build(applicationId, input, outs, opts.guarantee, c)
		}
		const apps: FlowApp[] = Array.from({ length: opts.instances }, () => mk(opts.failCommits))
		const rnd = lcg(opts.seed)
		const table = new Map<string, number>() // ground truth: last value per key
		try {
			await Promise.all(apps.map(a => a.start()))
			let extra: FlowApp | null = null
			for (let round = 0; round < 30; round++) {
				const batch: Array<{ key: string; value: Buffer | null }> = []
				for (let i = 0; i < 8; i++) {
					const u = `u${Math.floor(rnd() * USERS)}`
					const r = rnd()
					if (r < 0.2) {
						batch.push({ key: u, value: null })
						table.delete(u)
					} else {
						const n = 1 + Math.floor(rnd() * 40)
						batch.push({ key: u, value: Buffer.from(JSON.stringify(n)) })
						table.set(u, n)
					}
				}
				await producer.send(input, batch)
				await sleep(60 + rnd() * 120)
				if (opts.churn && round === 8) {
					extra = mk(false)
					await extra.start()
				}
				if (opts.churn && round === 18 && extra) {
					await extra.close().catch(() => {})
					extra = null
				}
				if (opts.churn && round === 22) {
					await apps[0]!.close().catch(() => {})
					apps[0] = mk(false)
					await apps[0].start()
				}
			}
			await extra?.close().catch(() => {})
			const wantCount: Record<string, number> = {}
			const wantSum: Record<string, number> = {}
			const wantAgg: Record<string, number> = {}
			for (const n of table.values()) {
				const g = `g${n % GROUPS}`
				wantCount[g] = (wantCount[g] ?? 0) + 1
				wantSum[g] = (wantSum[g] ?? 0) + n
				wantAgg[g] = (wantAgg[g] ?? 0) + n * 2
			}
			const deadline = Date.now() + 40_000
			let got = { count: {}, sum: {}, agg: {} } as Record<'count' | 'sum' | 'agg', Record<string, number>>
			while (Date.now() < deadline) {
				got = {
					count: await readCommittedTable(admin, outs.count, num, 1_200),
					sum: await readCommittedTable(admin, outs.sum, num, 1_200),
					agg: await readCommittedTable(admin, outs.agg, num, 1_200),
				}
				if (
					JSON.stringify(got) ===
					JSON.stringify({ count: wantCount, sum: wantSum, agg: wantAgg }, (_k, v) =>
						v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).sort()) : v
					)
				)
					break
				await sleep(500)
			}
			const norm = (o: Record<string, number>) => Object.fromEntries(Object.entries(o).sort())
			return {
				got: { count: norm(got.count), sum: norm(got.sum), agg: norm(got.agg) },
				want: { count: norm(wantCount), sum: norm(wantSum), agg: norm(wantAgg) },
				commits,
			}
		} finally {
			for (const a of apps) await a.close().catch(() => {})
		}
	}

	for (const guarantee of ['exactly_once', 'at_least_once'] as const) {
		it(`${guarantee}: 4 instances, moves/tombstones/re-adds + churn => final per group equals truth`, async () => {
			const r = await scenario({ guarantee, failCommits: false, churn: true, instances: 4, seed: 7 })
			expect(r.got).toEqual(r.want)
		}, 85_000)
	}

	it('exactly_once: injected commit failures + 4 instances => final per group equals truth', async () => {
		const r = await scenario({ guarantee: 'exactly_once', failCommits: true, churn: false, instances: 4, seed: 11 })
		expect(r.commits).toBeGreaterThan(7)
		expect(r.got).toEqual(r.want)
	}, 85_000)
})
