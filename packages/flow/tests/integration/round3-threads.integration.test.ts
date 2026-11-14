/**
 * Round 3 / attack 6: numStreamThreads 2 and 3 on 4-partition topics with injected commit failures: count,
 * groupBy(newKey).count, stream-stream join, stream-table join must all be exact (no loss, no duplicates), and the
 * co-partitioned join inputs must end up on the same worker (join results complete).
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { TimeWindows, codec, flow } from '../../src/index.js'
import { createTopics, uniqueTopicName } from './test-helpers.js'
import { injectCommitFailures, newClient, readAllCommitted, sleep } from './eos-test-util.js'

const str = codec.string()
const num = codec.json<number>()
const KEYS = 10
const N = 30

describe('round3: multi-thread EOS topologies with commit failures', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('r3-th-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0, idempotent: true })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	for (const threads of [2, 3]) {
		it(`exactly_once numStreamThreads=${threads}: count, groupBy, stream-stream join, stream-table join are exact`, async () => {
			const t = (n: string) => uniqueTopicName(`r3-th${threads}-${n}`)
			const T = {
				a: t('a'),
				b: t('b'),
				tbl: t('tbl'),
				s: t('s'),
				oCount: t('ocnt'),
				oGroup: t('ogrp'),
				oJoin: t('ojoin'),
				oTj: t('otj'),
			}
			const applicationId = uniqueTopicName('r3-th-app')
			await createTopics(admin, [
				{ name: T.a, partitions: 4 },
				{ name: T.b, partitions: 4 },
				{ name: T.tbl, partitions: 4 },
				{ name: T.s, partitions: 4 },
				{ name: T.oCount },
				{ name: T.oGroup },
				{ name: T.oJoin },
				{ name: T.oTj },
			])
			const client = newClient('r3-th-app')
			let commits = 0
			injectCommitFailures(client, () => {
				commits += 1
				return commits % 9 === 0 && commits <= 90
			})
			const app = flow({
				applicationId,
				client,
				numStreamThreads: threads,
				processingGuarantee: 'exactly_once',
				commitIntervalMs: 100,
				consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
				producer: { lingerMs: 0 },
				changelog: { restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 5_000, checkIntervalMs: 50 } },
			})
			const a = app.stream(T.a, { key: str, value: str })
			a.groupByKey()
				.count({ storeName: 'cnt', key: str, value: num })
				.toStream()
				.to(T.oCount, { key: str, value: num })
			a.groupBy((k, _v) => `grp${Number(k!.slice(1)) % 3}`, { key: str, value: str })
				.count({ storeName: 'gcnt', key: str, value: num })
				.toStream()
				.to(T.oGroup, { key: str, value: num })
			a.join(app.stream(T.b, { key: str, value: str }), (l, r) => `${l}|${r}`, {
				within: TimeWindows.of(60_000),
				key: str,
				value: str,
				otherValue: str,
			}).to(T.oJoin, { key: str, value: str })
			app.stream(T.s, { key: str, value: str })
				.join(app.table(T.tbl, { key: str, value: str }), (sv, tv) => `${sv}|${tv}`)
				.to(T.oTj, { key: str, value: str })
			try {
				await app.start()
				await sleep(1_500)
				// table rows first, let them settle
				await producer.send(
					T.tbl,
					Array.from({ length: KEYS }, (_, k) => ({ key: `k${k}`, value: Buffer.from(`T${k}`) }))
				)
				await sleep(3_000)
				const now = Date.now()
				for (let i = 0; i < N; i++) {
					await producer.send(
						T.a,
						Array.from({ length: KEYS }, (_, k) => ({
							key: `k${k}`,
							value: Buffer.from(`a${i}`),
							timestamp: new Date(now),
						}))
					)
					if (i < 3)
						await producer.send(
							T.b,
							Array.from({ length: KEYS }, (_, k) => ({
								key: `k${k}`,
								value: Buffer.from(`b${i}`),
								timestamp: new Date(now),
							}))
						)
					await producer.send(
						T.s,
						Array.from({ length: KEYS }, (_, k) => ({ key: `k${k}`, value: Buffer.from(`s${i}`) }))
					)
					await sleep(120)
				}
				// expectations
				const wantJoin = new Set<string>()
				for (let k = 0; k < KEYS; k++)
					for (let i = 0; i < N; i++) for (let j = 0; j < 3; j++) wantJoin.add(`k${k}:a${i}|b${j}`)
				const wantTj = new Set<string>()
				for (let k = 0; k < KEYS; k++) for (let i = 0; i < N; i++) wantTj.add(`k${k}:s${i}|T${k}`)

				const deadline = Date.now() + 45_000
				let counts: Map<string, number[]> = new Map()
				let groups: Map<string, number[]> = new Map()
				let joins: string[] = []
				let tjs: string[] = []
				const load = async () => {
					counts = new Map()
					groups = new Map()
					for (const r of await readAllCommitted(admin, T.oCount, 1_200)) {
						const k = r.key!.toString()
						counts.set(k, [...(counts.get(k) ?? []), num.decode(r.value!)])
					}
					for (const r of await readAllCommitted(admin, T.oGroup, 1_200)) {
						const k = r.key!.toString()
						groups.set(k, [...(groups.get(k) ?? []), num.decode(r.value!)])
					}
					joins = (await readAllCommitted(admin, T.oJoin, 1_200)).map(
						r => `${r.key!.toString()}:${r.value!.toString()}`
					)
					tjs = (await readAllCommitted(admin, T.oTj, 1_200)).map(
						r => `${r.key!.toString()}:${r.value!.toString()}`
					)
				}
				for (;;) {
					await load()
					if (
						joins.length >= wantJoin.size &&
						tjs.length >= wantTj.size &&
						counts.size === KEYS &&
						groups.size === 3
					) {
						const fin = [...counts.values()].every(v => v.length >= N)
						if (fin) break
					}
					if (Date.now() > deadline) break
					await sleep(1_000)
				}
				expect(commits).toBeGreaterThan(9)
				const wantSeq = Array.from({ length: N }, (_, i) => i + 1)
				for (let k = 0; k < KEYS; k++) expect(counts.get(`k${k}`), `count k${k}`).toEqual(wantSeq)
				// group counts: per group the total number of records; each output sequence must be strictly 1..M
				for (const [g, seq] of groups) {
					const members = Array.from({ length: KEYS }, (_, k) => k).filter(k => `grp${k % 3}` === g).length
					expect(seq, g).toEqual(Array.from({ length: members * N }, (_, i) => i + 1))
				}
				expect(joins.length, 'join duplicates/missing').toBe(wantJoin.size)
				expect(new Set(joins)).toEqual(wantJoin)
				expect(tjs.length, 'table-join duplicates/missing').toBe(wantTj.size)
				expect(new Set(tjs)).toEqual(wantTj)
			} finally {
				await app.close().catch(() => {})
			}
		}, 88_000)
	}
})
