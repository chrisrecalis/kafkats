/**
 * Round 3 / attack 3: session windows (count / reduce / aggregate+merger) under exactly_once with out-of-order
 * records, a mid-run restart, 2 instances and commit failures: the final live sessions must equal ground truth
 * and merged-away sessions must be retracted (tombstoned).
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import { codec, flow, SessionWindows, type FlowApp } from '../../src/index.js'
import { createTopics, uniqueTopicName } from './test-helpers.js'
import { injectCommitFailures, newClient, readAllCommitted, sleep } from './eos-test-util.js'

const str = codec.string()
const num = codec.json<number>()
const GAP = 1_000
const KEYS = 6

function lcg(seed: number) {
	let s = seed >>> 0
	return () => {
		s = (Math.imul(s, 1664525) + 1013904223) >>> 0
		return s / 2 ** 32
	}
}

type Ev = { key: string; ts: number; v: number }

/** Ground truth sessions per key: records within GAP of each other (inclusive) form a session. */
function truthSessions(events: Ev[]): Record<string, { count: number; sum: number }> {
	const out: Record<string, { count: number; sum: number }> = {}
	for (let k = 0; k < KEYS; k++) {
		const evs = events.filter(e => e.key === `k${k}`).sort((a, b) => a.ts - b.ts)
		let start = 0
		let end = 0
		let count = 0
		let sum = 0
		const flush = () => {
			if (count > 0) out[`k${k}@${start}-${end}`] = { count, sum }
		}
		for (const e of evs) {
			if (count > 0 && e.ts - end <= GAP) {
				end = Math.max(end, e.ts)
				count++
				sum += e.v
			} else {
				flush()
				start = e.ts
				end = e.ts
				count = 1
				sum = e.v
			}
		}
		flush()
	}
	return out
}

describe('round3: session windows under EOS', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('r3-ss-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0, idempotent: true })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	const keyOf = (_v: unknown, k: unknown) => {
		const w = k as { key: string; window: { start: number; end: number } }
		return `${w.key}@${w.window.start}-${w.window.end}`
	}

	const build = (
		applicationId: string,
		input: string,
		outs: Record<'count' | 'reduce' | 'agg', string>,
		client: KafkaClient
	): FlowApp => {
		const app = flow({
			applicationId,
			client,
			processingGuarantee: 'exactly_once',
			commitIntervalMs: 100,
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			producer: { lingerMs: 0 },
			changelog: { restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 5_000, checkIntervalMs: 50 } },
		})
		const grouped = () => app.stream(input, { key: str, value: num }).groupByKey()
		grouped()
			.windowedBy(SessionWindows.withInactivityGap(GAP))
			.count({ storeName: 's-count', value: num })
			.toStream()
			.selectKey(keyOf)
			.to(outs.count, { key: str, value: num })
		grouped()
			.windowedBy(SessionWindows.withInactivityGap(GAP))
			.reduce((a, b) => a + b, { storeName: 's-reduce', value: num })
			.toStream()
			.selectKey(keyOf)
			.to(outs.reduce, { key: str, value: num })
		grouped()
			.windowedBy(SessionWindows.withInactivityGap(GAP))
			.aggregate(
				() => 0,
				(_k, v, a) => a + v,
				{ storeName: 's-agg', value: num, merger: (a: number, b: number) => a + b } as never
			)
			.toStream()
			.selectKey(keyOf)
			.to(outs.agg, { key: str, value: num })
		return app
	}

	async function view(topic: string): Promise<{ live: Record<string, number>; tombstones: number }> {
		const m = new Map<string, number | null>()
		let tombstones = 0
		for (const r of await readAllCommitted(admin, topic, 1_200)) {
			const v = r.value === null ? null : num.decode(r.value)
			if (v === null) tombstones++
			m.set(r.key!.toString(), v)
		}
		return {
			live: Object.fromEntries([...m].filter(([, v]) => v !== null).sort()) as Record<string, number>,
			tombstones,
		}
	}

	for (const variant of ['commit failures + 2 instances', 'restart mid-run'] as const) {
		it(`exactly_once: out-of-order sessions equal ground truth (${variant})`, async () => {
			const input = uniqueTopicName('r3-ss-in')
			const outs = {
				count: uniqueTopicName('r3-ss-c'),
				reduce: uniqueTopicName('r3-ss-r'),
				agg: uniqueTopicName('r3-ss-a'),
			}
			const applicationId = uniqueTopicName('r3-ss-app')
			await createTopics(admin, [{ name: input, partitions: 4 }, ...Object.values(outs).map(name => ({ name }))])
			const base = Math.floor(Date.now() / 1000) * 1000
			const rnd = lcg(variant.length)
			// events spread over ~20s of event time per key, delivered in scrambled order
			const events: Ev[] = []
			for (let k = 0; k < KEYS; k++) {
				let t = 0
				for (let i = 0; i < 14; i++) {
					// keep total event-time span < retention (24 x gap) so nothing is legitimately dropped as late
					t += rnd() < 0.3 ? 1_500 + Math.floor(rnd() * 1_500) : Math.floor(rnd() * 900)
					events.push({ key: `k${k}`, ts: base + t, v: 1 + Math.floor(rnd() * 5) })
				}
			}
			const order = events
				.map(e => ({ e, r: rnd() }))
				.sort((a, b) => a.r - b.r)
				.map(x => x.e)

			let commits = 0
			const mk = (fail: boolean) => {
				const c = newClient('r3-ss-app')
				if (fail)
					injectCommitFailures(c, () => {
						commits += 1
						return commits % 6 === 0 && commits <= 60
					})
				return build(applicationId, input, outs, c)
			}
			const apps: FlowApp[] = variant.startsWith('commit') ? [mk(true), mk(true)] : [mk(false)]
			try {
				await Promise.all(apps.map(a => a.start()))
				for (let i = 0; i < order.length; i += 6) {
					await producer.send(
						input,
						order.slice(i, i + 6).map(e => ({
							key: e.key,
							value: Buffer.from(JSON.stringify(e.v)),
							timestamp: new Date(e.ts),
						}))
					)
					await sleep(40)
					if (variant === 'restart mid-run' && i === 60) {
						await apps[0]!.close()
						apps[0] = mk(false)
						await apps[0].start()
					}
				}
				const truth = truthSessions(order)
				const wantCount = Object.fromEntries(Object.entries(truth).map(([k, v]) => [k, v.count]))
				const wantSum = Object.fromEntries(Object.entries(truth).map(([k, v]) => [k, v.sum]))
				const deadline = Date.now() + 40_000
				let c = await view(outs.count)
				let r = await view(outs.reduce)
				let a = await view(outs.agg)
				while (
					Date.now() < deadline &&
					JSON.stringify([c.live, r.live, a.live]) !== JSON.stringify([wantCount, wantSum, wantSum])
				) {
					await sleep(800)
					;[c, r, a] = [await view(outs.count), await view(outs.reduce), await view(outs.agg)]
				}
				if (variant.startsWith('commit')) expect(commits).toBeGreaterThan(6)
				expect(c.tombstones, 'merged-away sessions must be retracted').toBeGreaterThan(0)
				expect(c.live).toEqual(wantCount)
				expect(r.live).toEqual(wantSum)
				expect(a.live).toEqual(wantSum)
			} finally {
				for (const app of apps) await app.close().catch(() => {})
			}
		}, 85_000)
	}
})
