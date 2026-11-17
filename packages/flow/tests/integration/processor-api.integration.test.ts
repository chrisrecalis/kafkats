/**
 * The Processor API against the broker (6.21, DL8), under both guarantees: a wall-clock punctuator emits without any
 * input; `context.commit()` commits at once, whatever the commit interval; a logged key-value store and a logged window
 * store of a processor are restored from their changelogs after a restart. Under exactly-once, what a punctuator
 * forwards is committed in the transaction of the offsets and store changes it goes with: an aborted transaction hides
 * it, and its replay forwards from the committed state.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import type { KafkaClient, Producer } from '@kafkats/client'
import {
	codec,
	flow,
	Stores,
	StreamRecord,
	type FlowApp,
	type FlowConfig,
	type Processor,
	type ProcessorContext,
} from '../../src/index.js'
import {
	createTopics,
	fetchCommitted,
	injectCommitFailures,
	newClient,
	readAllCommitted,
	sleep,
	uniqueTopicName,
} from './test-helpers.js'

const str = codec.string()
const num = codec.json<number>()
const GUARANTEES = ['at_least_once', 'exactly_once'] as const
const BASE = 1_700_000_000_000

describe('Processor API', () => {
	let admin: KafkaClient
	let producer: Producer
	beforeAll(async () => {
		admin = newClient('processor-api-admin')
		await admin.connect()
		producer = admin.producer({ lingerMs: 0 })
	})
	afterAll(async () => {
		await producer.disconnect()
		await admin.disconnect()
	})

	async function setup(prefix: string) {
		const names = {
			input: uniqueTopicName(`${prefix}-in`),
			output: uniqueTopicName(`${prefix}-out`),
			applicationId: uniqueTopicName(`${prefix}-app`),
		}
		await createTopics(admin, [{ name: names.input }, { name: names.output }])
		return names
	}

	/** A processor whose `init` and `process` get its context. */
	function processor<KOut, VOut>(
		process: (context: ProcessorContext<KOut, VOut>, record: StreamRecord<string | null, string | null>) => unknown,
		init?: (context: ProcessorContext<KOut, VOut>) => void
	): () => Processor<string | null, string | null, KOut, VOut> {
		return () => {
			let context!: ProcessorContext<KOut, VOut>
			return {
				init: c => {
					context = c
					init?.(c)
				},
				process: async record => {
					await process(context, record)
				},
			}
		}
	}

	const rows = async (topic: string, quietMs = 800) =>
		(await readAllCommitted(admin, topic, quietMs)).map(m => `${m.key!.toString()}=${m.value!.toString()}`)

	describe.each(GUARANTEES)('%s', processingGuarantee => {
		const config = (applicationId: string, extra: Partial<FlowConfig> = {}): FlowConfig => ({
			applicationId,
			client: newClient('processor-api-app'),
			processingGuarantee,
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			changelog: { restoration: { idleTimeoutMs: 1_000, initialIdleTimeoutMs: 5_000, checkIntervalMs: 50 } },
			...extra,
			producer: { lingerMs: 0, ...extra.producer },
		})

		it('a wall-clock punctuator emits without any input until it is cancelled', async () => {
			const { input, output, applicationId } = await setup('papi-punctuate')
			const app = flow(config(applicationId))
			let beats = 0
			app.stream(input, { key: str, value: str })
				.process(
					processor<string, string>(
						() => {},
						context => {
							const punctuation = context.schedule(200, 'wall-clock', async now => {
								await context.forward(new StreamRecord('beat', `${++beats}`, now))
								if (beats === 3) punctuation.cancel()
							})
						}
					)
				)
				.to(output, { key: str, value: str })
			try {
				await app.start()
				let got: string[] = []
				for (const deadline = Date.now() + 15_000; got.length < 3 && Date.now() < deadline;) {
					await sleep(500)
					got = await rows(output, 300)
				}
				expect(got).toEqual(['beat=1', 'beat=2', 'beat=3'])
				// Cancelled after the third.
				await sleep(1_000)
				expect(await rows(output, 300)).toEqual(['beat=1', 'beat=2', 'beat=3'])
			} finally {
				await app.close()
			}
		}, 40_000)

		it('context.commit() commits at once with a commit interval of a minute', async () => {
			const { input, output, applicationId } = await setup('papi-commit')
			const app = flow(
				config(applicationId, { commitIntervalMs: 60_000, producer: { transactionTimeoutMs: 60_000 } })
			)
			let requestedAt = 0
			app.stream(input, { key: str, value: str })
				.process(
					processor<string, string>((context, record) => {
						if (record.value === 'commit') {
							requestedAt = Date.now()
							context.commit()
						}
					})
				)
				.to(output, { key: str, value: str })
			const committed = async () => (await fetchCommitted(admin, applicationId, input)).offset
			try {
				await app.start()
				await producer.send(input, { key: 'k', value: Buffer.from('wait') })
				await sleep(1_500)
				expect(await committed(), 'no commit before the interval').toBeLessThan(1n)
				await producer.send(input, { key: 'k', value: Buffer.from('commit') })
				let seenAt = 0
				for (const deadline = Date.now() + 10_000; seenAt === 0 && Date.now() < deadline; await sleep(50)) {
					if ((await committed()) === 2n) seenAt = Date.now()
				}
				expect(requestedAt, 'commit() was called').toBeGreaterThan(0)
				expect(seenAt, 'the offsets were committed').toBeGreaterThan(0)
				expect(seenAt - requestedAt).toBeLessThan(1_000)
			} finally {
				await app.close()
			}
		}, 40_000)

		it('a logged key-value store and a logged window store are restored after a restart', async () => {
			const { input, output, applicationId } = await setup('papi-stores')
			const totals = Stores.keyValueStore({ name: 'totals', key: str, value: num })
			const windows = Stores.windowStore({
				name: 'windows',
				key: str,
				value: str,
				retention: '1h',
				windowSize: '1m',
			})
			const build = (): FlowApp => {
				const app = flow(config(applicationId))
				app.stream(input, { key: str, value: str })
					.process(
						processor<string, string>(async (context, record) => {
							const key = record.key!
							const total = ((await context.getStateStore(totals).get(key)) ?? 0) + 1
							await context.getStateStore(totals).put(key, total)
							const start = Math.floor(record.timestamp / 60_000) * 60_000
							const window = { key, windowStart: start, windowEnd: start + 60_000 }
							const values = ((await context.getStateStore(windows).get(window)) ?? '') + record.value
							await context.getStateStore(windows).put(window, values)
							await context.forward(record.withKey(key).withValue(`${total}:${values}`))
						}),
						{ stores: [totals, windows] }
					)
					.to(output, { key: str, value: str })
				return app
			}
			const send = (records: Array<[key: string, value: string]>) =>
				producer.send(
					input,
					records.map(([key, value], i) => ({
						key,
						value: Buffer.from(value),
						timestamp: new Date(BASE + 1_000 + i),
					}))
				)
			let app = build()
			try {
				await app.start()
				await send([
					['a', 'x'],
					['a', 'y'],
					['b', 'z'],
				])
				for (const deadline = Date.now() + 15_000; Date.now() < deadline; await sleep(200)) {
					if ((await rows(output, 300)).length >= 3) break
				}
				await app.close()
				app = build()
				await app.start()
				await send([
					['a', 'w'],
					['b', 'v'],
				])
				let got: string[] = []
				for (const deadline = Date.now() + 15_000; got.length < 5 && Date.now() < deadline;) {
					await sleep(200)
					got = await rows(output, 300)
				}
				expect(got).toEqual(['a=1:x', 'a=2:xy', 'b=1:z', 'a=3:xyw', 'b=2:zv'])
			} finally {
				await app.close()
			}
		}, 60_000)
	})

	// The punctuator forwards the number of records the processor counted in its logged store, with a sequence number
	// of its own. Aborted transactions roll the count back: had their punctuator output been visible, the visible
	// counts would go down after each abort.
	it('exactly_once: punctuator output commits in the transaction of the offsets and store changes it goes with', async () => {
		const { input, output, applicationId } = await setup('papi-eos-punctuate')
		const client = newClient('processor-api-app')
		let commits = 0
		injectCommitFailures(client, () => ++commits % 3 === 2 && commits <= 18)
		const app = flow({
			applicationId,
			client,
			processingGuarantee: 'exactly_once',
			commitIntervalMs: 100,
			consumer: { autoOffsetReset: 'earliest', maxWaitMs: 50 },
			producer: { lingerMs: 0 },
		})
		const counted = Stores.keyValueStore({ name: 'counted', key: str, value: num })
		const emitted: string[] = []
		let sequence = 0
		app.stream(input, { key: str, value: str })
			.process(
				processor<string, string>(
					async context => {
						const store = context.getStateStore(counted)
						await store.put('n', ((await store.get('n')) ?? 0) + 1)
					},
					context => {
						context.schedule(30, 'wall-clock', async now => {
							const count = (await context.getStateStore(counted).get('n')) ?? 0
							const row = `${++sequence}:${count}`
							emitted.push(row)
							await context.forward(new StreamRecord('count', row, now))
						})
					}
				),
				{ stores: [counted] }
			)
			.to(output, { key: str, value: str })
		const N = 60
		try {
			await app.start()
			for (let i = 0; i < N; i++) {
				await producer.send(input, { key: `k${i}`, value: Buffer.from('x') })
				await sleep(25)
			}
			for (const deadline = Date.now() + 20_000; Date.now() < deadline; await sleep(300)) {
				if ((await fetchCommitted(admin, applicationId, input)).offset === BigInt(N)) break
			}
			await sleep(500)
		} finally {
			await app.close()
		}
		expect(commits, 'commit failures were injected').toBeGreaterThanOrEqual(5)
		const visible = (await rows(output)).map(row => row.slice('count='.length))
		const counts = visible.map(row => Number(row.split(':')[1]))
		expect(counts.at(-1), 'the last count is the input').toBe(N)
		for (let i = 1; i < counts.length; i++) {
			expect(
				counts[i],
				`the visible counts never go down (${visible[i - 1]} -> ${visible[i]})`
			).toBeGreaterThanOrEqual(counts[i - 1]!)
		}
		// Every visible row was forwarded, and some forwarded rows were hidden with their aborted transaction.
		expect(visible.filter(row => !emitted.includes(row))).toEqual([])
		expect(visible.length).toBeLessThan(emitted.length)
	}, 60_000)
})
