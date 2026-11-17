import { describe, expect, it } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import { internals } from '@/app/internal.js'
import { codec } from '@/codec.js'
import type {
	FixedKeyProcessorContext,
	MaybePromise,
	ProcessorContext,
	ProcessorSupplier,
	RecordMetadata,
} from '@/processor/api.js'
import { StreamRecord } from '@/processor/record.js'
import { Stores } from '@/state/store-builders.js'
import { TestDriver, type TestContextApi } from '@/testing/driver.js'
import { GUARANTEES } from '../support/guarantees.js'

const str = codec.string()
const S = { key: str, value: str }
const START = 1_000_000

/** A processor whose callbacks get its context. */
function processor<KIn, VIn, KOut = never, VOut = never>(
	process: (context: ProcessorContext<KOut, VOut>, record: StreamRecord<KIn, VIn>) => MaybePromise<void>,
	init?: (context: ProcessorContext<KOut, VOut>) => MaybePromise<void>
): ProcessorSupplier<KIn, VIn, KOut, VOut> {
	return () => {
		let context!: ProcessorContext<KOut, VOut>
		return {
			init: async c => {
				context = c
				await init?.(c)
			},
			process: record => process(context, record),
		}
	}
}

/** Sends a string value with a string key at a timestamp. */
function send(ctx: TestContextApi, topic: string, k: string, value: string, timestamp = 0) {
	return ctx.send(topic, Buffer.from(value), { key: Buffer.from(k), timestamp })
}

const records = (ctx: TestContextApi, topic = 'out') => ctx.output(topic, S).map(r => [r.key, r.value])
const topics = (ctx: TestContextApi) => [...new Set(ctx.producer.messages.map(m => m.topic))]
/** The repartition topics of the topology built so far. */
const repartitionTopics = (driver: TestDriver) =>
	[...internals(driver.flow).build().internalTopics.values()].filter(spec => spec.kind === 'repartition')
const all = async <T>(entries: AsyncIterable<T>) => {
	const found: T[] = []
	for await (const entry of entries) found.push(entry)
	return found
}

describe('KStream.process (6.21, KS KStreamImpl.process)', () => {
	it('forwards any number of records per input, with new keys: a key-based operation downstream repartitions', async () => {
		const driver = new TestDriver()
		const words = driver.input('in', S).process(
			processor<string | null, string | null, string, string>(async (context, record) => {
				for (const word of record.value!.split(' ')) {
					await context.forward(record.withKey(word).withValue(word.toUpperCase()))
				}
			})
		)
		words.to('words', S)
		words
			.groupByKey({ key: str, value: str })
			.count()
			.toStream()
			.mapValues(n => String(n))
			.to('counts', S)
		await driver.run(async ctx => {
			await send(ctx, 'in', 'k', 'a b a')
			expect(records(ctx, 'words')).toEqual([
				['a', 'A'],
				['b', 'B'],
				['a', 'A'],
			])
			expect(records(ctx, 'counts')).toEqual([
				['a', '1'],
				['b', '1'],
				['a', '2'],
			])
			expect(topics(ctx).filter(topic => topic.endsWith('-repartition'))).toHaveLength(1)
		})
	})

	it('forward(record, childName) reaches only that child; the record keeps the input timestamp and headers', async () => {
		const driver = new TestDriver()
		const routed = driver.input('in', S).process(
			processor<string | null, string | null, string, string>((context, record) =>
				context.forward(record.withKey(record.key!).withValue(record.value!), record.value!)
			),
			{ name: 'router' }
		)
		routed.filter(() => true, { name: 'left' }).to('left', S)
		routed.filter(() => true, { name: 'right' }).to('right', S)
		await driver.run(async ctx => {
			await ctx.send('in', Buffer.from('left'), {
				key: Buffer.from('k'),
				timestamp: 7,
				headers: { h: Buffer.from('x') },
			})
			await send(ctx, 'in', 'k', 'right')
			expect(records(ctx, 'left')).toEqual([['k', 'left']])
			expect(records(ctx, 'right')).toEqual([['k', 'right']])
			const [left] = ctx.producer.messagesFor('left')
			expect(left).toMatchObject({ timestamp: 7n, headers: { h: Buffer.from('x') } })
		})
	})

	it('gives the input record metadata in process(), none in init() and punctuators; stream and system time', async () => {
		const driver = new TestDriver({ initialWallClockTimeMs: START })
		const seen: { where: string; metadata: RecordMetadata | null; streamTime: number; systemTime: number }[] = []
		const note = (where: string, context: ProcessorContext<never, never>) =>
			void seen.push({
				where,
				metadata: context.recordMetadata(),
				streamTime: context.currentStreamTimeMs(),
				systemTime: context.currentSystemTimeMs(),
			})
		driver.input('in', S).process(
			processor<string | null, string | null>(
				context => note('process', context),
				context => {
					note('init', context)
					context.schedule(100, 'wall-clock', () => note('punctuator', context))
				}
			)
		)
		await driver.run(async ctx => {
			await send(ctx, 'in', 'k', 'a', 5)
			await ctx.advanceWallClockTime(100)
		})
		expect(seen).toEqual([
			{ where: 'init', metadata: null, streamTime: -1, systemTime: START },
			{
				where: 'process',
				metadata: expect.objectContaining({ topic: 'in', partition: 0, offset: 0n }),
				streamTime: 5,
				systemTime: START,
			},
			{ where: 'punctuator', metadata: null, streamTime: 5, systemTime: START + 100 },
		])
	})

	it('a processor whose init() did not run or threw is never closed (KS)', async () => {
		const driver = new TestDriver()
		const events: string[] = []
		const node = (name: string, init: () => void) => () => ({
			init: () => {
				events.push(`${name}.init`)
				init()
			},
			process() {},
			close: () => void events.push(`${name}.close`),
		})
		driver
			.input('in', S)
			.process(node('a', () => {}))
			.process(
				node('b', () => {
					throw new Error('boom')
				})
			)
			.process(node('c', () => {}))
		await expect(driver.start()).rejects.toThrow("failed to initialize processor 'KSTREAM-PROCESSOR-0000000002'")
		await driver.stop()
		expect(events).toEqual(['a.init', 'b.init', 'a.close'])
	})

	it('SI9: a supplier returning the same processor twice is a TopologyError', () => {
		const driver = new TestDriver()
		const shared = { process: () => {} }
		const stream = driver.input('in', S)
		expect(() => stream.process(() => shared)).toThrow(TopologyError)
		expect(() => stream.processValues(() => shared)).toThrow(TopologyError)
	})
})

describe('KStream.processValues (6.21, KS KStreamImpl.processValues)', () => {
	it('forwards FixedKeyRecords: keys, their codec and the repartition flag are kept; stores are reachable', async () => {
		const driver = new TestDriver()
		const seen = Stores.keyValueStore({ name: 'seen', key: str, value: codec.json<number>() })
		const numbered = driver.input('in', S).processValues<string>(
			() => {
				let context!: FixedKeyProcessorContext<string | null, string>
				return {
					init: c => {
						context = c
					},
					process: async record => {
						const store = context.getStateStore(seen)
						const n = ((await store.get(record.key!)) ?? 0) + 1
						await store.put(record.key!, n)
						await context.forward(record.withValue(`${record.value}#${n}`).withTimestamp(9))
					},
				}
			},
			{ stores: [seen] }
		)
		numbered.to('numbered', { value: str })
		numbered
			.groupByKey({ value: str })
			.count()
			.toStream()
			.mapValues(n => String(n))
			.to('counts', { value: str })
		await driver.run(async ctx => {
			await send(ctx, 'in', 'k', 'a')
			await send(ctx, 'in', 'k', 'b')
			expect(records(ctx, 'numbered')).toEqual([
				['k', 'a#1'],
				['k', 'b#2'],
			])
			expect(records(ctx, 'counts')).toEqual([
				['k', '1'],
				['k', '2'],
			])
			expect(topics(ctx).some(topic => topic.endsWith('-repartition'))).toBe(false)
			expect(ctx.producer.messagesFor('counts').map(m => m.timestamp)).toEqual([9n, 9n])
		})
	})
})

describe('the stream format and repartition flag after process() and processValues() (6.0, KS KStreamImpl)', () => {
	const pass = <V>() =>
		processor<string | null, V | null, string, V>((context, record) =>
			context.forward(record.withKey(record.key!).withValue(record.value!))
		)
	const passValues = () => ({ process() {} })

	it('process(): both codecs are dropped and a key-based operation downstream repartitions', () => {
		const driver = new TestDriver()
		const processed = driver.input('in', S).process(pass<string>())
		expect(() => processed.groupByKey().count()).toThrow(
			new TopologyError('count() needs a key codec: the keys were changed upstream, so pass one in its options')
		)
		expect(() => processed.groupByKey({ key: str }).reduce((a, b) => a + b)).toThrow('reduce() needs a value codec')
		processed.groupByKey({ key: str, value: str }).count()
		expect(repartitionTopics(driver)).toHaveLength(1)
	})

	it('processValues(): the key codec is kept, the value codec dropped; the repartition flag is kept', () => {
		const driver = new TestDriver()
		const processed = driver.input('in', S).processValues<string>(passValues)
		expect(() => processed.groupByKey().reduce((a, b) => a + b)).toThrow('reduce() needs a value codec')
		processed.groupByKey().count()
		expect(repartitionTopics(driver)).toEqual([])

		const rekeyed = new TestDriver()
		rekeyed
			.input('in', S)
			.selectKey(value => value)
			.processValues<string>(passValues)
			.groupByKey({ key: str, value: str })
			.count()
		expect(repartitionTopics(rekeyed)).toHaveLength(1)
	})
})

describe('Stores and connected stores (6.21)', () => {
	it('a processor reads and writes its typed store, logged to its changelog with the store codecs', async () => {
		const driver = new TestDriver()
		const seen = Stores.keyValueStore({ name: 'seen', key: str, value: codec.json<number>() })
		driver
			.input('in', S)
			.process(
				processor<string | null, string | null, string, string>(async (context, record) => {
					const store = context.getStateStore(seen)
					const count = ((await store.get(record.key!)) ?? 0) + 1
					await store.put(record.key!, count)
					if (count === 1) await context.forward(record.withKey(record.key!).withValue(record.value!))
				}),
				{ stores: [seen] }
			)
			.to('out', S)
		await driver.run(async ctx => {
			await send(ctx, 'in', 'a', '1')
			await send(ctx, 'in', 'a', '2')
			await send(ctx, 'in', 'b', '3')
			expect(records(ctx)).toEqual([
				['a', '1'],
				['b', '3'],
			])
			expect(ctx.output('test-app-seen-changelog', { key: str, value: codec.json<number>() })).toMatchObject([
				{ key: 'a', value: 1 },
				{ key: 'a', value: 2 },
				{ key: 'b', value: 1 },
			])
			expect(await ctx.getStateStore(seen).get('a')).toBe(2)
		})
	})

	it('a timestamped store keeps { value, timestamp } as [int64BE timestamp][value]; logging options apply', async () => {
		const driver = new TestDriver()
		const latest = Stores.timestampedKeyValueStore({
			name: 'latest',
			key: str,
			value: str,
			logging: { topicName: 'latest-log' },
		})
		const unlogged = Stores.keyValueStore({ name: 'unlogged', key: str, value: str, logging: false })
		driver.input('in', S).process(
			processor<string | null, string | null>(async (context, record) => {
				await context
					.getStateStore(latest)
					.put(record.key!, { value: record.value!, timestamp: record.timestamp })
				await context.getStateStore(unlogged).put(record.key!, record.value!)
			}),
			{ stores: [latest, unlogged] }
		)
		await driver.run(async ctx => {
			await send(ctx, 'in', 'k', 'v', 258)
			const [logged] = ctx.producer.messagesFor('latest-log')
			expect(logged?.value).toEqual(Buffer.from([0, 0, 0, 0, 0, 0, 1, 2, 0x76]))
			expect(topics(ctx)).toEqual(['latest-log'])
			expect(await ctx.getStateStore(latest).get('k')).toEqual({ value: 'v', timestamp: 258 })
		})
	})

	it('window and session stores: reads keep to the retention without any expiry, as in Kafka Streams', async () => {
		const driver = new TestDriver()
		const windows = Stores.windowStore({ name: 'w', key: str, value: str, retention: 20, windowSize: 10 })
		const sessions = Stores.sessionStore({ name: 's', key: str, value: str, retention: 20 })
		driver.input('in', S).process(
			processor<string | null, string | null>(async (context, record) => {
				const window = { key: record.key!, windowStart: record.timestamp, windowEnd: record.timestamp + 10 }
				await context.getStateStore(windows).put(window, record.value!)
				const session = { key: record.key!, windowStart: record.timestamp, windowEnd: record.timestamp }
				await context.getStateStore(sessions).put(session, record.value!)
			}),
			{ stores: [windows, sessions] }
		)
		await driver.run(async ctx => {
			await send(ctx, 'in', 'k', 'old', 0)
			await send(ctx, 'in', 'k', 'new', 100_000)
			expect(await all(ctx.getStateStore(windows).fetch('k', 0, 200_000))).toEqual([
				[{ key: 'k', windowStart: 100_000, windowEnd: 100_010 }, 'new'],
			])
			expect(await all(ctx.getStateStore(sessions).findSessions('k', 0, 200_000))).toEqual([
				[{ key: 'k', windowStart: 100_000, windowEnd: 100_000 }, 'new'],
			])
			// Not deleted: no processor expired them.
			expect(await ctx.getStateStore(windows).approximateNumEntries()).toBe(2)
		})
	})

	it('window and session stores: expireOldWindows / expireOldSessions reclaim space (here from a punctuator)', async () => {
		const driver = new TestDriver()
		const windows = Stores.windowStore({
			name: 'windows',
			key: str,
			value: codec.json<number>(),
			retention: 20,
			windowSize: 10,
		})
		const sessions = Stores.sessionStore({ name: 'sessions', key: str, value: str, retention: '20ms' })
		const expired: [number, number][] = []
		driver.input('in', S).process(
			processor<string | null, string | null>(
				async (context, record) => {
					const key = record.key!
					const windowStart = Math.floor(record.timestamp / 10) * 10
					const window = { key, windowStart, windowEnd: windowStart + 10 }
					const store = context.getStateStore(windows)
					await store.put(window, ((await store.get(window)) ?? 0) + 1)
					const session = { key, windowStart: record.timestamp, windowEnd: record.timestamp }
					await context.getStateStore(sessions).put(session, record.value!)
				},
				context =>
					void context.schedule(10, 'stream-time', async streamTime => {
						const windowCount = await context.getStateStore(windows).expireOldWindows(streamTime)
						const sessionKeys = await context.getStateStore(sessions).expireOldSessions(streamTime)
						expired.push([windowCount, sessionKeys.length])
					})
			),
			{ stores: [windows, sessions] }
		)
		await driver.run(async ctx => {
			for (const timestamp of [0, 5, 15]) await send(ctx, 'in', 'k', `v${timestamp}`, timestamp)
			const windowStore = ctx.getStateStore(windows)
			const sessionStore = ctx.getStateStore(sessions)
			expect(await all(windowStore.fetch('k', 0, 100))).toEqual([
				[{ key: 'k', windowStart: 0, windowEnd: 10 }, 2],
				[{ key: 'k', windowStart: 10, windowEnd: 20 }, 1],
			])
			expect(await all(sessionStore.findSessions('k', 0, 100))).toHaveLength(3)

			// Window end 50, session end 45: the older ones are past the retention, though not deleted yet.
			await send(ctx, 'in', 'k', 'v45', 45)
			expect(await all(windowStore.fetch('k', 0, 100))).toEqual([
				[{ key: 'k', windowStart: 40, windowEnd: 50 }, 1],
			])
			expect(await all(sessionStore.findSessions('k', 0, 100))).toEqual([
				[{ key: 'k', windowStart: 45, windowEnd: 45 }, 'v45'],
			])
			expect(await windowStore.approximateNumEntries()).toBe(3)

			// Expiry deletes what the reads stopped returning as of the last commit.
			await send(ctx, 'in', 'k', 'v55', 55)
			expect(expired).toEqual([
				[0, 0],
				[0, 0],
				[0, 0],
				[2, 3],
			])
			expect(await windowStore.approximateNumEntries()).toBe(2)
			// The changelog keys are windowed: [key][start int64BE][end int64BE].
			const [first] = ctx.producer.messagesFor('test-app-windows-changelog')
			expect(first?.key).toEqual(
				Buffer.concat([Buffer.from('k'), Buffer.alloc(8), Buffer.from([0, 0, 0, 0, 0, 0, 0, 10])])
			)
		})
	})

	it('a store connected to several processors is shared: one subtopology, one store per task', async () => {
		const driver = new TestDriver()
		const names = Stores.keyValueStore({ name: 'names', key: str, value: str })
		driver.flow.addStateStore(names)
		driver.input('users', S).process(
			processor<string | null, string | null>((context, record) =>
				context.getStateStore(names).put(record.key!, record.value!)
			),
			{ stores: ['names'] }
		)
		driver
			.input('events', S)
			.process(
				processor<string | null, string | null, string, string>(async (context, record) => {
					const name = (await context.getStateStore(names).get(record.key!)) ?? '?'
					await context.forward(record.withKey(record.key!).withValue(`${name}:${record.value}`))
				}),
				{ stores: [names] }
			)
			.to('out', S)
		const { subtopologies } = driver.flow.describe()
		expect(subtopologies).toHaveLength(1)
		expect(subtopologies[0]!.nodes.filter(node => node.stores.includes('names'))).toHaveLength(2)
		await driver.run(async ctx => {
			await send(ctx, 'events', 'u1', 'login')
			await send(ctx, 'users', 'u1', 'ann')
			await send(ctx, 'events', 'u1', 'logout')
			expect(records(ctx)).toEqual([
				['u1', '?:login'],
				['u1', 'ann:logout'],
			])
		})
	})

	it('a store shared on both sides of a repartition: one subtopology writes and reads the topic, sized by its input', async () => {
		const driver = new TestDriver({ partitions: { in: 2 } })
		const seen = Stores.keyValueStore({ name: 'seen', key: str, value: str })
		const swap = processor<string | null, string | null, string, string>(async (context, record) => {
			await context.getStateStore(seen).put(record.key!, record.value!)
			await context.forward(record.withKey(record.value!).withValue(record.key!))
		})
		driver
			.input('in', S)
			.process(swap, { stores: [seen] })
			.repartition(S)
			.process(swap, { stores: [seen] })
			.to('out', S)
		const [subtopology, ...others] = driver.flow.describe().subtopologies
		expect(others).toEqual([])
		expect(subtopology!.nodes.filter(node => node.stores.includes('seen'))).toHaveLength(2)
		await driver.run(async ctx => {
			await send(ctx, 'in', 'k', 'v')
			await send(ctx, 'in', 'j', 'w', 1)
			expect(records(ctx)).toEqual([
				['k', 'v'],
				['j', 'w'],
			])
			expect(topics(ctx).filter(topic => topic.endsWith('-repartition'))).toHaveLength(1)
		})
	})

	it('connecting stores: names must be registered, definitions made by Stores, every name used once (B5)', () => {
		const driver = new TestDriver()
		const stream = driver.input('in', S)
		const pass = () => ({ process: () => {} })
		const a = Stores.keyValueStore({ name: 'a', key: str, value: str })
		expect(() => stream.process(pass, { stores: ['a'] })).toThrow(/State store 'a' .* is not registered/)
		expect(() => stream.process(pass, { stores: [{ name: 'fake' }] })).toThrow(
			"The store definition 'fake' was not made by Stores"
		)
		driver.flow.addStateStore(a).addStateStore(a)
		stream.process(pass, { stores: [a, 'a'] })
		const other = Stores.windowStore({ name: 'a', key: str, value: str, retention: 10, windowSize: 10 })
		expect(() => driver.flow.addStateStore(other)).toThrow(
			"State store 'a' already exists as a key-value store and cannot be used as a window store"
		)
		stream.groupByKey().count({ storeName: 'counts' })
		const counts = Stores.keyValueStore({ name: 'counts', key: str, value: str })
		expect(() => stream.process(pass, { stores: [counts] })).toThrow(TopologyError)
		expect(() =>
			driver.flow.addStateStore(Stores.keyValueStore({ name: 'no spaces', key: str, value: str }))
		).toThrow(TopologyError)
	})

	it('a processor reading a store it is not connected to fails with a TopologyError', async () => {
		const driver = new TestDriver()
		driver.flow.addStateStore(Stores.keyValueStore({ name: 'other', key: str, value: str }))
		driver.input('in', S).process(processor((context: ProcessorContext) => void context.getStateStore('other')))
		await driver.run(async ctx => {
			await expect(send(ctx, 'in', 'k', 'v')).rejects.toThrow(TopologyError)
		})
	})
})

describe.each(GUARANTEES)('punctuation and commits (4.9; %s)', (_guarantee, config) => {
	it('stream-time punctuators fire as stream time reaches their due time, with the stream time; missed ones are skipped', async () => {
		const driver = new TestDriver(config)
		driver
			.input('in', S)
			.process(
				processor<string | null, string | null, string, string>(
					(context, record) => context.forward(record.withKey(record.key!).withValue(record.value!)),
					context =>
						void context.schedule(10, 'stream-time', streamTime =>
							context.forward(new StreamRecord('tick', String(streamTime), streamTime))
						)
				)
			)
			.to('out', S)
		await driver.run(async ctx => {
			for (const timestamp of [0, 5, 25, 29, 30]) await send(ctx, 'in', 'k', String(timestamp), timestamp)
			expect(records(ctx)).toEqual([
				['k', '0'],
				['tick', '0'],
				['k', '5'],
				['k', '25'],
				['tick', '25'],
				['k', '29'],
				['k', '30'],
				['tick', '30'],
			])
		})
	})

	it('wall-clock punctuators fire as the driver advances its wall clock, without input; cancel() stops them', async () => {
		const driver = new TestDriver({ ...config, initialWallClockTimeMs: START })
		const seen = Stores.keyValueStore({ name: 'ticks', key: str, value: codec.json<number>() })
		driver
			.input('in', S)
			.process(
				processor<string | null, string | null, string, string>(
					() => {},
					context => {
						const schedule = context.schedule(100, 'wall-clock', async now => {
							const ticks = ((await context.getStateStore(seen).get('ticks')) ?? 0) + 1
							await context.getStateStore(seen).put('ticks', ticks)
							await context.forward(new StreamRecord('tick', String(now - START), now))
							if (ticks === 3) schedule.cancel()
						})
					}
				),
				{ stores: [seen] }
			)
			.to('out', S)
		await driver.run(async ctx => {
			await ctx.advanceWallClockTime(99)
			expect(records(ctx)).toEqual([])
			await ctx.advanceWallClockTime(1)
			await ctx.advanceWallClockTime(250)
			await ctx.advanceWallClockTime(50)
			await ctx.advanceWallClockTime(1000)
			expect(records(ctx)).toEqual([
				['tick', '100'],
				['tick', '350'],
				['tick', '400'],
			])
			expect(ctx.producer.messagesFor('out').map(m => m.timestamp)).toEqual([
				BigInt(START + 100),
				BigInt(START + 350),
				BigInt(START + 400),
			])
			expect(await ctx.getStateStore(seen).get('ticks')).toBe(3)
		})
	})

	it('context.commit() commits at the next commit point', async () => {
		const driver = new TestDriver({ ...config, commitEvery: 'stop' })
		driver.input('in', S).process(
			processor<string | null, string | null>((context, record) => {
				if (record.value === 'commit') context.commit()
			})
		)
		await driver.run(async ctx => {
			await send(ctx, 'in', 'k', 'a')
			expect(ctx.consumer.committed.size).toBe(0)
			await send(ctx, 'in', 'k', 'commit')
			expect(ctx.consumer.committed.get('in:0')?.offset).toBe(2n)
			await send(ctx, 'in', 'k', 'b')
			expect(ctx.consumer.committed.get('in:0')?.offset).toBe(2n)
		})
	})
})
