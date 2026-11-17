import { describe, expect, it, vi } from 'vitest'
import { FailedProcessingError } from '@/api/errors.js'
import { codec } from '@/codec.js'
import type { InternalProcessorContext, ProcessorContext } from '@/processor/api.js'
import { TimestampExtractors } from '@/processor/timestamps.js'
import type { KeyValueStore } from '@/state.js'
import { Stores } from '@/state/store-builders.js'
import { TestDriver, type TestDriverConfig } from '@/testing/driver.js'

const str = codec.string()
const START = 1_000_000

/** A string key-value store named `name`. */
const stringStore = (name: string) => Stores.keyValueStore({ name, key: str, value: str })

/** in -> mapValues(upper) -> out, with string codecs. */
function upperCase(config: TestDriverConfig = {}): TestDriver {
	const driver = new TestDriver({ initialWallClockTimeMs: START, ...config })
	driver
		.input('in', { key: str, value: str })
		.mapValues(v => v?.toUpperCase() ?? null)
		.to('out', { key: str, value: str })
	return driver
}

const key = (k: string) => ({ key: Buffer.from(k) })

describe('TestDriver', () => {
	it('has one consumer and one producer once started, which stay readable after stop', async () => {
		const driver = upperCase()
		expect(driver.consumers).toHaveLength(0)
		expect(driver.producers).toHaveLength(0)
		await driver.run(async ctx => {
			expect(driver.consumers).toEqual([ctx.consumer])
			expect(driver.producers).toEqual([ctx.producer])
		})
		expect(driver.producers).toHaveLength(1)
	})

	it('processes a record and everything it leads to before send() resolves, and commits after every record', async () => {
		const driver = upperCase()
		await driver.run(async ({ send, output, consumer }) => {
			await send('in', Buffer.from('a'), key('k'))
			expect(output('out', { key: str, value: str })).toEqual([
				{ topic: 'out', key: 'k', value: 'A', partition: undefined },
			])
			expect(consumer.committed.get('in:0')?.offset).toBe(1n)
			await send('in', Buffer.from('b'), key('k'))
			expect(consumer.committed.get('in:0')?.offset).toBe(2n)
		})
	})

	it('sends null as a tombstone, other values as JSON or with their codec, and keys with their codec', async () => {
		const driver = new TestDriver()
		const seen: unknown[] = []
		driver.input('in').peek((k, v) => seen.push([k?.toString() ?? null, v?.toString() ?? null]))
		await driver.run(async ({ send }) => {
			await send('in', null, key('a'))
			await send('in', { n: 1 })
			await send('in', 'x', { codec: str, keyValue: 'b', keyCodec: str })
		})
		expect(seen).toEqual([
			['a', null],
			[null, '{"n":1}'],
			['b', 'x'],
		])
	})

	it('stamps records with the mock wall clock unless they carry a timestamp', async () => {
		const driver = new TestDriver({ initialWallClockTimeMs: START })
		driver.input('in').to('out')
		await driver.run(async ({ send, producer, advanceWallClockTime }) => {
			await send('in', Buffer.from('a'))
			await advanceWallClockTime(5)
			await send('in', Buffer.from('b'))
			await send('in', Buffer.from('c'), { timestamp: 7n })
			expect(producer.messages.map(m => m.timestamp)).toEqual([BigInt(START), BigInt(START + 5), 7n])
		})
	})

	it('is deterministic: the same inputs give the same outputs and transactions', async () => {
		const runOnce = async () => {
			const driver = new TestDriver({ processingGuarantee: 'exactly_once', initialWallClockTimeMs: START })
			const table = driver.input('in', { key: str, value: str }).through('loop', { key: str, value: str })
			table.mapValues(v => `${v}!`).to('out', { key: str, value: str })
			await driver.run(async ({ send }) => {
				for (const [k, v] of [
					['a', '1'],
					['b', '2'],
					['a', '3'],
				] as const) {
					await send('in', Buffer.from(v), key(k))
				}
			})
			const { messages, transactions } = driver.producers[0]!
			return { messages, transactions }
		}
		const first = await runOnce()
		expect(first.transactions).toHaveLength(6)
		expect(await runOnce()).toEqual(first)
	})

	it('feeds records written to topics the topology reads back in head-timestamp order', async () => {
		const driver = new TestDriver()
		const seen: number[] = []
		// One input record leads to a record of x (ts 20), then one of y (ts 10).
		const fanOut = driver.input('in').process<Buffer | null, Buffer | null>(() => {
			let context: ProcessorContext<Buffer | null, Buffer | null>
			return {
				init: ctx => {
					context = ctx
				},
				process: async record => {
					await context.forward(record.withTimestamp(20), 'x-sink')
					await context.forward(record.withTimestamp(10), 'y-sink')
				},
			}
		})
		fanOut.to('x', { name: 'x-sink' })
		fanOut.to('y', { name: 'y-sink' })
		const x = driver.flow.stream('x')
		const y = driver.flow.stream('y')
		x.merge(y).process(() => ({ process: record => void seen.push(record.timestamp) }))
		await driver.run(async ({ send }) => {
			await send('in', Buffer.from('v'), { timestamp: 30n })
		})
		expect(seen).toEqual([10, 20])
	})

	it('creates the tasks of partition 0 at start, the others for the first record of their partition', async () => {
		const driver = new TestDriver()
		const last = stringStore('last')
		driver.input('in', { key: str, value: str }).process(
			() => {
				let store: KeyValueStore<string, string>
				return {
					init: ctx => {
						store = ctx.getStateStore(last)
					},
					process: record => store.put(record.key!, record.value!),
				}
			},
			{ stores: [last] }
		)
		await driver.run(async ({ send, getStateStore }) => {
			expect(await getStateStore<KeyValueStore<string, string>>('last').approximateNumEntries()).toBe(0)
			expect(() => getStateStore('last', 1)).toThrow("No task of partition 1 has a state store named 'last'")
			await send('in', Buffer.from('one'), { ...key('k'), partition: 1 })
			expect(await getStateStore<KeyValueStore<string, string>>('last', 1).get('k')).toBe('one')
			expect(await getStateStore<KeyValueStore<string, string>>('last').get('k')).toBeUndefined()
			expect(() => getStateStore('nope')).toThrow("No task of partition 0 has a state store named 'nope'")
		})
	})

	it('rejects a record for a partition beyond a configured partition count', async () => {
		const driver = upperCase({ partitions: { in: 2 } })
		await driver.run(async ({ send }) => {
			await send('in', Buffer.from('a'), { partition: 1 })
			await expect(send('in', Buffer.from('b'), { partition: 2 })).rejects.toThrow(
				"Topic 'in' has 2 partitions in this test, so it has no partition 2"
			)
		})
	})

	it('reports the configured partition count to partitioners, 1 otherwise', async () => {
		const driver = new TestDriver({ partitions: { wide: 4 } })
		const counts: number[] = []
		const stream = driver.input('in')
		const partitioner = (_k: unknown, _v: unknown, count: number) => {
			counts.push(count)
			return count - 1
		}
		stream.to('wide', { partitioner })
		stream.to('narrow', { partitioner })
		await driver.run(async ({ send, output }) => {
			await send('in', Buffer.from('a'))
			expect(output('wide').map(r => r.partition)).toEqual([3])
			expect(output('narrow').map(r => r.partition)).toEqual([0])
		})
		expect(counts).toEqual([4, 1])
	})

	it('runs wall-clock punctuations when the mock wall clock advances, with the current wall clock', async () => {
		const driver = new TestDriver({ initialWallClockTimeMs: START })
		const fired: number[] = []
		driver.input('in').process(() => ({
			init: ctx => void ctx.schedule(100, 'wall-clock', now => void fired.push(now)),
			process: () => {},
		}))
		await driver.run(async ({ advanceWallClockTime }) => {
			await advanceWallClockTime(99)
			expect(fired).toEqual([])
			await advanceWallClockTime(151)
			expect(fired).toEqual([START + 250])
			await expect(advanceWallClockTime(-1)).rejects.toThrow(RangeError)
		})
	})

	it('runs stream-time punctuations as records advance the stream time', async () => {
		const driver = new TestDriver()
		const fired: number[] = []
		driver.input('in').process(() => ({
			init: ctx => void ctx.schedule(10, 'stream-time', time => void fired.push(time)),
			process: () => {},
		}))
		await driver.run(async ({ send }) => {
			await send('in', Buffer.from('a'), { timestamp: 5 })
			await send('in', Buffer.from('b'), { timestamp: 8 })
			await send('in', Buffer.from('c'), { timestamp: 25 })
		})
		expect(fired).toEqual([5, 25])
	})

	it('commits only when it stops with commitEvery: stop', async () => {
		const driver = upperCase({ commitEvery: 'stop' })
		await driver.run(async ({ send, consumer }) => {
			await send('in', Buffer.from('a'))
			await send('in', Buffer.from('b'))
			expect(consumer.committed.size).toBe(0)
		})
		expect(driver.consumers[0]!.committed.get('in:0')?.offset).toBe(2n)
	})

	it('commits whenever the mock wall clock moved the given ms since the last commit', async () => {
		const driver = upperCase({ commitEvery: 1000 })
		await driver.run(async ({ send, consumer, advanceWallClockTime }) => {
			await send('in', Buffer.from('a'))
			await advanceWallClockTime(999)
			expect(consumer.committed.size).toBe(0)
			await advanceWallClockTime(1)
			expect(consumer.committed.get('in:0')?.offset).toBe(1n)
		})
	})

	it('commits at the next step when a processor requests it', async () => {
		const driver = new TestDriver({ commitEvery: 'stop' })
		driver.input('in').process(() => {
			let context: ProcessorContext
			return {
				init: ctx => {
					context = ctx
				},
				process: record => {
					if (record.value?.toString() === 'commit') context.commit()
				},
			}
		})
		await driver.run(async ({ send, consumer }) => {
			await send('in', Buffer.from('a'))
			expect(consumer.committed.size).toBe(0)
			await send('in', Buffer.from('commit'))
			expect(consumer.committed.get('in:0')?.offset).toBe(2n)
		})
	})

	it('commits one transaction per record under exactly_once, or one at stop', async () => {
		const perRecord = upperCase({ processingGuarantee: 'exactly_once' })
		await perRecord.run(async ({ send }) => {
			await send('in', Buffer.from('a'))
			await send('in', Buffer.from('b'))
		})
		expect(
			perRecord.producers[0]!.transactions.map(t => [t.messages.length, t.offsets.map(o => o.offset)])
		).toEqual([
			[1, [1n]],
			[1, [2n]],
		])

		const atStop = upperCase({ processingGuarantee: 'exactly_once', commitEvery: 'stop' })
		await atStop.run(async ({ send, producer }) => {
			await send('in', Buffer.from('a'))
			await send('in', Buffer.from('b'))
			expect(producer.transactions).toHaveLength(0)
		})
		const { transactions } = atStop.producers[0]!
		expect(transactions.map(t => [t.messages.length, t.offsets.map(o => o.offset)])).toEqual([[2, [2n]]])
		expect(atStop.consumers[0]!.committed.get('in:0')?.offset).toBe(2n)
	})

	it('gives processors the configured emit interval, 0 by default', async () => {
		const intervals: number[] = []
		for (const config of [{}, { emitIntervalMs: 250 }]) {
			const driver = new TestDriver(config)
			driver.input('in').process(() => ({
				init: ctx => void intervals.push((ctx as InternalProcessorContext<never, never>).config.emitIntervalMs),
				process: () => {},
			}))
			await driver.run(async () => {})
		}
		expect(intervals).toEqual([0, 250])
	})

	it('rejects send() with a processing failure and stops: later sends reject, the transaction is aborted', async () => {
		const uncaught = vi.fn(() => 'shutdown' as const)
		const driver = new TestDriver({ processingGuarantee: 'exactly_once', uncaughtExceptionHandler: uncaught })
		driver
			.input('in', { value: str })
			.peek((_k, v) => {
				if (v === 'boom') throw new Error('boom')
			})
			.to('out', { value: str })
		await driver.run(async ({ send, output }) => {
			await send('in', Buffer.from('ok'))
			const failure = send('in', Buffer.from('boom'))
			await expect(failure).rejects.toThrow(FailedProcessingError)
			const error = await failure.catch((e: unknown) => e)
			expect((error as Error).cause).toEqual(new Error('boom'))
			expect(uncaught).toHaveBeenCalledWith(error)
			await expect(send('in', Buffer.from('later'))).rejects.toBe(error)
			expect(output('out', { value: str }).map(r => r.value)).toEqual(['ok'])
		})
		expect(driver.producers[0]!.transactions).toHaveLength(1)
	})

	it('applies records of a global topic to its store', async () => {
		const driver = new TestDriver()
		driver.input('in')
		driver.globalTable('users', { key: str, value: str, materialized: { storeName: 'users' } })
		await driver.run(async ({ send, getStateStore }) => {
			await send('users', Buffer.from('Alice'), key('u1'))
			const store = getStateStore<KeyValueStore<string, { value: string; timestamp: number }>>('users')
			expect((await store.get('u1'))?.value).toBe('Alice')
		})
	})

	it('skips a global record the deserialization handler lets go, and one with a negative timestamp', async () => {
		const handled: unknown[] = []
		const driver = new TestDriver({
			deserializationExceptionHandler: (ctx, record) => {
				handled.push([ctx.topic, ctx.offset, record.value?.toString()])
				return 'continue'
			},
		})
		driver.input('in')
		driver.globalTable('users', {
			key: str,
			value: codec.json<string>(),
			timestampExtractor: TimestampExtractors.logAndSkipOnInvalid,
			materialized: { storeName: 'users' },
		})
		await driver.run(async ({ send, getStateStore }) => {
			await send('users', Buffer.from('{not json'), key('u1'))
			await send('users', 'late', { ...key('u2'), timestamp: -5 })
			await send('users', 'Bob', key('u3'))
			const store = getStateStore<KeyValueStore<string, unknown>>('users')
			expect(await store.approximateNumEntries()).toBe(1)
		})
		expect(handled).toEqual([['users', 0n, '{not json']])
	})

	it('grows topics without a configured count together, so co-partitioned topics stay assignable', async () => {
		const driver = new TestDriver()
		const seen = stringStore('seen')
		const merged = driver.input('a', { key: str, value: str }).merge(driver.input('b', { key: str, value: str }))
		merged.process(
			() => {
				let store: KeyValueStore<string, string>
				return {
					init: ctx => {
						store = ctx.getStateStore(seen)
					},
					process: record => store.put(record.key!, record.value!),
				}
			},
			{ stores: [seen] }
		)
		await driver.run(async ({ send, getStateStore }) => {
			await send('b', Buffer.from('v'), { ...key('k'), partition: 2 })
			await send('a', Buffer.from('w'), { ...key('j'), partition: 2 })
			const store = getStateStore<KeyValueStore<string, string>>('seen', 2)
			expect([await store.get('k'), await store.get('j')]).toEqual(['v', 'w'])
		})
	})

	it('fails start() when a processor fails to initialize, and stops cleanly', async () => {
		const driver = new TestDriver()
		driver.input('in').process(() => ({
			init: () => {
				throw new Error('no init')
			},
			process: () => {},
		}))
		await expect(driver.start()).rejects.toThrow("failed to initialize processor 'KSTREAM-PROCESSOR")
		await driver.stop()
		await expect(new TestDriver().stop()).resolves.toBeUndefined()
	})

	it('builds the topology at start: later DSL calls throw', async () => {
		const driver = upperCase()
		await driver.run(async () => {
			expect(() => driver.input('late')).toThrow('topology already built')
		})
	})
})
