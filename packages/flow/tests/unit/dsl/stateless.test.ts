import { describe, expect, it } from 'vitest'
import { FailedProcessingError, TopologyError } from '@/api/errors.js'
import { codec } from '@/codec.js'
import type { KStream } from '@/api/kstream.js'
import type { KStreamImpl } from '@/dsl/kstream.js'
import type { KTableImpl } from '@/dsl/ktable.js'
import type { Change } from '@/operators/change.js'
import type { FixedKeyProcessorContext, InternalProcessorContext, ProcessorContext } from '@/processor/api.js'
import type { FixedKeyRecord, StreamRecord } from '@/processor/record.js'
import type { KeyValueStore, ValueAndTimestamp } from '@/state.js'
import { TestDriver } from '@/testing/driver.js'
import type { TopicNameExtractorContext } from '@/topology/nodes.js'

const str = codec.string()
const key = (k: string) => ({ key: Buffer.from(k) })

describe('stateless operators (copied from flow.test.ts)', () => {
	it('processes map/filter/to pipelines', async () => {
		const driver = new TestDriver()

		type Order = { id: string; total: number }
		type Enriched = { id: string; total: number; large: boolean }

		driver
			.input('orders', { value: codec.json<Order>() })
			.mapValues(order => {
				if (order === null) {
					throw new Error('unexpected tombstone in mapValues() test')
				}
				return { ...order, large: order.total > 100 } satisfies Enriched
			})
			.filter((_key, value) => Boolean(value?.large))
			.to('large-orders', { value: codec.json<Enriched>() })

		await driver.run(async ({ send, producer }) => {
			await send('orders', { id: 'o1', total: 250 })
			await send('orders', { id: 'o2', total: 50 })

			expect(producer.messages).toHaveLength(1)
			const payload = JSON.parse(producer.messages[0]!.value!.toString()) as Enriched
			expect(payload).toEqual({ id: 'o1', total: 250, large: true })
		})
	})

	it('supports through to continue the pipeline', async () => {
		const driver = new TestDriver()

		driver
			.input('input', { value: codec.json<{ id: string }>() })
			.through('audit', { value: codec.json() })
			.mapValues(value => ({ ...value, tagged: true }))
			.to('output', { value: codec.json() })

		await driver.run(async ({ send, producer }) => {
			await send('input', { id: 'a1' })
			expect(producer.messages.map(msg => msg.topic)).toEqual(['audit', 'output'])
		})
	})

	it('routes to first matching branch', async () => {
		const driver = new TestDriver()

		const branches = driver.input('events', { value: codec.json<{ type: string }>() }).branch(
			(_key, value) => value?.type === 'a',
			(_key, value) => value?.type === 'b'
		)

		branches[0]!.to('topic-a', { value: codec.json() })
		branches[1]!.to('topic-b', { value: codec.json() })

		await driver.run(async ({ send, producer }) => {
			await send('events', { type: 'b' })
			expect(producer.messages).toHaveLength(1)
			expect(producer.messages[0]!.topic).toBe('topic-b')
		})
	})

	it('routes a record that matches several predicates only to the first of them', async () => {
		const driver = new TestDriver()
		const num = codec.json<number>()
		const [positive, even] = driver.input('in', { value: num }).branch(
			(_key, value) => value! > 0,
			(_key, value) => value! % 2 === 0
		)
		positive!.to('positive', { value: num })
		even!.to('even', { value: num })
		await driver.run(async ({ sendAll, output }) => {
			await sendAll('in', [2, -2, -1])
			expect(output('positive', { value: num }).map(r => r.value)).toEqual([2])
			expect(output('even', { value: num }).map(r => r.value)).toEqual([-2])
		})
	})

	it('merges streams from different topics', async () => {
		const driver = new TestDriver()

		const left = driver.input('left', { value: codec.json<{ id: string }>() })
		const right = driver.input('right', { value: codec.json<{ id: string }>() })

		left.merge(right)
			.mapValues(value => ({ ...value, merged: true }))
			.to('merged', { value: codec.json() })

		await driver.run(async ({ send, producer }) => {
			await send('left', { id: 'l1' })
			await send('right', { id: 'r1' })

			expect(producer.messages).toHaveLength(2)
			const ids = producer.messages.map(msg => JSON.parse(msg.value!.toString()).id)
			expect(ids).toEqual(['l1', 'r1'])
		})
	})

	// The message changed: a serialization failure goes through the production handler ('fail' by default).
	it('throws when codecs are missing for non-buffer values', async () => {
		const driver = new TestDriver()

		driver
			.input('raw', { value: codec.json<{ id: string }>() })
			.mapValues(value => ({ ...value, enriched: true }))
			.to('out')

		await driver.run(async ({ send }) => {
			const failure = send('raw', { id: 'x1' })
			await expect(failure).rejects.toThrow(FailedProcessingError)
			await expect(failure).rejects.toThrow("serialization for topic 'out' failed")
			const error = (await failure.catch((e: unknown) => e)) as Error
			expect((error.cause as Error).message).toBe('no value codec for a value that is not a Buffer')
		})
	})
})

describe('stateless operators (copied from operator-semantics.test.ts)', () => {
	it('to() forwards the event timestamp of the record to the producer', async () => {
		const driver = new TestDriver()
		driver
			.input('in', { key: str, value: str })
			.mapValues(v => v)
			.to('out', { key: str, value: str })
		await driver.run(async ctx => {
			const sent: { timestamp?: Date }[] = []
			const producer = driver.producers[0]!
			const orig = producer.send.bind(producer)
			producer.send = ((topic: string, msg: { timestamp?: Date }) => {
				sent.push(msg)
				return orig(topic, msg as never)
			}) as typeof producer.send
			await ctx.send('in', Buffer.from('v'), { key: Buffer.from('k'), timestamp: 123_456n })
			expect(sent).toHaveLength(1)
			// Kafka Streams preserves the input record timestamp on output records
			expect(sent[0]!.timestamp).toEqual(new Date(123_456))
		})
	})
})

describe('stateless operators', () => {
	it('filterNot drops the records the predicate matches', async () => {
		const driver = new TestDriver()
		driver
			.input('in', { value: str })
			.filterNot((_k, v) => v === 'drop')
			.to('out', { value: str })
		await driver.run(async ({ sendAll, output }) => {
			await sendAll('in', ['keep', 'drop', 'also'], { codec: str })
			expect(output('out', { value: str }).map(r => r.value)).toEqual(['keep', 'also'])
		})
	})

	it('map, flatMap and selectKey change keys; selectKey takes (key, value)', async () => {
		const driver = new TestDriver()
		const stream = driver.input('in', { key: str, value: str })
		stream.map((k, v) => [`${v}`, `${k}`] as const).to('mapped', { key: str, value: str })
		stream
			.flatMap((k, v): [string, string | null][] => [
				[`${k}1`, v],
				[`${k}2`, v],
			])
			.to('flat', { key: str, value: str })
		stream.selectKey((k, v) => `${k}:${v}`).to('selected', { key: str, value: str })
		await driver.run(async ({ send, output }) => {
			await send('in', 'v', { ...key('k'), codec: str })
			const pairs = (topic: string) => output(topic, { key: str, value: str }).map(r => [r.key, r.value])
			expect(pairs('mapped')).toEqual([['v', 'k']])
			expect(pairs('flat')).toEqual([
				['k1', 'v'],
				['k2', 'v'],
			])
			expect(pairs('selected')).toEqual([['k:v', 'v']])
		})
	})

	it('mapValues and flatMapValues get the key when the mapper declares two parameters', async () => {
		const driver = new TestDriver()
		const stream = driver.input('in', { key: str, value: str })
		stream.mapValues(v => `${v}!`).to('value-only', { key: str, value: str })
		stream.mapValues((k: string | null, v: string | null) => `${k}=${v}`).to('with-key', { key: str, value: str })
		stream
			.flatMapValues((k: string | null, v: string | null) => [k ?? '', v ?? ''])
			.to('flat', { key: str, value: str })
		stream.flatMapValues(v => [v, v]).to('flat-values', { key: str, value: str })
		// The documented caveat: a parameter with a default does not count in fn.length, so this mapper gets the value only.
		stream.mapValues((v: string | null, suffix = '?') => `${v}${suffix}`).to('default', { key: str, value: str })
		await driver.run(async ({ send, output }) => {
			await send('in', 'v', { ...key('k'), codec: str })
			const values = (topic: string) => output(topic, { value: str }).map(r => r.value)
			expect(values('value-only')).toEqual(['v!'])
			expect(values('with-key')).toEqual(['k=v'])
			expect(values('flat')).toEqual(['k', 'v'])
			expect(values('flat-values')).toEqual(['v', 'v'])
			expect(values('default')).toEqual(['v?'])
		})
	})

	it('flatMapValues forwards each result once the previous one settled: an async count downstream sees them all', async () => {
		const driver = new TestDriver()
		const counts: unknown[] = []
		driver
			.input('lines', { key: str, value: str })
			.flatMapValues(line => line?.split(' ') ?? [])
			.groupByKey()
			.count()
			.toStream()
			.peek((k, v) => counts.push([k, v]))
		await driver.run(async ({ send }) => {
			await send('lines', 'a b a', { ...key('k'), codec: str })
		})
		expect(counts).toEqual([
			['k', 1],
			['k', 2],
			['k', 3],
		])
	})

	it('flatMap forwards each result once the previous one settled: an async processor downstream sees them all', async () => {
		const driver = new TestDriver()
		const seen: unknown[] = []
		const words = driver
			.input('lines', { key: str, value: str })
			.flatMap((k, line) => (line ?? '').split(' ').map(word => [`${k}-${word}`, word] as const))
		const { ctx, node } = words as KStreamImpl<string, string>
		ctx.processor('SLOW', undefined, [node], () => ({
			process: async (record: StreamRecord<string, string>) => {
				await new Promise(resolve => setImmediate(resolve))
				seen.push([record.key, record.value])
			},
		}))
		await driver.run(async ({ send }) => {
			await send('lines', 'a b', { ...key('k'), codec: str })
		})
		expect(seen).toEqual([
			['k-a', 'a'],
			['k-b', 'b'],
		])
	})

	it('flatMap flags the stream for repartitioning; filter, filterNot, flatMapValues and peek keep the flag (6.0)', async () => {
		const driver = new TestDriver()
		const stream = driver.input('in', { key: str, value: str })
		const counts: unknown[] = []
		const keepFlag = (s: KStream<string, string>) =>
			s
				.filter(() => true)
				.filterNot(() => false)
				.flatMapValues(v => [v])
				.peek(() => {})
		keepFlag(stream.flatMap((_k, v) => [[v!, v!] as const]))
			.groupByKey({ key: str, value: str })
			.count()
			.toStream()
			.foreach((k, count) => counts.push([k, count]))
		keepFlag(stream).groupByKey().count()
		await driver.run(async ({ send, producer }) => {
			await send('in', 'v', { ...key('k'), codec: str })
			expect(producer.messages.map(m => m.topic).filter(topic => topic.endsWith('-repartition'))).toEqual([
				'test-app-repartition-0-repartition',
			])
		})
		expect(counts).toEqual([['v', 1]])
	})

	it('peek and foreach see every record with its key and value, nulls included', async () => {
		const driver = new TestDriver()
		const peeked: unknown[] = []
		const seen: unknown[] = []
		driver
			.input('in', { key: str, value: str })
			.peek((k, v) => peeked.push([k, v]))
			.foreach((k, v) => seen.push([k, v]))
		await driver.run(async ({ send }) => {
			await send('in', 'a', { ...key('k'), codec: str })
			await send('in', null)
		})
		expect(peeked).toEqual([
			['k', 'a'],
			[null, null],
		])
		expect(seen).toEqual(peeked)
	})

	it('keeps the timestamp and headers of the input on every result', async () => {
		const driver = new TestDriver()
		driver
			.input('in', { key: str, value: str })
			.flatMap((k, v) => [[k, v] as const])
			.mapValues(v => v)
			.to('out', { key: str, value: str })
		await driver.run(async ({ send, producer }) => {
			await send('in', 'v', { ...key('k'), codec: str, timestamp: 42n, headers: { h: Buffer.from('1') } })
			expect(producer.messages[0]).toMatchObject({ timestamp: 42n, headers: { h: Buffer.from('1') } })
		})
	})

	it('to() takes a topic name extractor and a partitioner', async () => {
		const driver = new TestDriver({ partitions: { 'out-b': 3 } })
		driver.input('in', { key: str, value: str }).to((_k, v) => `out-${v}`, {
			key: str,
			value: str,
			partitioner: (k, _v, count) => (k === 'last' ? count - 1 : 0),
		})
		await driver.run(async ({ send, output }) => {
			await send('in', 'a', { ...key('k'), codec: str })
			await send('in', 'b', { ...key('last'), codec: str })
			expect(output('out-a').map(r => r.partition)).toEqual([0])
			expect(output('out-b').map(r => r.partition)).toEqual([2])
		})
	})

	it('a topic name extractor gets the origin of the input record; a result that is no topic name fails the record', async () => {
		const driver = new TestDriver()
		const contexts: TopicNameExtractorContext[] = []
		driver.input('in', { key: str, value: str }).to(
			(_k, v, context) => {
				contexts.push(context)
				return v === 'none' ? '' : `out-${v}`
			},
			{ key: str, value: str }
		)
		await driver.run(async ({ send, output }) => {
			await send('in', 'a', { ...key('k'), codec: str, partition: 1, timestamp: 5 })
			expect(output('out-a', { key: str, value: str }).map(r => [r.key, r.value])).toEqual([['k', 'a']])
			const failure = send('in', 'none', { codec: str })
			await expect(failure).rejects.toThrow(FailedProcessingError)
			const error = (await failure.catch((e: unknown) => e)) as Error
			expect((error.cause as Error).message).toBe(
				"the topic name extractor of sink 'KSTREAM-SINK-0000000001' returned '', not a topic name"
			)
		})
		expect(contexts).toMatchObject([
			{ topic: 'in', partition: 1, offset: 0n, timestamp: 5 },
			{ topic: 'in', partition: 0, offset: 0n },
		])
		expect(Object.keys(contexts[0]!).sort()).toEqual(['headers', 'offset', 'partition', 'timestamp', 'topic'])
	})

	it("a topic name extractor gets the timestamp and headers of the record it routes, as Kafka Streams' RecordContext", async () => {
		const driver = new TestDriver({ initialWallClockTimeMs: 1_000 })
		const contexts: TopicNameExtractorContext[] = []
		let tick!: FixedKeyRecord<string | null, string | null>
		driver
			.input('in', { key: str, value: str })
			.processValues<string | null>(() => {
				let context!: FixedKeyProcessorContext<string | null, string | null>
				return {
					init: c => {
						context = c
						context.schedule(100, 'wall-clock', now => context.forward(tick.withTimestamp(now)))
					},
					process: record => {
						tick = record
						return context.forward(record.withHeaders({ tenant: Buffer.from('beta') }).withTimestamp(99))
					},
				}
			})
			.to(
				(_k, _v, context) => {
					contexts.push(context)
					return `orders-${context.headers.tenant ?? 'none'}`
				},
				{ key: str, value: str }
			)
		await driver.run(async ({ send, output, advanceWallClockTime }) => {
			await send('in', 'a', { ...key('k'), codec: str, timestamp: 5, headers: { tenant: Buffer.from('acme') } })
			await advanceWallClockTime(100)
			expect(output('orders-beta')).toHaveLength(1)
			expect(output('orders-acme')).toHaveLength(1)
		})
		expect(contexts).toEqual([
			{ topic: 'in', partition: 0, offset: 0n, timestamp: 99, headers: { tenant: Buffer.from('beta') } },
			{ topic: null, partition: -1, offset: -1n, timestamp: 1_100, headers: { tenant: Buffer.from('acme') } },
		])
	})
})

describe('repartitioning', () => {
	it('toTable() after a key change writes through a repartition topic and reads it back', async () => {
		const driver = new TestDriver()
		const seen: unknown[] = []
		driver
			.input('in', { key: str, value: str })
			.selectKey((_k, v) => `by-${v}`)
			.toTable({ key: str })
			.toStream()
			.peek((k, v) => seen.push([k, v]))
		await driver.run(async ({ send, output }) => {
			await send('in', 'x', { ...key('k'), codec: str })
			expect(output('test-app-repartition-0-repartition', { key: str, value: str })).toEqual([
				{ topic: 'test-app-repartition-0-repartition', key: 'by-x', value: 'x', partition: undefined },
			])
		})
		expect(seen).toEqual([['by-x', 'x']])
	})

	it('a repartition topic is read with failOnInvalid, never the default timestamp extractor (6.17)', async () => {
		type Order = { id: string; createdAt: number }
		const driver = new TestDriver({ defaultTimestampExtractor: record => (record.value as Order).createdAt })
		driver
			.input('orders', { key: str, value: codec.json<Order>() })
			// The default extractor would find no createdAt in the new values read back from the repartition topic.
			.map((_k, order) => [order!.id, { renamed: order!.id }] as const)
			.groupByKey({ key: str, value: codec.json() })
			.count()
			.toStream()
			.to('counts')
		await driver.run(async ({ sendAll, producer }) => {
			await sendAll('orders', [{ id: 'a', createdAt: 100 }], { timestamp: 5n })
			await sendAll('orders', [{ id: 'a', createdAt: 200 }], { timestamp: 6n })
			// The counts carry the timestamps the source extracted.
			expect(producer.messagesFor('counts').map(r => r.timestamp)).toEqual([100n, 200n])
		})
	})

	it('a named toTable() names its repartition topic', async () => {
		const driver = new TestDriver()
		driver
			.input('in', { key: str, value: str })
			.selectKey((_k, v) => v)
			.toTable({ name: 'by-value', key: str })
		await driver.run(async ({ send, output }) => {
			await send('in', 'x', { ...key('k'), codec: str })
			expect(output('test-app-by-value-repartition')).toHaveLength(1)
		})
	})

	it('a repartition topic drops records with a null key', async () => {
		const driver = new TestDriver()
		const seen: unknown[] = []
		driver
			.input('in', { key: str, value: str })
			.selectKey((_k, v) => (v === 'none' ? null : v))
			.toTable({ key: str })
			.toStream()
			.peek((k, v) => seen.push([k, v]))
		await driver.run(async ({ sendAll }) => {
			await sendAll('in', ['none', 'a'], { codec: str })
		})
		expect(seen).toEqual([['a', 'a']])
	})

	it('a key-based operation after a key change needs both codecs: a TopologyError names the operator', () => {
		const driver = new TestDriver()
		const rekeyed = driver.input('in', { key: str, value: str }).map((_k, v) => [v, v] as const)
		expect(() => rekeyed.toTable()).toThrow(TopologyError)
		expect(() => rekeyed.toTable()).toThrow('toTable() needs a key codec')
		expect(() => rekeyed.toTable({ key: str, value: str })).not.toThrow()
	})
})

describe('table sources', () => {
	type Stored = KeyValueStore<string, ValueAndTimestamp<string>>

	it('table() materializes its topic and forwards the new value of each update to toStream()', async () => {
		const driver = new TestDriver()
		const seen: unknown[] = []
		driver
			.table('users', { key: str, value: str, materialized: { storeName: 'users' } })
			.toStream()
			.peek((k, v) => seen.push([k, v]))
		await driver.run(async ({ send, getStateStore }) => {
			await send('users', 'Alice', { ...key('u1'), codec: str, timestamp: 5n })
			expect(await getStateStore<Stored>('users').get('u1')).toEqual({ value: 'Alice', timestamp: 5 })
			await send('users', null, key('u1'))
			expect(await getStateStore<Stored>('users').get('u1')).toBeUndefined()
		})
		expect(seen).toEqual([
			['u1', 'Alice'],
			['u1', null],
		])
	})

	it('B1: toStream() forwards new values only, never old values', async () => {
		const driver = new TestDriver()
		const seen: unknown[] = []
		driver
			.table('t', { key: str, value: str })
			.toStream()
			.peek((_k, v) => seen.push(v))
		await driver.run(async ({ send }) => {
			await send('t', 'a', { ...key('k'), codec: str })
			await send('t', 'b', { ...key('k'), codec: str })
		})
		expect(seen).toEqual(['a', 'b'])
	})

	it('B6: toTable(Materialized) codecs become the format of the table', async () => {
		const driver = new TestDriver()
		driver
			.input('in')
			.mapValues(v => v)
			.toTable({ key: codec.buffer(), value: codec.buffer() })
			.toStream()
			.to('out')
		await driver.run(async ({ send, output }) => {
			await send('in', Buffer.from('v'), key('k'))
			expect(output('out').map(r => r.value?.toString())).toEqual(['v'])
		})
	})

	it('table() drops records with a null key', async () => {
		const driver = new TestDriver()
		const seen: unknown[] = []
		driver
			.table('t', { key: str, value: str })
			.toStream()
			.peek((k, v) => seen.push([k, v]))
		await driver.run(async ({ send }) => {
			await send('t', 'no key', { codec: str })
			await send('t', 'keyed', { ...key('k'), codec: str })
		})
		expect(seen).toEqual([['k', 'keyed']])
	})

	it('toStream(mapper) re-keys the stream with (key, new value), so a key-based operation repartitions', async () => {
		const driver = new TestDriver()
		const mapped: unknown[] = []
		driver
			.table('t', { key: str, value: str })
			.toStream((k, v) => {
				mapped.push([k, v])
				return `${k}/${v}`
			})
			.toTable({ key: str, value: str, storeName: 'rekeyed' })
		await driver.run(async ({ send, output, getStateStore }) => {
			await send('t', 'v', { ...key('k'), codec: str })
			await send('t', null, key('k'))
			expect((await getStateStore<Stored>('rekeyed').get('k/v'))?.value).toBe('v')
			const repartitioned = output('test-app-repartition-0-repartition', { key: str, value: str })
			expect(repartitioned.map(r => [r.key, r.value])).toEqual([
				['k/v', 'v'],
				['k/null', null],
			])
		})
		expect(mapped).toEqual([
			['k', 'v'],
			['k', null],
		])
	})

	it('sends old values once downstream asks for them; its value getter reads the latest value and timestamp', async () => {
		const driver = new TestDriver()
		const table = driver.table('t', { key: str, value: str }) as KTableImpl<string, string>
		const changes: Change<string>[] = []
		const looked: unknown[] = []
		expect(table.table.enableSendingOldValues(false)).toBe(true)
		table.ctx.processor('CHANGES', undefined, [table.node], () => ({
			process: (record: StreamRecord<string, Change<string>>) => void changes.push(record.value),
		}))
		const lookups = driver.input('lookups', { key: str }) as KStreamImpl<string, Buffer>
		const getter = table.table.valueGetter
		lookups.ctx.processor(
			'LOOKUP',
			undefined,
			[lookups.node],
			() => {
				const values = getter.create()
				return {
					init: (context: ProcessorContext) =>
						values.init(context as InternalProcessorContext<unknown, unknown>),
					process: async (record: StreamRecord<string, Buffer>) =>
						void looked.push(await values.get(record.key)),
				}
			},
			getter.storeNames
		)
		await driver.run(async ({ send }) => {
			await send('t', 'a', { ...key('k'), codec: str, timestamp: 1n })
			await send('t', 'b', { ...key('k'), codec: str, timestamp: 2n })
			await send('lookups', Buffer.from(''), key('k'))
			await send('lookups', Buffer.from(''), key('missing'))
		})
		expect(changes).toEqual([
			{ newValue: 'a', oldValue: null },
			{ newValue: 'b', oldValue: 'a' },
		])
		expect(looked).toEqual([{ value: 'b', timestamp: 2 }, null])
	})

	it('globalTable() keeps the latest value of every key of its topic', async () => {
		const driver = new TestDriver()
		driver.input('events')
		driver.globalTable('users', { key: str, value: str, materialized: { storeName: 'all-users' } })
		await driver.run(async ({ send, getStateStore }) => {
			await send('users', 'Alice', { ...key('u1'), codec: str })
			await send('users', 'Bob', { ...key('u1'), codec: str })
			expect((await getStateStore<Stored>('all-users').get('u1'))?.value).toBe('Bob')
		})
	})
})
