import { describe, expect, it } from 'vitest'
import { FailedProcessingError, TopologyError } from '@/api/errors.js'
import { codec } from '@/codec.js'
import type { KeyValueStore, ValueAndTimestamp } from '@/state.js'
import { TestDriver } from '@/testing/driver.js'

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
		await driver.run(async ({ send, output }) => {
			await send('in', 'v', { ...key('k'), codec: str })
			const values = (topic: string) => output(topic, { value: str }).map(r => r.value)
			expect(values('value-only')).toEqual(['v!'])
			expect(values('with-key')).toEqual(['k=v'])
			expect(values('flat')).toEqual(['k', 'v'])
		})
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

	it('toStream(mapper) re-keys the stream, so a key-based operation repartitions', async () => {
		const driver = new TestDriver()
		driver
			.table('t', { key: str, value: str })
			.toStream((k, v) => `${k}/${v}`)
			.toTable({ key: str, value: str, storeName: 'rekeyed' })
		await driver.run(async ({ send, getStateStore }) => {
			await send('t', 'v', { ...key('k'), codec: str })
			expect((await getStateStore<Stored>('rekeyed').get('k/v'))?.value).toBe('v')
		})
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
