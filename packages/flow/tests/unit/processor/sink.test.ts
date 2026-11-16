import { describe, expect, it, vi } from 'vitest'
import { FailedProcessingError, StreamsError } from '@/api/errors.js'
import type { ProductionExceptionHandler } from '@/api/handlers.js'
import type { StreamPartitioner } from '@/topology/nodes.js'
import { json, string } from '@/codec.js'
import { type HarnessOptions, ProcessorHarness, sinkDef, sourceDef } from '../support/processor-harness.js'
import type { SinkDef } from '@/topology/nodes.js'

async function sinkTo(topic: SinkDef['topic'], extra: Partial<SinkDef> = {}, options: HarnessOptions = {}) {
	return ProcessorHarness.start([sourceDef('src', ['in']), sinkDef('sink', ['src'], topic, extra)], options)
}

describe('SinkNode', () => {
	it('encodes with its codecs and sends the record timestamp and headers', async () => {
		const harness = await sinkTo('out', { keyCodec: string(), valueCodec: json() })
		const headers = { trace: Buffer.from('t') }
		await harness.process('in', { key: 'k', value: { n: 1 }, timestamp: 0, headers })
		await harness.process('in', { key: 'k', value: null, timestamp: 42 })
		expect(harness.sent).toEqual([
			{
				kind: 'sink',
				record: { topic: 'out', key: Buffer.from('k'), value: Buffer.from('{"n":1}'), timestamp: 0, headers },
			},
			{ kind: 'sink', record: { topic: 'out', key: Buffer.from('k'), value: null, timestamp: 42, headers: {} } },
		])
	})

	it('passes Buffers through without codecs and sends null for null or undefined', async () => {
		const harness = await sinkTo('out')
		await harness.process('in', { key: Buffer.from('k'), value: Buffer.from('v'), timestamp: 1 })
		await harness.process('in', { key: null, value: undefined, timestamp: 2 })
		expect(harness.output('out').map(record => [record.key, record.value])).toEqual([
			[Buffer.from('k'), Buffer.from('v')],
			[null, null],
		])
	})

	it('repartition sinks drop null keys silently unless a relaxed join is downstream', async () => {
		const dropping = await sinkTo('rep', { internal: 'repartition', nullKeys: 'drop' })
		const unmarked = await sinkTo('rep', { internal: 'repartition' })
		const keeping = await sinkTo('rep', { internal: 'repartition', nullKeys: 'keep' })
		const plain = await sinkTo('out')
		for (const harness of [dropping, unmarked, keeping, plain]) {
			await harness.process('in', { key: null, value: Buffer.from('v'), timestamp: 1 })
			await harness.process('in', { key: Buffer.from('k'), value: Buffer.from('v'), timestamp: 2 })
		}
		expect(dropping.sent.map(sent => sent.kind)).toEqual(['repartition'])
		expect(unmarked.sent).toHaveLength(1)
		expect(keeping.sent).toHaveLength(2)
		expect(plain.sent.map(sent => sent.kind)).toEqual(['sink', 'sink'])
		expect(dropping.task.droppedCount('null-key')).toBe(0)
	})

	it('picks the topic with a topic name extractor', async () => {
		const extractor = vi.fn((key: unknown) => `out-${String(key)}`)
		const harness = await sinkTo(extractor, { keyCodec: string(), valueCodec: string() })
		await harness.process('in', { key: 'a', value: 'v', timestamp: 1, offset: 3n })
		expect(harness.output('out-a')).toHaveLength(1)
		expect(extractor).toHaveBeenCalledWith(
			'a',
			'v',
			expect.objectContaining({ topic: 'in', partition: 0, offset: 3n })
		)
	})
})

describe('SinkNode partitioner', () => {
	const codecs = { keyCodec: string(), valueCodec: string() }

	it('sends to the partition it picks, or lets the producer pick on undefined', async () => {
		const partitioner = vi.fn<StreamPartitioner<unknown, unknown>>((_, key) => (key === 'a' ? 2 : undefined))
		const harness = await sinkTo('out', { ...codecs, partitioner }, { output: { partitionCount: () => 3 } })
		await harness.process('in', { key: 'a', value: 'v', timestamp: 1 })
		await harness.process('in', { key: 'b', value: 'v', timestamp: 1 })
		expect(harness.output('out').map(record => record.partition)).toEqual([2, undefined])
		expect('partition' in harness.output('out')[1]!).toBe(false)
		expect(partitioner).toHaveBeenCalledWith('out', 'a', 'v', 3)
	})

	it('loads the partition count on a cache miss', async () => {
		const loadPartitionCount = vi.fn(async () => 4)
		const harness = await sinkTo(
			'out',
			{ ...codecs, partitioner: (_topic, _key, _value, count) => count - 1 },
			{ output: { partitionCount: () => undefined, loadPartitionCount } }
		)
		await harness.process('in', { key: 'a', value: 'v', timestamp: 1 })
		expect(harness.output('out').map(record => record.partition)).toEqual([3])
		expect(loadPartitionCount).toHaveBeenCalledWith('out')
	})

	it.each([3, 1.5, -1, Number.NaN])(
		'a partition %s outside [0, 3) is a StreamsError for the handler',
		async partition => {
			const options = { output: { partitionCount: () => 3 } }
			const failing = await sinkTo('out', { ...codecs, partitioner: () => partition }, options)
			const error = await failing.process('in', { key: 'a', value: 'v', timestamp: 1 }).catch((e: unknown) => e)
			expect(error).toBeInstanceOf(FailedProcessingError)
			expect((error as Error).cause).toBeInstanceOf(StreamsError)
			expect(failing.sent).toHaveLength(0)

			const continuing = await sinkTo(
				'out',
				{ ...codecs, partitioner: () => partition },
				{ ...options, processing: () => 'continue' }
			)
			await continuing.process('in', { key: 'a', value: 'v', timestamp: 1 })
			expect(continuing.sent).toHaveLength(0)
			expect(continuing.task.droppedCount('processing')).toBe(1)
		}
	)
})

describe('SinkNode serialization errors', () => {
	const failingCodec = {
		encode: () => {
			throw new Error('cannot encode')
		},
		decode: () => null,
	}

	it("go to the production handler with origin 'serialization'; 'continue' drops the record", async () => {
		const production = vi.fn<ProductionExceptionHandler>(() => 'continue')
		const harness = await sinkTo('out', { keyCodec: string(), valueCodec: failingCodec }, { production })
		await harness.process('in', { key: 'k', value: 'v', timestamp: 7, offset: 2n })
		expect(harness.sent).toHaveLength(0)
		expect(harness.task.droppedCount('production')).toBe(1)
		const [ctx, record, error, origin] = production.mock.calls[0]!
		expect(origin).toBe('serialization')
		expect((error as Error).message).toBe('cannot encode')
		expect(record).toEqual({ topic: 'out', key: Buffer.from('k'), value: null, timestamp: 7, headers: {} })
		expect(ctx).toMatchObject({ topic: 'in', offset: 2n, processorNodeId: 'sink', timestamp: 7 })
	})

	it("'fail', or a non-Buffer without a codec under 'fail', is fatal", async () => {
		const failing = await sinkTo('out', { valueCodec: failingCodec })
		await expect(failing.process('in', { key: null, value: 'v', timestamp: 1 })).rejects.toMatchObject({
			name: 'FailedProcessingError',
			cause: expect.objectContaining({ message: 'cannot encode' }),
		})
		const noCodec = await sinkTo('out')
		await expect(noCodec.process('in', { key: 'not a buffer', value: null, timestamp: 1 })).rejects.toMatchObject({
			name: 'FailedProcessingError',
			cause: expect.any(TypeError),
		})
	})
})
