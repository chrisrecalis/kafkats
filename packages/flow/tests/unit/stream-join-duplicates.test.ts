import { describe, it, expect } from 'vitest'

import { StreamStreamJoinNode } from '@/processors/joins/stream-stream.js'
import { Processor, type StreamRecord, type WorkerContext } from '@/processors/base.js'
import { InMemoryWindowStore, codec } from '../../src/index.js'

const stringValueCodec = codec.string()

class CollectorNode<K, V> extends Processor<K, V> {
	readonly records: Array<StreamRecord<K, V>> = []

	clone(worker: WorkerContext): Processor<K, V, K, V> {
		void worker
		return this
	}

	async process(record: StreamRecord<K, V>): Promise<void> {
		this.records.push(record)
	}
}

function record(key: string, value: string, ts: number, offset: number): StreamRecord<string, string> {
	return {
		key,
		value,
		timestamp: BigInt(ts),
		topic: 't',
		partition: 0,
		offset: BigInt(offset),
		headers: {},
	}
}

describe('Stream-stream join buffer retains duplicates', () => {
	it('joins BOTH records when two left records share key and timestamp (retainDuplicates)', async () => {
		const joinWindowMs = 5000
		const makeStore = (name: string) =>
			new InMemoryWindowStore<string, string>(name, {
				keyCodec: codec.string(),
				valueCodec: stringValueCodec,
				retentionMs: joinWindowMs * 2,
				windowSizeMs: joinWindowMs,
			})
		const leftStore = makeStore('left')
		const rightStore = makeStore('right')
		const leftRef = { store: leftStore }
		const rightRef = { store: rightStore }

		const leftNode = new StreamStreamJoinNode<string, string, string, string>(
			leftRef,
			rightRef,
			(l, r) => `${l}+${r}`,
			joinWindowMs
		)
		const rightNode = new StreamStreamJoinNode<string, string, string, string>(
			rightRef,
			leftRef,
			(r, l) => `${l}+${r}`,
			joinWindowMs
		)
		const results = new CollectorNode<string, string>()
		leftNode.connect(results)
		rightNode.connect(results)

		// Two distinct left records with EQUAL key AND timestamp. Kafka Streams stores join
		// records with retainDuplicates, so both must survive in the join buffer.
		await leftNode.process(record('k', 'v1', 1000, 0))
		await leftNode.process(record('k', 'v2', 1000, 1))

		// A right record within the window must join with BOTH buffered left records.
		await rightNode.process(record('k', 'r', 1500, 0))

		const values = results.records.map(r => r.value).sort()
		expect(values).toEqual(['v1+r', 'v2+r'])
	})

	it('retains duplicates on both sides symmetrically', async () => {
		const joinWindowMs = 5000
		const makeStore = (name: string) =>
			new InMemoryWindowStore<string, string>(name, {
				keyCodec: codec.string(),
				valueCodec: stringValueCodec,
				retentionMs: joinWindowMs * 2,
				windowSizeMs: joinWindowMs,
			})
		const leftRef = { store: makeStore('left') }
		const rightRef = { store: makeStore('right') }

		const leftNode = new StreamStreamJoinNode<string, string, string, string>(
			leftRef,
			rightRef,
			(l, r) => `${l}+${r}`,
			joinWindowMs
		)
		const rightNode = new StreamStreamJoinNode<string, string, string, string>(
			rightRef,
			leftRef,
			(r, l) => `${l}+${r}`,
			joinWindowMs
		)
		const results = new CollectorNode<string, string>()
		leftNode.connect(results)
		rightNode.connect(results)

		await rightNode.process(record('k', 'r1', 2000, 0))
		await rightNode.process(record('k', 'r2', 2000, 1))
		await leftNode.process(record('k', 'v', 2500, 0))

		const values = results.records.map(r => r.value).sort()
		expect(values).toEqual(['v+r1', 'v+r2'])
	})
})
