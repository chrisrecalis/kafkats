import { describe, expect, it, vi } from 'vitest'
import { codec } from '@/codec.js'
import { listCodec } from '@/operators/list-value.js'
import { OuterJoinStore } from '@/operators/outer-join-store.js'
import { InMemoryKeyValueStore } from '@/state/memory.js'

const str = codec.string()
const num = codec.json<number>()
const hex = (...parts: string[]) => parts.join('')

/** The outer-join store of one task and its views from the left side (string values) and the right side (numbers). */
function outerStore() {
	const raw = new InMemoryKeyValueStore<Buffer, Buffer[]>('outer', {
		keyCodec: codec.buffer(),
		valueCodec: listCodec(codec.buffer()),
	})
	const left = new OuterJoinStore(raw, 'left', { key: str, value: str, otherValue: num })
	const right = new OuterJoinStore(raw, 'right', { key: str, value: num, otherValue: str })
	return { raw, left, right }
}

async function entries<V, VO>(store: OuterJoinStore<string, V, VO>) {
	const result: [number, string, boolean, readonly (V | VO)[]][] = []
	for await (const entry of store.pending()) {
		result.push([entry.timestamp, entry.key, entry.own, entry.values])
	}
	return result
}

describe('OuterJoinStore', () => {
	it('keys are [int64BE ts][u8 side: 1 left, 0 right][key bytes]; values list the values in arrival order', async () => {
		const { raw, left, right } = outerStore()
		await left.add('k', 256, 'v')
		await left.add('k', 256, 'w')
		await right.add('k', 256, 7)
		const stored: [string, string[]][] = []
		for await (const [key, values] of raw.range(Buffer.alloc(8), Buffer.from([0xff]))) {
			stored.push([key.toString('hex'), values.map(v => v.toString())])
		}
		expect(stored).toEqual([
			[hex('0000000000000100', '00', '6b'), ['7']],
			[hex('0000000000000100', '01', '6b'), ['v', 'w']],
		])
	})

	it('iterates by timestamp, then right before left, then key; each side sees its own entries as own', async () => {
		const { left, right } = outerStore()
		await left.add('b', 100, 'L1')
		await left.add('a', 100, 'L2')
		await right.add('c', 100, 1)
		await left.add('a', 50, 'L0')
		await left.add('a', 100, 'L3')
		expect(await entries(left)).toEqual([
			[50, 'a', true, ['L0']],
			[100, 'c', false, [1]],
			[100, 'a', true, ['L2', 'L3']],
			[100, 'b', true, ['L1']],
		])
		expect((await entries(right)).map(([timestamp, key, own]) => [timestamp, key, own])).toEqual([
			[50, 'a', false],
			[100, 'c', true],
			[100, 'a', false],
			[100, 'b', false],
		])
	})

	it("removeOther() deletes the other side's entry of a key and timestamp, and writes nothing when there is none", async () => {
		const { raw, left, right } = outerStore()
		await left.add('a', 100, 'L')
		await left.add('a', 200, 'M')
		const deletes = vi.spyOn(raw, 'delete')
		await right.removeOther('a', 100)
		await right.removeOther('a', 150)
		await left.removeOther('a', 200)
		expect(deletes).toHaveBeenCalledTimes(1)
		expect(await entries(left)).toEqual([[200, 'a', true, ['M']]])
	})

	it('delete() removes an entry while the scan goes on', async () => {
		const { left, right } = outerStore()
		await left.add('a', 100, 'L')
		await right.add('b', 150, 2)
		await left.add('c', 200, 'N')
		for await (const entry of left.pending()) {
			if (entry.timestamp < 200) await left.delete(entry)
		}
		expect(await entries(left)).toEqual([[200, 'c', true, ['N']]])
	})

	it("the sides of a task share one emit state per store; init() seeds its minTime with the oldest entry's timestamp", async () => {
		const { raw, left, right } = outerStore()
		expect(left.emitState).toBe(right.emitState)
		expect(left.emitState).toEqual({ minTime: Number.POSITIVE_INFINITY, nextEmit: 0 })
		await left.add('a', 300, 'L')
		await right.add('b', 200, 2)

		// A re-created task gets new store objects over the same entries, and a new emit state.
		const restarted = new InMemoryKeyValueStore<Buffer, Buffer[]>('outer', {
			keyCodec: codec.buffer(),
			valueCodec: listCodec(codec.buffer()),
		})
		for await (const [key, values] of raw.range(Buffer.alloc(8), Buffer.from([0xff]))) {
			await restarted.put(key, values)
		}
		const view = new OuterJoinStore(restarted, 'left', { key: str, value: str, otherValue: num })
		expect(view.emitState).not.toBe(left.emitState)
		await view.init()
		expect(view.emitState.minTime).toBe(200)
	})
})
