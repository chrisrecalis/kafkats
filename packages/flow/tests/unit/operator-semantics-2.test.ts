/** The store cases of the legacy operator-semantics-2 suite (its operator cases moved to tests/unit/dsl/*). */
import { describe, expect, it } from 'vitest'
import { codec } from '@/codec.js'
import { InMemoryKeyValueStore, InMemoryWindowStore } from '@/state/memory.js'

const str = codec.string()

describe('operator semantics 2: stores', () => {
	it('KV range is inclusive and ordered; prefix keys fine', async () => {
		const s = new InMemoryKeyValueStore<string, string>('s', { keyCodec: str, valueCodec: str })
		for (const k of ['b', 'a', 'ab', 'c']) await s.put(k, k)
		const out: string[] = []
		for await (const [k] of s.range('a', 'b')) out.push(k)
		expect(out).toEqual(['a', 'ab', 'b'])
	})

	it('window store fetch with prefix-overlapping keys does not leak', async () => {
		const s = new InMemoryWindowStore<string, string>('s', {
			keyCodec: str,
			valueCodec: str,
			retentionMs: 1000,
			windowSizeMs: 10,
		})
		await s.put({ key: 'a', windowStart: 0, windowEnd: 10 }, '1')
		await s.put({ key: 'ab', windowStart: 0, windowEnd: 10 }, '2')
		const out: string[] = []
		for await (const [, v] of s.fetch('a', 0, 10)) out.push(v)
		expect(out).toEqual(['1'])
	})
})
