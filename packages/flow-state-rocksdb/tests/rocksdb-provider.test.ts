import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { codec } from '@kafkats/flow'
import { rocksdb } from '@/rocksdb.js'

const str = codec.string()
const options = { keyCodec: str, valueCodec: str }

describe('RocksDB provider', () => {
	let stateDir: string
	beforeEach(() => {
		stateDir = mkdtempSync(join(tmpdir(), 'rocksdb-provider-'))
	})
	afterEach(() => {
		rmSync(stateDir, { recursive: true, force: true })
	})

	it('keeps stores apart, also when one name is a prefix of another', async () => {
		const provider = rocksdb({ stateDir })
		const a = provider.createKeyValueStore('app.counts.p1', options)
		const b = provider.createKeyValueStore('app.counts.p10', options)
		await a.put('k', 'a')
		await b.put('k', 'b')
		await b.put('z', 'b2')

		expect(await a.get('k')).toBe('a')
		expect(await b.get('k')).toBe('b')
		const all = async (store: typeof a) => {
			const entries: Array<[string, string]> = []
			for await (const entry of store.all()) entries.push(entry)
			return entries
		}
		expect(await all(a)).toEqual([['k', 'a']])
		expect(await all(b)).toEqual([
			['k', 'b'],
			['z', 'b2'],
		])
		await provider.close()
	})

	it('two providers on one directory never share a keyspace between different stores', async () => {
		const first = rocksdb({ stateDir })
		const second = rocksdb({ stateDir })
		// Both were opened before either created a store.
		const a = first.createKeyValueStore('a', options)
		const b = second.createKeyValueStore('b', options)
		const aAgain = second.createKeyValueStore('a', options)
		await a.put('k', 'from-a')
		await b.put('k', 'from-b')

		expect(await a.get('k')).toBe('from-a')
		expect(await b.get('k')).toBe('from-b')
		expect(await aAgain.get('k')).toBe('from-a')
		await first.close()
		await second.close()
	})

	it('finds stores and checkpoints again after close and reopen', async () => {
		const provider = rocksdb({ stateDir })
		await provider.createKeyValueStore('a', options).put('k', 'v')
		await provider.createKeyValueStore('b', options).put('k', 'w')
		const checkpoints = provider.getChangelogCheckpointStore!()!
		await checkpoints.set('changelog', 3, 42n)
		await checkpoints.flush?.()
		await provider.close()

		const reopened = rocksdb({ stateDir })
		expect(await reopened.createKeyValueStore('b', options).get('k')).toBe('w')
		expect(await reopened.createKeyValueStore('a', options).get('k')).toBe('v')
		expect(await reopened.createKeyValueStore('c', options).get('k')).toBeUndefined()
		expect(await reopened.getChangelogCheckpointStore!()!.get('changelog', 3)).toBe(42n)
		expect(await reopened.getChangelogCheckpointStore!()!.get('changelog', 4)).toBeUndefined()
		await reopened.close()
	})

	it('reports an empty store as empty and a non-empty one as non-empty', async () => {
		const provider = rocksdb({ stateDir })
		const store = provider.createKeyValueStore('a', options)
		const other = provider.createKeyValueStore('b', options)
		await other.put('k', 'v')
		expect(await store.approximateNumEntries()).toBe(0)
		await store.put('k', 'v')
		expect(await store.approximateNumEntries()).toBeGreaterThan(0)
		await store.delete('k')
		expect(await store.approximateNumEntries()).toBe(0)
		await provider.close()
	})
})
