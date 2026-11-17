import { describe, expect, it } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import type { StoreBuilder } from '@/topology/nodes.js'
import { builder, chain, globalStore, store } from './support.js'

type Kind = StoreBuilder['kind']

describe('state stores sharing a name', () => {
	const kinds: Kind[] = ['key-value', 'window', 'session']
	const mismatches = kinds.flatMap(existing =>
		kinds.filter(k => k !== existing).map(requested => [existing, requested])
	)

	it.each(mismatches)('asking for a %s store as a %s store throws', (existing, requested) => {
		const b = builder()
		b.addStateStore(store('shared', { kind: existing }))

		expect(() => b.addStateStore(store('shared', { kind: requested }))).toThrow(
			`State store 'shared' already exists as a ${existing} store and cannot be used as a ${requested} store`
		)
	})

	it.each(kinds)('B5: registering a %s store name again throws', kind => {
		const b = builder()
		b.addStateStore(store('shared', { kind }))

		expect(() => b.addStateStore(store('shared', { kind }))).toThrow(TopologyError)
		expect(() => b.addStateStore(store('shared', { kind }))).toThrow("State store 'shared' already exists")
	})

	it("asking for a global table's store as a window store throws", () => {
		const b = builder()
		b.addGlobalStore(globalStore('shared', 'lookup'))

		expect(() => b.addStateStore(store('shared', { kind: 'window' }))).toThrow(
			"State store 'shared' already exists as a key-value store and cannot be used as a window store"
		)
		expect(() => b.addStateStore(store('shared'))).toThrow(TopologyError)
	})

	it('a shared Processor API store registered again connects more processors', () => {
		const b = builder()
		const shared = store('shared')
		chain(b, 'a', ['a'], 'pa')
		chain(b, 'b', ['b'], 'pb')
		b.addStateStore(shared, ['pa'], { shared: true })
		b.addStateStore(shared, ['pb'], { shared: true })

		const [subtopology, ...others] = b.build().subtopologies
		expect(others).toEqual([])
		expect(subtopology?.stores).toEqual([shared])
	})

	it('a shared name with a different store, or registered unshared, throws', () => {
		const b = builder()
		b.addStateStore(store('shared'), [], { shared: true })

		expect(() => b.addStateStore(store('shared'), [], { shared: true })).toThrow(TopologyError)
		expect(() => b.addStateStore(store('shared'))).toThrow(TopologyError)
	})
})
