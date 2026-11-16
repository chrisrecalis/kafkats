import { describe, expect, it } from 'vitest'
import { NodePrefix, StorePrefix, repartitionName, storeName } from '@/dsl/naming.js'
import { InternalTopologyBuilder } from '@/topology/builder.js'

describe('generated names', () => {
	it('names nodes KS style, with explicit names taking precedence', () => {
		const builder = new InternalTopologyBuilder('app')
		expect(builder.nodeName(NodePrefix.SOURCE)).toBe('KSTREAM-SOURCE-0000000000')
		expect(builder.nodeName(NodePrefix.TABLE_FILTER, 'active')).toBe('active')
		expect(builder.nodeName(NodePrefix.TABLE_FILTER)).toBe('KTABLE-FILTER-0000000001')
	})

	it("keeps today's store names: a generated name per store, counted apart from nodes", () => {
		const builder = new InternalTopologyBuilder('app')
		builder.nodeName(NodePrefix.SOURCE)
		expect(storeName(builder, StorePrefix.table('orders'))).toBe('table-orders-0')
		expect(storeName(builder, StorePrefix.COUNT, 'counts')).toBe('counts')
		expect(storeName(builder, StorePrefix.COUNT)).toBe('count-store-1')
		expect(storeName(builder, StorePrefix.globalTable('users'))).toBe('global-table-users-2')
	})

	it('names repartition topics repartition-<N> unless named', () => {
		const builder = new InternalTopologyBuilder('app')
		expect(repartitionName(builder)).toBe('repartition-0')
		expect(repartitionName(builder, 'by-user')).toBe('by-user')
		expect(builder.addRepartitionTopic(repartitionName(builder), {})).toBe('app-repartition-1-repartition')
	})
})
