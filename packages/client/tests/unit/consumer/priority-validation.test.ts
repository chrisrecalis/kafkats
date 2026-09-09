import { describe, expect, expectTypeOf, it } from 'vitest'
import { strict, validatePriorityStrategy } from '@/consumer/priority.js'
import type { PriorityStrategy } from '@/consumer/priority.js'
import type { Consumer } from '@/consumer/consumer.js'

const assertConsumerPriorityTypes = (consumer: Consumer) => {
	void consumer.runEach(['live', 'bulk'] as const, async () => {}, { priority: strict({ order: ['live'] }) })
	// @ts-expect-error typo is not one of the literal subscription topics
	void consumer.runEach(['live', 'bulk'] as const, async () => {}, { priority: strict({ order: ['live', 'typo'] }) })
}

describe('priority strategy validation', () => {
	it('rejects referenced topics outside the subscription', () => {
		expect(() => validatePriorityStrategy(strict({ order: ['live', 'typo'] }), ['live', 'bulk'])).toThrow(
			'priority strategy references topic "typo" that is not in the subscription'
		)
	})

	it('requires a schedule function', () => {
		expect(() => validatePriorityStrategy({ schedule: null } as never, ['live'])).toThrow(
			'priority strategy schedule must be a function'
		)
	})

	it('keeps built-in topic names covariant while accepting custom strategies', () => {
		expectTypeOf(assertConsumerPriorityTypes).toBeFunction()
		const partial: PriorityStrategy<'live' | 'bulk'> = strict({ order: ['live'] })
		const custom: PriorityStrategy<'live'> = { schedule: () => ({ drain: [], fetch: [] }) }
		expectTypeOf(partial).toMatchTypeOf<PriorityStrategy<'live' | 'bulk'>>()
		expectTypeOf(custom).toMatchTypeOf<PriorityStrategy<'live'>>()

		// @ts-expect-error built-in strategy references a topic outside the target subscription
		const invalid: PriorityStrategy<'live' | 'bulk'> = strict({ order: ['live', 'typo'] })
		expect(invalid).toBeDefined()
	})
})
