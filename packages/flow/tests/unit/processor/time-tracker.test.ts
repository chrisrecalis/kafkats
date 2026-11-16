import { describe, expect, it } from 'vitest'
import type { InternalProcessorContext, Processor } from '@/processor/api.js'
import { ProcessorMetadataImpl } from '@/processor/metadata.js'
import { CLEANUP_INTERVAL_MS, ObservedStreamTime } from '@/processor/time-tracker.js'
import { ProcessorHarness, processorDef, sourceDef } from '../support/processor-harness.js'

function seeded(entries: Record<string, number>): ProcessorMetadataImpl {
	const metadata = new ProcessorMetadataImpl()
	metadata.replaceAll([new Map(Object.entries(entries))], new Set(Object.keys(entries)))
	return metadata
}

describe('ObservedStreamTime', () => {
	it('starts from the seeded metadata, -1 when unknown', () => {
		const metadata = seeded({ store: 100 })
		expect(new ObservedStreamTime({ metadata }, 'store').get()).toBe(100)
		expect(new ObservedStreamTime({ metadata }, 'other').get()).toBe(-1)
	})

	it('only an increase is written to the metadata and needs a commit', () => {
		const metadata = seeded({ store: 100 })
		const clock = new ObservedStreamTime({ metadata }, 'store')
		expect(clock.advance(50)).toBe(100)
		expect(clock.advance(100)).toBe(100)
		expect(metadata.needsCommit).toBe(false)
		expect(clock.advance(150)).toBe(150)
		expect(metadata.get('store')).toBe(150)
		expect(metadata.needsCommit).toBe(true)
	})

	it('clocks with the same key share their time (the two sides of a join)', () => {
		const metadata = new ProcessorMetadataImpl()
		const left = new ObservedStreamTime({ metadata }, 'left-store')
		const right = new ObservedStreamTime({ metadata }, 'left-store')
		left.advance(10)
		expect(right.get()).toBe(10)
		right.advance(20)
		expect(left.get()).toBe(20)
	})

	it('expiry is due once the clock moved more than CLEANUP_INTERVAL_MS past the last expiry', () => {
		const clock = new ObservedStreamTime({ metadata: new ProcessorMetadataImpl() }, 'store')
		expect(clock.expiryDue()).toBe(false)
		clock.advance(CLEANUP_INTERVAL_MS)
		expect(clock.expiryDue()).toBe(false)
		clock.advance(CLEANUP_INTERVAL_MS + 1)
		expect(clock.expiryDue()).toBe(true)
		expect(clock.expiryDue()).toBe(false)
		clock.advance(2 * CLEANUP_INTERVAL_MS + 1)
		expect(clock.expiryDue()).toBe(false)
		clock.advance(2 * CLEANUP_INTERVAL_MS + 2)
		expect(clock.expiryDue()).toBe(true)
	})

	it('a processor clock is seeded from the task metadata and persisted into it', async () => {
		const seen: number[] = []
		const supplier = (): Processor<string, string> => {
			let clock: ObservedStreamTime
			return {
				init: context => {
					clock = new ObservedStreamTime(context as InternalProcessorContext, 'agg-store')
				},
				process: record => {
					seen.push(clock.get())
					clock.advance(record.timestamp)
				},
			}
		}
		const harness = await ProcessorHarness.start(
			[sourceDef('src', ['in']), processorDef('agg', ['src'], supplier)],
			{
				metadata: { 'agg-store': 500 },
			}
		)
		await harness.process('in', { key: 'k', value: 'a', timestamp: 100 })
		expect(harness.metadata.needsCommit).toBe(false)
		await harness.process('in', { key: 'k', value: 'b', timestamp: 700 })
		expect(seen).toEqual([500, 500])
		expect(harness.metadata.get('agg-store')).toBe(700)
		expect(harness.metadata.needsCommit).toBe(true)
	})
})
