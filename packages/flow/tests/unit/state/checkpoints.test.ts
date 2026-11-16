import { describe, expect, it } from 'vitest'
import { Checkpoints } from '@/state/checkpoints.js'
import { checkpointStore } from './support.js'

describe('Checkpoints', () => {
	it('uses the provider checkpoint store when there is one (durable)', async () => {
		const store = checkpointStore()
		const checkpoints = new Checkpoints(store, () => {})
		expect(checkpoints.durable).toBe(true)

		await checkpoints.set('changelog', 3, 10n)
		await checkpoints.flush()
		expect(store.values.get('changelog:3')).toBe(10n)
		expect(store.flushes).toBe(1)
		expect(await checkpoints.get('changelog', 3)).toBe(10n)
	})

	it('keeps checkpoints in process otherwise, until clearInProcess()', async () => {
		const checkpoints = new Checkpoints(undefined, () => {})
		expect(checkpoints.durable).toBe(false)
		expect(await checkpoints.get('changelog', 0)).toBeUndefined()

		await checkpoints.set('changelog', 0, 7n)
		await checkpoints.flush()
		expect(await checkpoints.get('changelog', 0)).toBe(7n)

		checkpoints.clearInProcess()
		expect(await checkpoints.get('changelog', 0)).toBeUndefined()
		// The floor went with it: the store it described is gone.
		await checkpoints.set('changelog', 0, 3n)
		expect(await checkpoints.get('changelog', 0)).toBe(3n)
	})

	it('W12: a checkpoint never moves back until it is invalidated', async () => {
		const store = checkpointStore()
		const checkpoints = new Checkpoints(store, () => {})
		await checkpoints.set('changelog', 0, 10n)
		await checkpoints.set('changelog', 0, 8n)
		expect(store.values.get('changelog:0')).toBe(10n)
		await checkpoints.set('changelog', 0, 12n)
		expect(store.values.get('changelog:0')).toBe(12n)
		// Floors are per changelog partition.
		await checkpoints.set('changelog', 1, 5n)
		expect(store.values.get('changelog:1')).toBe(5n)
	})

	it('invalidate() writes the -1 sentinel, flushes it and resets the floor', async () => {
		const store = checkpointStore()
		const checkpoints = new Checkpoints(store, () => {})
		await checkpoints.set('changelog', 0, 10n)

		await checkpoints.invalidate('changelog', 0)
		expect(store.values.get('changelog:0')).toBe(-1n)
		expect(store.flushes).toBe(1)

		// A rebuild starts over: its progress may be below the previous checkpoint.
		await checkpoints.set('changelog', 0, 4n)
		expect(store.values.get('changelog:0')).toBe(4n)
	})

	it('invalidates in-process checkpoints the same way', async () => {
		const checkpoints = new Checkpoints(undefined, () => {})
		await checkpoints.set('changelog', 0, 10n)
		await checkpoints.invalidate('changelog', 0)
		expect(await checkpoints.get('changelog', 0)).toBe(-1n)
	})
})
