import { describe, test, expect, beforeEach } from 'vitest'
import { PartitionTracker } from '../../../src/consumer/partition-tracker.js'

describe('PartitionTracker', () => {
	let tracker: PartitionTracker

	beforeEach(() => {
		tracker = new PartitionTracker()
	})

	describe('assign', () => {
		test('should track assigned partitions', () => {
			tracker.assign([
				{ topic: 'topic1', partition: 0 },
				{ topic: 'topic1', partition: 1 },
			])

			expect(tracker.isAssigned('topic1', 0)).toBe(true)
			expect(tracker.isAssigned('topic1', 1)).toBe(true)
			expect(tracker.isAssigned('topic1', 2)).toBe(false)
			expect(tracker.isAssigned('topic2', 0)).toBe(false)
		})

		test('should not assign partition that is being revoked', async () => {
			tracker.assign([{ topic: 'topic1', partition: 0 }])

			// Start processing
			tracker.startProcessing('topic1', 0)

			// Start revoke (will be pending since partition is processing)
			const revokePromise = tracker.revoke([{ topic: 'topic1', partition: 0 }])

			// Try to assign the same partition while it's being revoked
			tracker.assign([{ topic: 'topic1', partition: 0 }])

			// Partition should not be assigned because it's being revoked
			expect(tracker.isAssigned('topic1', 0)).toBe(false)

			// Complete revoke
			tracker.endProcessing('topic1', 0)
			await revokePromise

			// Now it can be assigned
			tracker.assign([{ topic: 'topic1', partition: 0 }])
			expect(tracker.isAssigned('topic1', 0)).toBe(true)
		})
	})

	describe('revoke', () => {
		test('should remove partitions from assigned set immediately', async () => {
			tracker.assign([
				{ topic: 'topic1', partition: 0 },
				{ topic: 'topic1', partition: 1 },
			])

			await tracker.revoke([{ topic: 'topic1', partition: 0 }])

			expect(tracker.isAssigned('topic1', 0)).toBe(false)
			expect(tracker.isAssigned('topic1', 1)).toBe(true)
		})

		test('should wait for in-flight processing to complete', async () => {
			tracker.assign([{ topic: 'topic1', partition: 0 }])

			// Start processing
			expect(tracker.startProcessing('topic1', 0)).toBe(true)
			expect(tracker.isProcessing('topic1', 0)).toBe(true)

			let revokeCompleted = false
			const revokePromise = tracker.revoke([{ topic: 'topic1', partition: 0 }]).then(() => {
				revokeCompleted = true
			})

			// Revoke should not complete yet
			await new Promise(resolve => setTimeout(resolve, 10))
			expect(revokeCompleted).toBe(false)

			// End processing
			tracker.endProcessing('topic1', 0)

			// Now revoke should complete
			await revokePromise
			expect(revokeCompleted).toBe(true)
		})

		test('should handle multiple partitions with mixed in-flight state', async () => {
			tracker.assign([
				{ topic: 'topic1', partition: 0 },
				{ topic: 'topic1', partition: 1 },
				{ topic: 'topic1', partition: 2 },
			])

			// Only partition 1 is processing
			tracker.startProcessing('topic1', 1)

			let revokeCompleted = false
			const revokePromise = tracker
				.revoke([
					{ topic: 'topic1', partition: 0 },
					{ topic: 'topic1', partition: 1 },
				])
				.then(() => {
					revokeCompleted = true
				})

			// Revoke should not complete yet (partition 1 is processing)
			await new Promise(resolve => setTimeout(resolve, 10))
			expect(revokeCompleted).toBe(false)

			// Partition 0 should already be removed from assigned
			expect(tracker.isAssigned('topic1', 0)).toBe(false)
			// Partition 1 should also be removed from assigned
			expect(tracker.isAssigned('topic1', 1)).toBe(false)
			// Partition 2 should still be assigned
			expect(tracker.isAssigned('topic1', 2)).toBe(true)

			// End processing for partition 1
			tracker.endProcessing('topic1', 1)

			// Now revoke should complete
			await revokePromise
			expect(revokeCompleted).toBe(true)
		})
	})

	describe('startProcessing', () => {
		test('should return true for assigned partition', () => {
			tracker.assign([{ topic: 'topic1', partition: 0 }])
			expect(tracker.startProcessing('topic1', 0)).toBe(true)
		})

		test('should return false for unassigned partition', () => {
			expect(tracker.startProcessing('topic1', 0)).toBe(false)
		})

		test('should return false for partition being revoked', async () => {
			tracker.assign([{ topic: 'topic1', partition: 0 }])
			tracker.startProcessing('topic1', 0)

			// Start revoke
			const revokePromise = tracker.revoke([{ topic: 'topic1', partition: 0 }])

			// New startProcessing should return false
			expect(tracker.startProcessing('topic1', 0)).toBe(false)

			tracker.endProcessing('topic1', 0)
			await revokePromise
		})
	})

	describe('endProcessing', () => {
		test('should mark partition as no longer processing', () => {
			tracker.assign([{ topic: 'topic1', partition: 0 }])
			tracker.startProcessing('topic1', 0)
			expect(tracker.isProcessing('topic1', 0)).toBe(true)

			tracker.endProcessing('topic1', 0)
			expect(tracker.isProcessing('topic1', 0)).toBe(false)
		})

		test('should be safe to call for non-processing partition', () => {
			tracker.assign([{ topic: 'topic1', partition: 0 }])
			// Should not throw
			tracker.endProcessing('topic1', 0)
		})
	})

	describe('getAssigned', () => {
		test('should return all assigned partitions', () => {
			tracker.assign([
				{ topic: 'topic1', partition: 0 },
				{ topic: 'topic1', partition: 1 },
				{ topic: 'topic2', partition: 0 },
			])

			const assigned = tracker.getAssigned()
			expect(assigned).toHaveLength(3)
			expect(assigned).toContainEqual({ topic: 'topic1', partition: 0 })
			expect(assigned).toContainEqual({ topic: 'topic1', partition: 1 })
			expect(assigned).toContainEqual({ topic: 'topic2', partition: 0 })
		})

		test('should exclude partitions being revoked', async () => {
			tracker.assign([
				{ topic: 'topic1', partition: 0 },
				{ topic: 'topic1', partition: 1 },
			])
			tracker.startProcessing('topic1', 0)

			const revokePromise = tracker.revoke([{ topic: 'topic1', partition: 0 }])

			const assigned = tracker.getAssigned()
			expect(assigned).toHaveLength(1)
			expect(assigned).toContainEqual({ topic: 'topic1', partition: 1 })

			tracker.endProcessing('topic1', 0)
			await revokePromise
		})
	})

	describe('clear', () => {
		test('should clear all state', async () => {
			tracker.assign([
				{ topic: 'topic1', partition: 0 },
				{ topic: 'topic1', partition: 1 },
			])
			tracker.startProcessing('topic1', 0)

			tracker.clear()

			expect(tracker.isAssigned('topic1', 0)).toBe(false)
			expect(tracker.isAssigned('topic1', 1)).toBe(false)
			expect(tracker.isProcessing('topic1', 0)).toBe(false)
			expect(tracker.getAssigned()).toHaveLength(0)
		})

		test('should resolve pending revoke waits', async () => {
			tracker.assign([{ topic: 'topic1', partition: 0 }])
			tracker.startProcessing('topic1', 0)

			let revokeCompleted = false
			const revokePromise = tracker.revoke([{ topic: 'topic1', partition: 0 }]).then(() => {
				revokeCompleted = true
			})

			// Clear should resolve the pending revoke
			tracker.clear()

			await revokePromise
			expect(revokeCompleted).toBe(true)
		})
	})

	describe('getState', () => {
		test('should return current state for diagnostics', () => {
			tracker.assign([
				{ topic: 'topic1', partition: 0 },
				{ topic: 'topic1', partition: 1 },
			])
			tracker.startProcessing('topic1', 0)

			const state = tracker.getState()
			expect(state.assigned).toHaveLength(2)
			expect(state.assigned).toContain('topic1:0')
			expect(state.assigned).toContain('topic1:1')
			expect(state.inFlight).toHaveLength(1)
			expect(state.inFlight).toContain('topic1:0')
			expect(state.revoking).toHaveLength(0)
		})
	})

	describe('revokeTimeoutMs', () => {
		test('should timeout if handler does not complete', async () => {
			const trackerWithTimeout = new PartitionTracker({ revokeTimeoutMs: 50 })

			trackerWithTimeout.assign([{ topic: 'topic1', partition: 0 }])
			trackerWithTimeout.startProcessing('topic1', 0)

			const start = Date.now()
			await trackerWithTimeout.revoke([{ topic: 'topic1', partition: 0 }])
			const elapsed = Date.now() - start

			// Should have timed out around 50ms
			expect(elapsed).toBeGreaterThanOrEqual(40)
			expect(elapsed).toBeLessThan(200)
		})
	})
})
