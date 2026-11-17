/** StartPositions (4.7.4): 'latest' pins, the start-position commit, the settled positions. */
import { describe, expect, it } from 'vitest'
import type { TopicPartition } from '@kafkats/client'
import { decodeCommitMetadata } from '@/processor/metadata.js'
import type { CommittedOffset } from '@/runtime/io.js'
import { StartPositions } from '@/runtime/start-positions.js'
import type { ResetPolicy } from '@/topology/nodes.js'
import { FakeGroupConsumer } from '../support/fake-io.js'
import { RecordingLogger } from '../support/task-harness.js'

/** `latest` and `earliest` read from those policies; `rep` is a repartition topic ('earliest'). */
const POLICIES: Record<string, ResetPolicy> = { latest: 'latest', earliest: 'earliest', rep: 'earliest' }

/** A task's input partitions and their starts: `'latest:0': 5n` resolved at 5 without a commit, or a commit. */
function task(starts: Record<string, bigint | { committed: CommittedOffset; resolved: bigint }>) {
	const inputPartitions: TopicPartition[] = Object.keys(starts).map(key => {
		const [topic, partition] = key.split(':')
		return { topic: topic!, partition: Number(partition) }
	})
	return {
		inputPartitions,
		starts: new Map(
			Object.entries(starts).map(([key, start]) => [
				key,
				typeof start === 'bigint' ? { committed: null, resolved: start } : start,
			])
		),
	}
}

function harness() {
	const logger = new RecordingLogger()
	const positions = new StartPositions({ policy: topic => POLICIES[topic]!, logger })
	const consumer = new FakeGroupConsumer()
	const settle = (t: ReturnType<typeof task>, signal = AbortSignal.timeout(5000), on = consumer) =>
		positions.settle(t, on, signal)
	return { logger, consumer, settle }
}

describe('StartPositions', () => {
	it("returns the client's positions of uncommitted 'earliest' partitions; no seek, no commit", async () => {
		const { consumer, settle } = harness()
		const committed = { committed: { offset: 7n, metadata: null }, resolved: 7n }
		const settled = await settle(task({ 'earliest:0': 3n, 'rep:1': 0n, 'earliest:1': committed }))
		expect(settled).toEqual(
			new Map([
				['earliest:0', 3n],
				['rep:1', 0n],
			])
		)
		expect(consumer.calls).toEqual([])
	})

	it("pins a 'latest' partition at its first position and commits it with empty commit metadata", async () => {
		const { consumer, settle } = harness()
		const settled = await settle(task({ 'latest:0': 5n, 'earliest:0': 2n }))
		expect(settled).toEqual(
			new Map([
				['latest:0', 5n],
				['earliest:0', 2n],
			])
		)
		expect(consumer.calls).toEqual(['commit latest:0@5'])
		const [commit] = consumer.commits[0]!
		expect(decodeCommitMetadata(commit!.metadata)).toEqual({ partitionTime: -1, metadata: new Map() })
	})

	it('a later assignment without a commit, on another consumer too, goes back to the pin', async () => {
		const { consumer, settle } = harness()
		await settle(task({ 'latest:0': 5n }))
		await settle(task({ 'latest:0': 5n }))
		expect(consumer.calls).toEqual(['commit latest:0@5', 'commit latest:0@5'])

		// A new run (recovery) whose client resolved 'latest' to a newer end: back to where this process started.
		const next = new FakeGroupConsumer()
		const settled = await settle(task({ 'latest:0': 9n }), undefined, next)
		expect(settled.get('latest:0')).toBe(5n)
		expect(next.calls).toEqual(['seek latest:0 5', 'commit latest:0@5'])
	})

	it('a commit ends the pin: a partition without a commit later is pinned afresh', async () => {
		const { consumer, settle } = harness()
		await settle(task({ 'latest:0': 5n }))
		await settle(task({ 'latest:0': { committed: { offset: 8n, metadata: null }, resolved: 8n } }))
		const settled = await settle(task({ 'latest:0': 12n }))
		expect(settled.get('latest:0')).toBe(12n)
		expect(consumer.calls).toEqual(['commit latest:0@5', 'commit latest:0@12'])
	})

	it('a failed commit is logged; the pin keeps the position in this process', async () => {
		const { consumer, logger, settle } = harness()
		consumer.steps.commit.next(new Error('coordinator not available'))
		expect((await settle(task({ 'latest:0': 5n }))).get('latest:0')).toBe(5n)
		expect(logger.warnings).toEqual(['committing start positions failed: they are kept in this process only'])

		expect((await settle(task({ 'latest:0': 9n }))).get('latest:0')).toBe(5n)
		expect(consumer.calls).toEqual(['commit latest:0@5', 'seek latest:0 5', 'commit latest:0@5'])
	})

	it('a commit that hangs is given up when the signal aborts (the 5 s cap of the pipeline, CR8)', async () => {
		const { consumer, logger, settle } = harness()
		consumer.steps.commit.next('hang')
		const abort = new AbortController()
		const settling = settle(task({ 'latest:0': 5n, 'earliest:0': 1n }), abort.signal)
		abort.abort()
		expect(await settling).toEqual(
			new Map([
				['latest:0', 5n],
				['earliest:0', 1n],
			])
		)
		expect(logger.warnings).toHaveLength(1)
	})

	it('does nothing once its signal aborted (the pipeline was stopped)', async () => {
		const { consumer, settle } = harness()
		await settle(task({ 'latest:0': 5n }))
		const abort = new AbortController()
		abort.abort()
		await expect(settle(task({ 'latest:0': 9n }), abort.signal)).rejects.toThrow()
		expect(consumer.calls).toEqual(['commit latest:0@5'])
	})

	it('keys pins by topic and partition', async () => {
		const { consumer, settle } = harness()
		await settle(task({ 'latest:0': 5n, 'latest:1': 6n }))
		await settle(task({ 'latest:1': 10n }))
		expect(consumer.calls).toEqual(['commit latest:0@5 latest:1@6', 'seek latest:1 6', 'commit latest:1@6'])
	})
})
