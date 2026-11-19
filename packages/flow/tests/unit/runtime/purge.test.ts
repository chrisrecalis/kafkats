/** RepartitionPurger (4.8.6): DeleteRecords after commits, once per interval, one request at a time, errors logged once. */
import { describe, expect, it, vi } from 'vitest'
import type { OffsetAndMetadata } from '@/runtime/io.js'
import { RepartitionPurger } from '@/runtime/purge.js'
import { FakeAdmin, FakeCluster } from '../support/fake-io.js'
import { ManualClock } from '../support/manual-clock.js'
import { RecordingLogger } from '../support/task-harness.js'

const REPARTITION = 'app-r-repartition'

function harness(intervalMs = 1000) {
	const cluster = new FakeCluster()
	cluster.createTopic(REPARTITION, 2)
	cluster.createTopic('in', 1)
	for (const partition of [0, 1]) {
		cluster.append(REPARTITION, partition, ...Array.from({ length: 10 }, (_, i) => ({ key: `k${i}`, value: 'v' })))
	}
	const admin = new FakeAdmin(cluster)
	const clock = new ManualClock(0)
	const logger = new RecordingLogger()
	const hardStop = new AbortController()
	const purger = new RepartitionPurger({
		admin,
		topics: new Set([REPARTITION]),
		intervalMs,
		clock,
		signal: hardStop.signal,
		logger,
	})
	/** A commit of these `topic:partition@offset` at `time`. */
	const commit = (time: number, ...offsets: string[]) => {
		clock.advance(time - clock.time)
		purger.noteCommitted(offsets.map(parse))
	}
	return { cluster, admin, clock, logger, hardStop, commit }
}

function parse(spec: string): OffsetAndMetadata {
	const [tp, offset] = spec.split('@')
	const [topic, partition] = tp!.split(':')
	return { topic: topic!, partition: Number(partition), offset: BigInt(offset!), metadata: '' }
}

describe('RepartitionPurger', () => {
	it('deletes the records of repartition topics below the last committed offsets, at most once per interval', async () => {
		const { admin, cluster, commit } = harness()
		commit(500, `${REPARTITION}:0@2`, 'in:0@4')
		expect(admin.calls).toEqual([])

		commit(1000, `${REPARTITION}:0@3`, `${REPARTITION}:1@5`, 'in:0@6')
		expect(admin.calls).toEqual([`deleteRecords ${REPARTITION}:0@3 ${REPARTITION}:1@5`])
		await vi.waitFor(() => expect(cluster.log(REPARTITION, 1).start).toBe(5n))
		expect(cluster.log(REPARTITION, 0).start).toBe(3n)

		// The next purge sends only what was committed since, one interval later.
		commit(1500, `${REPARTITION}:0@7`)
		commit(2000)
		expect(admin.calls).toHaveLength(2)
		expect(admin.calls[1]).toBe(`deleteRecords ${REPARTITION}:0@7`)
		await vi.waitFor(() => expect(cluster.log(REPARTITION, 0).start).toBe(7n))
	})

	it('sends no request without a repartition offset', () => {
		const { admin, commit } = harness()
		commit(5000, 'in:0@6')
		commit(10_000)
		expect(admin.calls).toEqual([])
	})

	it('keeps one request in flight; the commits meanwhile go with the next one', async () => {
		const { admin, commit } = harness()
		admin.steps.deleteRecords.next('hang')
		commit(1000, `${REPARTITION}:0@3`)
		commit(2000, `${REPARTITION}:0@4`)
		expect(admin.calls).toHaveLength(1)

		admin.steps.deleteRecords.answerAll()
		await vi.waitFor(() => {
			commit(3000)
			expect(admin.calls).toEqual([`deleteRecords ${REPARTITION}:0@3`, `deleteRecords ${REPARTITION}:0@4`])
		})
	})

	it('after a failure the next purge sends its offsets again, unless newer ones were committed; logs each message once', async () => {
		const { admin, logger, commit } = harness()
		const denied = new Error('TOPIC_AUTHORIZATION_FAILED')
		admin.steps.deleteRecords.always(denied)
		commit(1000, `${REPARTITION}:0@3`, `${REPARTITION}:1@4`)
		await vi.waitFor(() => expect(logger.warnings).toHaveLength(1))

		commit(2000, `${REPARTITION}:1@6`)
		await vi.waitFor(() => expect(admin.calls).toHaveLength(2))
		expect(admin.calls[1]).toBe(`deleteRecords ${REPARTITION}:0@3 ${REPARTITION}:1@6`)
		commit(3000)
		await vi.waitFor(() => expect(admin.calls).toHaveLength(3))
		expect(logger.warnings).toEqual(['purging repartition topics failed: retried after a later commit'])

		admin.steps.deleteRecords.always(new Error('NOT_LEADER_OR_FOLLOWER'))
		commit(4000)
		await vi.waitFor(() => expect(logger.warnings).toHaveLength(2))
	})

	it("cuts a request in flight at the run's hard stop", async () => {
		const { admin, logger, hardStop, commit } = harness()
		admin.steps.deleteRecords.next('hang')
		commit(1000, `${REPARTITION}:0@3`)
		hardStop.abort()
		await vi.waitFor(() => expect(logger.warnings).toHaveLength(1))
	})

	it('purges nothing with an interval of 0', () => {
		const { admin, commit } = harness(0)
		commit(1000, `${REPARTITION}:0@3`)
		commit(60_000, `${REPARTITION}:0@4`)
		expect(admin.calls).toEqual([])
	})
})
