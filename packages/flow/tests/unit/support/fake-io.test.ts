import { describe, expect, it } from 'vitest'
import { ErrorCode, InvalidTxnStateError, KafkaProtocolError, type Message, type TopicPartition } from '@kafkats/client'
import type { AssignedPartition, GroupConsumerHooks } from '@/runtime/io.js'
import { tpKey } from '@/runtime/task-id.js'
import { defaultPartition } from '@/testing/partitioner.js'
import {
	FakeAdmin,
	FakeCluster,
	FakeGroupConsumer,
	FakeIO,
	FakeProducer,
	FakeRestoreConsumer,
	Steps,
	type FakeTransaction,
} from './fake-io.js'

const tick = () => new Promise(resolve => setImmediate(resolve))
const never = new AbortController().signal
const keys = (tps: readonly TopicPartition[]) => tps.map(tpKey).join(',')
const offsets = (messages: readonly Message<Buffer, Buffer>[]) => messages.map(m => m.offset)
const value = (text: string) => ({ key: null, value: Buffer.from(text), timestamp: 0n, headers: {} })

describe('Steps', () => {
	it('ends calls with the queued outcomes in order, then with the standing one', async () => {
		const steps = new Steps().next(new Error('first'), 'ok').always(new Error('standing'))
		let effects = 0
		const call = () => steps.perform(() => ++effects)
		await expect(call()).rejects.toThrow('first')
		await expect(call()).resolves.toBe(1)
		await expect(call()).rejects.toThrow('standing')
		await expect(call()).rejects.toThrow('standing')
		expect(effects).toBe(1)
	})

	it('a function outcome runs when the call is made and decides its outcome', async () => {
		const seen: string[] = []
		const steps = new Steps().next(() => (seen.push('called'), new Error('no')))
		const result = steps.perform(() => seen.push('effect'))
		expect(seen).toEqual(['called'])
		await expect(result).rejects.toThrow('no')
	})

	it("a hung call waits for the test's answer; an answered error rejects it", async () => {
		const steps = new Steps().always('hang')
		let effect = 0
		const first = steps.perform(() => ++effect)
		const second = steps.perform(() => ++effect)
		expect(steps.hung).toHaveLength(2)
		steps.hung[1]!.answer(new Error('refused'))
		steps.hung[0]!.answer()
		expect(steps.hung).toEqual([])
		await expect(first).resolves.toBe(1)
		await expect(second).rejects.toThrow('refused')
	})

	it('a hung call with a signal rejects with an AbortError once it aborts; a late answer still applies its effect', async () => {
		const steps = new Steps().always('hang')
		const abort = new AbortController()
		let effect = 0
		const call = steps.perform(() => ++effect, abort.signal)
		abort.abort()
		await expect(call).rejects.toMatchObject({ name: 'AbortError' })
		steps.answerAll()
		expect(effect).toBe(1)
	})

	it('a call made with an aborted signal rejects at once, takes no outcome and has no effect', async () => {
		const steps = new Steps().next(new Error('kept'))
		let effect = 0
		await expect(steps.perform(() => ++effect, AbortSignal.abort())).rejects.toMatchObject({ name: 'AbortError' })
		await expect(steps.perform(() => ++effect)).rejects.toThrow('kept')
		expect(effect).toBe(0)
	})
})

/** A running consumer whose hooks log what they see and await what the test sets in `hooks`. */
function member(consumer = new FakeGroupConsumer()) {
	const events: string[] = []
	const hooks: { revoked?: () => Promise<void>; assigned?: () => Promise<void>; lost?: () => Promise<void> } = {}
	const handlers: GroupConsumerHooks = {
		onPartitionsRevoked: async tps => {
			events.push(`revoked ${keys(tps)}`)
			await hooks.revoked?.()
		},
		onPartitionsAssigned: async tps => {
			const held = tps.filter(tp => consumer.partition(tp).held).length
			events.push(`assigned ${keys(tps)} gen ${consumer.groupMetadata()?.generationId} held ${held}`)
			await hooks.assigned?.()
		},
		onPartitionsLost: async tps => {
			events.push(`lost ${keys(tps)}`)
			await hooks.lost?.()
			events.push('lost handled')
		},
	}
	consumer.on('rebalancePending', () => events.push('pending'))
	consumer.on('rebalanceComplete', () => events.push('complete'))
	const run = consumer.run(
		['a'],
		(tp, messages) => events.push(`batch ${tpKey(tp)} ${offsets(messages).join(',')}`),
		handlers
	)
	return { consumer, events, hooks, run }
}

describe('FakeGroupConsumer', () => {
	it('C4: the first join signals and revokes nothing; a later rebalance revokes, joins anew, then assigns', async () => {
		const { consumer, events } = member()
		await consumer.rebalance('a:0', 'a:1')
		expect(events).toEqual(['assigned a:0,a:1 gen 1 held 2'])
		expect(consumer.rebalancePending).toBe(false)

		events.length = 0
		await consumer.rebalance('a:1')
		expect(events).toEqual(['pending', 'revoked a:0,a:1', 'assigned a:1 gen 2 held 1', 'complete'])
		expect([...consumer.partitions.keys()]).toEqual(['a:1'])
		expect(consumer.groupMetadata()).toEqual({ groupId: 'app', generationId: 2, memberId: 'member' })
	})

	it('the revocation hook is awaited; its commit is what the next assignment starts from (C2)', async () => {
		const { consumer, events, hooks } = member()
		await consumer.rebalance('a:0')
		hooks.revoked = () =>
			consumer.commit([{ topic: 'a', partition: 0, offset: 7n, metadata: 'm' }], { signal: never })
		await consumer.rebalance('a:0', { topic: 'a', partition: 1, offset: 3n, committed: null })
		expect(consumer.partition('a:0').position).toBe(7n)
		expect(consumer.partition('a:1').position).toBe(3n)
		expect(events.at(-2)).toBe('assigned a:0,a:1 gen 2 held 2')
		expect(await consumer.committed([{ topic: 'a', partition: 0 }], { signal: never })).toEqual(
			new Map([['a:0', { offset: 7n, metadata: 'm' }]])
		)
	})

	it('records fetched while a partition is held wait for the assignment hook; one it paused stays paused (C4, C5)', async () => {
		const { consumer, events, hooks } = member()
		hooks.assigned = async () => {
			consumer.deliver('a:0', [{ value: 'x' }])
			consumer.deliver('a:1', [{ value: 'y' }])
			consumer.pause([{ topic: 'a', partition: 1 }])
		}
		await consumer.rebalance('a:0', 'a:1')
		expect(events.slice(1)).toEqual(['batch a:0 0'])
		expect(consumer.nextOffset({ topic: 'a', partition: 1 })).toBe(0n)
		consumer.resume([{ topic: 'a', partition: 1 }])
		expect(events.at(-1)).toBe('batch a:1 0')
		expect(consumer.nextOffset({ topic: 'a', partition: 1 })).toBe(1n)
	})

	it('C5/C6: a paused partition keeps its batches; nextOffset is their first offset, the lag unknown; seek drops them', async () => {
		const { consumer, events } = member()
		await consumer.rebalance('a:0')
		const tp = { topic: 'a', partition: 0 }
		expect([consumer.nextOffset(tp), consumer.currentLag(tp)]).toEqual([0n, undefined])
		consumer.deliver(tp, [{ value: '1' }, { value: '2' }], 5n)
		expect([consumer.nextOffset(tp), consumer.currentLag(tp)]).toEqual([2n, 3n])
		consumer.pause([tp])
		consumer.deliver(tp, [{ value: '3' }])
		expect([consumer.nextOffset(tp), consumer.currentLag(tp)]).toEqual([2n, undefined])
		consumer.seek(tp, 1n)
		consumer.resume([tp])
		consumer.deliver(tp, [{ value: '2 again' }])
		expect(events.filter(e => e.startsWith('batch'))).toEqual(['batch a:0 0,1', 'batch a:0 1'])
		expect([consumer.nextOffset(tp), consumer.currentLag(tp)]).toEqual([2n, 3n])
		expect(consumer.calls).toEqual(['run a', 'pause a:0', 'seek a:0 1', 'resume a:0'])
	})

	it('a loss: the hook starts synchronously, the next rebalance waits for it and revokes none of the lost partitions', async () => {
		const { consumer, events, hooks } = member()
		await consumer.rebalance('a:0')
		let release!: () => void
		hooks.lost = () => new Promise(resolve => (release = resolve))
		void consumer.lose()
		expect(events.slice(1)).toEqual(['lost a:0', 'pending'])
		expect([consumer.groupMetadata(), consumer.partitions.size, consumer.rebalancePending]).toEqual([
			undefined,
			0,
			true,
		])

		const rebalanced = consumer.rebalance('a:0')
		await tick()
		expect(events.at(-1)).toBe('pending')
		release()
		await rebalanced
		expect(events.slice(3)).toEqual(['lost handled', 'assigned a:0 gen 2 held 1', 'complete'])
	})

	it('a partition lost while the assignment hook runs is not released', async () => {
		const { consumer, hooks } = member()
		hooks.assigned = async () => void consumer.lose()
		await consumer.rebalance('a:0')
		expect(consumer.partitions.size).toBe(0)
		expect(consumer.rebalancePending).toBe(true)
	})

	it('commit outcomes: a failure, a hang bounded by the signal; only successful commits are recorded', async () => {
		const consumer = new FakeGroupConsumer()
		const offset = [{ topic: 'a', partition: 0, offset: 1n, metadata: '' }]
		consumer.steps.commit.next(new KafkaProtocolError(ErrorCode.RebalanceInProgress), 'hang')
		await expect(consumer.commit(offset, { signal: never })).rejects.toBeInstanceOf(KafkaProtocolError)
		await expect(consumer.commit(offset, { signal: AbortSignal.timeout(5) })).rejects.toMatchObject({
			name: 'AbortError',
		})
		await consumer.commit(offset, { signal: never })
		expect(consumer.commits).toEqual([offset])
		expect(consumer.calls).toEqual(['commit a:0@1', 'commit a:0@1', 'commit a:0@1'])
	})

	it('stop waits for the hook in progress (4.3 deadlock rule), then ends the run', async () => {
		const { consumer, hooks, run } = member()
		let release!: () => void
		hooks.assigned = () => new Promise(resolve => (release = resolve))
		const assigned = consumer.rebalance('a:0')
		await tick()
		await expect(consumer.stop({ signal: AbortSignal.timeout(5) })).rejects.toMatchObject({ name: 'AbortError' })
		const stopped = consumer.stop({ signal: never })
		release()
		await Promise.all([assigned, stopped, run])
		expect(() => consumer.rebalance('a:0')).toThrow('running consumer')
	})

	it('a hook that rejects fails the run; fail() does too', async () => {
		const first = member()
		first.hooks.assigned = () => Promise.reject(new Error('hook failed'))
		await first.consumer.rebalance('a:0')
		await expect(first.run).rejects.toThrow('hook failed')

		const second = member()
		second.consumer.fail(new KafkaProtocolError(ErrorCode.FencedInstanceId))
		await expect(second.run).rejects.toBeInstanceOf(KafkaProtocolError)
	})

	it('emits the events a test raises to their listeners', () => {
		const consumer = new FakeGroupConsumer()
		const seen: unknown[] = []
		consumer.on('offsetOutOfRange', e => seen.push(e))
		consumer.emit('offsetOutOfRange', { topic: 'a', partition: 0, offset: 5n })
		expect(seen).toEqual([{ topic: 'a', partition: 0, offset: 5n }])
	})
})

describe('FakeProducer', () => {
	it('acknowledges sends at the next offset of their partition; without one, where the default partitioner puts it', async () => {
		const cluster = new FakeCluster()
		cluster.createTopic('out', 4)
		const producer = new FakeProducer(cluster)
		const key = Buffer.from('k')
		const sent = [
			await producer.send('out', { key, value: Buffer.from('1') }),
			await producer.send('out', { key, value: Buffer.from('2') }),
			await producer.send('out', { key: null, value: Buffer.from('3'), partition: 3 }),
		]
		const partition = defaultPartition(key, 4)
		expect(sent.map(r => [r.partition, r.offset])).toEqual([
			[partition, 0n],
			[partition, 1n],
			[3, 0n],
		])
		expect(cluster.log('out', partition).end).toBe(2n)
		await expect(producer.send('out', { key, value: null, partition: 4 })).rejects.toMatchObject({
			errorCode: ErrorCode.UnknownTopicOrPartition,
		})
	})

	it("a transaction's records are read_committed only once it committed, past its marker; its offsets become the group's", async () => {
		const cluster = new FakeCluster()
		const producer = new FakeProducer(cluster)
		const transaction = (await producer.beginTransaction({ signal: never })) as FakeTransaction
		const { offset } = await transaction.send('changelog', { key: null, value: Buffer.from('v') })
		const log = cluster.log('changelog', 0)
		expect([log.lastStable, log.end, log.read(0n, 'read_committed').messages]).toEqual([0n, 1n, []])
		const group = { groupId: 'app', generationId: 1, memberId: 'm' }
		await transaction.sendOffsets({
			consumerGroupMetadata: group,
			offsets: [{ topic: 'in', partition: 0, offset: 5n, metadata: 'x' }],
		})
		await transaction.commit({ signal: never })
		expect([log.lastStable, log.end]).toEqual([offset + 2n, offset + 2n])
		expect(offsets(log.read(0n, 'read_committed').messages)).toEqual([0n])
		expect(cluster.offsets('app').get('in:0')).toEqual({ offset: 5n, metadata: 'x' })
		expect(transaction.calls).toEqual(['send', 'sendOffsets', 'commit'])
	})

	it('an aborted transaction is invisible to read_committed, not to read_uncommitted; an ended one takes no call', async () => {
		const cluster = new FakeCluster()
		const producer = new FakeProducer(cluster)
		const transaction = (await producer.beginTransaction({ signal: never })) as FakeTransaction
		await transaction.send('t', { key: null, value: Buffer.from('a') })
		await transaction.abort({ signal: never })
		const log = cluster.log('t', 0)
		expect(log.read(0n, 'read_committed')).toEqual({ messages: [], next: 2n })
		expect(offsets(log.read(0n, 'read_uncommitted').messages)).toEqual([0n])
		await expect(transaction.commit({ signal: never })).rejects.toBeInstanceOf(InvalidTxnStateError)
		await expect(transaction.send('t', { key: null, value: null })).rejects.toBeInstanceOf(InvalidTxnStateError)
	})

	it('one transaction at a time; each step fails or hangs as scripted, leaving the transaction open', async () => {
		const producer = new FakeProducer()
		const transaction = (await producer.beginTransaction({ signal: never })) as FakeTransaction
		await expect(producer.beginTransaction({ signal: never })).rejects.toBeInstanceOf(InvalidTxnStateError)
		producer.steps.sendOffsets.next(new Error('sendOffsets failed'))
		producer.steps.commit.next('hang')
		const group = { groupId: 'app', generationId: 1, memberId: 'm' }
		await expect(transaction.sendOffsets({ consumerGroupMetadata: group, offsets: [] })).rejects.toThrow(
			'sendOffsets failed'
		)
		await expect(transaction.commit({ signal: AbortSignal.timeout(5) })).rejects.toMatchObject({
			name: 'AbortError',
		})
		expect(transaction.state).toBe('open')
		await transaction.abort({ signal: never })
		expect(producer.transactions.map(t => t.state)).toEqual(['aborted'])
		expect(producer.committedRecords('t')).toEqual([])
	})

	it('without a cluster, every topic has one partition and acks count per partition', async () => {
		const producer = new FakeProducer()
		const acks = [
			await producer.send('a', { key: Buffer.from('x'), value: null }),
			await producer.send('a', { key: Buffer.from('y'), value: null }),
			await producer.send('b', { key: null, value: null, partition: 2 }),
		]
		expect(acks.map(a => `${a.topic}:${a.partition}@${a.offset}`)).toEqual(['a:0@0', 'a:0@1', 'b:2@0'])
		expect(await producer.partitionCount('a', { signal: never })).toBe(1)
		expect(producer.sent).toHaveLength(3)
	})
})

describe('FakeAdmin', () => {
	it('answers from the cluster: partition counts, log start, end and last stable offset; creates and truncates', async () => {
		const cluster = new FakeCluster()
		const admin = new FakeAdmin(cluster)
		await admin.createTopics(
			[
				{
					name: 'app-s-changelog',
					kind: 'changelog',
					configs: { 'cleanup.policy': 'compact' },
					validateOnly: false,
				},
				{ name: 'app-r-repartition', kind: 'repartition', partitions: 3, configs: {}, validateOnly: false },
			],
			new Map([['app-s-changelog', 2]]),
			{ signal: never }
		)
		expect(
			await admin.describeTopics(['app-s-changelog', 'app-r-repartition', 'missing'], { signal: never })
		).toEqual(
			new Map([
				['app-s-changelog', 2],
				['app-r-repartition', 3],
			])
		)
		expect(cluster.topics.get('app-s-changelog')!.configs).toEqual({ 'cleanup.policy': 'compact' })

		cluster.append('app-s-changelog', 0, { value: 'a' }, { value: 'b' })
		const producer = new FakeProducer(cluster)
		const transaction = await producer.beginTransaction({ signal: never })
		await transaction.send('app-s-changelog', { key: null, value: null, partition: 0 })
		await admin.deleteRecords([{ topic: 'app-s-changelog', partition: 0, offset: 1n }], { signal: never })
		const tp = [{ topic: 'app-s-changelog', partition: 0 }]
		const listed = await Promise.all([
			admin.listOffsets(tp, 'earliest', 'read_committed', { signal: never }),
			admin.listOffsets(tp, 'latest', 'read_committed', { signal: never }),
			admin.listOffsets(tp, 'latest', 'read_uncommitted', { signal: never }),
		])
		expect(listed.map(m => m.get('app-s-changelog:0'))).toEqual([1n, 2n, 3n])
		await expect(
			admin.deleteRecords([{ topic: 'app-s-changelog', partition: 0, offset: 9n }], { signal: never })
		).rejects.toMatchObject({ errorCode: ErrorCode.OffsetOutOfRange })
		await expect(
			admin.listOffsets([{ topic: 'missing', partition: 0 }], 'earliest', 'read_committed', { signal: never })
		).rejects.toMatchObject({
			errorCode: ErrorCode.UnknownTopicOrPartition,
		})
	})

	it('a step hangs until its signal aborts', async () => {
		const admin = new FakeAdmin(new FakeCluster())
		admin.steps.listOffsets.always('hang')
		await expect(
			admin.listOffsets([], 'latest', 'read_committed', { signal: AbortSignal.timeout(5) })
		).rejects.toMatchObject({
			name: 'AbortError',
		})
		expect(admin.calls).toEqual(['listOffsets latest read_committed '])
	})
})

describe('FakeRestoreConsumer', () => {
	/** Runs the consumer, logging each batch as `t:p offsets` and each out-of-range report. */
	function reading(consumer: FakeRestoreConsumer, onBatch: () => Promise<void> = async () => {}) {
		const seen: string[] = []
		const run = consumer.run(
			async (tp, messages) => {
				seen.push(`${tpKey(tp)} ${offsets(messages).join(',')}`)
				await onBatch()
			},
			(tp, offset) => seen.push(`out of range ${tpKey(tp)}@${offset}`)
		)
		return { seen, run }
	}

	it('reads committed records in batches, up to the last stable offset, then follows the log', async () => {
		const cluster = new FakeCluster()
		cluster.append('c', 0, { value: '0' }, { value: '1' }, { value: '2' })
		const producer = new FakeProducer(cluster)
		const open = await producer.beginTransaction({ signal: never })
		await open.send('c', { key: null, value: null, partition: 0 })
		const consumer = new FakeRestoreConsumer(cluster, 'app-restore-0_0-1')
		consumer.batchSize = 2
		consumer.assign([{ topic: 'c', partition: 0, offset: 0n }])
		const { seen, run } = reading(consumer)
		await tick()
		expect(seen).toEqual(['c:0 0,1', 'c:0 2'])
		expect(consumer.position({ topic: 'c', partition: 0 })).toBe(3n)

		await open.commit({ signal: never })
		cluster.log('c', 0).append(value('5'))
		await tick()
		expect(seen.slice(2)).toEqual(['c:0 3,5'])
		expect(consumer.position({ topic: 'c', partition: 0 })).toBe(6n)
		await consumer.stop({ signal: never })
		await run
	})

	it('a position outside the log is out of range: reported once, the partition paused', async () => {
		const cluster = new FakeCluster()
		cluster.append('c', 0, { value: '0' }, { value: '1' })
		cluster.log('c', 0).deleteBefore(1n)
		const consumer = new FakeRestoreConsumer(cluster, 'g')
		consumer.assign([
			{ topic: 'c', partition: 0, offset: 0n },
			{ topic: 'c', partition: 1, offset: 4n },
		])
		const { seen, run } = reading(consumer)
		await tick()
		expect(seen).toEqual(['out of range c:0@0', 'out of range c:1@4'])
		await consumer.stop({ signal: never })
		await run
	})

	it('stop waits for the batch in flight; fail() rejects the run after it', async () => {
		const cluster = new FakeCluster()
		cluster.append('c', 0, { value: '0' })
		let release!: () => void
		const consumer = new FakeRestoreConsumer(cluster, 'g')
		consumer.assign([{ topic: 'c', partition: 0, offset: 0n }])
		const { run } = reading(consumer, () => new Promise(resolve => (release = resolve)))
		await tick()
		consumer.fail(new Error('broker gone'))
		await expect(consumer.stop({ signal: AbortSignal.timeout(5) })).rejects.toMatchObject({ name: 'AbortError' })
		release()
		await expect(run).rejects.toThrow('broker gone')
		expect(consumer.calls).toEqual(['assign c:0@0', 'run', 'stop'])
	})
})

describe('FakeIO', () => {
	it("keeps every port it creates; a later Run's consumer is assigned the offsets committed before", async () => {
		const io = new FakeIO({ groupId: 'app', time: 1000 })
		const first = io.consumer()
		void first.run(['in'], () => {}, hooksDoingNothing)
		await first.rebalance('in:0')
		await first.commit([{ topic: 'in', partition: 0, offset: 4n, metadata: 'm' }], { signal: never })

		const producer = io.producer()
		const transaction = await producer.beginTransaction({ signal: never })
		await transaction.sendOffsets({
			consumerGroupMetadata: first.groupMetadata()!,
			offsets: [{ topic: 'in', partition: 1, offset: 9n, metadata: 'n' }],
		})
		await transaction.commit({ signal: never })

		const second = io.consumer()
		const assigned: AssignedPartition[] = []
		void second.run(['in'], () => {}, {
			...hooksDoingNothing,
			onPartitionsAssigned: async tps => void assigned.push(...tps),
		})
		await second.rebalance('in:0', 'in:1')
		expect(assigned).toEqual([
			{ topic: 'in', partition: 0, offset: 4n, committed: { offset: 4n, metadata: 'm' } },
			{ topic: 'in', partition: 1, offset: 9n, committed: { offset: 9n, metadata: 'n' } },
		])
		io.restoreConsumers('app-restore-0_0-1')
		io.globalConsumers('app-global-s-p')
		expect([io.created.consumers, io.created.producers]).toEqual([[first, second], [producer]])
		expect([io.created.restoreConsumers[0]!.groupId, io.created.globalConsumers[0]!.groupId]).toEqual([
			'app-restore-0_0-1',
			'app-global-s-p',
		])
		expect(io.clock.now()).toBe(1000)
	})
})

const hooksDoingNothing: GroupConsumerHooks = {
	onPartitionsLost: async () => {},
	onPartitionsRevoked: async () => {},
	onPartitionsAssigned: async () => {},
}
