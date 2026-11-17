import { describe, expect, it } from 'vitest'
import { TopologyError } from '@/api/errors.js'
import type { KStream } from '@/api/kstream.js'
import { codec } from '@/codec.js'
import { TestDriver } from '@/testing/driver.js'

const str = codec.string()
const S = { key: str, value: str }
const key = (k: string) => ({ key: Buffer.from(k) })

/** The repartition topics written to, in order of first use. */
function repartitionTopics(messages: readonly { topic: string }[]): string[] {
	return [...new Set(messages.map(m => m.topic).filter(topic => topic.endsWith('-repartition')))]
}

/** Writes every branch to the topic named like its key in the result. */
function toTopics(branches: Record<string, KStream<string, string>>): void {
	for (const [name, branch] of Object.entries(branches)) {
		branch.to(name, S)
	}
}

describe('KStream.split (6.20, KS BranchedKStreamImpl, KStreamBranch)', () => {
	it('sends each record to the first matching branch only, the rest to the default branch', async () => {
		const driver = new TestDriver()
		const branches = driver
			.input('in', S)
			.split({ name: 'kind-' })
			.branch((_k, v) => v!.startsWith('a'), { name: 'a' })
			.branch((_k, v) => v!.startsWith('ab'), { name: 'ab' })
			.defaultBranch({ name: 'other' })
		expect(Object.keys(branches)).toEqual(['kind-a', 'kind-ab', 'kind-other'])
		toTopics(branches)
		await driver.run(async ({ sendAll, output }) => {
			await sendAll('in', ['abc', 'b', 'a'], { codec: str })
			const values = (topic: string) => output(topic, S).map(r => r.value)
			expect(values('kind-a')).toEqual(['abc', 'a'])
			expect(values('kind-ab')).toEqual([])
			expect(values('kind-other')).toEqual(['b'])
		})
	})

	it('noDefaultBranch() drops the records no branch matched', async () => {
		const driver = new TestDriver()
		const seen: unknown[] = []
		toTopics(
			driver
				.input('in', S)
				.peek((_k, v) => seen.push(v))
				.split({ name: 's-' })
				.branch((_k, v) => v === 'x', { name: 'x' })
				.noDefaultBranch()
		)
		await driver.run(async ({ sendAll, producer }) => {
			await sendAll('in', ['x', 'y'], { codec: str })
			expect(producer.messages.map(m => [m.topic, m.value?.toString()])).toEqual([['s-x', 'x']])
		})
		expect(seen).toEqual(['x', 'y'])
	})

	it('branch names default to their 1-based position and the default branch to 0, after the split name', () => {
		const stream = new TestDriver().input('in', S)
		const named = stream
			.split({ name: 's-' })
			.branch(() => false)
			.branch(() => false, { name: 'b' })
			.branch(() => false)
			.defaultBranch()
		expect(Object.keys(named)).toEqual(['s-1', 's-b', 's-3', 's-0'])
		// Without a split name, the generated name of the split node prefixes the branch names (KS).
		const unnamed = stream
			.split()
			.branch(() => false)
			.defaultBranch()
		expect(Object.keys(unnamed)).toEqual(['KSTREAM-BRANCH-00000000011', 'KSTREAM-BRANCH-00000000010'])
	})

	it('a chain builds on its branch: what it returns replaces the branch, and a chain returning nothing leaves it out', async () => {
		const driver = new TestDriver()
		const branches = driver
			.input('in', S)
			.split({ name: 's-' })
			.branch((_k, v) => v === 'x', { name: 'x', chain: s => s.mapValues(v => `${v}!`) })
			.branch((_k, v) => v === 'y', { name: 'y', chain: s => s.to('y-out', S) })
			.defaultBranch({ name: 'rest', chain: s => s.filter((_k, v) => v !== 'drop') })
		expect(Object.keys(branches)).toEqual(['s-x', 's-rest'])
		toTopics(branches)
		await driver.run(async ({ sendAll, output }) => {
			await sendAll('in', ['x', 'y', 'z', 'drop'], { codec: str })
			const values = (topic: string) => output(topic, S).map(r => r.value)
			expect(values('s-x')).toEqual(['x!'])
			expect(values('y-out')).toEqual(['y'])
			expect(values('s-rest')).toEqual(['z'])
		})
	})

	it('predicates take (key, value), nulls included; branches keep the key, timestamp and headers', async () => {
		const driver = new TestDriver()
		const tested: unknown[] = []
		toTopics(
			driver
				.input('in', S)
				.split({ name: 's-' })
				.branch((k, v) => {
					tested.push([k, v])
					return v === null
				})
				.defaultBranch()
		)
		await driver.run(async ({ send, producer }) => {
			await send('in', null, { timestamp: 5n })
			await send('in', 'v', { ...key('k'), codec: str, timestamp: 7n, headers: { h: Buffer.from('1') } })
			expect(producer.messages).toMatchObject([
				{ topic: 's-1', key: null, value: null, timestamp: 5n },
				{
					topic: 's-0',
					key: Buffer.from('k'),
					value: Buffer.from('v'),
					timestamp: 7n,
					headers: { h: Buffer.from('1') },
				},
			])
		})
		expect(tested).toEqual([
			[null, null],
			['k', 'v'],
		])
	})

	it('branches keep the codecs and the repartition flag of the split stream (6.0)', async () => {
		const driver = new TestDriver()
		const stream = driver.input('in', S)
		const { 'rekeyed-1': rekeyed } = stream
			.selectKey((_k, v) => v)
			.split({ name: 'rekeyed-' })
			.branch(() => true)
			.noDefaultBranch()
		rekeyed!.groupByKey({ key: str }).count({ storeName: 'by-value' })
		const { 'same-1': same } = stream
			.split({ name: 'same-' })
			.branch(() => true)
			.noDefaultBranch()
		same!.groupByKey().count({ storeName: 'by-key' })
		await driver.run(async ({ send, producer }) => {
			await send('in', 'v', { ...key('k'), codec: str })
			expect(repartitionTopics(producer.messages)).toEqual(['test-app-repartition-0-repartition'])
			expect(producer.messages.find(m => m.topic.endsWith('-repartition'))?.value?.toString()).toBe('v')
		})
	})

	it('a split ends with defaultBranch() or noDefaultBranch(): adding to it afterwards is a TopologyError', () => {
		const stream = new TestDriver().input('in', S)
		const ended = stream.split({ name: 's-' })
		ended.noDefaultBranch()
		const message = "split() 's-' already returned its branches"
		expect(() => ended.branch(() => true)).toThrow(TopologyError)
		expect(() => ended.branch(() => true)).toThrow(message)
		expect(() => ended.defaultBranch()).toThrow(message)
		expect(() => ended.noDefaultBranch()).toThrow(message)
		const withDefault = stream.split({ name: 't-' })
		withDefault.defaultBranch()
		expect(() => withDefault.branch(() => true)).toThrow(message.replace('s-', 't-'))
	})

	it('two branches of one name are a TopologyError', () => {
		const branched = new TestDriver()
			.input('in', S)
			.split({ name: 's-' })
			.branch(() => true, { name: 'a' })
		expect(() => branched.branch(() => true, { name: 'a' })).toThrow("A node named 's-a' already exists")
	})
})
