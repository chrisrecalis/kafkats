import { EventEmitter } from 'node:events'
import { describe, expect, it, vi, afterEach } from 'vitest'
import { KafkaClient, type Message } from '@kafkats/client'
import { codec, flow, TimeWindows } from '../../src/index.js'

type TestMessage = Message<Buffer>
type TestContext = { signal: AbortSignal; topic: string; partition: number; offset: bigint }
type TestHandler = (message: TestMessage, ctx: TestContext) => Promise<void>

class TestConsumer extends EventEmitter {
	private handler: TestHandler | null = null
	private stopResolve: (() => void) | null = null
	consumerGroup = { currentMemberId: 'member-1', currentGenerationId: 1 }

	async runEach(_subscription: string[], handler: TestHandler): Promise<void> {
		this.handler = handler
		this.emit('running')
		return new Promise(resolve => {
			this.stopResolve = () => {
				this.emit('stopped')
				resolve()
			}
		})
	}

	stop(): void {
		this.stopResolve?.()
		this.stopResolve = null
	}

	async emitMessage(topic: string, value: Buffer, key?: Buffer | null, timestamp?: bigint): Promise<void> {
		if (!this.handler) {
			throw new Error('consumer not started')
		}
		const message: TestMessage = {
			topic,
			partition: 0,
			offset: 0n,
			timestamp: timestamp ?? 0n,
			key: key ?? null,
			value,
			headers: {},
		}
		const ctx = {
			signal: new AbortController().signal,
			topic,
			partition: 0,
			offset: 0n,
		}
		await this.handler(message, ctx)
	}
}

class TestProducer {
	messages: Array<{ topic: string; key?: Buffer | null; value: Buffer; partition?: number }> = []
	transactions: Array<{ offsets: Array<{ topic: string; partition: number; offset: bigint }> }> = []

	async send(topic: string, message: { key?: Buffer | null; value: Buffer; partition?: number }): Promise<void> {
		this.messages.push({ topic, key: message.key, value: message.value, partition: message.partition })
	}

	async transaction(
		fn: (tx: {
			send: (topic: string, message: { key?: Buffer | null; value: Buffer; partition?: number }) => Promise<void>
			sendOffsets: (params: {
				groupId?: string
				consumerGroupMetadata?: {
					groupId: string
					generationId: number
					memberId: string
					groupInstanceId?: string | null
				}
				offsets: Array<{ topic: string; partition: number; offset: bigint }>
			}) => Promise<void>
		}) => Promise<void>
	): Promise<void> {
		const offsets: Array<{ topic: string; partition: number; offset: bigint }> = []
		await fn({
			send: async (topic, message) => {
				await this.send(topic, message)
			},
			sendOffsets: async params => {
				offsets.push(...params.offsets)
			},
		})
		this.transactions.push({ offsets })
	}

	async disconnect(): Promise<void> {
		return
	}
}

function createTestApp(overrides: Partial<Parameters<typeof flow>[0]> = {}) {
	const client = new KafkaClient({ clientId: 'test-app', brokers: ['localhost:9092'] })
	const consumers: TestConsumer[] = []
	const producers: TestProducer[] = []

	vi.spyOn(client, 'connect').mockResolvedValue()
	vi.spyOn(client, 'disconnect').mockResolvedValue()
	vi.spyOn(client, 'consumer').mockImplementation(() => {
		const consumer = new TestConsumer()
		consumers.push(consumer)
		return consumer as unknown as never
	})
	vi.spyOn(client, 'producer').mockImplementation(() => {
		const producer = new TestProducer()
		producers.push(producer)
		return producer as unknown as never
	})

	const app = flow({ applicationId: 'test-app', client, ...overrides })
	return { app, consumers, producers }
}

afterEach(() => {
	vi.restoreAllMocks()
})

describe('flow', () => {
	it('processes map/filter/to pipelines', async () => {
		const { app, consumers, producers } = createTestApp()

		type Order = { id: string; total: number }
		type Enriched = { id: string; total: number; large: boolean }

		app.stream('orders', { value: codec.json<Order>() })
			.mapValues(order => ({ ...order, large: order.total > 100 }) satisfies Enriched)
			.filter((_key, value) => value.large)
			.to('large-orders', { value: codec.json<Enriched>() })

		await app.start()
		const consumer = consumers[0]!
		const producer = producers[0]!

		await consumer.emitMessage('orders', Buffer.from(JSON.stringify({ id: 'o1', total: 250 })))
		await consumer.emitMessage('orders', Buffer.from(JSON.stringify({ id: 'o2', total: 50 })))

		expect(producer.messages).toHaveLength(1)
		const payload = JSON.parse(producer.messages[0]!.value.toString()) as Enriched
		expect(payload).toEqual({ id: 'o1', total: 250, large: true })

		await app.close()
	})

	it('supports through to continue the pipeline', async () => {
		const { app, consumers, producers } = createTestApp()

		app.stream('input', { value: codec.json<{ id: string }>() })
			.through('audit', { value: codec.json() })
			.mapValues(value => ({ ...value, tagged: true }))
			.to('output', { value: codec.json() })

		await app.start()
		const consumer = consumers[0]!
		const producer = producers[0]!
		await consumer.emitMessage('input', Buffer.from(JSON.stringify({ id: 'a1' })))

		const topics = producer.messages.map(msg => msg.topic)
		expect(topics).toEqual(['audit', 'output'])

		await app.close()
	})

	it('routes to first matching branch', async () => {
		const { app, consumers, producers } = createTestApp()

		const branches = app.stream('events', { value: codec.json<{ type: string }>() }).branch(
			(_key, value) => value.type === 'a',
			(_key, value) => value.type === 'b'
		)

		branches[0]!.to('topic-a', { value: codec.json() })
		branches[1]!.to('topic-b', { value: codec.json() })

		await app.start()
		const consumer = consumers[0]!
		const producer = producers[0]!
		await consumer.emitMessage('events', Buffer.from(JSON.stringify({ type: 'b' })))

		expect(producer.messages).toHaveLength(1)
		expect(producer.messages[0]!.topic).toBe('topic-b')

		await app.close()
	})

	it('merges streams from different topics', async () => {
		const { app, consumers, producers } = createTestApp()

		const left = app.stream('left', { value: codec.json<{ id: string }>() })
		const right = app.stream('right', { value: codec.json<{ id: string }>() })

		left.merge(right)
			.mapValues(value => ({ ...value, merged: true }))
			.to('merged', { value: codec.json() })

		await app.start()
		const consumer = consumers[0]!
		const producer = producers[0]!
		await consumer.emitMessage('left', Buffer.from(JSON.stringify({ id: 'l1' })))
		await consumer.emitMessage('right', Buffer.from(JSON.stringify({ id: 'r1' })))

		expect(producer.messages).toHaveLength(2)
		const ids = producer.messages.map(msg => JSON.parse(msg.value.toString()).id)
		expect(ids).toEqual(['l1', 'r1'])

		await app.close()
	})

	it('throws when codecs are missing for non-buffer values', async () => {
		const { app, consumers } = createTestApp()

		app.stream('raw', { value: codec.json<{ id: string }>() })
			.mapValues(value => ({ ...value, enriched: true }))
			.to('out')

		await app.start()
		const consumer = consumers[0]!
		await expect(consumer.emitMessage('raw', Buffer.from(JSON.stringify({ id: 'x1' })))).rejects.toThrow(
			'No value codec provided'
		)

		await app.close()
	})

	it('uses transactions and sends offsets for exactly_once', async () => {
		const { app, consumers, producers } = createTestApp({ processingGuarantee: 'exactly_once' })

		app.stream('events', { value: codec.json<{ id: string }>() })
			.mapValues(value => ({ ...value, handled: true }))
			.to('out', { value: codec.json() })

		await app.start()
		const consumer = consumers[0]!
		const producer = producers[0]!

		await consumer.emitMessage('events', Buffer.from(JSON.stringify({ id: 't1' })))

		// With batched commits, transactions are committed on close()
		await app.close()

		expect(producer.transactions).toHaveLength(1)
		expect(producer.transactions[0]!.offsets).toEqual([
			{
				topic: 'events',
				partition: 0,
				offset: 1n,
			},
		])
	})

	it('spawns one worker per stream thread', async () => {
		const { app, consumers, producers } = createTestApp({ numStreamThreads: 2 })

		app.stream('events')

		await app.start()

		expect(consumers).toHaveLength(2)
		expect(producers).toHaveLength(2)

		await app.close()
	})
})

describe('stream-table joins', () => {
	it('joins stream records with table lookups (inner join)', async () => {
		const { app, consumers } = createTestApp()

		type Event = { userId: string; action: string }
		type User = { name: string; tier: string }
		type Enriched = { userId: string; action: string; userName: string; userTier: string }

		const results: Enriched[] = []

		const users = app.table('users', { key: codec.string(), value: codec.json<User>() })

		app.stream('events', { key: codec.string(), value: codec.json<Event>() })
			.join(users, (event, user) => ({
				userId: event.userId,
				action: event.action,
				userName: user.name,
				userTier: user.tier,
			}))
			.peek((_key, value) => {
				results.push(value)
			})

		await app.start()
		const consumer = consumers[0]!

		// First, populate the table with user data
		await consumer.emitMessage(
			'users',
			Buffer.from(JSON.stringify({ name: 'Alice', tier: 'gold' })),
			Buffer.from('user1')
		)
		await consumer.emitMessage(
			'users',
			Buffer.from(JSON.stringify({ name: 'Bob', tier: 'silver' })),
			Buffer.from('user2')
		)

		// Now send events - only matching users should be joined
		await consumer.emitMessage(
			'events',
			Buffer.from(JSON.stringify({ userId: 'user1', action: 'click' })),
			Buffer.from('user1')
		)
		await consumer.emitMessage(
			'events',
			Buffer.from(JSON.stringify({ userId: 'user2', action: 'view' })),
			Buffer.from('user2')
		)
		await consumer.emitMessage(
			'events',
			Buffer.from(JSON.stringify({ userId: 'user3', action: 'buy' })),
			Buffer.from('user3')
		)

		// Only user1 and user2 events should be joined (inner join)
		expect(results).toHaveLength(2)
		expect(results[0]).toEqual({ userId: 'user1', action: 'click', userName: 'Alice', userTier: 'gold' })
		expect(results[1]).toEqual({ userId: 'user2', action: 'view', userName: 'Bob', userTier: 'silver' })

		await app.close()
	})

	it('left joins stream records with table lookups', async () => {
		const { app, consumers } = createTestApp()

		type Event = { action: string }
		type User = { name: string }
		type Result = { action: string; userName: string | null }

		const results: Result[] = []

		const users = app.table('users', { key: codec.string(), value: codec.json<User>() })

		app.stream('events', { key: codec.string(), value: codec.json<Event>() })
			.leftJoin(users, (event, user) => ({
				action: event.action,
				userName: user?.name ?? null,
			}))
			.peek((_key, value) => {
				results.push(value)
			})

		await app.start()
		const consumer = consumers[0]!

		// Populate table with one user
		await consumer.emitMessage('users', Buffer.from(JSON.stringify({ name: 'Alice' })), Buffer.from('user1'))

		// Send events for both existing and non-existing users
		await consumer.emitMessage('events', Buffer.from(JSON.stringify({ action: 'click' })), Buffer.from('user1'))
		await consumer.emitMessage('events', Buffer.from(JSON.stringify({ action: 'view' })), Buffer.from('user2'))

		// Left join should include all events
		expect(results).toHaveLength(2)
		expect(results[0]).toEqual({ action: 'click', userName: 'Alice' })
		expect(results[1]).toEqual({ action: 'view', userName: null })

		await app.close()
	})

	it('updates table state and joins reflect latest values', async () => {
		const { app, consumers } = createTestApp()

		type User = { score: number }
		type Event = { action: string }
		type Result = { action: string; score: number }

		const results: Result[] = []

		const users = app.table('users', { key: codec.string(), value: codec.json<User>() })

		app.stream('events', { key: codec.string(), value: codec.json<Event>() })
			.join(users, (event, user) => ({
				action: event.action,
				score: user.score,
			}))
			.peek((_key, value) => {
				results.push(value)
			})

		await app.start()
		const consumer = consumers[0]!

		// Insert initial user score
		await consumer.emitMessage('users', Buffer.from(JSON.stringify({ score: 100 })), Buffer.from('user1'))

		// Event joins with score=100
		await consumer.emitMessage('events', Buffer.from(JSON.stringify({ action: 'a' })), Buffer.from('user1'))

		// Update user score
		await consumer.emitMessage('users', Buffer.from(JSON.stringify({ score: 200 })), Buffer.from('user1'))

		// Event now joins with score=200
		await consumer.emitMessage('events', Buffer.from(JSON.stringify({ action: 'b' })), Buffer.from('user1'))

		expect(results).toHaveLength(2)
		expect(results[0]).toEqual({ action: 'a', score: 100 })
		expect(results[1]).toEqual({ action: 'b', score: 200 })

		await app.close()
	})
})

describe('stream-stream joins', () => {
	it('joins streams within a time window (inner join)', async () => {
		const { app, consumers } = createTestApp()

		type Click = { page: string }
		type Impression = { ad: string }
		type Result = { page: string; ad: string }

		const results: Array<{ key: string; value: Result }> = []

		const clicks = app.stream('clicks', { key: codec.string(), value: codec.json<Click>() })
		const impressions = app.stream('impressions', { key: codec.string(), value: codec.json<Impression>() })

		clicks
			.join(
				impressions,
				(click, impression) => ({
					page: click.page,
					ad: impression.ad,
				}),
				{ within: TimeWindows.of('5s') }
			)
			.peek((key, value) => {
				results.push({ key, value })
			})

		await app.start()
		const consumer = consumers[0]!

		// Impression at t=1000ms
		await consumer.emitMessage(
			'impressions',
			Buffer.from(JSON.stringify({ ad: 'banner1' })),
			Buffer.from('user1'),
			1000n
		)

		// Click at t=2000ms (within 5s window) - should join
		await consumer.emitMessage('clicks', Buffer.from(JSON.stringify({ page: 'home' })), Buffer.from('user1'), 2000n)

		// Click at t=10000ms (outside 5s window from impression) - no join from click side
		await consumer.emitMessage(
			'clicks',
			Buffer.from(JSON.stringify({ page: 'about' })),
			Buffer.from('user1'),
			10000n
		)

		// Exactly one join: click at t=2000ms joins with impression at t=1000ms (within 5s window)
		// Click at t=10000ms should NOT join (outside window from impression at t=1000ms)
		expect(results).toHaveLength(1)
		expect(results[0]).toEqual({ key: 'user1', value: { page: 'home', ad: 'banner1' } })

		await app.close()
	})

	it('left joins streams - left side always emits', async () => {
		const { app, consumers } = createTestApp()

		type Click = { page: string }
		type Impression = { ad: string }
		type Result = { page: string; ad: string | null }

		const results: Array<{ key: string; value: Result }> = []

		const clicks = app.stream('clicks', { key: codec.string(), value: codec.json<Click>() })
		const impressions = app.stream('impressions', { key: codec.string(), value: codec.json<Impression>() })

		clicks
			.leftJoin(
				impressions,
				(click, impression) => ({
					page: click.page,
					ad: impression?.ad ?? null,
				}),
				{ within: TimeWindows.of('5s') }
			)
			.peek((key, value) => {
				results.push({ key, value })
			})

		await app.start()
		const consumer = consumers[0]!

		// Click without matching impression - should emit with null
		await consumer.emitMessage('clicks', Buffer.from(JSON.stringify({ page: 'home' })), Buffer.from('user1'), 1000n)

		expect(results).toHaveLength(1)
		expect(results[0]).toEqual({ key: 'user1', value: { page: 'home', ad: null } })

		// Add impression, then click - should join
		await consumer.emitMessage(
			'impressions',
			Buffer.from(JSON.stringify({ ad: 'banner1' })),
			Buffer.from('user2'),
			2000n
		)
		await consumer.emitMessage(
			'clicks',
			Buffer.from(JSON.stringify({ page: 'products' })),
			Buffer.from('user2'),
			2500n
		)

		// Exactly 2 results: click without match (null ad) + click with matching impression
		expect(results).toHaveLength(2)
		expect(results[1]).toEqual({ key: 'user2', value: { page: 'products', ad: 'banner1' } })

		await app.close()
	})

	it('outer joins streams - both sides emit', async () => {
		const { app, consumers } = createTestApp()

		type Click = { page: string }
		type Impression = { ad: string }
		type Result = { page: string | null; ad: string | null }

		const results: Array<{ key: string; value: Result }> = []

		const clicks = app.stream('clicks', { key: codec.string(), value: codec.json<Click>() })
		const impressions = app.stream('impressions', { key: codec.string(), value: codec.json<Impression>() })

		clicks
			.outerJoin(
				impressions,
				(click, impression) => ({
					page: click?.page ?? null,
					ad: impression?.ad ?? null,
				}),
				{ within: TimeWindows.of('5s') }
			)
			.peek((key, value) => {
				results.push({ key, value })
			})

		await app.start()
		const consumer = consumers[0]!

		// Click alone - should emit with null ad
		await consumer.emitMessage('clicks', Buffer.from(JSON.stringify({ page: 'home' })), Buffer.from('user1'), 1000n)

		expect(results).toHaveLength(1)
		expect(results[0]).toEqual({ key: 'user1', value: { page: 'home', ad: null } })

		// Impression alone - should emit with null page
		await consumer.emitMessage(
			'impressions',
			Buffer.from(JSON.stringify({ ad: 'banner1' })),
			Buffer.from('user2'),
			2000n
		)

		expect(results).toHaveLength(2)
		expect(results[1]).toEqual({ key: 'user2', value: { page: null, ad: 'banner1' } })

		await app.close()
	})
})

describe('table-table joins', () => {
	it('inner joins two tables by key', async () => {
		const { app, consumers } = createTestApp()

		type User = { name: string }
		type Account = { balance: number }
		type Result = { name: string; balance: number }

		const results: Array<{ key: string; value: Result }> = []

		const users = app.table('users', { key: codec.string(), value: codec.json<User>() })
		const accounts = app.table('accounts', { key: codec.string(), value: codec.json<Account>() })

		users
			.join(accounts, (user, account) => ({
				name: user.name,
				balance: account.balance,
			}))
			.toStream()
			.peek((key, value) => {
				results.push({ key, value })
			})

		await app.start()
		const consumer = consumers[0]!

		// Add user without account - no join result
		await consumer.emitMessage('users', Buffer.from(JSON.stringify({ name: 'Alice' })), Buffer.from('user1'))
		expect(results).toHaveLength(0)

		// Add account for user1 - now we get a join result
		await consumer.emitMessage('accounts', Buffer.from(JSON.stringify({ balance: 100 })), Buffer.from('user1'))
		expect(results).toHaveLength(1)
		expect(results[0]).toEqual({ key: 'user1', value: { name: 'Alice', balance: 100 } })

		// Update user - join result with existing account
		await consumer.emitMessage(
			'users',
			Buffer.from(JSON.stringify({ name: 'Alice Updated' })),
			Buffer.from('user1')
		)
		expect(results).toHaveLength(2)
		expect(results[1]).toEqual({ key: 'user1', value: { name: 'Alice Updated', balance: 100 } })

		await app.close()
	})

	it('left joins tables - left side always emits', async () => {
		const { app, consumers } = createTestApp()

		type User = { name: string }
		type Account = { balance: number }
		type Result = { name: string; balance: number | null }

		const results: Array<{ key: string; value: Result }> = []

		const users = app.table('users', { key: codec.string(), value: codec.json<User>() })
		const accounts = app.table('accounts', { key: codec.string(), value: codec.json<Account>() })

		users
			.leftJoin(accounts, (user, account) => ({
				name: user.name,
				balance: account?.balance ?? null,
			}))
			.toStream()
			.peek((key, value) => {
				results.push({ key, value })
			})

		await app.start()
		const consumer = consumers[0]!

		// Add user without account - left join still emits
		await consumer.emitMessage('users', Buffer.from(JSON.stringify({ name: 'Alice' })), Buffer.from('user1'))
		expect(results).toHaveLength(1)
		expect(results[0]).toEqual({ key: 'user1', value: { name: 'Alice', balance: null } })

		// Add account - join now includes balance
		await consumer.emitMessage('accounts', Buffer.from(JSON.stringify({ balance: 100 })), Buffer.from('user1'))
		expect(results).toHaveLength(2)
		expect(results[1]).toEqual({ key: 'user1', value: { name: 'Alice', balance: 100 } })

		await app.close()
	})

	it('outer joins tables - both sides emit', async () => {
		const { app, consumers } = createTestApp()

		type User = { name: string }
		type Account = { balance: number }
		type Result = { name: string | null; balance: number | null }

		const results: Array<{ key: string; value: Result }> = []

		const users = app.table('users', { key: codec.string(), value: codec.json<User>() })
		const accounts = app.table('accounts', { key: codec.string(), value: codec.json<Account>() })

		users
			.outerJoin(accounts, (user, account) => ({
				name: user?.name ?? null,
				balance: account?.balance ?? null,
			}))
			.toStream()
			.peek((key, value) => {
				results.push({ key, value })
			})

		await app.start()
		const consumer = consumers[0]!

		// Add user without account - emits with null balance
		await consumer.emitMessage('users', Buffer.from(JSON.stringify({ name: 'Alice' })), Buffer.from('user1'))
		expect(results).toHaveLength(1)
		expect(results[0]).toEqual({ key: 'user1', value: { name: 'Alice', balance: null } })

		// Add account without user - emits with null name
		await consumer.emitMessage('accounts', Buffer.from(JSON.stringify({ balance: 200 })), Buffer.from('user2'))
		expect(results).toHaveLength(2)
		expect(results[1]).toEqual({ key: 'user2', value: { name: null, balance: 200 } })

		// Add user2 - now both sides have data
		await consumer.emitMessage('users', Buffer.from(JSON.stringify({ name: 'Bob' })), Buffer.from('user2'))
		expect(results).toHaveLength(3)
		expect(results[2]).toEqual({ key: 'user2', value: { name: 'Bob', balance: 200 } })

		await app.close()
	})
})

describe('table aggregations (KGroupedTable)', () => {
	it('counts by grouped key using table.groupBy().count()', async () => {
		const { app, consumers } = createTestApp()

		type Item = { category: string; name: string }

		const results: Array<{ key: string; count: number }> = []

		app.table('items', { key: codec.string(), value: codec.json<Item>() })
			.groupBy((_key, value) => [value.category, value] as const, { key: codec.string() })
			.count()
			.toStream()
			.peek((key, count) => {
				results.push({ key, count })
			})

		await app.start()
		const consumer = consumers[0]!

		// Add items with different categories
		await consumer.emitMessage(
			'items',
			Buffer.from(JSON.stringify({ category: 'books', name: 'Book 1' })),
			Buffer.from('item1')
		)
		await consumer.emitMessage(
			'items',
			Buffer.from(JSON.stringify({ category: 'electronics', name: 'Phone' })),
			Buffer.from('item2')
		)
		await consumer.emitMessage(
			'items',
			Buffer.from(JSON.stringify({ category: 'books', name: 'Book 2' })),
			Buffer.from('item3')
		)

		expect(results).toHaveLength(3)
		expect(results[0]).toEqual({ key: 'books', count: 1 })
		expect(results[1]).toEqual({ key: 'electronics', count: 1 })
		expect(results[2]).toEqual({ key: 'books', count: 2 })

		await app.close()
	})

	it('aggregates by grouped key using table.groupBy().aggregate()', async () => {
		const { app, consumers } = createTestApp()

		type Order = { region: string; amount: number }
		type Stats = { total: number; count: number }

		const results: Array<{ key: string; stats: Stats }> = []

		app.table('orders', { key: codec.string(), value: codec.json<Order>() })
			.groupBy((_key, value) => [value.region, value] as const, { key: codec.string() })
			.aggregate<Stats>(
				() => ({ total: 0, count: 0 }),
				(_key, value, agg) => ({
					total: agg.total + value.amount,
					count: agg.count + 1,
				}),
				{ value: codec.json<Stats>() }
			)
			.toStream()
			.peek((key, stats) => {
				results.push({ key, stats })
			})

		await app.start()
		const consumer = consumers[0]!

		await consumer.emitMessage(
			'orders',
			Buffer.from(JSON.stringify({ region: 'US', amount: 100 })),
			Buffer.from('o1')
		)
		await consumer.emitMessage(
			'orders',
			Buffer.from(JSON.stringify({ region: 'EU', amount: 200 })),
			Buffer.from('o2')
		)
		await consumer.emitMessage(
			'orders',
			Buffer.from(JSON.stringify({ region: 'US', amount: 150 })),
			Buffer.from('o3')
		)

		expect(results).toHaveLength(3)
		expect(results[0]).toEqual({ key: 'US', stats: { total: 100, count: 1 } })
		expect(results[1]).toEqual({ key: 'EU', stats: { total: 200, count: 1 } })
		expect(results[2]).toEqual({ key: 'US', stats: { total: 250, count: 2 } })

		await app.close()
	})
})

describe('aggregations', () => {
	it('counts records per key with groupByKey().count()', async () => {
		const { app, consumers } = createTestApp()

		const results: Array<{ key: string; count: number }> = []

		app.stream('events', { key: codec.string(), value: codec.json<{ type: string }>() })
			.groupByKey()
			.count()
			.toStream()
			.peek((key, count) => {
				results.push({ key, count })
			})

		await app.start()
		const consumer = consumers[0]!

		// Send events with keys
		await consumer.emitMessage('events', Buffer.from(JSON.stringify({ type: 'click' })), Buffer.from('user1'))
		await consumer.emitMessage('events', Buffer.from(JSON.stringify({ type: 'click' })), Buffer.from('user1'))
		await consumer.emitMessage('events', Buffer.from(JSON.stringify({ type: 'click' })), Buffer.from('user2'))
		await consumer.emitMessage('events', Buffer.from(JSON.stringify({ type: 'click' })), Buffer.from('user1'))

		// Verify counts
		expect(results).toHaveLength(4)
		expect(results[0]).toEqual({ key: 'user1', count: 1 })
		expect(results[1]).toEqual({ key: 'user1', count: 2 })
		expect(results[2]).toEqual({ key: 'user2', count: 1 })
		expect(results[3]).toEqual({ key: 'user1', count: 3 })

		await app.close()
	})

	it('reduces records per key with groupByKey().reduce()', async () => {
		const { app, consumers } = createTestApp()

		type Order = { amount: number }
		const results: Array<{ key: string; total: Order }> = []

		app.stream('orders', { key: codec.string(), value: codec.json<Order>() })
			.groupByKey()
			.reduce((agg, value) => ({ amount: agg.amount + value.amount }))
			.toStream()
			.peek((key, total) => {
				results.push({ key, total })
			})

		await app.start()
		const consumer = consumers[0]!

		await consumer.emitMessage('orders', Buffer.from(JSON.stringify({ amount: 100 })), Buffer.from('user1'))
		await consumer.emitMessage('orders', Buffer.from(JSON.stringify({ amount: 50 })), Buffer.from('user1'))
		await consumer.emitMessage('orders', Buffer.from(JSON.stringify({ amount: 200 })), Buffer.from('user2'))
		await consumer.emitMessage('orders', Buffer.from(JSON.stringify({ amount: 75 })), Buffer.from('user1'))

		expect(results).toHaveLength(4)
		expect(results[0]).toEqual({ key: 'user1', total: { amount: 100 } })
		expect(results[1]).toEqual({ key: 'user1', total: { amount: 150 } })
		expect(results[2]).toEqual({ key: 'user2', total: { amount: 200 } })
		expect(results[3]).toEqual({ key: 'user1', total: { amount: 225 } })

		await app.close()
	})

	it('aggregates records per key with groupByKey().aggregate()', async () => {
		const { app, consumers } = createTestApp()

		type Event = { type: string; count: number }
		type Stats = { total: number; max: number }
		const results: Array<{ key: string; stats: Stats }> = []

		app.stream('events', { key: codec.string(), value: codec.json<Event>() })
			.groupByKey()
			.aggregate<Stats>(
				() => ({ total: 0, max: 0 }),
				(_key, value, agg) => ({
					total: agg.total + value.count,
					max: Math.max(agg.max, value.count),
				}),
				{ value: codec.json<Stats>() }
			)
			.toStream()
			.peek((key, stats) => {
				results.push({ key, stats })
			})

		await app.start()
		const consumer = consumers[0]!

		await consumer.emitMessage('events', Buffer.from(JSON.stringify({ type: 'a', count: 5 })), Buffer.from('k1'))
		await consumer.emitMessage('events', Buffer.from(JSON.stringify({ type: 'b', count: 10 })), Buffer.from('k1'))
		await consumer.emitMessage('events', Buffer.from(JSON.stringify({ type: 'a', count: 3 })), Buffer.from('k1'))

		expect(results).toHaveLength(3)
		expect(results[0]).toEqual({ key: 'k1', stats: { total: 5, max: 5 } })
		expect(results[1]).toEqual({ key: 'k1', stats: { total: 15, max: 10 } })
		expect(results[2]).toEqual({ key: 'k1', stats: { total: 18, max: 10 } })

		await app.close()
	})

	it('groups by a different key with groupBy()', async () => {
		const { app, consumers } = createTestApp()

		type Event = { category: string; value: number }
		const results: Array<{ key: string; count: number }> = []

		app.stream('events', { key: codec.string(), value: codec.json<Event>() })
			.groupBy((_key, value) => value.category, { key: codec.string() })
			.count()
			.toStream()
			.peek((key, count) => {
				results.push({ key, count })
			})

		await app.start()
		const consumer = consumers[0]!

		await consumer.emitMessage(
			'events',
			Buffer.from(JSON.stringify({ category: 'electronics', value: 100 })),
			Buffer.from('item1')
		)
		await consumer.emitMessage(
			'events',
			Buffer.from(JSON.stringify({ category: 'books', value: 20 })),
			Buffer.from('item2')
		)
		await consumer.emitMessage(
			'events',
			Buffer.from(JSON.stringify({ category: 'electronics', value: 200 })),
			Buffer.from('item3')
		)

		expect(results).toHaveLength(3)
		expect(results[0]).toEqual({ key: 'electronics', count: 1 })
		expect(results[1]).toEqual({ key: 'books', count: 1 })
		expect(results[2]).toEqual({ key: 'electronics', count: 2 })

		await app.close()
	})

	it('skips records with null keys in aggregations', async () => {
		const { app, consumers } = createTestApp()

		const results: Array<{ key: string; count: number }> = []

		app.stream('events', { key: codec.string(), value: codec.json<{ type: string }>() })
			.groupByKey()
			.count()
			.toStream()
			.peek((key, count) => {
				results.push({ key, count })
			})

		await app.start()
		const consumer = consumers[0]!

		// Message with key
		await consumer.emitMessage('events', Buffer.from(JSON.stringify({ type: 'a' })), Buffer.from('user1'))
		// Message without key (null) - should be skipped
		await consumer.emitMessage('events', Buffer.from(JSON.stringify({ type: 'b' })), null)
		// Another message with key
		await consumer.emitMessage('events', Buffer.from(JSON.stringify({ type: 'c' })), Buffer.from('user1'))

		expect(results).toHaveLength(2)
		expect(results[0]).toEqual({ key: 'user1', count: 1 })
		expect(results[1]).toEqual({ key: 'user1', count: 2 })

		await app.close()
	})
})
