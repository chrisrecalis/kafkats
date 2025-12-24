/**
 * Event Generator - Simulates web traffic for the clickstream example
 *
 * Run with: pnpm clickstream:produce
 */

import { KafkaClient } from '@kafkats/client'
import type { ClickEvent, PageViewEvent, WebEvent } from './types.js'
import { TOPICS } from './types.js'

const BROKERS = process.env.KAFKA_BROKERS?.split(',') ?? ['localhost:9092']
const EVENTS_PER_SECOND = Number(process.env.EVENTS_PER_SECOND) || 5

// Simulated users
const USERS = ['user-1', 'user-2', 'user-3', 'user-4', 'user-5', 'user-6', 'user-7', 'user-8', 'user-9', 'user-10']

// Pages in the website
const PAGES = ['/home', '/products', '/products/laptop', '/products/phone', '/checkout', '/about', '/contact', '/blog']

// Clickable elements
const ELEMENTS = ['nav-home', 'nav-products', 'add-to-cart', 'buy-now', 'subscribe', 'learn-more', 'close-modal']

function randomChoice<T>(arr: T[]): T {
	return arr[Math.floor(Math.random() * arr.length)]!
}

function createPageView(userId: string): PageViewEvent {
	const currentPage = randomChoice(PAGES)
	const hasReferrer = Math.random() > 0.3
	return {
		type: 'page_view',
		userId,
		page: currentPage,
		referrer: hasReferrer ? randomChoice(PAGES) : undefined,
		timestamp: Date.now(),
	}
}

function createClick(userId: string): ClickEvent {
	return {
		type: 'click',
		userId,
		elementId: randomChoice(ELEMENTS),
		page: randomChoice(PAGES),
		timestamp: Date.now(),
	}
}

function createEvent(): WebEvent {
	const userId = randomChoice(USERS)
	// 70% page views, 30% clicks
	return Math.random() > 0.3 ? createPageView(userId) : createClick(userId)
}

async function sleep(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms))
}

async function main(): Promise<void> {
	console.log('Clickstream Event Generator')
	console.log('===========================')
	console.log(`Brokers: ${BROKERS.join(', ')}`)
	console.log(`Events per second: ${EVENTS_PER_SECOND}`)
	console.log('')

	const client = new KafkaClient({
		brokers: BROKERS,
		clientId: 'clickstream-producer',
	})

	await client.connect()
	console.log('Connected to Kafka')

	const producer = client.producer({
		lingerMs: 100,
		maxBatchBytes: 16384,
	})

	const intervalMs = 1000 / EVENTS_PER_SECOND
	let eventCount = 0

	console.log('Generating events... Press Ctrl+C to stop')
	console.log('')

	// Handle graceful shutdown
	process.on('SIGINT', async () => {
		console.log('')
		console.log(`Shutting down... (${eventCount} events sent)`)
		await producer.flush()
		await producer.disconnect()
		await client.disconnect()
		process.exit(0)
	})

	// Generate events continuously
	while (true) {
		const event = createEvent()

		await producer.send(TOPICS.WEB_EVENTS, {
			key: Buffer.from(event.userId),
			value: Buffer.from(JSON.stringify(event)),
			timestamp: new Date(event.timestamp),
		})

		eventCount++

		// Log every 10 events
		if (eventCount % 10 === 0) {
			console.log(`[${new Date().toISOString()}] Sent ${eventCount} events`)
		}

		await sleep(intervalMs)
	}
}

main().catch(err => {
	console.error('Fatal error:', err)
	process.exit(1)
})
