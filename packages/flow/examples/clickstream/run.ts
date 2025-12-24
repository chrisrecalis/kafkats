/**
 * Clickstream Analytics - Main Entry Point
 *
 * Run with: pnpm clickstream
 *
 * This script:
 * 1. Creates required topics
 * 2. Seeds user profile data
 * 3. Starts the streaming topology
 */

import { flow } from '@kafkats/flow'
import { KafkaClient } from '@kafkats/client'
import type { UserProfile } from './types.js'
import { TOPICS } from './types.js'
import { buildTopology } from './topology.js'

const BROKERS = process.env.KAFKA_BROKERS?.split(',') ?? ['localhost:9092']

// Sample user profiles for enrichment
const USER_PROFILES: UserProfile[] = [
	{ userId: 'user-1', tier: 'premium', country: 'US', signupDate: '2024-01-15' },
	{ userId: 'user-2', tier: 'free', country: 'UK', signupDate: '2024-03-22' },
	{ userId: 'user-3', tier: 'premium', country: 'DE', signupDate: '2024-02-10' },
	{ userId: 'user-4', tier: 'free', country: 'FR', signupDate: '2024-04-05' },
	{ userId: 'user-5', tier: 'free', country: 'US', signupDate: '2024-05-18' },
	{ userId: 'user-6', tier: 'premium', country: 'CA', signupDate: '2024-01-28' },
	{ userId: 'user-7', tier: 'free', country: 'AU', signupDate: '2024-06-12' },
	{ userId: 'user-8', tier: 'free', country: 'US', signupDate: '2024-07-03' },
	{ userId: 'user-9', tier: 'premium', country: 'JP', signupDate: '2024-02-14' },
	{ userId: 'user-10', tier: 'free', country: 'BR', signupDate: '2024-08-20' },
]

async function sleep(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms))
}

async function createTopics(client: KafkaClient): Promise<void> {
	console.log('Creating topics...')

	const topics = Object.values(TOPICS).map(name => ({
		name,
		numPartitions: 3,
		replicationFactor: 1,
	}))

	try {
		await client.createTopics(topics)
		console.log(`Created ${topics.length} topics:`)
		for (const topic of topics) {
			console.log(`  - ${topic.name}`)
		}
	} catch (err) {
		// Topics may already exist
		const error = err as Error
		if (!error.message?.includes('TOPIC_ALREADY_EXISTS')) {
			throw err
		}
		console.log('Topics already exist')
	}
}

async function seedUserProfiles(client: KafkaClient): Promise<void> {
	console.log('')
	console.log('Seeding user profiles...')

	const producer = client.producer({ lingerMs: 0 })

	for (const profile of USER_PROFILES) {
		await producer.send(TOPICS.USER_PROFILES, {
			key: Buffer.from(profile.userId),
			value: Buffer.from(JSON.stringify(profile)),
		})
	}

	await producer.flush()
	await producer.disconnect()

	console.log(`Seeded ${USER_PROFILES.length} user profiles`)
}

async function main(): Promise<void> {
	console.log('=================================')
	console.log('  Clickstream Analytics Example')
	console.log('=================================')
	console.log('')
	console.log(`Brokers: ${BROKERS.join(', ')}`)
	console.log('')

	// Create admin client for topic management
	const adminClient = new KafkaClient({
		brokers: BROKERS,
		clientId: 'clickstream-admin',
	})
	await adminClient.connect()

	// Setup
	await createTopics(adminClient)
	await seedUserProfiles(adminClient)
	await adminClient.disconnect()

	// Wait for user profile table to be populated
	console.log('')
	console.log('Waiting for data to propagate...')
	await sleep(2000)

	// Create and configure the Flow application
	console.log('')
	console.log('Building streaming topology...')

	const app = flow({
		applicationId: 'clickstream-analytics',
		client: {
			brokers: BROKERS,
			clientId: 'clickstream-flow',
		},
		consumer: {
			autoOffsetReset: 'earliest', // Read user-profiles from beginning
		},
	})

	// Build the topology
	buildTopology(app)

	// Handle graceful shutdown
	process.on('SIGINT', async () => {
		console.log('')
		console.log('Shutting down...')
		await app.close()
		process.exit(0)
	})

	process.on('SIGTERM', async () => {
		await app.close()
		process.exit(0)
	})

	// Start the application
	console.log('')
	console.log('Starting topology...')
	await app.start()

	console.log('')
	console.log('=================================')
	console.log('  Topology is running!')
	console.log('=================================')
	console.log('')
	console.log('Input topics:')
	console.log(`  - ${TOPICS.WEB_EVENTS} (events from web clients)`)
	console.log(`  - ${TOPICS.USER_PROFILES} (user data table)`)
	console.log('')
	console.log('Output topics:')
	console.log(`  - ${TOPICS.USER_SESSIONS} (detected user sessions)`)
	console.log(`  - ${TOPICS.PAGE_METRICS} (page view counts per minute)`)
	console.log(`  - ${TOPICS.PREMIUM_EVENTS} (events from premium users)`)
	console.log(`  - ${TOPICS.FREE_EVENTS} (events from free users)`)
	console.log('')
	console.log('To generate events, run in another terminal:')
	console.log('  pnpm clickstream:produce')
	console.log('')
	console.log('Press Ctrl+C to stop')

	// Keep the process running
	await new Promise(() => {})
}

main().catch(err => {
	console.error('Fatal error:', err)
	process.exit(1)
})
