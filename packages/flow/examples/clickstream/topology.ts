import { codec, TimeWindows, SessionWindows, type FlowApp, type Windowed, type KeyValue } from '@kafkats/flow'
import type { EnrichedEvent, PageStats, SessionSummary, UserProfile, WebEvent } from './types.js'
import { TOPICS } from './types.js'

/**
 * Builds the clickstream analytics topology.
 *
 * This topology demonstrates:
 * 1. Stream-table joins for user enrichment
 * 2. Session windows for user session detection
 * 3. Tumbling windows for page metrics
 * 4. Branching for priority routing
 */
export function buildTopology(app: FlowApp): void {
	// ============================================
	// Source: User profiles table for enrichment
	// ============================================
	const usersTable = app.table(TOPICS.USER_PROFILES, {
		key: codec.string(),
		value: codec.json<UserProfile>(),
	})

	// ============================================
	// Source: Web events stream
	// ============================================
	const events = app.stream(TOPICS.WEB_EVENTS, {
		key: codec.string(), // userId is the key
		value: codec.json<WebEvent>(),
	})

	// ============================================
	// Stream-Table Join: Enrich events with user data
	// ============================================
	const enrichedEvents = events.leftJoin(
		usersTable,
		(event, user): EnrichedEvent => ({
			...event,
			userTier: user?.tier ?? 'free',
			country: user?.country ?? 'unknown',
		})
	)

	// ============================================
	// Branching: Route by user tier
	// ============================================
	const [premiumEvents, freeEvents] = enrichedEvents.branch(
		(_, e) => e.userTier === 'premium',
		(_, e) => e.userTier === 'free'
	)

	// Premium users go to priority topic
	if (premiumEvents) {
		premiumEvents.to(TOPICS.PREMIUM_EVENTS, {
			key: codec.string(),
			value: codec.json<EnrichedEvent>(),
		})
	}

	// Free users go to standard topic
	if (freeEvents) {
		freeEvents.to(TOPICS.FREE_EVENTS, {
			key: codec.string(),
			value: codec.json<EnrichedEvent>(),
		})
	}

	// ============================================
	// Session Windows: Detect user sessions
	// ============================================
	// A session ends after 30 seconds of inactivity (short for demo purposes)
	enrichedEvents
		.groupByKey()
		.windowedBy(SessionWindows.withInactivityGap('30s'))
		.aggregate<SessionSummary>(
			() => ({
				userId: '',
				sessionStart: 0,
				sessionEnd: 0,
				pageViews: 0,
				clicks: 0,
				pages: [],
			}),
			(key, event, session) => ({
				userId: key,
				sessionStart: session.sessionStart || event.timestamp,
				sessionEnd: event.timestamp,
				pageViews: session.pageViews + (event.type === 'page_view' ? 1 : 0),
				clicks: session.clicks + (event.type === 'click' ? 1 : 0),
				pages:
					event.type === 'page_view' && !session.pages.includes(event.page)
						? [...session.pages, event.page]
						: session.pages,
			}),
			{ value: codec.json<SessionSummary>() }
		)
		.toStream()
		.map(
			(windowed: Windowed<string>, session): KeyValue<string, SessionSummary> => [
				windowed.key,
				{
					...session,
					sessionStart: windowed.window.start,
					sessionEnd: windowed.window.end,
				},
			]
		)
		.to(TOPICS.USER_SESSIONS, {
			key: codec.string(),
			value: codec.json<SessionSummary>(),
		})

	// ============================================
	// Tumbling Windows: Page view counts per minute
	// ============================================
	events
		.filter((_, e) => e.type === 'page_view')
		.groupBy((_, e) => (e as { page: string }).page, { key: codec.string() })
		.windowedBy(TimeWindows.of('1m'))
		.count()
		.toStream()
		.map(
			(windowed: Windowed<string>, count): KeyValue<string, PageStats> => [
				windowed.key,
				{
					page: windowed.key,
					windowStart: windowed.window.start,
					viewCount: count,
				},
			]
		)
		.to(TOPICS.PAGE_METRICS, {
			key: codec.string(),
			value: codec.json<PageStats>(),
		})

	console.log('Topology built successfully!')
	console.log('')
	console.log('Data flow:')
	console.log('  web-events')
	console.log('    |')
	console.log('    +--[join user-profiles]-->')
	console.log('    |     |')
	console.log('    |     +--[branch]-->  premium-events')
	console.log('    |     |               free-events')
	console.log('    |     |')
	console.log('    |     +--[session windows 30s]-->  user-sessions')
	console.log('    |')
	console.log('    +--[filter page_view]--[tumbling window 1m]-->  page-metrics')
}
