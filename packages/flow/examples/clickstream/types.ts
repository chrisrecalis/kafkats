// Raw web events from clients
export type PageViewEvent = {
	type: 'page_view'
	userId: string
	page: string
	referrer?: string
	timestamp: number
}

export type ClickEvent = {
	type: 'click'
	userId: string
	elementId: string
	page: string
	timestamp: number
}

export type WebEvent = PageViewEvent | ClickEvent

// User profile for enrichment
export type UserProfile = {
	userId: string
	tier: 'free' | 'premium'
	country: string
	signupDate: string
}

// Enriched event with user context
export type EnrichedEvent = WebEvent & {
	userTier: 'free' | 'premium'
	country: string
}

// Session summary output
export type SessionSummary = {
	userId: string
	sessionStart: number
	sessionEnd: number
	pageViews: number
	clicks: number
	pages: string[]
}

// Page metrics output
export type PageStats = {
	page: string
	windowStart: number
	viewCount: number
}

// Topics used in the example
export const TOPICS = {
	WEB_EVENTS: 'web-events',
	USER_PROFILES: 'user-profiles',
	USER_SESSIONS: 'user-sessions',
	PAGE_METRICS: 'page-metrics',
	PREMIUM_EVENTS: 'premium-events',
	FREE_EVENTS: 'free-events',
} as const
