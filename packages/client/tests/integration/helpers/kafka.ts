import { KafkaClient } from '@/client/index.js'
import type { LogLevel } from '@/logger.js'

export function requireKafkaBrokers(): string[] {
	const raw = process.env.KAFKA_BROKERS
	if (!raw) {
		throw new Error('Missing env var KAFKA_BROKERS (set by kafka.global-setup.ts)')
	}
	return raw.split(',').map(b => b.trim())
}

export function getLogLevel(): LogLevel {
	return (process.env.KAFKA_TS_LOG_LEVEL as LogLevel) ?? 'silent'
}

export function createClient(clientId: string): KafkaClient {
	return new KafkaClient({
		brokers: requireKafkaBrokers(),
		clientId,
		logLevel: getLogLevel(),
	})
}
