import { flow, type FlowApp, type FlowConfig } from '@kafkats/flow'
import { KafkaClient, type LogLevel, type Admin } from '@kafkats/client'

export function getBrokerAddress(): string {
	const addr = process.env.KAFKA_BROKER_ADDRESS
	if (!addr) throw new Error('KAFKA_BROKER_ADDRESS not set - is globalSetup running?')
	return addr
}

export function getLogLevel(): LogLevel {
	return (process.env.KAFKA_TS_LOG_LEVEL as LogLevel) ?? 'silent'
}

export function createClient(clientId: string): KafkaClient {
	return new KafkaClient({
		brokers: [getBrokerAddress()],
		clientId,
		logLevel: getLogLevel(),
	})
}

export function createFlowApp(config: Omit<FlowConfig, 'client'>): FlowApp {
	return flow({
		...config,
		client: {
			brokers: [getBrokerAddress()],
			clientId: `${config.applicationId}-client`,
			logLevel: getLogLevel(),
		},
	})
}

/**
 * Wait for topics to be ready (have leaders assigned)
 * Uses admin.describeTopics() to check partition leader status
 */
export async function waitForTopicsReady(
	admin: Admin,
	topics: string[],
	options?: { timeoutMs?: number; pollIntervalMs?: number }
): Promise<void> {
	const timeoutMs = options?.timeoutMs ?? 30000
	const pollIntervalMs = options?.pollIntervalMs ?? 500
	const startTime = Date.now()

	while (Date.now() - startTime < timeoutMs) {
		const descriptions = await admin.describeTopics(topics)

		const allReady = descriptions.every(topic => topic.partitions.every(partition => partition.leaderId >= 0))

		if (allReady && descriptions.length === topics.length) {
			return
		}

		await new Promise(resolve => setTimeout(resolve, pollIntervalMs))
	}

	throw new Error(`Topics not ready after ${timeoutMs}ms: ${topics.join(', ')}`)
}

/**
 * Clean up topics (best effort, ignores errors)
 */
export async function cleanupTopics(admin: Admin, topics: string[]): Promise<void> {
	try {
		await admin.deleteTopics(topics)
	} catch {
		// Ignore errors - topics may not exist or may be in use
	}
}

/**
 * Clean up consumer groups (best effort, ignores errors)
 */
export async function cleanupGroups(admin: Admin, groupIds: string[]): Promise<void> {
	try {
		await admin.deleteGroups(groupIds)
	} catch {
		// Ignore errors - groups may not exist or may have active members
	}
}
