import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Broker } from '@/client/broker.js'
import { KafkaProtocolError, UnsupportedVersionError } from '@/client/errors.js'
import { ConnectionClosedError, NetworkError } from '@/network/errors.js'
import { ApiKey } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { Encoder } from '@/protocol/primitives/encoder.js'
import { encodeApiVersionsResponse } from '@/protocol/messages/responses/api-versions.js'
import { encodeMetadataResponse } from '@/protocol/messages/responses/metadata.js'
import { encodeProduceResponse } from '@/protocol/messages/responses/produce.js'

/**
 * Create a mock Connection that can simulate various failure scenarios
 */
function createMockConnection() {
	let isConnected = false
	let sendHandler: ((apiKey: ApiKey, apiVersion: number) => Promise<Buffer>) | null = null

	const mockConnection = {
		get isConnected() {
			return isConnected
		},
		connect: vi.fn().mockImplementation(async () => {
			isConnected = true
		}),
		close: vi.fn().mockImplementation(async () => {
			isConnected = false
		}),
		send: vi
			.fn()
			.mockImplementation(
				async (apiKey: ApiKey, apiVersion: number, encodePayload: (encoder: Encoder) => void) => {
					if (!isConnected) {
						throw new ConnectionClosedError('Connection closed')
					}
					if (sendHandler) {
						return sendHandler(apiKey, apiVersion)
					}
					throw new Error('No send handler configured')
				}
			),
		setSendHandler(handler: (apiKey: ApiKey, apiVersion: number) => Promise<Buffer>) {
			sendHandler = handler
		},
		simulateDisconnect() {
			isConnected = false
		},
	}

	return mockConnection
}

/**
 * Build an ApiVersions response buffer
 */
function buildApiVersionsResponse(versions: Array<{ apiKey: ApiKey; minVersion: number; maxVersion: number }>): Buffer {
	const encoder = new Encoder()
	// Correlation ID (will be stripped by Connection)
	encoder.writeInt32(0)
	// Error code
	encoder.writeInt16(ErrorCode.None)
	// API versions array - callback is (item, encoder)
	encoder.writeArray(versions, (v, e) => {
		e.writeInt16(v.apiKey)
		e.writeInt16(v.minVersion)
		e.writeInt16(v.maxVersion)
	})
	return encoder.toBuffer()
}

/**
 * Build a Metadata response buffer (v0-v8)
 */
function buildMetadataResponse(brokers: Array<{ nodeId: number; host: string; port: number }>): Buffer {
	const encoder = new Encoder()
	// Correlation ID
	encoder.writeInt32(0)
	// Brokers array - callback is (item, encoder)
	encoder.writeArray(brokers, (b, e) => {
		e.writeInt32(b.nodeId)
		e.writeString(b.host)
		e.writeInt32(b.port)
	})
	// Topics array (empty)
	encoder.writeArray([], () => {})
	return encoder.toBuffer()
}

/**
 * Build a Produce response buffer with error
 */
function buildProduceResponse(
	topics: Array<{
		name: string
		partitions: Array<{ partitionIndex: number; errorCode: ErrorCode; baseOffset: bigint }>
	}>
): Buffer {
	const encoder = new Encoder()
	// Correlation ID
	encoder.writeInt32(0)
	// Topics array - callback is (item, encoder)
	encoder.writeArray(topics, (t, e) => {
		e.writeString(t.name)
		e.writeArray(t.partitions, (p, pe) => {
			pe.writeInt32(p.partitionIndex)
			pe.writeInt16(p.errorCode)
			pe.writeInt64(p.baseOffset)
		})
	})
	return encoder.toBuffer()
}

describe('Broker', () => {
	beforeEach(() => {
		vi.clearAllMocks()
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	describe('connection handling', () => {
		it('connects both control and fetch connections on connect()', async () => {
			const controlConnection = createMockConnection()
			const fetchConnection = createMockConnection()

			const broker = new Broker({
				host: 'localhost',
				port: 9092,
				nodeId: 1,
				clientId: 'test-client',
			})

			// Inject mock connections
			;(broker as unknown as { connection: typeof controlConnection }).connection = controlConnection
			;(broker as unknown as { fetchConnection: typeof fetchConnection }).fetchConnection = fetchConnection

			// Set up ApiVersions response
			controlConnection.setSendHandler(async apiKey => {
				if (apiKey === ApiKey.ApiVersions) {
					return buildApiVersionsResponse([
						{ apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
						{ apiKey: ApiKey.Metadata, minVersion: 0, maxVersion: 12 },
						{ apiKey: ApiKey.Produce, minVersion: 0, maxVersion: 9 },
						{ apiKey: ApiKey.Fetch, minVersion: 0, maxVersion: 12 },
					])
				}
				throw new Error(`Unexpected API key: ${apiKey}`)
			})

			await broker.connect()

			expect(controlConnection.connect).toHaveBeenCalled()
			expect(fetchConnection.connect).toHaveBeenCalled()
			expect(broker.isConnected).toBe(true)
		})

		it('throws error when connect fails', async () => {
			const controlConnection = createMockConnection()
			const fetchConnection = createMockConnection()

			controlConnection.connect.mockRejectedValue(new Error('Connection refused'))

			const broker = new Broker({
				host: 'localhost',
				port: 9092,
				nodeId: 1,
				clientId: 'test-client',
			})

			;(broker as unknown as { connection: typeof controlConnection }).connection = controlConnection
			;(broker as unknown as { fetchConnection: typeof fetchConnection }).fetchConnection = fetchConnection

			await expect(broker.connect()).rejects.toThrow('Connection refused')
			expect(broker.isConnected).toBe(false)
		})

		it('clears API versions on disconnect', async () => {
			const controlConnection = createMockConnection()
			const fetchConnection = createMockConnection()

			const broker = new Broker({
				host: 'localhost',
				port: 9092,
				nodeId: 1,
				clientId: 'test-client',
			})

			;(broker as unknown as { connection: typeof controlConnection }).connection = controlConnection
			;(broker as unknown as { fetchConnection: typeof fetchConnection }).fetchConnection = fetchConnection

			controlConnection.setSendHandler(async apiKey => {
				if (apiKey === ApiKey.ApiVersions) {
					return buildApiVersionsResponse([
						{ apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
						{ apiKey: ApiKey.Metadata, minVersion: 0, maxVersion: 12 },
					])
				}
				throw new Error(`Unexpected API key: ${apiKey}`)
			})

			await broker.connect()
			expect(broker.getApiVersions().size).toBeGreaterThan(0)

			await broker.disconnect()
			expect(broker.getApiVersions().size).toBe(0)
			expect(broker.isConnected).toBe(false)
		})
	})

	describe('API version negotiation', () => {
		it('throws UnsupportedVersionError when API is not supported by broker', async () => {
			const controlConnection = createMockConnection()
			const fetchConnection = createMockConnection()

			const broker = new Broker({
				host: 'localhost',
				port: 9092,
				nodeId: 1,
				clientId: 'test-client',
			})

			;(broker as unknown as { connection: typeof controlConnection }).connection = controlConnection
			;(broker as unknown as { fetchConnection: typeof fetchConnection }).fetchConnection = fetchConnection

			// Only report ApiVersions, not Metadata
			controlConnection.setSendHandler(async apiKey => {
				if (apiKey === ApiKey.ApiVersions) {
					return buildApiVersionsResponse([{ apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 }])
				}
				throw new Error(`Unexpected API key: ${apiKey}`)
			})

			await broker.connect()

			// Trying to get version for unsupported API should throw
			expect(() => broker.getApiVersion(ApiKey.Metadata)).toThrow(UnsupportedVersionError)
		})

		it('throws KafkaProtocolError when ApiVersions returns error', async () => {
			const controlConnection = createMockConnection()
			const fetchConnection = createMockConnection()

			const broker = new Broker({
				host: 'localhost',
				port: 9092,
				nodeId: 1,
				clientId: 'test-client',
			})

			;(broker as unknown as { connection: typeof controlConnection }).connection = controlConnection
			;(broker as unknown as { fetchConnection: typeof fetchConnection }).fetchConnection = fetchConnection

			// Return error in ApiVersions response
			controlConnection.setSendHandler(async apiKey => {
				if (apiKey === ApiKey.ApiVersions) {
					const encoder = new Encoder()
					encoder.writeInt32(0) // correlation ID
					encoder.writeInt16(ErrorCode.ClusterAuthorizationFailed) // error
					encoder.writeArray([], () => {}) // empty versions
					return encoder.toBuffer()
				}
				throw new Error(`Unexpected API key: ${apiKey}`)
			})

			await expect(broker.connect()).rejects.toThrow(KafkaProtocolError)
		})
	})

	describe('request error propagation', () => {
		it('propagates ConnectionClosedError when connection drops during request', async () => {
			const controlConnection = createMockConnection()
			const fetchConnection = createMockConnection()

			const broker = new Broker({
				host: 'localhost',
				port: 9092,
				nodeId: 1,
				clientId: 'test-client',
			})

			;(broker as unknown as { connection: typeof controlConnection }).connection = controlConnection
			;(broker as unknown as { fetchConnection: typeof fetchConnection }).fetchConnection = fetchConnection

			controlConnection.setSendHandler(async apiKey => {
				if (apiKey === ApiKey.ApiVersions) {
					return buildApiVersionsResponse([
						{ apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
						{ apiKey: ApiKey.Metadata, minVersion: 0, maxVersion: 12 },
					])
				}
				if (apiKey === ApiKey.Metadata) {
					// Simulate connection drop during metadata request
					controlConnection.simulateDisconnect()
					throw new ConnectionClosedError('Connection lost during request')
				}
				throw new Error(`Unexpected API key: ${apiKey}`)
			})

			await broker.connect()

			await expect(broker.metadata({ topics: null, allowAutoTopicCreation: false })).rejects.toThrow(
				ConnectionClosedError
			)
		})

		it('propagates NetworkError when network fails', async () => {
			const controlConnection = createMockConnection()
			const fetchConnection = createMockConnection()

			const broker = new Broker({
				host: 'localhost',
				port: 9092,
				nodeId: 1,
				clientId: 'test-client',
			})

			;(broker as unknown as { connection: typeof controlConnection }).connection = controlConnection
			;(broker as unknown as { fetchConnection: typeof fetchConnection }).fetchConnection = fetchConnection

			controlConnection.setSendHandler(async apiKey => {
				if (apiKey === ApiKey.ApiVersions) {
					return buildApiVersionsResponse([
						{ apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
						{ apiKey: ApiKey.Metadata, minVersion: 0, maxVersion: 12 },
					])
				}
				if (apiKey === ApiKey.Metadata) {
					throw new NetworkError('ECONNRESET')
				}
				throw new Error(`Unexpected API key: ${apiKey}`)
			})

			await broker.connect()

			await expect(broker.metadata({ topics: null, allowAutoTopicCreation: false })).rejects.toThrow(NetworkError)
		})
	})

	describe('produce error handling', () => {
		it('propagates connection error during produce request', async () => {
			const controlConnection = createMockConnection()
			const fetchConnection = createMockConnection()

			const broker = new Broker({
				host: 'localhost',
				port: 9092,
				nodeId: 1,
				clientId: 'test-client',
			})

			;(broker as unknown as { connection: typeof controlConnection }).connection = controlConnection
			;(broker as unknown as { fetchConnection: typeof fetchConnection }).fetchConnection = fetchConnection

			controlConnection.setSendHandler(async apiKey => {
				if (apiKey === ApiKey.ApiVersions) {
					return buildApiVersionsResponse([
						{ apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
						{ apiKey: ApiKey.Produce, minVersion: 0, maxVersion: 9 },
					])
				}
				if (apiKey === ApiKey.Produce) {
					// Simulate connection drop during produce
					controlConnection.simulateDisconnect()
					throw new ConnectionClosedError('Connection lost during produce')
				}
				throw new Error(`Unexpected API key: ${apiKey}`)
			})

			await broker.connect()

			await expect(
				broker.produce({
					acks: -1,
					timeoutMs: 5000,
					transactionalId: null,
					topics: [
						{
							name: 'test-topic',
							partitions: [{ partitionIndex: 0, records: Buffer.alloc(0) }],
						},
					],
				})
			).rejects.toThrow(ConnectionClosedError)
		})
	})

	describe('fetch uses dedicated connection', () => {
		it('propagates connection error on fetch connection', async () => {
			const controlConnection = createMockConnection()
			const fetchConnection = createMockConnection()

			const broker = new Broker({
				host: 'localhost',
				port: 9092,
				nodeId: 1,
				clientId: 'test-client',
			})

			;(broker as unknown as { connection: typeof controlConnection }).connection = controlConnection
			;(broker as unknown as { fetchConnection: typeof fetchConnection }).fetchConnection = fetchConnection

			controlConnection.setSendHandler(async apiKey => {
				if (apiKey === ApiKey.ApiVersions) {
					return buildApiVersionsResponse([
						{ apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
						{ apiKey: ApiKey.Fetch, minVersion: 0, maxVersion: 12 },
					])
				}
				throw new Error('Control connection should not handle Fetch')
			})

			fetchConnection.setSendHandler(async apiKey => {
				if (apiKey === ApiKey.Fetch) {
					// Simulate fetch connection dropping
					fetchConnection.simulateDisconnect()
					throw new NetworkError('ETIMEDOUT')
				}
				throw new Error(`Unexpected API key on fetch connection: ${apiKey}`)
			})

			await broker.connect()

			await expect(
				broker.fetch({
					maxWaitMs: 100,
					minBytes: 1,
					maxBytes: 1024,
					isolationLevel: 0,
					sessionId: 0,
					sessionEpoch: -1,
					topics: [],
					forgottenTopicsData: [],
					rackId: '',
				})
			).rejects.toThrow(NetworkError)

			// Verify fetch used fetchConnection
			expect(fetchConnection.send).toHaveBeenCalled()
		})
	})
})
