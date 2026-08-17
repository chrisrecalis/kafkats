import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Broker } from '@/client/broker.js'
import { KafkaProtocolError, UnsupportedVersionError } from '@/client/errors.js'
import { ConnectionClosedError, NetworkError } from '@/network/errors.js'
import { ApiKey } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { Encoder } from '@/protocol/primitives/encoder.js'

/**
 * Create a mock Connection that can simulate various failure scenarios
 */
function createMockConnection() {
	let isConnected = false
	let sendHandler: ((apiKey: ApiKey, apiVersion: number, timeoutMs?: number) => Promise<Buffer>) | null = null

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
				async (
					apiKey: ApiKey,
					apiVersion: number,
					_encodePayload: (encoder: Encoder) => void,
					timeoutMs?: number
				) => {
					if (!isConnected) {
						throw new ConnectionClosedError('Connection closed')
					}
					if (sendHandler) {
						return sendHandler(apiKey, apiVersion, timeoutMs)
					}
					throw new Error('No send handler configured')
				}
			),
		sendNoResponse: vi
			.fn()
			.mockImplementation(
				async (_apiKey: ApiKey, _apiVersion: number, _encodePayload: (encoder: Encoder) => void) => {
					if (!isConnected) {
						throw new ConnectionClosedError('Connection closed')
					}
				}
			),
		setSendHandler(handler: (apiKey: ApiKey, apiVersion: number, timeoutMs?: number) => Promise<Buffer>) {
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

			// Report the preview Share APIs but not Metadata.
			controlConnection.setSendHandler(async apiKey => {
				if (apiKey === ApiKey.ApiVersions) {
					return buildApiVersionsResponse([
						{ apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
						{ apiKey: ApiKey.ShareFetch, minVersion: 1, maxVersion: 1 },
						{ apiKey: ApiKey.ShareAcknowledge, minVersion: 1, maxVersion: 1 },
					])
				}
				throw new Error(`Unexpected API key: ${apiKey}`)
			})

			await broker.connect()

			// Missing APIs and the Kafka 4.1 preview Share APIs are unsupported.
			expect(() => broker.getApiVersion(ApiKey.Metadata)).toThrow(UnsupportedVersionError)
			expect(() => broker.getApiVersion(ApiKey.ShareFetch)).toThrow(UnsupportedVersionError)
			expect(() => broker.getApiVersion(ApiKey.ShareAcknowledge)).toThrow(UnsupportedVersionError)
		})

		it('does not stay cached as connected when ApiVersions negotiation fails', async () => {
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

			// First ApiVersions request fails (e.g. transient network error), later ones succeed.
			let failNegotiation = true
			controlConnection.setSendHandler(async apiKey => {
				if (apiKey === ApiKey.ApiVersions) {
					if (failNegotiation) {
						failNegotiation = false
						throw new NetworkError('ECONNRESET during ApiVersions')
					}
					return buildApiVersionsResponse([
						{ apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
						{ apiKey: ApiKey.Metadata, minVersion: 0, maxVersion: 12 },
					])
				}
				throw new Error(`Unexpected API key: ${apiKey}`)
			})

			await expect(broker.connect()).rejects.toThrow(NetworkError)

			// A broker with failed negotiation must not present itself as connected —
			// its apiVersions map is empty and every request would throw a
			// non-retriable UnsupportedVersionError.
			expect(broker.isConnected).toBe(false)
			expect(controlConnection.close).toHaveBeenCalled()
			expect(fetchConnection.close).toHaveBeenCalled()

			// A subsequent connect() must retry negotiation cleanly and succeed.
			await broker.connect()
			expect(broker.isConnected).toBe(true)
			expect(broker.getApiVersion(ApiKey.Metadata)).toBeGreaterThanOrEqual(0)
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

	describe('broker-controlled wait timeouts', () => {
		it('allows group coordination to wait for the rebalance timeout plus the configured request timeout', async () => {
			const controlConnection = createMockConnection()
			const fetchConnection = createMockConnection()
			const broker = new Broker({
				host: 'localhost',
				port: 9092,
				nodeId: 1,
				clientId: 'test-client',
				requestTimeoutMs: 12000,
			})

			;(broker as unknown as { connection: typeof controlConnection }).connection = controlConnection
			;(broker as unknown as { fetchConnection: typeof fetchConnection }).fetchConnection = fetchConnection

			controlConnection.setSendHandler(async apiKey => {
				if (apiKey === ApiKey.ApiVersions) {
					return buildApiVersionsResponse([
						{ apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
						{ apiKey: ApiKey.Produce, minVersion: 9, maxVersion: 9 },
						{ apiKey: ApiKey.JoinGroup, minVersion: 5, maxVersion: 5 },
						{ apiKey: ApiKey.SyncGroup, minVersion: 3, maxVersion: 3 },
					])
				}
				throw new Error('Unexpected request')
			})

			await broker.connect()

			await expect(
				broker.joinGroup({
					groupId: 'test-group',
					sessionTimeoutMs: 30000,
					rebalanceTimeoutMs: 60000,
					memberId: '',
					protocolType: 'consumer',
					protocols: [],
				})
			).rejects.toThrow('Unexpected request')

			expect(controlConnection.send).toHaveBeenLastCalledWith(ApiKey.JoinGroup, 5, expect.any(Function), 72000)

			await expect(
				broker.syncGroup(
					{
						groupId: 'test-group',
						generationId: 1,
						memberId: 'test-member',
						assignments: [],
					},
					60000
				)
			).rejects.toThrow('Unexpected request')

			expect(controlConnection.send).toHaveBeenLastCalledWith(ApiKey.SyncGroup, 3, expect.any(Function), 72000)

			await expect(
				broker.produce({
					acks: -1,
					timeoutMs: 45000,
					topics: [],
				})
			).rejects.toThrow('Unexpected request')

			expect(controlConnection.send).toHaveBeenLastCalledWith(ApiKey.Produce, 9, expect.any(Function), 57000)
		})

		it('allows Fetch to wait for maxWaitMs plus the default request timeout', async () => {
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
						{ apiKey: ApiKey.Fetch, minVersion: 11, maxVersion: 11 },
						{ apiKey: ApiKey.ShareFetch, minVersion: 1, maxVersion: 2 },
						{ apiKey: ApiKey.ShareAcknowledge, minVersion: 1, maxVersion: 2 },
					])
				}
				throw new Error(`Unexpected API key: ${apiKey}`)
			})
			fetchConnection.setSendHandler(async () => {
				throw new Error('Unexpected request')
			})

			await broker.connect()

			await expect(
				broker.fetch({
					maxWaitMs: 45000,
					minBytes: 1,
					maxBytes: 1024,
					isolationLevel: 0,
					sessionId: 0,
					sessionEpoch: -1,
					topics: [],
					forgottenTopicsData: [],
					rackId: '',
				})
			).rejects.toThrow('Unexpected request')

			expect(fetchConnection.send).toHaveBeenLastCalledWith(ApiKey.Fetch, 11, expect.any(Function), 75000)

			await expect(
				broker.shareFetch({
					groupId: 'test-group',
					memberId: 'test-member',
					shareSessionEpoch: 0,
					maxWaitMs: 45000,
					minBytes: 1,
					maxBytes: 1024,
					maxRecords: 500,
					batchSize: 500,
					topics: [],
				})
			).rejects.toThrow('Unexpected request')

			expect(fetchConnection.send).toHaveBeenLastCalledWith(ApiKey.ShareFetch, 2, expect.any(Function), 75000)
		})
	})

	describe('produce with acks=0', () => {
		it('returns immediately when acks=0 (broker sends no response)', async () => {
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

			// ApiVersions during connect; after that, send should NEVER be awaited for produce
			// because acks=0 means the broker sends no response.
			controlConnection.setSendHandler(async apiKey => {
				if (apiKey === ApiKey.ApiVersions) {
					return buildApiVersionsResponse([
						{ apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
						{ apiKey: ApiKey.Metadata, minVersion: 0, maxVersion: 12 },
						{ apiKey: ApiKey.Produce, minVersion: 0, maxVersion: 9 },
					])
				}
				if (apiKey === ApiKey.Produce) {
					// Simulate the real broker: never resolve. If broker.produce
					// awaits this, the test will hang/timeout.
					return new Promise<Buffer>(() => {})
				}
				throw new Error(`Unexpected API key: ${apiKey}`)
			})

			await broker.connect()

			const produceCall = broker.produce({
				transactionalId: null,
				acks: 0,
				timeoutMs: 30000,
				topics: [
					{
						name: 'test-topic',
						partitions: [{ partitionIndex: 0, records: Buffer.alloc(0) }],
					},
				],
			})

			const result = await Promise.race([
				produceCall,
				new Promise<'TIMEOUT'>(resolve => setTimeout(() => resolve('TIMEOUT'), 100)),
			])
			expect(result).not.toBe('TIMEOUT')
			// Synthesized response: empty topics array is acceptable; no errors
			expect(typeof result).toBe('object')
		})
	})
})
