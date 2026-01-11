/**
 * SASL Authenticator - orchestrates SASL authentication flow
 */

import type { SaslConfig } from '@/network/types.js'
import type { SaslMechanism } from './sasl-mechanism.js'
import { createSaslMechanism } from './create-mechanism.js'
import { ApiKey } from '@/protocol/messages/api-keys.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { Encoder, Decoder } from '@/protocol/primitives/index.js'
import { encodeRequestHeader, decodeResponseHeader } from '@/protocol/messages/headers.js'
import { encodeSaslHandshakeRequest } from '@/protocol/messages/requests/sasl-handshake.js'
import { decodeSaslHandshakeResponse } from '@/protocol/messages/responses/sasl-handshake.js'
import { encodeSaslAuthenticateRequest } from '@/protocol/messages/requests/sasl-authenticate.js'
import { decodeSaslAuthenticateResponse } from '@/protocol/messages/responses/sasl-authenticate.js'
import {
	SaslAuthenticationError,
	UnsupportedSaslMechanismError,
	IllegalSaslStateError,
	KafkaProtocolError,
} from '@/client/errors.js'
import type { Logger } from '@/logger.js'

/**
 * Function to send a raw request and receive a raw response
 * Used during authentication before the normal request queue is active
 */
export type SendRawFn = (requestBuffer: Buffer) => Promise<Buffer>

export interface SaslAuthenticatorOptions {
	config: SaslConfig
	clientId: string
	brokerHost: string
	brokerPort: number
	logger?: Logger
	/** Function to send raw request and receive raw response */
	sendRaw: SendRawFn
}

export class SaslAuthenticator {
	private readonly config: SaslConfig
	private readonly clientId: string
	private readonly brokerHost: string
	private readonly brokerPort: number
	private readonly logger?: Logger
	private readonly sendRaw: SendRawFn
	private correlationId = 0
	private enabledMechanisms: string[] | null = null
	private _sessionLifetimeMs: bigint | undefined

	constructor(options: SaslAuthenticatorOptions) {
		this.config = options.config
		this.clientId = options.clientId
		this.brokerHost = options.brokerHost
		this.brokerPort = options.brokerPort
		this.logger = options.logger
		this.sendRaw = options.sendRaw
	}

	get sessionLifetimeMs(): bigint | undefined {
		return this._sessionLifetimeMs
	}

	/**
	 * Perform SASL authentication
	 */
	async authenticate(): Promise<void> {
		this.logger?.debug('starting SASL authentication', { mechanism: this.config.mechanism })

		await this.performHandshake()
		const mechanism = createSaslMechanism(this.config, {
			host: this.brokerHost,
			port: this.brokerPort,
			clientId: this.clientId,
		})
		await this.performAuthentication(mechanism)

		this.logger?.debug('SASL authentication successful', { mechanism: this.config.mechanism })
	}

	private async performHandshake(): Promise<void> {
		// Use v1 which is widely supported and returns enabled mechanisms
		const version = 1

		const requestBuffer = this.buildRequest(ApiKey.SaslHandshake, version, encoder => {
			encodeSaslHandshakeRequest(encoder, version, {
				mechanism: this.config.mechanism,
			})
		})

		const responseBuffer = await this.sendRaw(requestBuffer)
		const decoder = new Decoder(responseBuffer)

		decodeResponseHeader(decoder, ApiKey.SaslHandshake, version)
		const response = decodeSaslHandshakeResponse(decoder, version)

		if (response.errorCode === ErrorCode.UnsupportedSaslMechanism) {
			throw new UnsupportedSaslMechanismError(this.config.mechanism, response.enabledMechanisms)
		}

		if (response.errorCode === ErrorCode.IllegalSaslState) {
			throw new IllegalSaslStateError(
				`SASL handshake failed: illegal SASL state (mechanism: ${this.config.mechanism})`
			)
		}

		if (response.errorCode !== ErrorCode.None) {
			// Preserve the original Kafka error code for retriability semantics
			throw new KafkaProtocolError(
				response.errorCode,
				`SASL handshake failed (mechanism: ${this.config.mechanism})`
			)
		}

		this.logger?.debug('SASL handshake completed', {
			enabledMechanisms: response.enabledMechanisms,
		})
		this.enabledMechanisms = response.enabledMechanisms
	}

	private async performAuthentication(mechanism: SaslMechanism): Promise<void> {
		// Use v1 which is widely supported and includes session lifetime
		// v2 uses flexible encoding which older brokers may not support
		const version = 1
		const authGenerator = mechanism.authenticate()

		// First call to get initial auth bytes
		let result = await authGenerator.next()

		while (!result.done) {
			const authBytes = result.value

			this.logger?.debug('sending SASL authenticate', {
				bytesLength: authBytes.length,
			})

			const requestBuffer = this.buildRequest(ApiKey.SaslAuthenticate, version, encoder => {
				encodeSaslAuthenticateRequest(encoder, version, { authBytes })
			})

			const responseBuffer = await this.sendRaw(requestBuffer)
			const decoder = new Decoder(responseBuffer)

			decodeResponseHeader(decoder, ApiKey.SaslAuthenticate, version)
			const response = decodeSaslAuthenticateResponse(decoder, version)

			if (response.errorCode !== ErrorCode.None) {
				// Handle specific SASL error codes with their proper types
				if (response.errorCode === ErrorCode.IllegalSaslState) {
					throw new IllegalSaslStateError(
						response.errorMessage ?? `Illegal SASL state during ${mechanism.name} authentication`
					)
				}

				if (response.errorCode === ErrorCode.UnsupportedSaslMechanism) {
					throw new UnsupportedSaslMechanismError(mechanism.name, this.enabledMechanisms ?? [])
				}

				if (response.errorCode === ErrorCode.SaslAuthenticationFailed) {
					// Use SaslAuthenticationError for actual auth failures
					throw new SaslAuthenticationError(mechanism.name, response.errorMessage ?? undefined)
				}

				// For other error codes, preserve the original Kafka error for retriability semantics
				throw new KafkaProtocolError(
					response.errorCode,
					`${mechanism.name} authentication failed${response.errorMessage ? `: ${response.errorMessage}` : ''}`
				)
			}

			this._sessionLifetimeMs = response.sessionLifetimeMs

			// Pass server response to mechanism for next round
			result = await authGenerator.next(response.authBytes)
		}
	}

	/**
	 * Build a request buffer with header and payload
	 */
	private buildRequest(apiKey: ApiKey, apiVersion: number, encodePayload: (encoder: Encoder) => void): Buffer {
		const correlationId = this.correlationId++
		const encoder = new Encoder()

		// Leave space for 4-byte length prefix
		encoder.writeInt32(0)

		// Encode request header
		encodeRequestHeader(encoder, {
			apiKey,
			apiVersion,
			correlationId,
			clientId: this.clientId,
		})

		// Encode request body
		encodePayload(encoder)

		// Get buffer and fill in length prefix
		const buffer = encoder.toBuffer()
		const messageLength = buffer.length - 4
		buffer.writeInt32BE(messageLength, 0)

		return buffer
	}
}
