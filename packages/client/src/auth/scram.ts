/**
 * SASL SCRAM (Salted Challenge Response Authentication Mechanism) implementation
 *
 * Supports SCRAM-SHA-256 and SCRAM-SHA-512
 *
 * SCRAM is a multi-round challenge-response protocol:
 * 1. Client sends client-first-message
 * 2. Server responds with server-first-message (salt, iteration count, nonce)
 * 3. Client sends client-final-message (proof)
 * 4. Server responds with server-final-message (verifier)
 *
 * @see RFC 5802 - Salted Challenge Response Authentication Mechanism (SCRAM)
 */

import * as crypto from 'node:crypto'
import type { SaslMechanism, SaslMechanismConfig } from './sasl-mechanism.js'
import { SaslAuthenticationError } from '@/client/errors.js'

type ScramDigest = 'sha256' | 'sha512'

export class ScramMechanism implements SaslMechanism {
	readonly name: string
	private readonly digest: ScramDigest
	private readonly digestLength: number
	private readonly config: SaslMechanismConfig
	private readonly clientNonce: string

	constructor(config: SaslMechanismConfig, digest: ScramDigest) {
		this.config = config
		this.digest = digest
		this.digestLength = digest === 'sha256' ? 32 : 64
		this.name = digest === 'sha256' ? 'SCRAM-SHA-256' : 'SCRAM-SHA-512'
		this.clientNonce = crypto.randomBytes(24).toString('base64')
	}

	// eslint-disable-next-line @typescript-eslint/require-await
	async *authenticate(): AsyncGenerator<Buffer, void, Buffer> {
		const username = this.saslName(this.config.username)
		const clientFirstMessageBare = `n=${username},r=${this.clientNonce}`
		// gs2-header is "n,," for no channel binding
		const clientFirstMessage = `n,,${clientFirstMessageBare}`

		const serverFirstMessage = yield Buffer.from(clientFirstMessage, 'utf-8')
		const serverFirstStr = serverFirstMessage.toString('utf-8')
		const { serverNonce, salt, iterations } = this.parseServerFirst(serverFirstStr)

		// Validate server nonce starts with client nonce
		if (!serverNonce.startsWith(this.clientNonce)) {
			throw new SaslAuthenticationError(this.name, 'Server nonce does not start with client nonce')
		}

		const saltBuffer = Buffer.from(salt, 'base64')
		const saltedPassword = this.hi(this.config.password, saltBuffer, iterations)

		const clientKey = this.hmac(saltedPassword, 'Client Key')
		const storedKey = this.hash(clientKey)

		// Channel binding data - base64 of "n,," (no channel binding)
		const channelBinding = Buffer.from('n,,').toString('base64')
		const clientFinalMessageWithoutProof = `c=${channelBinding},r=${serverNonce}`

		const authMessage = `${clientFirstMessageBare},${serverFirstStr},${clientFinalMessageWithoutProof}`

		const clientSignature = this.hmac(storedKey, authMessage)
		const clientProof = this.xor(clientKey, clientSignature)

		const clientFinalMessage = `${clientFinalMessageWithoutProof},p=${clientProof.toString('base64')}`

		// Compute expected server signature for verification
		const serverKey = this.hmac(saltedPassword, 'Server Key')
		const expectedServerSignature = this.hmac(serverKey, authMessage)

		const serverFinalMessage = yield Buffer.from(clientFinalMessage, 'utf-8')
		const serverFinalStr = serverFinalMessage.toString('utf-8')
		const { serverSignature, error } = this.parseServerFinal(serverFinalStr)

		if (error) {
			throw new SaslAuthenticationError(this.name, error)
		}

		const actualServerSignature = Buffer.from(serverSignature, 'base64')
		if (!actualServerSignature.equals(expectedServerSignature)) {
			throw new SaslAuthenticationError(this.name, 'Server signature verification failed')
		}

		// Authentication complete
	}

	private parseServerFirst(message: string): {
		serverNonce: string
		salt: string
		iterations: number
	} {
		const parts = message.split(',')
		let serverNonce = ''
		let salt = ''
		let iterations = 0

		for (const part of parts) {
			if (part.startsWith('r=')) {
				serverNonce = part.slice(2)
			} else if (part.startsWith('s=')) {
				salt = part.slice(2)
			} else if (part.startsWith('i=')) {
				const parsed = parseInt(part.slice(2), 10)
				if (!Number.isNaN(parsed) && parsed > 0) {
					iterations = parsed
				}
			}
		}

		if (!serverNonce || !salt || iterations === 0) {
			throw new SaslAuthenticationError(this.name, 'Invalid server-first-message')
		}

		return { serverNonce, salt, iterations }
	}

	private parseServerFinal(message: string): {
		serverSignature: string
		error?: string
	} {
		// Server-final-message can have comma-separated extensions
		// Format: v=<signature>[,extensions] or e=<error>[,extensions]
		const parts = message.split(',')
		const firstPart = parts[0] ?? ''

		if (firstPart.startsWith('e=')) {
			return { serverSignature: '', error: firstPart.slice(2) }
		}

		if (firstPart.startsWith('v=')) {
			return { serverSignature: firstPart.slice(2) }
		}

		throw new SaslAuthenticationError(this.name, 'Invalid server-final-message')
	}

	/**
	 * Escape username per RFC 5802
	 * '=' becomes '=3D', ',' becomes '=2C'
	 */
	private saslName(name: string): string {
		return name.replace(/=/g, '=3D').replace(/,/g, '=2C')
	}

	/**
	 * Hi() function from RFC 5802 - PBKDF2 key derivation
	 */
	private hi(password: string, salt: Buffer, iterations: number): Buffer {
		return crypto.pbkdf2Sync(password, salt, iterations, this.digestLength, this.digest)
	}

	/**
	 * HMAC using the configured digest
	 */
	private hmac(key: Buffer, data: string | Buffer): Buffer {
		const hmac = crypto.createHmac(this.digest, key)
		hmac.update(data)
		return hmac.digest()
	}

	/**
	 * Hash using the configured digest
	 */
	private hash(data: Buffer): Buffer {
		const hash = crypto.createHash(this.digest)
		hash.update(data)
		return hash.digest()
	}

	/**
	 * XOR two buffers
	 */
	private xor(a: Buffer, b: Buffer): Buffer {
		const result = Buffer.alloc(a.length)
		for (let i = 0; i < a.length; i++) {
			result[i] = a[i]! ^ b[i]!
		}
		return result
	}
}

/**
 * SCRAM-SHA-256 mechanism
 */
export class ScramSha256Mechanism extends ScramMechanism {
	constructor(config: SaslMechanismConfig) {
		super(config, 'sha256')
	}
}

/**
 * SCRAM-SHA-512 mechanism
 */
export class ScramSha512Mechanism extends ScramMechanism {
	constructor(config: SaslMechanismConfig) {
		super(config, 'sha512')
	}
}
