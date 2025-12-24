/**
 * SASL PLAIN mechanism implementation
 *
 * PLAIN authentication sends credentials in a single message:
 * authzid NUL authcid NUL password
 *
 * Where:
 * - authzid is the authorization identity (empty for Kafka)
 * - authcid is the authentication identity (username)
 * - password is the password
 *
 * Format: \0username\0password
 */

import type { SaslMechanism, SaslMechanismConfig } from './sasl-mechanism.js'

export class PlainMechanism implements SaslMechanism {
	readonly name = 'PLAIN'
	private readonly config: SaslMechanismConfig

	constructor(config: SaslMechanismConfig) {
		this.config = config
	}

	// eslint-disable-next-line @typescript-eslint/require-await
	async *authenticate(): AsyncGenerator<Buffer, void, Buffer> {
		// PLAIN format: \0username\0password
		// authzid is empty, authcid is username
		const authString = `\0${this.config.username}\0${this.config.password}`
		const authBytes = Buffer.from(authString, 'utf-8')

		// Send auth bytes - PLAIN completes in a single round
		yield authBytes
		// Authentication complete after server accepts
	}
}
