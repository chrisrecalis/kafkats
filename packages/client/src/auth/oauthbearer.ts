/**
 * SASL OAUTHBEARER mechanism implementation (RFC 7628)
 *
 * Kafka's OAUTHBEARER mechanism is commonly used for AWS MSK IAM (SigV4 token)
 * and other bearer-token auth systems.
 */

import type { OAuthBearerProviderContext, OAuthBearerToken } from '@/network/types.js'
import type { SaslMechanism } from './sasl-mechanism.js'

export interface OAuthBearerMechanismConfig {
	provider: (context: OAuthBearerProviderContext) => OAuthBearerToken | Promise<OAuthBearerToken>
	context: OAuthBearerProviderContext
}

function validateNoCtrlA(value: string, label: string): void {
	if (value.includes('\u0001')) {
		throw new Error(`${label} must not contain U+0001`)
	}
}

function validateExtension(key: string, value: string): void {
	validateNoCtrlA(key, 'extension key')
	validateNoCtrlA(value, 'extension value')
	if (key.includes('=')) {
		throw new Error('extension key must not contain "="')
	}
}

function buildClientFirstMessage(token: OAuthBearerToken): Buffer {
	validateNoCtrlA(token.value, 'token')

	const fields: string[] = [`auth=Bearer ${token.value}`]

	if (token.extensions) {
		for (const [key, value] of Object.entries(token.extensions)) {
			validateExtension(key, value)
			fields.push(`${key}=${value}`)
		}
	}

	// gs2-header: "n,," (no channel binding, client doesn't support authzid)
	// message is: "n,,\x01" + kv-fields separated by \x01 + "\x01\x01"
	const message = `n,,\u0001${fields.join('\u0001')}\u0001\u0001`
	return Buffer.from(message, 'utf8')
}

export class OAuthBearerMechanism implements SaslMechanism {
	readonly name = 'OAUTHBEARER'

	private readonly provider: OAuthBearerMechanismConfig['provider']
	private readonly context: OAuthBearerProviderContext

	constructor(config: OAuthBearerMechanismConfig) {
		this.provider = config.provider
		this.context = config.context
	}

	async *authenticate(): AsyncGenerator<Buffer, void, Buffer> {
		const token = await this.provider(this.context)
		const authBytes = buildClientFirstMessage(token)

		// Yield the initial client response, then complete after one round trip.
		// Successful auth typically returns empty authBytes.
		yield authBytes
	}
}
