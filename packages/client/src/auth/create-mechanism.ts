import type { SaslConfig, OAuthBearerProviderContext } from '@/network/types.js'
import type { SaslMechanism } from './sasl-mechanism.js'
import { PlainMechanism } from './plain.js'
import { ScramSha256Mechanism, ScramSha512Mechanism } from './scram.js'
import { OAuthBearerMechanism } from './oauthbearer.js'
import { SaslAuthenticationError } from '@/client/errors.js'

export type SaslMechanismContext = OAuthBearerProviderContext

export function createSaslMechanism(config: SaslConfig, context: SaslMechanismContext): SaslMechanism {
	switch (config.mechanism) {
		case 'PLAIN':
			return new PlainMechanism({ username: config.username, password: config.password })
		case 'SCRAM-SHA-256':
			return new ScramSha256Mechanism({ username: config.username, password: config.password })
		case 'SCRAM-SHA-512':
			return new ScramSha512Mechanism({ username: config.username, password: config.password })
		case 'OAUTHBEARER':
			return new OAuthBearerMechanism({ provider: config.oauthBearerProvider, context })
		default: {
			const mechanism = (config as unknown as { mechanism: string }).mechanism
			throw new SaslAuthenticationError(mechanism, `Unsupported SASL mechanism: ${mechanism}`)
		}
	}
}
