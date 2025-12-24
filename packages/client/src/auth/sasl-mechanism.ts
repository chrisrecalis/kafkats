/**
 * SASL mechanism interface and types
 */

/**
 * Configuration for SASL mechanisms
 */
export interface SaslMechanismConfig {
	username: string
	password: string
}

/**
 * Base interface for SASL mechanisms
 *
 * SASL mechanisms implement a challenge-response protocol.
 * The authenticate() method returns an async generator that:
 * - Yields authentication bytes to send to the server
 * - Receives server response bytes via next()
 * - Completes when authentication is finished
 */
export interface SaslMechanism {
	/** The mechanism name (e.g., 'PLAIN', 'SCRAM-SHA-256') */
	readonly name: string

	/**
	 * Authenticate using this mechanism
	 *
	 * @returns An async generator that yields auth bytes and receives server responses
	 */
	authenticate(): AsyncGenerator<Buffer, void, Buffer>
}
