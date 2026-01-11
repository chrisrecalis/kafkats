# Authentication

kafkats supports SASL authentication for secure Kafka connections.

## Supported Mechanisms

| Mechanism       | Description                      |
| --------------- | -------------------------------- |
| `PLAIN`         | Username/password (use with TLS) |
| `SCRAM-SHA-256` | Challenge-response, SHA-256      |
| `SCRAM-SHA-512` | Challenge-response, SHA-512      |
| `OAUTHBEARER`   | Bearer token (e.g. AWS MSK IAM)  |

## SASL Options

SASL config is a discriminated union keyed by `mechanism`.

### PLAIN / SCRAM

| Option      | Type                                 | Required | Description         |
| ----------- | ------------------------------------ | -------- | ------------------- |
| `mechanism` | `'PLAIN' \| 'SCRAM-SHA-256' \| 'SCRAM-SHA-512'` | Yes      | SASL mechanism name |
| `username`  | `string`                             | Yes      | SASL username       |
| `password`  | `string`                             | Yes      | SASL password       |

### OAUTHBEARER

| Option                | Type                                                                      | Required | Description |
| --------------------- | ------------------------------------------------------------------------- | -------- | ----------- |
| `mechanism`           | `'OAUTHBEARER'`                                                           | Yes      | SASL mechanism name |
| `oauthBearerProvider` | `(context) => ({ value, extensions? })`                                   | Yes      | Returns the bearer token (and optional extensions) for the broker |
| `reauthenticationThresholdMs` | `number`                                                           | No       | Reauthenticate when this many milliseconds remain of broker session lifetime (default: `10000`) |

`context` includes `{ host, port, clientId }`.

## SASL/PLAIN

Simple username/password authentication. Always use with TLS:

```typescript
import { KafkaClient } from '@kafkats/client'

const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['kafka.example.com:9093'],
	tls: { enabled: true },
	sasl: {
		mechanism: 'PLAIN',
		username: 'my-username',
		password: 'my-password',
	},
})
```

::: warning Security
PLAIN mechanism sends credentials in base64 (not encrypted). Always use TLS (`tls: { enabled: true }`) to protect credentials in transit.
:::

## SCRAM-SHA-256

More secure challenge-response authentication:

```typescript
const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['kafka.example.com:9093'],
	tls: { enabled: true },
	sasl: {
		mechanism: 'SCRAM-SHA-256',
		username: 'my-username',
		password: 'my-password',
	},
})
```

## SCRAM-SHA-512

Strongest built-in authentication:

```typescript
const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['kafka.example.com:9093'],
	tls: { enabled: true },
	sasl: {
		mechanism: 'SCRAM-SHA-512',
		username: 'my-username',
		password: 'my-password',
	},
})
```

## OAUTHBEARER

Provide a bearer token per broker connection via `oauthBearerProvider`. Tokens are commonly short-lived, so generate them on demand and refresh when needed.

If the broker has periodic reauthentication enabled (`connections.max.reauth.ms`), kafkats will automatically reauthenticate on the existing connection using `SaslAuthenticate` before the session expires.

```typescript
import { KafkaClient } from '@kafkats/client'

const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['kafka.example.com:9093'],
	tls: { enabled: true },
	sasl: {
		mechanism: 'OAUTHBEARER',
		oauthBearerProvider: async ({ host, port }) => {
			const value = await getTokenForBroker(`${host}:${port}`)
			return { value }
		},
	},
})
```

## TLS Configuration

### TLS Options

| Option               | Type                                          | Default | Description                  |
| -------------------- | --------------------------------------------- | ------- | ---------------------------- |
| `enabled`            | `boolean`                                     | `false` | Enables TLS when `true`      |
| `ca`                 | `string \| Buffer \| Array<string \| Buffer>` | -       | CA certificate(s)            |
| `cert`               | `string \| Buffer`                            | -       | Client certificate (mTLS)    |
| `key`                | `string \| Buffer`                            | -       | Client private key (mTLS)    |
| `passphrase`         | `string`                                      | -       | Private key passphrase       |
| `rejectUnauthorized` | `boolean`                                     | `true`  | Validate broker certificates |
| `servername`         | `string`                                      | -       | SNI server name              |

### Basic TLS

Use system CA certificates:

```typescript
const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['kafka.example.com:9093'],
	tls: { enabled: true },
})
```

### Custom CA Certificate

```typescript
import { readFileSync } from 'fs'

const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['kafka.example.com:9093'],
	tls: {
		enabled: true,
		ca: readFileSync('/path/to/ca.pem'),
	},
})
```

### Mutual TLS (mTLS)

Client certificate authentication:

```typescript
const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['kafka.example.com:9093'],
	tls: {
		enabled: true,
		ca: readFileSync('/path/to/ca.pem'),
		cert: readFileSync('/path/to/client.pem'),
		key: readFileSync('/path/to/client-key.pem'),
	},
})
```

### Disable Certificate Verification

::: danger Not for Production
Only use this for development/testing with self-signed certificates.
:::

```typescript
const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['localhost:9093'],
	tls: {
		enabled: true,
		rejectUnauthorized: false,
	},
})
```

## Environment Variables

Common pattern for configuration:

```typescript
const client = new KafkaClient({
	clientId: process.env.KAFKA_CLIENT_ID || 'my-app',
	brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
	tls: process.env.KAFKA_TLS_ENABLED === 'true' ? { enabled: true } : undefined,
	sasl:
		process.env.KAFKA_SASL_MECHANISM === 'OAUTHBEARER'
			? {
					mechanism: 'OAUTHBEARER',
					oauthBearerProvider: async () => {
						// For Amazon MSK IAM, install the AWS signer:
						//   pnpm add aws-msk-iam-sasl-signer-js
						const { generateAuthToken } = await import('aws-msk-iam-sasl-signer-js')
						const { token } = await generateAuthToken({ region: process.env.AWS_REGION! })
						return { value: token }
					},
				}
			: process.env.KAFKA_SASL_USERNAME
				? {
						mechanism: (process.env.KAFKA_SASL_MECHANISM || 'SCRAM-SHA-256') as 'SCRAM-SHA-256',
						username: process.env.KAFKA_SASL_USERNAME,
						password: process.env.KAFKA_SASL_PASSWORD!,
					}
				: undefined,
})
```

## Broker Configuration

### Confluent Cloud

```typescript
const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['xxx.confluent.cloud:9092'],
	tls: { enabled: true },
	sasl: {
		mechanism: 'PLAIN',
		username: process.env.CONFLUENT_API_KEY!,
		password: process.env.CONFLUENT_API_SECRET!,
	},
})
```

### Amazon MSK (IAM)

Amazon MSK IAM can be used via SASL `OAUTHBEARER` by returning a SigV4-based token from `oauthBearerProvider`.

References:

- [AWS MSK Developer Guide: Configure clients for IAM access control](https://docs.aws.amazon.com/msk/latest/developerguide/configure-clients-for-iam-access-control.html)
- [AWS MSK IAM SASL signer for JavaScript (`aws-msk-iam-sasl-signer-js`)](https://github.com/aws/aws-msk-iam-sasl-signer-js#getting-started)

```typescript
import { KafkaClient } from '@kafkats/client'

async function createMskIamToken(options: { region: string }): Promise<string> {
	// Recommended: use AWS' official MSK IAM SASL signer (Node.js)
	// Install:
	//   pnpm add aws-msk-iam-sasl-signer-js
	// (AWS also documents installing from GitHub:
	//   npm install https://github.com/aws/aws-msk-iam-sasl-signer-js)
	// It also supports fetching creds from a profile or role:
	//   generateAuthTokenFromProfile({ region, awsProfileName })
	//   generateAuthTokenFromRole({ region, awsRoleArn, awsRoleSessionName? })
	const { generateAuthToken } = await import('aws-msk-iam-sasl-signer-js')
	const { token, expiryTime } = await generateAuthToken({ region: options.region })
	// expiryTime is milliseconds since epoch (useful if you want to cache/refresh)
	return token
}

const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['b-1.msk.example.amazonaws.com:9098'],
	tls: { enabled: true },
	sasl: {
		mechanism: 'OAUTHBEARER',
		oauthBearerProvider: async () => ({ value: await createMskIamToken({ region: 'us-east-1' }) }),
	},
})
```

::: warning Token lifetime
MSK IAM tokens are short-lived. Generate them on demand inside `oauthBearerProvider` rather than hard-coding a static token.
:::

::: tip How the token is built
The AWS signer generates a SigV4 presigned URL for the `kafka-cluster` service with `Action=kafka-cluster:Connect`, then base64url-encodes it for use as the OAUTHBEARER token.
:::

### Redpanda

```typescript
const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['redpanda.example.com:9092'],
	tls: { enabled: true },
	sasl: {
		mechanism: 'SCRAM-SHA-256',
		username: 'user',
		password: 'password',
	},
})
```

## Troubleshooting

### Authentication Failed

```
Error: SASL authentication failed: Invalid credentials
```

- Verify username and password
- Check if the mechanism matches broker configuration
- Ensure the user has proper ACLs

### Connection Refused

```
Error: Connection refused
```

- Check if broker address is correct
- Verify TLS port (usually 9093) vs plaintext (9092)
- Check firewall rules

### Certificate Errors

```
Error: unable to verify the first certificate
```

- Add the CA certificate to your configuration
- Or set `rejectUnauthorized: false` for testing
