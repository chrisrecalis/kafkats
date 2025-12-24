# Authentication

kafkats supports SASL authentication for secure Kafka connections.

## Supported Mechanisms

| Mechanism       | Description                      |
| --------------- | -------------------------------- |
| `PLAIN`         | Username/password (use with TLS) |
| `SCRAM-SHA-256` | Challenge-response, SHA-256      |
| `SCRAM-SHA-512` | Challenge-response, SHA-512      |

## SASL Options

| Option      | Type                                            | Required | Description         |
| ----------- | ----------------------------------------------- | -------- | ------------------- |
| `mechanism` | `'PLAIN' \| 'SCRAM-SHA-256' \| 'SCRAM-SHA-512'` | Yes      | SASL mechanism name |
| `username`  | `string`                                        | Yes      | SASL username       |
| `password`  | `string`                                        | Yes      | SASL password       |

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
	sasl: process.env.KAFKA_SASL_USERNAME
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

For MSK with IAM authentication, you'll need to implement a custom SASL mechanism or use AWS-specific libraries. kafkats supports the standard SASL mechanisms listed above.

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
