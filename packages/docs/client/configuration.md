# Configuration

## KafkaClient Configuration

```typescript
import { KafkaClient } from '@kafkats/client'

const client = new KafkaClient({
	// Required
	clientId: 'my-app',
	brokers: ['broker1:9092', 'broker2:9092'],

	// Optional
	tls: { enabled: true },
	sasl: { mechanism: 'PLAIN', username: 'user', password: 'pass' },
	connectionTimeoutMs: 10000,
	requestTimeoutMs: 30000,
})
```

### Options Reference

| Option                      | Type                                     | Default  | Description                                      |
| --------------------------- | ---------------------------------------- | -------- | ------------------------------------------------ |
| `clientId`                  | `string`                                 | -        | Required. Client identifier shown in broker logs |
| `brokers`                   | `string[]`                               | -        | Required. Bootstrap broker addresses             |
| `requestTimeoutMs`          | `number`                                 | `30000`  | Request timeout (ms)                             |
| `connectionTimeoutMs`       | `number`                                 | `10000`  | Connection timeout (ms)                          |
| `metadataRefreshIntervalMs` | `number`                                 | `300000` | How often to refresh cluster metadata (ms)       |
| `maxInFlightRequests`       | `number`                                 | `5`      | Max in-flight requests per broker connection     |
| `tls`                       | `TlsConfig`                              | -        | TLS configuration (omit for plaintext)           |
| `sasl`                      | `SaslConfig`                             | -        | SASL authentication configuration                |
| `logger`                    | `Logger`                                 | -        | Custom logger implementation                     |
| `logLevel`                  | `'debug' \| 'info' \| 'warn' \| 'error'` | `'info'` | Log level for the built-in logger                |

## Producer Configuration

```typescript
const producer = client.producer({
	acks: 'all',
	compression: 'gzip', // Snappy/LZ4/Zstd require codec registration
	lingerMs: 5,
	maxBatchBytes: 16384,
	retries: 3,
	idempotent: false,
	transactionalId: undefined,
})
```

### Options Reference

| Option                 | Type                                              | Default     | Description                |
| ---------------------- | ------------------------------------------------- | ----------- | -------------------------- |
| `acks`                 | `'all' \| 'leader' \| 'none'`                     | `'all'`     | Acknowledgment mode        |
| `compression`          | `'none' \| 'gzip' \| 'snappy' \| 'lz4' \| 'zstd'` | `'none'`    | Compression type           |
| `lingerMs`             | `number`                                          | `5`         | Batch wait time (ms)       |
| `maxBatchBytes`        | `number`                                          | `16384`     | Max batch size (bytes)     |
| `retries`              | `number`                                          | `3`         | Retry attempts             |
| `retryBackoffMs`       | `number`                                          | `100`       | Initial retry backoff (ms) |
| `maxRetryBackoffMs`    | `number`                                          | `1000`      | Max retry backoff (ms)     |
| `partitioner`          | `'murmur2' \| 'round-robin' \| Function`          | `'murmur2'` | Partitioning strategy      |
| `requestTimeoutMs`     | `number`                                          | `30000`     | Request timeout (ms)       |
| `idempotent`           | `boolean`                                         | `false`     | Enable idempotent producer |
| `maxInFlight`          | `number`                                          | `5`         | Max in-flight requests     |
| `transactionalId`      | `string`                                          | -           | Enable transactions        |
| `transactionTimeoutMs` | `number`                                          | `60000`     | Transaction timeout (ms)   |

### Acknowledgment Modes

| Value      | Meaning                       | Durability                        |
| ---------- | ----------------------------- | --------------------------------- |
| `'none'`   | Don't wait for acknowledgment | Lowest - may lose messages        |
| `'leader'` | Wait for leader to write      | Medium - may lose if leader fails |
| `'all'`    | Wait for all in-sync replicas | Highest - recommended             |

### Compression Options

| Type       | Speed   | Ratio | Notes                    |
| ---------- | ------- | ----- | ------------------------ |
| `'none'`   | Fastest | 1:1   | No compression           |
| `'gzip'`   | Slow    | Best  | Good for text            |
| `'snappy'` | Fast    | Good  | Balanced choice          |
| `'lz4'`    | Fastest | Good  | Best for high throughput |
| `'zstd'`   | Medium  | Best  | Modern, efficient        |

::: tip Register codecs
Gzip is built-in. Register codecs for `'snappy'`, `'lz4'`, or `'zstd'` before enabling them (see [Producer compression](/client/producer#compression)).
:::

## Consumer Configuration

```typescript
const consumer = client.consumer({
	groupId: 'my-group',
	sessionTimeoutMs: 30000,
	heartbeatIntervalMs: 3000,
	autoOffsetReset: 'latest',
	isolationLevel: 'read_committed',
})
```

### Options Reference

| Option                        | Type                                          | Default                | Description                   |
| ----------------------------- | --------------------------------------------- | ---------------------- | ----------------------------- |
| `groupId`                     | `string`                                      | -                      | Required. Consumer group ID   |
| `groupInstanceId`             | `string`                                      | -                      | Static membership ID          |
| `sessionTimeoutMs`            | `number`                                      | `30000`                | Session timeout (ms)          |
| `rebalanceTimeoutMs`          | `number`                                      | `60000`                | Rebalance timeout (ms)        |
| `heartbeatIntervalMs`         | `number`                                      | `3000`                 | Heartbeat interval (ms)       |
| `maxBytesPerPartition`        | `number`                                      | `1048576`              | Max fetch bytes per partition |
| `minBytes`                    | `number`                                      | `1`                    | Min bytes to fetch            |
| `maxWaitMs`                   | `number`                                      | `5000`                 | Max fetch wait time (ms)      |
| `autoOffsetReset`             | `'earliest' \| 'latest' \| 'none'`            | `'latest'`             | Offset reset strategy         |
| `isolationLevel`              | `'read_committed' \| 'read_uncommitted'`      | `'read_committed'`     | Transaction isolation         |
| `partitionAssignmentStrategy` | `'cooperative-sticky' \| 'sticky' \| 'range'` | `'cooperative-sticky'` | Assignment strategy           |

### Offset Reset Strategies

| Value        | Behavior                                    |
| ------------ | ------------------------------------------- |
| `'earliest'` | Start from beginning of topic               |
| `'latest'`   | Start from end of topic (new messages only) |
| `'none'`     | Throw error if no committed offset exists   |

### Isolation Levels

| Value                | Behavior                                  |
| -------------------- | ----------------------------------------- |
| `'read_committed'`   | Only see committed transactional messages |
| `'read_uncommitted'` | See all messages including uncommitted    |

## Environment-Based Configuration

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
