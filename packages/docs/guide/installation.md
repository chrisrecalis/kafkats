# Installation

## Requirements

- Node.js 18 or later
- A running Kafka broker (for integration)

## Installing the Client

Install the core client package:

::: code-group

```bash [pnpm]
pnpm add @kafkats/client
```

```bash [npm]
npm install @kafkats/client
```

```bash [yarn]
yarn add @kafkats/client
```

:::

## Installing Flow (Stream Processing)

For Kafka Streams-like processing, install the flow package:

::: code-group

```bash [pnpm]
pnpm add @kafkats/flow
```

```bash [npm]
npm install @kafkats/flow
```

```bash [yarn]
yarn add @kafkats/flow
```

:::

## Optional Packages

### Native CRC32C (Recommended)

For maximum producer/consumer throughput, install the optional native CRC32C implementation:

::: code-group

```bash [pnpm]
pnpm add @node-rs/crc32
```

```bash [npm]
npm install @node-rs/crc32
```

```bash [yarn]
yarn add @node-rs/crc32
```

:::

With native CRC32C enabled, kafkats can exceed the throughput of other popular Kafka clients.

If not installed, kafkats falls back to a pure TypeScript CRC32C implementation.

### Zod Codec

For schema validation with Zod:

```bash
pnpm add @kafkats/flow-codec-zod zod
```

### LMDB State Store

For persistent state in stream processing:

```bash
pnpm add @kafkats/flow-state-lmdb
```

::: warning Native Dependencies
The LMDB package includes native bindings. Make sure you have the necessary build tools installed on your system.
:::

## TypeScript Configuration

kafkats is written in TypeScript and includes type definitions. For the best experience, ensure your `tsconfig.json` includes:

```json
{
	"compilerOptions": {
		"module": "ESNext",
		"moduleResolution": "bundler",
		"strict": true,
		"esModuleInterop": true
	}
}
```

## Verifying Installation

Create a simple test file to verify the installation:

```typescript
import { KafkaClient } from '@kafkats/client'

const client = new KafkaClient({
	clientId: 'test',
	brokers: ['localhost:9092'],
})

console.log('kafkats installed successfully!')
```

Run it:

```bash
npx tsx test.ts
```
