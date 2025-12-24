# Getting Started

kafkats is a pure-protocol Kafka client and streams library for TypeScript. Unlike other Node.js Kafka clients that wrap librdkafka, kafkats implements the Kafka wire protocol directly in TypeScript.

## Why kafkats?

- **Pure TypeScript** - No native dependencies, works everywhere Node.js runs
- **Type-safe** - Full TypeScript support with comprehensive types
- **High performance** - Optimized for throughput with zero-copy operations
- **Modern** - ESM-first, async/await, tree-shakeable
- **Complete** - Producer, consumer, transactions, and stream processing

## Packages

| Package                                       | Description                                  |
| --------------------------------------------- | -------------------------------------------- |
| [@kafkats/client](/client/)                   | Core Kafka client with producer and consumer |
| [@kafkats/flow](/flow/)                       | Kafka Streams-like DSL for stream processing |
| [@kafkats/flow-codec-zod](/flow-codec-zod/)   | Zod schema validation codecs                 |
| [@kafkats/flow-state-lmdb](/flow-state-lmdb/) | LMDB-backed persistent state stores          |

## Next Steps

1. [Install the packages](/guide/installation)
2. [Follow the quick start tutorial](/guide/quick-start)
3. [Learn core concepts](/guide/concepts)
