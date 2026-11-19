---
layout: home

hero:
    name: kafkats
    text: Pure TypeScript Kafka Client
    tagline: High-performance Kafka client and streams library with zero native dependencies
    image:
        src: /logo.svg
        alt: kafkats logo
    actions:
        - theme: brand
          text: Get Started
          link: /guide/
        - theme: alt
          text: View on GitHub
          link: https://github.com/chrisrecalis/kafkats

features:
    - icon: ⚡
      title: Pure Protocol Implementation
      details: No wrappers around librdkafka. Direct Kafka wire protocol implementation in TypeScript for maximum control and transparency.
    - icon: 🌊
      title: Kafka Streams-like DSL
      details: Familiar APIs for stream processing with KStream, KTable, windowing, aggregations, and joins.
    - icon: 🔒
      title: Type-Safe Codecs
      details: Strong typing from producer to consumer with built-in codecs and Zod validation support.
    - icon: 🎯
      title: Exactly-Once Semantics
      details: Full transactional support for idempotent producers and exactly-once processing guarantees.
    - icon: 💾
      title: Pluggable State Stores
      details: In-memory and LMDB backends for stateful stream processing with windowed aggregations.
    - icon: 🚀
      title: Modern TypeScript
      details: ESM-first, async/await, full TypeScript support with comprehensive types and tree-shaking.
---

<script setup>
import { withBase } from 'vitepress'
</script>

<div class="vp-doc" style="padding: 0 24px;">

## Packages

<div class="packages-grid">

<a :href="withBase('/client/')" class="package-card">
  <h3>@kafkats/client</h3>
  <p>Core Kafka client with producer, consumer, SASL authentication, and low-level protocol access.</p>
</a>

<a :href="withBase('/flow/')" class="package-card">
  <h3>@kafkats/flow</h3>
  <p>Kafka Streams-like DSL with KStream, KTable, windowing, aggregations, and joins.</p>
</a>

<a :href="withBase('/flow-codec-zod/')" class="package-card">
  <h3>@kafkats/codec-zod</h3>
  <p>Zod schema validation for type-safe message encoding and decoding.</p>
</a>

<a :href="withBase('/flow-state-lmdb/')" class="package-card">
  <h3>@kafkats/flow-state-lmdb</h3>
  <p>LMDB-backed persistent state stores for production stream processing.</p>
</a>

<a :href="withBase('/flow-state-rocksdb/')" class="package-card">
  <h3>@kafkats/flow-state-rocksdb</h3>
  <p>RocksDB-backed persistent state stores for write-heavy and larger-than-memory state.</p>
</a>

</div>

## Quick Example

```typescript
import { KafkaClient } from '@kafkats/client'

const client = new KafkaClient({
	clientId: 'my-app',
	brokers: ['localhost:9092'],
})

// Producer
const producer = client.producer()
await producer.send('events', [{ value: JSON.stringify({ type: 'click', page: '/home' }) }])

// Consumer
const consumer = client.consumer({ groupId: 'my-group' })
await consumer.runEach('events', async message => {
	console.log('Received:', message.value.toString())
})
```

</div>
