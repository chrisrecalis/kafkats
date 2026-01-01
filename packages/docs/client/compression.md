# Compression

kafkats supports multiple compression algorithms for reducing network bandwidth and storage. Compression is applied at the RecordBatch level - the producer compresses batches before sending, and consumers automatically decompress.

## Quick Start

```typescript
import { KafkaClient, CompressionType, compressionCodecs, createSnappyCodec } from '@kafkats/client'
import snappy from 'snappy'

// Register a compression codec
compressionCodecs.register(CompressionType.Snappy, createSnappyCodec(snappy))

// Use compression in producer
const producer = client.producer({
	compression: 'snappy',
})
```

## Compression Types

| Type       | Speed     | Ratio | Built-in | Notes                        |
| ---------- | --------- | ----- | -------- | ---------------------------- |
| `'none'`   | Fastest   | 1:1   | Yes      | No compression               |
| `'gzip'`   | Slow      | Best  | Yes      | Uses Node.js zlib            |
| `'snappy'` | Fast      | Good  | No       | Balanced choice              |
| `'lz4'`    | Very fast | Good  | No       | Best for high throughput     |
| `'zstd'`   | Medium    | Best  | No       | Modern, efficient, versatile |

## Built-in Codecs

GZIP is built-in and requires no additional setup:

```typescript
const producer = client.producer({
	compression: 'gzip',
})
```

## Pluggable Compression Libraries

For Snappy, LZ4, and Zstd, you must install a compression library and register it. kafkats supports multiple libraries for each algorithm.

### Snappy

| Library    | Type    | Performance |
| ---------- | ------- | ----------- |
| `snappy`   | Native  | Fastest     |
| `snappyjs` | Pure JS | Good        |

#### snappy (Recommended)

```bash
npm install snappy
```

```typescript
import snappy from 'snappy'
import { CompressionType, compressionCodecs, createSnappyCodec } from '@kafkats/client'

compressionCodecs.register(CompressionType.Snappy, createSnappyCodec(snappy))
```

#### snappyjs

```bash
npm install snappyjs
```

```typescript
import * as SnappyJS from 'snappyjs'
import { CompressionType, compressionCodecs, createSnappyCodec } from '@kafkats/client'

compressionCodecs.register(CompressionType.Snappy, createSnappyCodec(SnappyJS))
```

### LZ4

| Library    | Type    | Performance |
| ---------- | ------- | ----------- |
| `lz4-napi` | Native  | Fastest     |
| `lz4`      | Native  | Fast        |
| `lz4js`    | Pure JS | Good        |

#### lz4-napi (Recommended)

```bash
npm install lz4-napi
```

```typescript
import * as lz4 from 'lz4-napi'
import { CompressionType, compressionCodecs, createLz4Codec } from '@kafkats/client'

compressionCodecs.register(CompressionType.Lz4, createLz4Codec(lz4))
```

#### node-lz4

```bash
npm install lz4
```

```typescript
import lz4 from 'lz4'
import { CompressionType, compressionCodecs, createLz4Codec } from '@kafkats/client'

compressionCodecs.register(CompressionType.Lz4, createLz4Codec(lz4))
```

#### lz4js

```bash
npm install lz4js
```

```typescript
import * as lz4js from 'lz4js'
import { CompressionType, compressionCodecs, createLz4Codec } from '@kafkats/client'

compressionCodecs.register(CompressionType.Lz4, createLz4Codec(lz4js))
```

### Zstd

| Library            | Type   | Performance |
| ------------------ | ------ | ----------- |
| `@mongodb-js/zstd` | Native | Fastest     |
| `zstd-napi`        | Native | Fastest     |
| `zstd-codec`       | WASM   | Good        |

#### @mongodb-js/zstd (Recommended)

```bash
npm install @mongodb-js/zstd
```

```typescript
import { compress, decompress } from '@mongodb-js/zstd'
import { CompressionType, compressionCodecs, createZstdCodec } from '@kafkats/client'

compressionCodecs.register(CompressionType.Zstd, createZstdCodec({ compress, decompress }))
```

#### zstd-napi

```bash
npm install zstd-napi
```

```typescript
import { compress, decompress } from 'zstd-napi'
import { CompressionType, compressionCodecs, createZstdCodec } from '@kafkats/client'

compressionCodecs.register(CompressionType.Zstd, createZstdCodec({ compress, decompress }))
```

#### zstd-codec

```bash
npm install zstd-codec
```

```typescript
import { ZstdCodec } from 'zstd-codec'
import { CompressionType, compressionCodecs, createZstdCodec } from '@kafkats/client'

// Initialize and register within callback
ZstdCodec.run(zstd => {
	const simple = new zstd.Simple()
	compressionCodecs.register(CompressionType.Zstd, createZstdCodec(simple))
})
```

## Compression Options

### Zstd Compression Level

Zstd supports compression levels from 1-22 (default: 3). Lower levels are faster, higher levels achieve better compression:

```typescript
import { compress, decompress } from '@mongodb-js/zstd'

compressionCodecs.register(CompressionType.Zstd, createZstdCodec({ compress, decompress }, { level: 6 }))
```

## Transparent Decompression

Consumers automatically detect and decompress messages without any configuration. The compression type is stored in the RecordBatch header, so consumers can decode messages regardless of which compression was used by the producer.

```typescript
// Producer uses snappy compression
const producer = client.producer({ compression: 'snappy' })
await producer.send('my-topic', [{ value: 'compressed data' }])

// Consumer automatically decompresses - no config needed!
const consumer = client.consumer({ groupId: 'my-group' })
await consumer.subscribe('my-topic')

for await (const batch of consumer) {
	// Messages are automatically decompressed
	console.log(batch.messages[0].value) // 'compressed data'
}
```

::: tip
Make sure the compression codec is registered before consuming messages that use that compression type. GZIP works out of the box, but Snappy/LZ4/Zstd codecs must be registered.
:::

## Performance Considerations

Choose your compression strategy based on your use case:

| Use Case                 | Recommended  | Why                               |
| ------------------------ | ------------ | --------------------------------- |
| High throughput, low CPU | LZ4 or None  | Fastest compression/decompression |
| Network-constrained      | Zstd or Gzip | Best compression ratio            |
| Balanced workload        | Snappy       | Good mix of speed and compression |
| Log/text data            | Gzip or Zstd | Text compresses well with these   |

## Supported Libraries Summary

### Snappy

- **Native**: [`snappy`](https://www.npmjs.com/package/snappy) - Fastest, napi-rs based
- **Pure JS**: [`snappyjs`](https://www.npmjs.com/package/snappyjs)

### LZ4

- **Native**: [`lz4-napi`](https://www.npmjs.com/package/lz4-napi) - Fastest, napi-rs based
- **Native**: [`lz4`](https://www.npmjs.com/package/lz4) - node-lz4, encode/decode API
- **Pure JS**: [`lz4js`](https://www.npmjs.com/package/lz4js)

### Zstd

- **Native**: [`@mongodb-js/zstd`](https://www.npmjs.com/package/@mongodb-js/zstd) - MongoDB's binding
- **Native**: [`zstd-napi`](https://www.npmjs.com/package/zstd-napi) - Node-API binding
- **WASM**: [`zstd-codec`](https://www.npmjs.com/package/zstd-codec) - Emscripten based

## Custom Codecs

You can also implement your own compression codec:

```typescript
import { CompressionCodec, CompressionType, compressionCodecs } from '@kafkats/client'

const myCodec: CompressionCodec = {
	async compress(data: Buffer): Promise<Buffer> {
		// Your compression logic
		return compressedData
	},
	async decompress(data: Buffer): Promise<Buffer> {
		// Your decompression logic
		return decompressedData
	},
}

compressionCodecs.register(CompressionType.Snappy, myCodec)
```

## Next Steps

- [Producer API](/client/producer) - Configure producer compression
- [Configuration](/client/configuration) - Full configuration reference
- [Codecs](/client/codecs) - Message serialization (different from compression)
