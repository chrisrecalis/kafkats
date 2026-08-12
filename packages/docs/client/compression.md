# Compression

kafkats supports multiple compression algorithms for reducing network bandwidth and storage. Compression is applied at the RecordBatch level - the producer compresses batches before sending, and consumers automatically decompress.

## Quick Start

Install a supported compression library and use it — kafkats detects and registers it automatically:

```bash
npm install snappy
```

```typescript
const producer = client.producer({
	compression: 'snappy',
})
```

No registration call is needed. When a codec is first looked up, kafkats checks for the supported libraries (see below) and registers the first one it finds.

## Compression Types

| Type       | Speed     | Ratio | Built-in | Notes                                       |
| ---------- | --------- | ----- | -------- | ------------------------------------------- |
| `'none'`   | Fastest   | 1:1   | Yes      | No compression                              |
| `'gzip'`   | Slow      | Best  | Yes      | Uses Node.js zlib                           |
| `'snappy'` | Fast      | Good  | No       | Balanced choice, auto-detected library      |
| `'lz4'`    | Very fast | Good  | No       | Best for throughput, auto-detected library  |
| `'zstd'`   | Medium    | Best  | No       | Modern and efficient, auto-detected library |

## Built-in Codecs

GZIP is built-in and requires no additional setup:

```typescript
const producer = client.producer({
	compression: 'gzip',
})
```

## Automatic Codec Registration

For Snappy, LZ4, and Zstd, install one of the supported libraries and kafkats picks it up automatically — no registration code required. When several are installed, the first match in the table below (fastest first) wins.

### Snappy

| Library    | Type    | Performance | Auto-detected |
| ---------- | ------- | ----------- | ------------- |
| `snappy`   | Native  | Fastest     | Yes (1st)     |
| `snappyjs` | Pure JS | Good        | Yes (2nd)     |

```bash
npm install snappy
```

### LZ4

| Library    | Type    | Performance | Auto-detected |
| ---------- | ------- | ----------- | ------------- |
| `lz4-napi` | Native  | Fastest     | Yes (1st)     |
| `lz4`      | Native  | Fast        | Yes (2nd)     |
| `lz4js`    | Pure JS | Good        | Yes (3rd)     |

```bash
npm install lz4-napi
```

::: warning
`lz4-napi` 2.x or later is required — Kafka needs the LZ4 frame format, which older versions don't expose.
:::

### Zstd

| Library            | Type   | Performance | Auto-detected |
| ------------------ | ------ | ----------- | ------------- |
| `@mongodb-js/zstd` | Native | Fastest     | Yes (1st)     |
| `zstd-napi`        | Native | Fastest     | Yes (2nd)     |
| `zstd-codec`       | WASM   | Good        | No (manual)   |

```bash
npm install @mongodb-js/zstd
```

::: warning
`@mongodb-js/zstd` v7+ requires Node 20.19 or later. On Node 18, install `@mongodb-js/zstd@2` (or `zstd-napi`) instead.
:::

### Disabling auto-registration

If you want full control over which codecs are used, turn auto-registration off and register codecs explicitly:

```typescript
import { compressionCodecs } from '@kafkats/client'

compressionCodecs.autoRegister = false
```

## Manual Registration

Manual registration is still available — it always takes precedence over auto-detection. Use it for custom codecs, for `zstd-codec` (which needs async initialization), or to pass options like the Zstd compression level.

### Snappy

```typescript
import snappy from 'snappy' // or: import * as SnappyJS from 'snappyjs'
import { CompressionType, compressionCodecs, createSnappyCodec } from '@kafkats/client'

compressionCodecs.register(CompressionType.Snappy, createSnappyCodec(snappy))
```

### LZ4

```typescript
import * as lz4 from 'lz4-napi' // or: 'lz4', 'lz4js'
import { CompressionType, compressionCodecs, createLz4Codec } from '@kafkats/client'

compressionCodecs.register(CompressionType.Lz4, createLz4Codec(lz4))
```

### Zstd

```typescript
import { compress, decompress } from '@mongodb-js/zstd' // or: 'zstd-napi'
import { CompressionType, compressionCodecs, createZstdCodec } from '@kafkats/client'

compressionCodecs.register(CompressionType.Zstd, createZstdCodec({ compress, decompress }))
```

#### zstd-codec (WASM, manual only)

`zstd-codec` initializes asynchronously, so it cannot be auto-detected and must be registered manually:

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
// Producer uses gzip compression (built-in)
const producer = client.producer({ compression: 'gzip' })
await producer.send('my-topic', [{ value: Buffer.from('compressed data') }])

// Consumer automatically decompresses
const consumer = client.consumer({ groupId: 'my-group' })
for await (const { message } of consumer.stream('my-topic')) {
	console.log(message.value.toString()) // 'compressed data'
}
```

::: tip
Make sure a compression library for the topic's compression type is installed (or a codec manually registered) before consuming. GZIP works out of the box; Snappy/LZ4/Zstd need one of the supported libraries installed.
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
