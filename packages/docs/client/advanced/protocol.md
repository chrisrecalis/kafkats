# Protocol Internals

kafkats implements the Kafka wire protocol directly in TypeScript. This page covers the protocol layer for advanced users who need low-level access.

## Binary Encoding

### Encoder

The Encoder class builds binary buffers:

```typescript
import { Encoder } from '@kafkats/client'

const encoder = new Encoder()
encoder.writeInt32(42)
encoder.writeString('hello')
encoder.writeBytes(Buffer.from([1, 2, 3]))

const buffer = encoder.toBuffer()
```

### Encoder Methods

| Method                  | Description                          |
| ----------------------- | ------------------------------------ |
| `writeInt8(n)`          | Write signed 8-bit integer           |
| `writeInt16(n)`         | Write signed 16-bit integer          |
| `writeInt32(n)`         | Write signed 32-bit integer          |
| `writeInt64(n)`         | Write signed 64-bit integer (bigint) |
| `writeUInt32(n)`        | Write unsigned 32-bit integer        |
| `writeVarInt(n)`        | Write variable-length integer        |
| `writeVarLong(n)`       | Write variable-length long (bigint)  |
| `writeString(s)`        | Write length-prefixed string         |
| `writeBytes(b)`         | Write length-prefixed bytes          |
| `writeCompactString(s)` | Write compact string (varint length) |
| `writeCompactBytes(b)`  | Write compact bytes (varint length)  |
| `writeArray(arr, fn)`   | Write array with encoder function    |

### Size Calculation

Pre-calculate buffer sizes for efficiency:

```typescript
const size = Encoder.sizeOfInt32() + Encoder.sizeOfString('hello') + Encoder.sizeOfBytes(data)

const encoder = new Encoder(size) // Pre-allocated
```

### Decoder

The Decoder class reads binary buffers:

```typescript
import { Decoder } from '@kafkats/client'

const decoder = new Decoder(buffer)
const num = decoder.readInt32()
const str = decoder.readString()
const bytes = decoder.readBytes()
```

### Decoder Methods

| Method                | Description                         |
| --------------------- | ----------------------------------- |
| `readInt8()`          | Read signed 8-bit integer           |
| `readInt16()`         | Read signed 16-bit integer          |
| `readInt32()`         | Read signed 32-bit integer          |
| `readInt64()`         | Read signed 64-bit integer (bigint) |
| `readUInt32()`        | Read unsigned 32-bit integer        |
| `readVarInt()`        | Read variable-length integer        |
| `readVarLong()`       | Read variable-length long (bigint)  |
| `readString()`        | Read length-prefixed string         |
| `readBytes()`         | Read length-prefixed bytes          |
| `readCompactString()` | Read compact string                 |
| `readCompactBytes()`  | Read compact bytes                  |
| `readArray(fn)`       | Read array with decoder function    |

## Record Batches

Kafka messages are grouped into record batches:

```typescript
import { RecordBatch, Record } from '@kafkats/client'

// Create a record
const record = Record.create({
	key: Buffer.from('key'),
	value: Buffer.from('value'),
	headers: { header: Buffer.from('value') },
	timestamp: Date.now(),
})

// Create a batch
const batch = RecordBatch.create({
	records: [record],
	compression: 0, // 0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd
})
```

### RecordBatch Structure

```typescript
interface RecordBatch {
	baseOffset: bigint
	batchLength: number
	partitionLeaderEpoch: number
	magic: number // Always 2 for current format
	crc: number
	attributes: number
	lastOffsetDelta: number
	baseTimestamp: bigint
	maxTimestamp: bigint
	producerId: bigint
	producerEpoch: number
	baseSequence: number
	records: Record[]
}
```

### Record Structure

```typescript
interface Record {
	length: number
	attributes: number
	timestampDelta: bigint
	offsetDelta: number
	key: Buffer | null
	value: Buffer
	headers: Array<{ key: string; value: Buffer }>
}
```

## Protocol Requests

Access raw request/response types:

```typescript
import { requests, responses } from '@kafkats/client'

// Request types
type ProduceRequest = requests.ProduceRequest
type FetchRequest = requests.FetchRequest

// Response types
type ProduceResponse = responses.ProduceResponse
type FetchResponse = responses.FetchResponse
```

## API Keys

Kafka API identifiers:

```typescript
import { ApiKeys } from '@kafkats/client'

ApiKeys.Produce // 0
ApiKeys.Fetch // 1
ApiKeys.ListOffsets // 2
ApiKeys.Metadata // 3
// ... etc
```

## Error Codes

Kafka protocol error codes:

```typescript
import { ErrorCode } from '@kafkats/client'

ErrorCode.None // 0
ErrorCode.UnknownTopicOrPartition // 3
ErrorCode.LeaderNotAvailable // 5
ErrorCode.NotLeaderForPartition // 6
// ... etc
```

## Request Header

All requests include a header:

```typescript
interface RequestHeader {
	apiKey: number
	apiVersion: number
	correlationId: number
	clientId: string
}
```

## Response Header

All responses include a header:

```typescript
interface ResponseHeader {
	correlationId: number
}
```

## Compression

Record batches can be compressed:

```typescript
import { compress, decompress, CompressionType } from '@kafkats/client'

// Compress
const compressed = await compress(CompressionType.Snappy, uncompressedBuffer)

// Decompress
const decompressed = await decompress(CompressionType.Snappy, compressedBuffer)
```

### Compression Types

| Value | Name   |
| ----- | ------ |
| 0     | None   |
| 1     | GZIP   |
| 2     | Snappy |
| 3     | LZ4    |
| 4     | ZSTD   |

## Variable-Length Encoding

Kafka uses variable-length integers for efficiency:

```typescript
// VarInt (signed, zig-zag encoded)
const encoded = Encoder.sizeOfVarInt(value)

// VarLong (64-bit, zig-zag encoded)
const encoded = Encoder.sizeOfVarLong(value)
```

## CRC32C

Kafka uses CRC32C for checksums:

```typescript
import { crc32c, verifyCrc32c } from '@kafkats/client'

const checksum = crc32c(buffer)
const isValid = verifyCrc32c(buffer, expectedChecksum)
```

## Best Practices

1. **Use high-level APIs** - Protocol details are abstracted
2. **Pre-calculate sizes** - For better performance
3. **Handle all error codes** - Many operations can fail
4. **Check API versions** - Protocol evolves over time
