---
"@kafkats/client": patch
"@kafkats/flow": patch
"@kafkats/codec-zod": patch
"@kafkats/flow-state-lmdb": patch
---

fix(compression): emit LZ4 frame format for lz4-napi (Kafka rejected raw-block LZ4)

`createLz4Codec` used `lz4-napi`'s `compress`/`uncompress`, which produce the raw LZ4
*block* format. Kafka's RecordBatch v2 requires the LZ4 *frame* format (magic bytes
`0x184D2204`), so every `lz4-napi`-compressed produce was rejected by the broker with
`UnknownServerError` ("invalid magic bytes").

When the `lz4-napi` instance exposes the framed API (`compressFrame`/`decompressFrame`,
available in lz4-napi >= 2.x), the codec now uses it. Older `lz4-napi` builds that only
expose the raw-block API now throw a clear error at codec-creation time instead of
silently producing data the broker rejects. The `lz4` (node-lz4) and `lz4js` paths
already use framed APIs and are unaffected.
