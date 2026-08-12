---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
---

Automatic compression codec registration. When a Snappy, LZ4, or Zstd codec is needed and none is registered, the client now looks for a supported compression library (`snappy`, `snappyjs`, `lz4-napi`, `lz4`, `lz4js`, `@mongodb-js/zstd`, `zstd-napi`) and registers the first one it finds — installing the library is all that's required. Manual registration via `compressionCodecs.register()` still works and takes precedence, and auto-detection can be disabled with `compressionCodecs.autoRegister = false`. Missing-codec errors now include an install hint.
