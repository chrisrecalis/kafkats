---
"@kafkats/client": minor
"@kafkats/flow": minor
"@kafkats/codec-zod": minor
"@kafkats/flow-state-lmdb": minor
"@kafkats/lag": minor
---

Add `@kafkats/lag`, a consumer-group time-lag exporter (seconds behind, per partition) for Prometheus that
complements kafka_exporter, with `--mode exact|estimate|auto` to trade record fetches for rate-based estimates on
large clusters, and `admin.listConsumerGroupOffsets()` for reading a group's committed offsets. `admin.listGroups()` gains a
`strict` option that throws instead of returning a partial result when a broker cannot be queried.

Zstd compression now works out of the box on Node 22.15+ / 23.8+ via Node's built-in `zlib` support, used when no
Zstd library is installed. `@mongodb-js/zstd` is no longer auto-detected (`zstd-napi` still is); register it
manually with `createZstdCodec` if you depend on it.
