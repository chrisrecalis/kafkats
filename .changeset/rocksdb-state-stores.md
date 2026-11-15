---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
'@kafkats/flow-state-rocksdb': minor
---

Add `@kafkats/flow-state-rocksdb`, a persistent RocksDB state store provider for `@kafkats/flow`.

- `rocksdb({ stateDir })` is used like the LMDB provider. Writes do not wait for the disk, so stateful topologies under at-least-once are not limited by one disk commit per record. Requires Node.js 22.18 or later.
- `@kafkats/flow` exports `OrderedKeyValueStore`, `OrderedWindowStore`, `OrderedSessionStore` and `OrderedChangelogCheckpointStore`: flow's stores on top of any engine that maps byte keys to byte values in byte order (`OrderedByteStore`). The LMDB provider now uses them; its on-disk format and API are unchanged.
- `@kafkats/flow-state-lmdb` now requires the `@kafkats/flow` release it is published with.
