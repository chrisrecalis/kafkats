---
"@kafkats/client": minor
"@kafkats/flow": minor
"@kafkats/codec-zod": minor
"@kafkats/flow-state-lmdb": minor
---

### Consumer Refactor

Simplified the consumer fetch architecture with a buffered poll model:

- Removed callback-based fetch approach (~600 lines removed)
- FetchManager now uses a background fetch loop with internal buffering
- Added `StreamOptions` with manual partition assignment support for `consumer.stream()`

### Rebalance Improvements

Fixed race conditions during cooperative rebalance:

- Rebalance is now synchronous with the poll loop via `checkAndHandleRebalance()`
- Added `PartitionTracker` to coordinate partition ownership and in-flight handler state
- Partition assignment protection in `OffsetManager` prevents committing offsets for revoked partitions
- Fixed cooperative rebalance losing tracked partitions when only newly added partitions are reported

### Bug Fixes

- Fixed metadata refresh deduplication incorrectly caching topic-specific requests, which caused consumers sharing a `Cluster` instance to receive wrong metadata and get 0 partitions assigned
