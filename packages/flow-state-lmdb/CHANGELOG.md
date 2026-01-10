# @kafkats/flow-state-lmdb

## 0.6.0

### Minor Changes

- 9c3180b: ### Consumer Refactor

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

### Patch Changes

- Updated dependencies [9c3180b]
    - @kafkats/flow@0.6.0

## 0.5.0

### Minor Changes

- 31136b9: ### ACL Support

    Added Access Control List (ACL) management to the admin client:
    - `admin.describeAcls()` - Query ACLs by filter criteria
    - `admin.createAcls()` - Create ACL bindings for principals
    - `admin.deleteAcls()` - Delete ACLs matching filters

    Includes full protocol support for DescribeAcls, CreateAcls, and DeleteAcls APIs (v2-v3).

    ### Consumer Seek

    Added `consumer.seek(topic, partition, offset)` method to reposition the fetch offset for a partition. Enables replaying messages or skipping ahead to a specific offset. Works with the pause/seek/resume pattern for controlled repositioning.

    ### Documentation

    Added comprehensive KafkaJS migration guide covering client configuration, producer, consumer, transactions, and admin API differences.

### Patch Changes

- Updated dependencies [31136b9]
    - @kafkats/flow@0.5.0

## 0.4.0

### Minor Changes

- 34b2c63: ### Share Groups Support

    Added KIP-932 Share Groups implementation with `ShareConsumer` class for scalable, lock-free message consumption without partition assignment.

    ### Consumer Refactoring
    - Refactored consumer to use modular batch-based processing architecture
    - Simplified stream mode with poll-based API, removing separate stream-mode module
    - Fixed `sessionLost` flag not being cleared after successful rejoin

### Patch Changes

- Updated dependencies [34b2c63]
    - @kafkats/flow@0.4.0

## 0.3.0

### Minor Changes

- d7a02fe: - fix(client): fix compression
    - docs: update compression documentation

### Patch Changes

- Updated dependencies [d7a02fe]
    - @kafkats/flow@0.3.0

## 0.2.0

### Minor Changes

- 58c770f: - perf(client): optional native CRC32C via @node-rs/crc32 (now an optional peer dependency) with JS fallback; faster Record/RecordBatch encoding (more single-pass, fewer intermediate allocations); encoder uses Buffer.allocUnsafe; producer avoids per-message remapping. Also adds benchmark heap snapshot runners and updates docs.
    - chore(packages): add per-package README.md plus npm metadata (keywords, homepage, repository, bugs) for @kafkats/client, @kafkats/flow, @kafkats/codec-zod,
      @kafkats/flow-state-lmdb to improve npm package visibility.

### Patch Changes

- Updated dependencies [58c770f]
    - @kafkats/flow@0.2.0
