# @kafkats/flow-state-lmdb

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
