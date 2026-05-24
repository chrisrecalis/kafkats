---
'@kafkats/client': patch
'@kafkats/flow': patch
'@kafkats/codec-zod': patch
'@kafkats/flow-state-lmdb': patch
---

Correctness and reliability fixes across the producer, consumer, client protocol, and flow state stores.

### Producer

- `flush()` now waits for deferred drains so it no longer resolves while writes are still buffered.
- `transactionalSend` freezes its partition set up front to avoid double-calling the partitioner.
- Retry `TxnOffsetCommit` on retriable and rebalance-in-progress errors instead of failing the transaction.
- Fence the producer on `OUT_OF_ORDER_SEQUENCE_NUMBER` to surface the idempotence violation rather than silently corrupting the stream.
- Reject orphaned in-flight send promises during disconnect so callers are not left hanging.

### Consumer

- Always reschedule the heartbeat after non-fatal errors, preventing the consumer from dropping out of the group.
- Surface auto-commit errors to the consumer instead of swallowing them.
- Clear only the successfully committed partitions on a partial commit failure so the rest are retried.
- Fence stale fetch responses across partition replacement to avoid delivering records from a revoked assignment.

### Client / protocol

- Guard against varint shift overflow in the decoder.
- Close a `ConnectionPool.acquire` race that could hand out a connection mid-teardown.
- Clamp the SASL reauthentication delay to the `setTimeout` maximum.
- Don't await a response on `Produce` with `acks=0` (the broker sends none).
- Always use the v0 response header for `ApiVersions`, matching broker behavior across versions.

### Share consumer

- Release (rather than acknowledge) the last yielded message on shutdown so unprocessed records are redelivered.

### Flow

- Write to the local store before the changelog (local-first ordering) to shrink the inconsistency window on crash.
- Serialize the EOS commit triggered by a rebalance against the next message, and close the EOS rebalance offset-commit gap via an awaitable rebalance hook (shared with the consumer fix above).
- Use stream time (not wall-clock time) for window expiry.
- Implement hopping windows (`TimeWindows.advanceBy`).
- Throw on `SlidingWindows` instead of silently misbehaving (not yet implemented).
- Correct `TableGroupByNode` mapping ordering.
- Apply retention to `reduce` and session aggregators, emitting changelog tombstones for expired state.
- Surface checkpoint persistence errors instead of swallowing them.

### flow-state-lmdb

- Make `range()` upper bound inclusive, matching the in-memory store.
- Await `fsync` on checkpoint writes so a checkpoint is durable before it is reported complete.
- Correct `WindowStore` time-bound math.
- Use order-preserving signed-i64 encoding for time keys so range scans iterate in correct chronological order.
