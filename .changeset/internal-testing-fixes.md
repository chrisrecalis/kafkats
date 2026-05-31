---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
---

Various fixes from internal testing:

- Surface internal connection and coordinator errors without crashing the process.
- Time-bound coordinator retries for transactional producer init and consumer offset fetch/commit. Configurable via the new producer `maxBlockMs` and consumer `defaultApiTimeoutMs` options (both default 60000).
- Re-discover the group coordinator on `NotCoordinator` during offset commit.
- Add human-readable messages for `ProducerFenced` and `InvalidProducerEpoch`.

Share consumer (KIP-932) reliability and correctness fixes:

- Ride through broker loss / coordinator failover instead of dying on a single broker's transport error.
- Gap-acknowledge control-batch and compacted offsets so the share-partition start offset advances (previously it could stall and redeliver forever).
- Rejoin on member fencing (`FencedMemberEpoch` / `UnknownMemberId`) instead of failing fatally.
- Deliver only records the broker actually acquired (no reprocessing of already-acked records under churn).
- Treat recoverable per-partition fetch errors as skip-and-continue rather than fatal.
- Fail (not retry) an acknowledgement on a leader change, matching the broker's record ownership.
- A failed auto-acknowledgement no longer terminates the consumer; the record redelivers.
- `stream()` surfaces fatal loop errors and applies back-pressure instead of buffering unboundedly.
- Validate consumer config and expose `acquisitionLockTimeoutMs`.
