---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
---

Parallel producer transactions via `transactionConcurrency`. The one-open-transaction limit is per transactional ID, so setting `transactionConcurrency: N` (default 1) makes the producer manage a pool of N internal transactional producers ("lanes") behind the unchanged `transaction()` API: lane 0 keeps the configured `transactionalId`, lanes 1..N-1 append `-1`..`-{N-1}`. Calls are admitted first-in first-out — a call takes a free lane or waits for the next one released — and `transaction:queued` now fires only when all lanes are busy. Transactions may commit out of call order (the same guarantee as independent transactions today); for consume-transform-produce, pass `consumerGroupMetadata` to `sendOffsets()` so zombie fencing works per consumer-group generation regardless of lane. `flush()` and `disconnect()` cover all lanes.
