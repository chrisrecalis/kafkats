---
'@kafkats/client': patch
'@kafkats/flow': patch
'@kafkats/codec-zod': patch
'@kafkats/flow-state-lmdb': patch
---

Allow JoinGroup, SyncGroup, Fetch, ShareFetch, and acknowledged Produce requests to wait for their request-declared protocol wait in addition to the client connection timeout. Rejoin promptly when a heartbeat reports a rebalance instead of admitting every partition batch already returned by the current poll, and fairly bound each consumer poll to a configurable `maxRecords` (default 500), retaining excess prefetched records for the next poll.
