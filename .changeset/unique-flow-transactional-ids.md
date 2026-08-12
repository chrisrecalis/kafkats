---
'@kafkats/client': patch
'@kafkats/flow': patch
'@kafkats/codec-zod': patch
'@kafkats/flow-state-lmdb': patch
---

Flow-managed transactional IDs now include an internal per-process UUID alongside `applicationId` and the worker index, so replicas of the same deployment no longer fence each other's transactional producers. Explicitly configured transactional IDs are preserved unchanged.
