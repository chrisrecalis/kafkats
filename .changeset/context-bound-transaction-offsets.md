---
'@kafkats/client': patch
'@kafkats/flow': patch
'@kafkats/codec-zod': patch
'@kafkats/flow-state-lmdb': patch
---

Delivery-time consumer group metadata is now threaded through `ConsumeContext`, so `tx.sendOffsets(ctx)` can fence offset commits without exposing consumer internals. `tx.sendOffsets(ctx, offsets)` covers batched Flow transactions; the old params shape still works.
