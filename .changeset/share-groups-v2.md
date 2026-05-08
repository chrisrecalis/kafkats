---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
---

ShareConsumer: Kafka 4.2 share groups GA support (ShareFetch/ShareAcknowledge v2)

- KIP-1206: new `acquireMode` config (`'batch_optimized'` default | `'record_limit'`) — `record_limit` strictly caps each fetch at `maxRecords`.
- KIP-1222: new `message.renew()` to extend the acquisition lock without finalizing delivery; safe to call multiple times per message.
- `ShareAcknowledgeResponse.acquisitionLockTimeoutMs` is now decoded on v2 responses.
- AckManager dedupes same-offset RENEW + finalizing-ack pairs and collapses duplicate same-offset RENEWs so the wire never carries overlapping batches.
- `@experimental` markers removed; share groups are GA in Kafka 4.2.
- Backward compatible: against Kafka 4.1 brokers the client negotiates v1 and produces byte-identical wire output to before this change. `record_limit` and `renew()` throw with a clear "requires v2 (Kafka 4.2+)" error against older brokers.
