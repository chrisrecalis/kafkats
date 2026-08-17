---
'@kafkats/client': patch
'@kafkats/flow': patch
'@kafkats/codec-zod': patch
'@kafkats/flow-state-lmdb': patch
---

Graduate `ShareConsumer` from experimental status now that Share Groups are production-ready in Kafka 4.2.
Require the production-ready ShareFetch and ShareAcknowledge v2 APIs, recommend Kafka 4.2.1 or newer, and cover
the acquire-mode and lock-renewal paths against Kafka 4.2.
