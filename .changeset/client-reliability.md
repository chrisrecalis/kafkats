---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
'@kafkats/flow-state-rocksdb': minor
'@kafkats/opentelemetry': minor
---

`@kafkats/client` recovers from broker failures, shuts down within a bound, and sends and fetches with less waiting.

- Consumers and producers recover after a broker restart or a dropped connection. Brokers re-dial dropped connections with backoff, the fetch loop backs off instead of spinning and reports failures through the `error` event, and group rebalance steps retry while the cluster is unreachable.
- `consumer.stop()`, `producer.disconnect()` and `client.disconnect()` are bounded (about 5 seconds each) when a broker does not respond. Sends still in flight at that point are rejected.
- Multiple consumers on one `KafkaClient` no longer deadlock: each consumer group member uses a dedicated coordinator connection, and every consumer fetches on connections of its own, so consumers sharing a client no longer wait for each other's long polls.
- Producer: batches of one partition that queue up behind an in-flight request are merged (in order, up to `maxBatchBytes`), and records are appended in the order `send()` was called, also for sends that are not awaited one by one.
- Consumer: `runBatch` handlers get `ctx.resolveOffset(offset)` to make progress within a batch committable before the handler returns; `commitStartPosition(partitions)` commits the position partitions start reading from, before any of their records has been fetched; `position(topic, partition)` returns the offset the next fetch of an assigned partition starts at.
- Consumer groups: the leader starts a rebalance once the partition counts of the subscribed topics differ from those it assigned with, like the Java client. A consumer that joined a topic created moments before, while metadata did not show it yet, is assigned its partitions once it shows up instead of staying without any; added partitions are assigned after the next metadata refresh. The leader assigns and watches the topics of every member's subscription (a topic only other members subscribe to was never assigned), a deleted topic's consumed offsets are dropped instead of failing the rebalance, and JoinGroup v9 lets a static leader that rejoins a stable group keep watching (KIP-814).
- Transactional producers: a non-retriable InitProducerId or FindCoordinator answer (a transactional id the principal may not use) rejects at once with a `KafkaProtocolError` carrying its code, also for every later transaction, instead of retrying until `maxBlockMs` or throwing a plain `Error`; `disconnect()` stops an initialization that is still retrying, so a producer given up never bumps the epoch of its transactional id after a successor initialized.
- `stop()` no longer cuts the group connection while the loop body of `stream()` is still working on a record; sends waiting for a topic whose partition lookup failed fail together with it.
