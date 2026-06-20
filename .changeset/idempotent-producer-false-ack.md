---
"@kafkats/client": patch
"@kafkats/flow": patch
"@kafkats/codec-zod": patch
"@kafkats/flow-state-lmdb": patch
---

fix(producer): prevent idempotent producer false-ack under leader failover

Under `acks=all` with an idempotent producer, a batch that was sent and committed by
the broker but whose response was lost (e.g. the partition leader is killed mid-flight)
could exhaust its retries and then have its sequence rolled back. A subsequent batch
reused the same base sequence, the broker answered `DUPLICATE_SEQUENCE_NUMBER` pointing
at the prior batch's offsets, and the client resolved the *new* batch's records with
those offsets — acknowledging records that were never written to the log.

On retry-exhaustion of an idempotent batch that was already put on the wire, the producer
now reinitializes the producer id/epoch (rather than reusing the sequence) and rejects the
affected sends, matching the Java client's handling of append-ambiguous outcomes. A
stale-epoch guard additionally prevents a response from a previous producer epoch from
mutating the new epoch's sequence state.
