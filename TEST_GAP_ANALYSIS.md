# kafkats Test Suite — Java-Parity Gap Analysis

> Comparison of the `kafkats` test suite against the Apache Kafka **Java** client's
> well-established test suites (KafkaProducerTest, RecordAccumulatorTest, SenderTest,
> FetcherTest, ConsumerCoordinatorTest, the assignor tests, MemoryRecordsTest,
> NetworkClientTest, TransactionManagerTest, KafkaAdminClientTest, SaslAuthenticatorTest,
> the KIP-932 share-consumer tests, and the Kafka Streams DSL tests).
>
> Produced by scanning every source and test file under `packages/` (117 test files)
> and mapping coverage to the equivalent Java scenarios. "Coverage" is rated
> **None / Partial / Good** from the perspective of the kafkats suite.

## How to read this

- **None** — behavior exists in source but has no test, OR the behavior/feature does not exist at all (noted inline).
- **Partial** — exercised only indirectly, only via integration, or only on a happy path.
- **Good** — has focused, regression-quality coverage comparable to the Java analog.

Each section ends with a prose summary of the most important gaps. Priorities are the
subagents' recommendations (High = correctness-critical and tractable; Med = valuable
deterministic coverage; Low = nice-to-have or feature-gated).

---

## Executive Summary

The kafkats suite is **strong on the pieces that are easy to test deterministically and
hard to get right**: idempotent-producer fencing, partitioner murmur2 Java-compatibility,
the partition-assignor algorithms (range/sticky/cooperative, including a 500-trial fuzz),
record-batch/varint/CRC primitives, frame decoding across TCP boundaries, metadata
epoch-merge logic, the consumer fetch fencing/corruption paths, and SASL happy/credential-
failure paths.

The recurring gaps cluster into four themes:

1. **State-machine internals tested only end-to-end.** The coordinator membership state
   machine (`consumer-group.ts`), the transaction state machine (`producer.ts`), and the
   idempotent sequence machinery are rich in error handling but are largely validated only
   through integration tests — the fast, deterministic unit coverage the Java suites
   concentrate on (ConsumerCoordinatorTest, TransactionManagerTest, SenderTest) is thin.

2. **Wire-compatibility is assumed, not proven.** Every codec test round-trips kafkats
   against itself. There are **no golden vectors / Java-produced byte fixtures** anywhere,
   only **gzip** is exercised through a real record-batch compression cycle (snappy/lz4/zstd
   use identity fakes), and only 3 of ~30 protocol message types have a dedicated ser/de test.

3. **Genuine feature/behavior gaps surface as "untestable" rows.** No fetch-session /
   incremental fetch, no leader-epoch tracking or truncation detection, no buffer-pool /
   `buffer.memory` backpressure, no batch-splitting on MESSAGE_TOO_LARGE, no transaction
   fatal-vs-abortable classification or epoch bump on abort, no `createPartitions` /
   config admin APIs, no `max.poll.records`. These are flagged because the Java suite covers
   them — each is a decision to implement-and-test or to document as out of scope.

4. **Decoded-but-ignored fields.** `throttle_time_ms` is decoded on every response and never
   acted on; high-watermark / last-stable-offset are read and never surfaced; KIP-368
   re-authentication is fully implemented in `connection.ts` yet has zero tests.

---

## Producer

| Scenario | kafkats coverage | Java reference test | Priority | Suggested test |
|----------|------------------|---------------------|----------|----------------|
| Buffer-pool memory accounting (`buffer.memory` bound, allocate/deallocate) | None — no buffer pool; `accumulator.ts` holds an unbounded `Map` with no byte cap | `BufferPoolTest.testSimple`, `testCantAllocateMoreMemoryThanWeHave` | High | Decide whether to implement `buffer.memory`; document as known architectural gap |
| `send()` blocks then times out on full buffer (`max.block.ms`) | None — `maxBlockMs` only gates InitProducerId, never send backpressure | `BufferPoolTest.testBlockTimeout` | Med | Assert send() with huge backlog doesn't block forever (or document no backpressure) |
| Accumulator splits oversized batch on MESSAGE_TOO_LARGE and re-sends halves | None — `producer-retry.test.ts` treats it as terminal | `RecordAccumulatorTest.testSplitAndReenqueue`, `SenderTest.testSplitBatchAndSend` | High | Implement-and-test split, or test that a multi-record batch isn't silently dropped |
| Linger timer batches records across appends | Good — `accumulator.test.ts` | `RecordAccumulatorTest.testLinger`, `testFull` | Low | — |
| `maxInFlight` per-partition ordering guarantee (muting) | Partial — only one integration test of `drainPendingBatches` muting | `SenderTest.testCanRetryWithoutResettingNextExpectedSeqNumber` | High | Unit: two batches same partition, first retries; assert second never sent first |
| Idempotent sequence reserve/commit/rollback | Partial — only indirect via fencing tests | `SenderTest.testSequenceNumberIncrement` | High | Unit: baseSequence advances on success, rolls back on definitive failure, holds on retriable |
| Sequence wrap at 2³¹-1 (split batch at boundary) | None — wrap+requeue logic in `producer.ts` untested | (TransactionManager seq overflow) | Med | Set committed seq near MAX, send crossing batch, assert split+requeue |
| OutOfOrderSequence triggers re-init | Good — `producer-retry.test.ts` | `SenderTest.testOutOfOrderSequenceNumber` | Low | — |
| UnknownProducerId recovery | None — `ErrorCode.UnknownProducerId` not handled, falls into generic path | `SenderTest.testRecoverFromUnknownProducerId` | High | Broker returns UNKNOWN_PRODUCER_ID → assert recovery, not misclassification |
| DuplicateSequence treated as success + stale-epoch guard | Good (stale-epoch branch untested → Med) | `SenderTest.testDuplicateSequenceAfterProducerReset` | Low | — |
| Epoch bump on re-init after fencing | Good — fencing tests assert `initCount` increments | `SenderTest.testEpochBumpOnOutOfOrderError` | Low | — |
| Producer fenced / InvalidProducerEpoch terminal | Good — `producer-retry.test.ts` | KafkaProducerTest fencing cases | Low | — |
| Ambiguous outcome (disconnect / omitted partition) fences for safety | Partial — only retry-exhaustion path tested | `SenderTest.testNodeNotReady` | Med | Broker omits a partition → fence+reinit; mid-flight disconnect after `everSent` → fence not rollback |
| Retriable vs fatal error classification | Good — broad coverage in `producer-retry.test.ts` | `SenderTest.testCanRetry` | Low | — |
| Retry backoff timing (exponential, capped) | None — retries counted, delay never asserted | `SenderTest` retry-backoff cases | Med | Fake timers: delay grows and caps at `maxRetryBackoffMs` |
| Metadata refresh on produce error + re-route to new leader | Good — `producer-retry.test.ts` | `SenderTest.testMetadataRefreshOnUnknownTopic` | Low | — |
| Per-record offset assignment / callback ordering | Partial — integration asserts offsets, no unit test | `ProducerBatchTest.testCompleteFutureAndFireCallbacks` | Med | 3-record batch baseOffset=100 → resolves 100,101,102 in order |
| Record timestamp handling (default/custom/>32-bit delta/base=min) | Good (integration `timestamps.test.ts`) | `ProducerBatchTest` timestamp tests | Low | A `baseTimestamp`=min unit test would be cheap |
| Murmur2 Java compat incl. negative-hash `toPositive` masking | Good — `partitioners.test.ts` | `UtilsTest.testMurmur2` | Low | — |
| Sticky partitioner rotate-on-complete / partition-count change | Partial — only integration | `StickyPartitionerTest.testStickyPartitioner` | Med | Keyless sends stick until `handleBatchSuccess`, then rotate; count change keeps index in range |
| Round-robin partitioner cycling / per-topic / bounded | Good — `partitioners.test.ts` | `RoundRobinPartitionerTest` | Low | (unavailable-partition behavior not modeled — Low) |
| Custom partitioner function honored | None — `resolvePartitioner` accepts a fn, no test uses one | KafkaProducerTest custom partitioner | Med | Pass a custom `PartitionerFunction`, assert invoked + result used |
| Explicit partition override + invalid rejection | Good (integration `partitioning.test.ts`) | `KafkaProducerTest.testInvalidPartition` | Low | — |
| flush() waits for in-flight + queued sends | Good — `producer-flush.test.ts` | `KafkaProducerTest.testFlushCompleteSendOfInflightBatches` | Low | — |
| close()/disconnect() rejects orphaned/queued sends; close during txn throws | Partial — two reject paths; no `close(0)` / disconnect-blocked-by-txn | `KafkaProducerTest.testCloseWithZeroTimeoutFromCallerThread` | Med | disconnect with active txn throws; abrupt disconnect rejects all pending |
| Config validation matrix (idempotent⇒acks=all, maxInFlight≤5, txn⇒idempotent) | Partial — only one integration test | `KafkaProducerTest.testConstructorWithInvalidConfigs` | Med | Unit matrix: each invalid combo throws the specific error |
| One ProduceRequest per broker (grouping) | Partial — `groupPreparedBatchesByLeader` untested directly | `SenderTest.testSendInOrder` | Med | Batches across 2 brokers/3 partitions → one produce per broker, partitions grouped |

**Summary.** The producer's strongest coverage is error classification and the idempotent
fencing state machine (`producer-retry.test.ts`), plus partitioner correctness and flush/
disconnect promise hygiene. The most important gaps are architectural and around sequence
internals: there is **no buffer-pool/memory-accounting layer** (the whole BufferPoolTest
family has no analog, and `maxBlockMs` is wired only to InitProducerId), **batch splitting
on MESSAGE_TOO_LARGE is absent**, the idempotent sequence machinery (`reserveSequence`/
`commitSequence`/`rollbackSequence` and the 2³¹-1 wrap/split) is tested only indirectly, and
**UNKNOWN_PRODUCER_ID is unhandled** and would be misclassified rather than recovered like
`SenderTest.testRecoverFromUnknownProducerId`. Cheap deterministic wins: sticky-partitioner
rotation, custom-partitioner wiring, the config-validation matrix, per-record offset
assignment, retry-backoff timing, and a dedicated unit test for the ordering-critical
`maxInFlight` muting that is currently only validated by one integration test.

---

## Consumer Fetch & Offsets

| Scenario | kafkats coverage | Java reference test | Priority | Suggested test |
|----------|------------------|---------------------|----------|----------------|
| OffsetOutOfRange → reset earliest/latest/none | Good — `fetch-offset-out-of-range.test.ts` | `FetcherTest.testFetchOffsetOutOfRange` | Low | — |
| autoOffsetReset start position | Good — `integration/consumer/offsets.test.ts` | `SubscriptionStateTest.testSeekUnvalidated` | Low | — |
| Position advance past control/compacted/empty batches | Good — `fetch-position-advance.test.ts` | `FetcherTest.testFetchSkipsControlRecords` | Low | — |
| CRC validation (`check.crcs`) | Good — `fetch-checkcrcs.test.ts`, `fetch-corrupt-message.test.ts` | `FetcherTest.testFetchWithInvalidCrc` | Low | — |
| Stale-response fencing (seek/pause/revoke mid-flight) | Good (exceeds Java) — `fetch-fencing.test.ts`, `fetch-pause-buffer.test.ts` | SubscriptionState position-validation analog | Low | — |
| Paused partition excluded from fetch | Partial — only throughput exclusion in integration | `SubscriptionStateTest.testPauseUnpause` | Med | Unit: paused partitions dropped from background fetch ready-set |
| **`max.poll.records` limiting** | **None** — config does not exist | `FetcherTest.testFetchMaxPollRecords` | High | poll() returns ≤N records, remainder retained for next poll |
| **`fetch.max.bytes` / `max.partition.fetch.bytes`** | **None** — `maxBytesPerPartition` plumbed but untested | `FetcherTest.testFetchMaxBytes`, `testFetchRequestWhenRecordTooLarge` | High | Assert request honors config + oversized-record handling |
| **Fetch session / incremental fetch / FETCH_SESSION_ID_NOT_FOUND** | **None** — full fetch every round; session fields never set | `FetchRequestManagerTest.testFetchSessions` | High | Behavior absent; document the gap |
| **Leader-epoch validation & truncation detection** | **None** — `currentLeaderEpoch:-1`, `lastFetchedEpoch:-1` hardcoded | `FetcherTest.testFetcherWithLeaderEpoch` | High | No FENCED/UNKNOWN_LEADER_EPOCH handling, no OffsetsForLeaderEpoch |
| Fetch backoff/refresh on NOT_LEADER / UNKNOWN_TOPIC | Partial — `handleFetchError` branch untested | `FetcherTest.testFetchNotLeaderForPartition` | Med | Unit: NotLeader/UnknownTopic → cache invalidation + metadata refresh |
| Empty fetch / high-watermark exposure | Partial — HWM/LSO read but never surfaced | `FetcherTest.testFetchResponseMetrics` | Med | Empty-fetch unit test; surface HWM |
| beginningOffsets / endOffsets / offsetsForTimes | None — public API missing (`getEarliest/LatestOffset` internal only) | `OffsetFetcherTest`, `FetcherTest.testGetOffsetsForTimes` | Med | Add public API + tests, or document unsupported |
| Committed-offset fetch retry (coordinator moved/loading) | Partial — `fetchCommittedOffsets` retry untested | `OffsetFetcherTest.testGetCommitted` | Med | Unit: coordinator rediscovery retry |
| OffsetCommit retries & partial-failure clearing | Partial — `partial-commit.test.ts` covers clearing | ConsumerCoordinatorTest commit paths | Low | NotCoordinator-driven rediscovery on commit |
| Auto-commit semantics (interval / on rebalance / on close) | Partial — `auto-commit-error.test.ts` (error path only) | `ConsumerCoordinatorTest.testAutoCommit` | Med | Commit-on-revoke timing (unit), commit-on-close |
| seek / seekToBeginning / seekToEnd | Partial — `seek()` covered; the others not implemented | `SubscriptionStateTest.testSeek` | Med | Implement + test seekToBeginning/End |
| read_committed aborted-transaction filtering | Partial — logic in `processBatches`, only indirect | `FetcherTest.testReadCommittedWithCommittedAndAbortedTransactions` | Med | Dedicated unit test feeding aborted-tx ranges + control markers |
| ConsumerRecords iteration / per-partition grouping | Partial — `PartitionBatch` grouping implicit | `ConsumerRecordsTest` | Low | Multi-partition drain ordering/grouping |

**Summary.** The most serious omissions are structural. There is **no fetch-session support**
(`fetch-manager.ts` issues a full fetch every round; `sessionId`/`sessionEpoch`/
`forgottenTopicsData` are never set), so the entire Java FetchSession surface has no analog.
Equally significant, there is **no leader-epoch tracking or truncation detection** — the
request hardcodes `currentLeaderEpoch:-1`/`lastFetchedEpoch:-1`, `handleFetchError` has no
FENCED/UNKNOWN_LEADER_EPOCH case, and there is no OffsetsForLeaderEpoch round-trip, so the
unclean-leader truncation safety of `FetcherTest.testFetcherWithLeaderEpoch` is absent.
**`max.poll.records` and byte limiting are untested** (and the per-poll record cap doesn't
exist), and the positional APIs (`beginningOffsets`/`endOffsets`/`offsetsForTimes`/
`seekToBeginning`/`seekToEnd`) are missing from the public consumer. The fencing and
corruption paths are the strong areas — `fetch-fencing.test.ts`, `fetch-pause-buffer.test.ts`,
`fetch-position-advance.test.ts`, and `fetch-checkcrcs.test.ts` give better mid-flight
seek/pause/revoke race coverage than the Java suite. Cheap test-only wins: paused-partition
exclusion, the `handleFetchError` metadata-refresh branch, an empty-fetch/HWM assertion, a
read_committed filtering unit test, and `fetchCommittedOffsets` rediscovery retries.

---

## Consumer Group & Assignors

| Scenario | kafkats coverage | Java reference test | Priority | Suggested test |
|----------|------------------|---------------------|----------|----------------|
| Range: uneven partitions / multi-topic distribution | Good — `range.test.ts` | `RangeAssignorTest.testTwoConsumersTwoTopicsSixPartitions` | Low | Add co-partitioned multi-topic case |
| Range: more consumers than partitions / nonexistent topic | Partial | `RangeAssignorTest.testOneConsumerOneTopic` | Med | Trailing members get none; zero-partition topic |
| RoundRobin assignor | **None** — not implemented (only doc-comment mentions) | `RoundRobinAssignorTest` | Low | N/A unless added; document absence |
| Sticky: stickiness preserved across rebalance | Good — `sticky.test.ts` | `AbstractStickyAssignorTest.testReassignmentAfterOneConsumerLeaves` | Low | — |
| Sticky: balance (max-min ≤ 1), heterogeneous subs | Good — `sticky.test.ts` + `balance.test.ts` (500-trial fuzz) | `AbstractStickyAssignorTest.testStickiness` | Low | — |
| Sticky: generation/userData conflict (two members claim same partition) | **None** — `sticky.ts` reads ownership with no generation | `StickyAssignorTest.testAssignmentWithMultipleGenerations` | High | Stale-generation conflict → assigned to exactly one |
| Cooperative two-phase revoke / withhold-in-transit | Good — `cooperative-sticky.test.ts` | `CooperativeStickyAssignorTest` | Low | — |
| ConsumerProtocol subscription/assignment ser/de | Partial — `consumer-protocol.test.ts` v0/v1 | `ConsumerProtocolTest.serializeDeserializeMetadata` | Med | Version tolerance (decode v2+); cross-client byte fixture |
| ConsumerProtocol v2 generationId field (KIP-792) | **None** — not in `encodeSubscriptionMetadata` | `ConsumerProtocolTest` generationId | Med | Graceful decode/encode if v2 supported |
| JoinGroup leader vs follower assignment | **None** (unit) | `ConsumerCoordinatorTest.testNormalJoinGroupLeader/Follower` | High | Leader runs assignor + per-member SyncGroup; follower empty |
| MEMBER_ID_REQUIRED two-round join (KIP-394) | **None** | `ConsumerCoordinatorTest.testJoinGroupRequestWithMemberIdRequired` | High | Retry reuses assigned memberId, second join succeeds |
| REBALANCE_IN_PROGRESS during Join/Sync → rejoin | Partial — heartbeat path only | `ConsumerCoordinatorTest.testRebalanceInProgressOnSyncGroup` | High | SyncGroup RebalanceInProgress re-runs join+sync |
| UNKNOWN_MEMBER_ID / ILLEGAL_GENERATION fencing → reset & rejoin | Partial — only heartbeat IllegalGeneration | `ConsumerCoordinatorTest.testUnknownMemberId` | High | Join/Sync error-path unit tests |
| Generation/epoch fencing of stale offset commit | Partial — `auto-commit-error.test.ts` | `ConsumerCoordinatorTest.testCommitOffsetIllegalGeneration` | Med | Assert `clearConsumedOffsets()` + rejoin |
| Heartbeat session timeout / poll-interval expiry | **None** | `AbstractCoordinatorTest.testHeartbeatThread`, `testMaxPollIntervalExceeded` | High | Missed HB → sessionLost/rebalance; poll-interval expiry → leave |
| onPartitionsRevoked commits before removing (cooperative) | Good — `session-lost.test.ts` | `ConsumerCoordinatorTest.testRevokeBeforeAssign` | Low | — |
| onPartitionsLost on fencing (no commit) | Partial — `session-lost.test.ts` | `ConsumerCoordinatorTest.testonPartitionsLost` | Med | Assert lost path does NOT commit, clears state |
| Static membership: skip LeaveGroup on close (KIP-345) | Good — `static-membership-leave.test.ts` | `ConsumerCoordinatorTest.testStaticMemberDoesNotSendLeaveGroup` | Low | — |
| Static membership: FENCED_INSTANCE_ID | Partial — integration only | `ConsumerCoordinatorTest.testFencedInstanceId` | Med | Unit: HB/Join/Sync FencedInstanceId → sessionLost + fatal |
| Static membership: rejoin replaces member without rebalance | Good (integration) | `ConsumerCoordinatorTest.testStaticMemberRejoin` | Low | — |
| Group leave on close (dynamic) | Good — `static-membership-leave.test.ts` | `ConsumerCoordinatorTest.testLeaveGroupOnClose` | Low | — |
| NOT_COORDINATOR / COORDINATOR_NOT_AVAILABLE refresh + retry | Partial — `ensureCoordinator` logic-only | `AbstractCoordinatorTest.testCoordinatorNotAvailable` | Med | Mock codes on Join/Sync/HB → invalidate + rejoin |
| Subscription-aware leader metadata (members with different topics) | Partial — assignor tests cover diff subs | `ConsumerCoordinatorTest.testMetadataChangeTriggersRebalance` | Med | `computeAssignments` fetches union of member topics |

**Summary.** The assignor algorithms are well covered — range, sticky, cooperative-sticky and
the KIP-54 relay balancer all have solid suites (including a 500-trial fuzz in `balance.test.ts`
and a convergence loop in `cooperative-sticky.test.ts`). The **largest gap is the coordinator/
membership state machine**: `consumer-group.ts` has rich handling for MEMBER_ID_REQUIRED,
UNKNOWN_MEMBER_ID, ILLEGAL_GENERATION, REBALANCE_IN_PROGRESS, NOT_COORDINATOR and
FENCED_INSTANCE_ID across Join/Sync/Heartbeat, yet unit tests touch only the heartbeat
reschedule path and `leave()`. The leader-vs-follower assignment flow, the MEMBER_ID_REQUIRED
two-round join, Sync-path rejoin, and session-timeout / max.poll.interval simulation are all
untested at the unit level — exactly what ConsumerCoordinatorTest concentrates on. Secondary
gaps: no protocol version-tolerance or cross-client byte fixtures (and no KIP-792 generationId)
in `consumer-protocol.test.ts`, no sticky stale-generation conflict test, and **no RoundRobin
assignor at all**.

---

## Protocol, Records & Codec

| Scenario | kafkats coverage | Java reference test | Priority | Suggested test |
|---|---|---|---|---|
| RecordBatch v2 full-attribute roundtrip (all bits at once) | Partial — flags tested individually; `HAS_DELETE_HORIZON_MASK` defined but never tested | `DefaultRecordBatchTest.testAttributes` | High | Encode all bits set; assert each helper + a deleteHorizon accessor (doesn't exist yet) |
| Compressed batch roundtrip per codec (gzip/snappy/lz4/zstd) | Partial — **only gzip** does a real batch cycle; snappy/lz4/zstd roundtrip through identity fakes | `MemoryRecordsTest` (parameterized over all codecs) | High | Real snappy/lz4/zstd through `encode/decodeRecordBatch` |
| Cross-compat with Java/broker-produced bytes (golden vectors) | None — everything roundtrips kafkats-against-itself | `DefaultRecordBatchTest` fixed-byte vectors | High | Commit a hex-dump of a real broker batch (compressed + uncompressed) |
| Control records (commit/abort markers, `ControlRecordType`) | None — `isControlBatch` read, but no marker-key parsing exists | `EndTransactionMarkerTest`, `ControlRecordTypeTest` | High | **Source gap**: add control-record parsing + test abort/commit markers |
| varint/varlong zigzag boundary & overflow | Good — `varint.test.ts` | `ByteUtilsTest.testVarintSerde` | Low | Add per-byte length thresholds for varlong |
| CRC32C known vectors & corruption detection | Good — `crc32c.test.ts`, `record-batch.test.ts` | `Crc32CTest.testUpdate` | Low | Add a second independent vector |
| Compact vs non-compact strings/arrays, nullable, unsigned varint, tagged fields (KIP-482) | Good — `encoder-decoder.test.ts`, `headers.test.ts` | `ProtocolSerializationTest` | Low | Large (multi-byte) tag number + unknown-tag round-trip |
| Record headers encode/decode (null/empty, count guards) | Good — `record.test.ts` | `DefaultRecordTest.testSerde` | Low | — |
| baseOffset/offsetDelta/timestampDelta math (LogAppendTime, CreateTime) | Good — `record-batch.test.ts`, `record.test.ts` | `DefaultRecordBatchTest.testStreamingIteratorConsistency` | Low | — |
| Partial / truncated buffer handling | Partial — primitive underflow tested; no truncated-batch test | `DefaultRecordBatchTest` partial-buffer | Med | Truncate a batch mid-records → clean error |
| Empty batch (zero records) | None — `validateRecordCount` allows 0 but untested | `MemoryRecordsTest.testBuildEmptyBatch` | Med | `createRecordBatch([])` → decode yields `records:[]` |
| Large/many-records batch | None — all tests use 1–2 tiny records | `MemoryRecordsBuilderTest` size/append-limit | Low | Multi-MB value + high record count |
| Compacted (non-dense) batch sequential fast-path fallback | Partial — guard exists, no test of `assumeSequentialOffsets:true` on a sparse batch | `DefaultRecordBatchTest` compacted iteration | Med | Gap in offsetDeltas, decode → real offsets used |
| Legacy v0/v1 format & down-conversion | None — kafkats hard-rejects `magic!==2` by design | `LegacyRecordTest` | N/A | One-line test asserting magic 0/1 throws |
| Per-message-type request/response ser-de | Sparse — only **FindCoordinator, ACLs, Share** have dedicated codec tests | `Produce/Fetch/MetadataRequestTest`, `RequestResponseTest` | High | Roundtrip Produce, Fetch, Metadata, ListOffsets, ApiVersions across flexible/non-flexible versions |

**Summary.** The primitives layer is genuinely well covered — `varint.test.ts`,
`encoder-decoder.test.ts`, and `crc32c.test.ts` hit the boundary, overflow, unsigned,
nullable, compact-vs-non-compact, and tagged-field cases of `ByteUtilsTest`/
`ProtocolSerializationTest`/`Crc32CTest`. The weakest areas are at the batch and message-type
level. **Compression is the single biggest hole**: despite four codecs in `compression.ts`,
only **gzip** is exercised through a real `encode/decodeRecordBatch` cycle — snappy, lz4, and
zstd use identity fakes that never actually compress, so codec-specific framing bugs (notably
the LZ4 frame-format requirement the code itself warns about) would pass undetected. There is
also **no golden-vector / Java-cross-compat test anywhere**, so the suite proves self-
consistency but not wire compatibility with a real broker. Two items are genuine source gaps:
control-record marker parsing (`ControlRecordType` commit/abort) does not exist, and
`HAS_DELETE_HORIZON_MASK` is defined but never decoded. Finally, only 3 of ~30 message types
have dedicated serialization tests — Produce, Fetch, and Metadata are completely untested at
the message-codec level.

---

## Network & Client

| Scenario | kafkats coverage | Java reference test | Priority | Suggested test |
|---|---|---|---|---|
| Correlation-id matching with interleaved/out-of-order responses | Partial — only a single in-flight request tested | `NetworkClientTest.testCorrelationId` | High | 3 pipelined requests, responses in reverse order, each resolves to its own payload |
| In-flight limit + FIFO send ordering | Partial — `request-queue.test.ts` checks queue/drain at maxInFlight=1; no FIFO assertion | `InFlightRequestsTest`, `NetworkClientTest.testMaxInFlight` | High | Enqueue >maxInFlight, assert dispatch in enqueue order |
| Request timeout frees a slot and drains | Good — `request-queue.test.ts` | `InFlightRequestsTest` | Low | — |
| Disconnect rejects in-flight + queued | Good — `connection.test.ts`, `connection-error-safety.test.ts` | `NetworkClientTest.testDisconnectDuringConnect` | Low | — |
| Frame decoding across TCP segment boundaries | Good — `kafka-frame-decoder.test.ts` | NetworkReceive / KafkaChannelTest | Low | — |
| Socket close mid-frame resets decoder + rejects in-flight | Partial — `reset()` called in source, no end-to-end test | `SelectorTest.testCloseConnectionInClosingState` | Med | Half a frame → close → reconnect → fresh frame decodes clean |
| Framing-error teardown (oversized/corrupt length) | Partial — decoder throws (unit), destroy-then-emit path untested | `SelectorTest.testCorruptMessage` | Med | Corrupt length to live `Connection` → socket destroyed + in-flight rejected |
| Reconnection: exponential backoff + jitter + cap | Good — `reconnection.test.ts` | `ClusterConnectionStatesTest.testExponentialReconnectBackoff` | Low | Assert jittered delay within bounds |
| `reconnect.backoff.max.ms` (distinct max) | None — single `maxDelayMs` | `ClusterConnectionStatesTest.testReconnectBackoffMax` | Low | N/A unless added |
| Reconnection wired into live Connection/Broker on disconnect | None — `ReconnectingConnection` tested in isolation, not shown wired in | `NetworkClientTest.testConnectionDelay` | High | Dropped broker connection triggers auto-reconnect + resumes requests |
| Connection state machine transitions | Partial — `connecting`/connect-coalescing untested; no `authenticating` state | `ClusterConnectionStatesTest` | Med | Concurrent `connect()` share one promise; `connecting` observable |
| API version negotiation: per-api intersection | Good — `broker.test.ts` | `ApiVersionsTest`, `NodeApiVersions.versionFor` | Low | — |
| ApiVersions UNSUPPORTED_VERSION fallback (v3→v0 retry) | None — `negotiateApiVersions` hardcodes v0 | `NetworkClientTest.testUnsupportedVersionDuringApiVersionsRequest` | Med | Broker rejects high version → retry lower |
| Version selection picks max of intersection / preferred range | Partial — `getApiVersion` returns max, untested | `NodeApiVersions.latestUsableVersion` | Low | Pin max-usable + preferredMin/Max |
| Throttling (`throttle_time_ms` / quota) honored | None — decoded on every response, never acted on | NetworkClient throttle handling | Med | Response with throttle>0 defers next request (once implemented) |
| Metadata refresh dedup (concurrent = one fetch) | Good — `cluster.test.ts` | `MetadataTest.testMetadataUpdateWaitTime` | Low | — |
| Metadata staleness / leader-epoch gating / topic-id recreation | Good — `cluster.test.ts` | `MetadataTest.testUpdateLastEpoch` | Low | — |
| Metadata refresh interval timer + on-error | None — `startMetadataRefreshInterval` untested | `MetadataTest.testTimeToNextUpdate` | Low | Fake timer triggers refresh + surfaces errors |
| Metadata-refresh-on-error-code | Good — `cluster.test.ts` | MetadataTest retriable handling | Low | — |
| Topic expiry from cache after TTL | None — never expires topics | MetadataTest topic expiry | Low | N/A unless added |
| Cluster/broker node tracking | Partial — controller lookup + coordinator-add untested | `ClusterTest`, MetadataTest controller | Med | `getControllerBroker`; coordinator-response adds unknown broker |
| leastLoadedNode selection | None — `getAnyBroker` returns first connected | `NetworkClientTest.testLeastLoadedNode` | Low | N/A unless added |
| Error code → typed exception mapping completeness | Partial — `createKafkaError` collapses most codes to generic `KafkaProtocolError` | `ApiErrorTest`, `ErrorsTest.testExceptionsAreNotGeneric` | Med | Table-driven: each ErrorCode → specific typed error + retriable flag |
| Connection pool race-safety on concurrent acquire | Good — `connection-pool.test.ts` | (client-specific) | Low | — |
| Pool acquire timeout / release-to-waiter / idle cleanup / disconnect refill | None | (client-specific) | Med | Acquire timeout reject; release wakes waiter; idle cleanup |
| SASL reauthentication scheduling/clamping (client-side bits) | Good — `connection.test.ts` | SaslAuthenticatorTest reauth | Low | — |
| `sendNoResponse` / acks=0 fire-and-forget | Good — `broker.test.ts` | (producer-level in Java) | Low | — |

**Summary.** Strongest areas: frame decoding, metadata merge/epoch logic, the reconnection
*strategy*, connection-pool race-safety, and SASL reauth (`kafka-frame-decoder.test.ts`,
`cluster.test.ts`, `reconnection.test.ts`, `connection-pool.test.ts`, `connection.test.ts`).
The most significant gaps are behavioral: response correlation is only tested for a single
in-flight request, so out-of-order/interleaved correlation and FIFO send ordering under
pipelining are unverified. API-version negotiation in `broker.ts` hardcodes ApiVersions v0
with no UNSUPPORTED_VERSION fallback, and **throttling is decoded everywhere but never
honored**. The `ReconnectingConnection` is tested in isolation but not demonstrably wired into
`Connection`/`Broker`/`Cluster`, leaving auto-recovery after a real disconnect untested. The
pool's timeout/waiter/idle-cleanup paths and the error-code→typed-exception mapping in
`errors.ts` (where `createKafkaError` collapses most codes to a generic error) are largely
uncovered relative to `ApiErrorTest`/`ErrorsTest`. kafkats also has no leastLoadedNode,
`reconnect.backoff.max`, or topic-expiry concepts — legitimate Java-parity gaps if added.

---

## Authentication (SASL/TLS)

| Scenario | kafkats coverage | Java reference test | Priority | Suggested test |
|---|---|---|---|---|
| SaslHandshake mechanism negotiation (request bytes) | Partial — only via `buildHandshakeResponse` helper | `SaslAuthenticatorTest#testValidSaslMechanism` | Med | Assert encoded handshake v1 carries configured mechanism |
| UNSUPPORTED_SASL_MECHANISM at handshake + enabled list | Partial — only auth-phase rejection tested | `SaslAuthenticatorTest#testInvalidSaslMechanism` | High | Handshake returns UnsupportedSaslMechanism → `supportedMechanisms` |
| ILLEGAL_SASL_STATE at handshake phase | Partial — only auth-phase | `SaslAuthenticatorTest#testIllegalSaslState` | Med | Handshake IllegalSaslState → error |
| Handshake-before-authenticate ordering | None | `SaslAuthenticatorTest#testDisallowedKafkaRequestsBeforeAuthentication` | Med | Assert handshake sent before any authenticate |
| SaslAuthenticate v0/v1/v2 framing round-trip | Partial — request encoder `sasl-authenticate.ts` untested | RequestResponseTest | Med | Encode/decode v0 (`writeBytes`), v2 (compact + tagged) |
| v0 raw-bytes vs v1-wrapped fallback | None — authenticator hardcodes v1 | `SaslAuthenticatorTest#testApiVersionsRequestWithUnsupportedVersion` | Low | Behavior when broker only supports v0 |
| PLAIN auth-bytes format `\0user\0pass` | None — `plain.ts` has no unit test | `PlainSaslServerTest` | High | Assert `PlainMechanism.authenticate()` yields exact bytes |
| PLAIN auth failure (wrong pw / no user) | Good (integration) | `SaslAuthenticatorTest#testInvalidPasswordSaslPlain` | Low | — |
| SCRAM client-first format (gs2 `n,,`, `n=user,r=nonce`) | None (unit) | `ScramSaslClientTest` | High | Drive `ScramMechanism.authenticate()`, assert client-first |
| SCRAM server-first parse + nonce-mismatch rejection | None (unit) | `ScramSaslClientTest#testServerNonceMismatch` | High | `r=` not starting with client nonce → error |
| SCRAM client-final proof (ClientKey/StoredKey/proof, `c=biws`) | None (unit) | `ScramFormatterTest`, RFC 5802 vectors | High | Fixed nonce/salt/iterations → `p=` matches known vector |
| SCRAM server-final signature verification (`v=`) | None (unit) | `ScramSaslClientTest#testServerSignatureVerification` | High | Wrong `v=` → error; correct → success |
| SCRAM server-final error (`e=`) | None | `ScramSaslClientTest` error cases | Med | `e=other-error` → `SaslAuthenticationError` |
| SCRAM-SHA-256 vs 512 digest/HMAC/PBKDF2 | Partial — integration round-trips both, no digest assertions | `ScramFormatterTest` | Med | Assert digestLength 32 vs 64 |
| SCRAM wrong-password proof failure | Good (integration, 256 + 512) | `SaslAuthenticatorTest#testInvalidPasswordSaslScram` | Low | — |
| SCRAM username SASLname escaping (`=`→`=3D`, `,`→`=2C`) | None | `ScramFormatterTest#testUsernameEscaping` | Med | `a=b,c` → `n=a=3Db=2Cc` |
| SASLprep / Normalize (RFC 4013) | None — not implemented | StringPrep/SASLprep tests | Low | Document non-normalization or add SASLprep |
| OAUTHBEARER client-first message format | Good (unit asserts exact bytes) | `OAuthBearerSaslClientTest#testAttachesExtensions` | Low | — |
| OAUTHBEARER extension validation (no U+0001, key has no `=`) | Partial — code validates, only happy path tested | `OAuthBearerClientInitialResponseTest` | Med | Bad extension value/key throws |
| OAUTHBEARER server failure (`\x01` error) → kvsep ack | None — mechanism yields once, never reads error round | `OAuthBearerSaslClientTest#testValidationFailure` | High | Server rejects token → `SaslAuthenticationError` |
| OAUTHBEARER token expiry / refresh / provider re-invoke | None | `ExpiringCredentialRefreshing*` | Med | Expired/short-lived token triggers reauth provider call |
| OAUTHBEARER integration round-trip | None | SaslAuthenticatorTest OAUTHBEARER | Med | `withKafkaSasl({mechanism:'OAUTHBEARER'})` |
| Re-authentication before expiry (KIP-368) | None — implemented in `connection.ts`, untested | `SaslAuthenticatorTest#testReauthentication` | High | Fake timers: reauth fires at `lifetime-threshold`, clamps, blocks non-auth sends |
| `sessionLifetimeMs` parse from authenticate v1 response | Partial — unit always sends 0 | `SaslAuthenticatorTest#testSessionExpiration` | Med | Nonzero lifetime → matches |
| Generic SASL error-code → `KafkaProtocolError` | None | SaslAuthenticatorTest error mapping | Low | Arbitrary code preserved |
| Unknown mechanism in `createSaslMechanism` | None | (config validation) | Low | Bad string → `SaslAuthenticationError` |
| TLS / SSL handshake, cert validation, SASL_SSL | None | `SslTransportLayerTest`, SaslAuthenticatorTest SASL_SSL | Med | SASL over TLS; cert-failure path |

**Summary.** kafkats has solid integration coverage of the happy path and credential-failure
path for PLAIN and SCRAM-256/512 (`integration/auth/sasl.test.ts`) and good unit coverage of
the OAUTHBEARER client-first bytes (`unit/auth/oauthbearer.test.ts`). The biggest gaps are at
the unit/protocol level: the **SCRAM RFC 5802 exchange** in `scram.ts` (proof computation,
server-signature verification, nonce-mismatch and `e=` handling, username escaping) is
exercised only end-to-end and never with deterministic vectors, so a regression in proof math
or channel binding (`c=biws`) would only surface as a flaky integration failure. The **PLAIN
auth-byte format** in `plain.ts` has no direct unit assertion, and OAUTHBEARER lacks server-
rejection, extension-validation-failure, expiry/refresh, and integration coverage. Most
critically, the **KIP-368 re-authentication machinery is fully implemented in `connection.ts`
yet has zero tests** — the unit helper hardcodes `sessionLifetimeMs = 0`, so neither the
lifetime parse nor the reauth timer is verified. SaslHandshake-phase error handling, the
SaslAuthenticate v0/v2 framing in `sasl-authenticate.ts`, SASLprep, and SASL_SSL/TLS are all
untested versus the Java `SaslAuthenticatorTest`/`ScramFormatterTest`/`OAuthBearerSaslClientTest`.

---

## Admin & Transactions (EOS)

### Transactions (EOS)

| Scenario | kafkats coverage | Java reference test | Priority | Suggested test |
|---|---|---|---|---|
| Full state-machine transitions (incl. COMMITTING/ABORTING→READY) | Partial — state is only `idle/in_transaction/committing/aborting`; never asserted directly | `TransactionManagerTest.testTransitions` | High | Assert `transactionState` at each step; re-entry resets partitions/offsets |
| Illegal-transition guards (begin while in-txn, send outside txn) | Partial — guards exist, only one path tested | `TransactionManagerTest.testFailIfNotReadyForSend` | Med | send/sendOffsets after committing/aborting → `InvalidTxnStateError` |
| InitProducerId with txnId, COORDINATOR_LOAD retry, fail-fast | Good — `producer-init-coordinator-load.test.ts` | `TransactionManagerTest.testInitProducerId` | Low | — |
| InitProducerId **epoch bump** (resume with existing id/epoch) | None — code passes them, no test drives reinit | `TransactionManagerTest.testBumpEpochAfterInitProducerIdRetry` | High | Fence mid-txn → reinit called with prior id/epoch |
| AddPartitionsToTxn dedup | Partial — logic exists, repeated-send dedup untested | `TransactionManagerTest.testMaybeAddPartitionToTransaction` | Med | Two sends same partition → addPartitions once |
| AddPartitionsToTxn CONCURRENT_TRANSACTIONS / coordinator move retry | Partial — generic retry tested, CONCURRENT_TRANSACTIONS not | `TransactionManagerTest.testHandleConcurrentTransactions` | High | Returns CONCURRENT_TRANSACTIONS then None → retried |
| AddOffsetsToTxn + TxnOffsetCommit with group metadata | Partial — passes metadata, never asserts gen/member/instanceId forwarded | `TransactionManagerTest.testSendOffsetWithGroupMetadata` | High | Assert txnOffsetCommit payload carries gen/member/instance |
| Fenced group / illegal-generation on TxnOffsetCommit | None | `TransactionManagerTest.testTransactionalOffsetCommitErrorFenced` | Med | IllegalGeneration/FencedInstanceId → non-retriable, abort |
| Fencing (ProducerFenced/InvalidProducerEpoch/InvalidTxnState) on coordinator RPCs | None — treated as generic non-retriable, no fatal classification | `TransactionManagerTest.testProducerFencedException` | High | endTxn/addPartitions ProducerFenced → fatal, not retried |
| Abort-on-error bumping epoch | None — `abortTransaction` just sends endTxn(false), no bump | `TransactionManagerTest.testAbortableErrorBumpsEpoch` | High | Error in txn → abort → next txn uses bumped epoch |
| Commit/abort retry on COORDINATOR_LOADING / NOT_COORDINATOR | Good (commit), Partial (abort — single call, no retry) | `TransactionManagerTest.testRetryCommit/testRetryAbort` | Med | endTxn(abort) CoordinatorLoad then None → retried |
| EndTxn(commit) failure → auto-abort | Good — `producer-txn-commit-retry.test.ts` | `TransactionManagerTest.testCommitTransactionFailureRetriedAsAbort` | Low | — |
| Transaction timeout aborts + signals | Good — `eos/transactions.test.ts` | `KafkaProducerTest.testTransactionTimeout` | Low | — |
| Interleaving produce with txn | Good — `producer-txn-roundrobin.test.ts` | `TransactionManagerTest.testProducerBatchesAreDrained` | Low | — |
| EndTxn retry idempotency (duplicate EndTxn) | None | `TransactionManagerTest.testRetryableEndTxn` | Low | Simulate EndTxn timeout+retry → single commit |
| Committed/aborted visibility to read_committed | Good — `eos/transactions.test.ts` | (broker behavior) | Low | — |

### Admin

| Scenario | kafkats coverage | Java reference test | Priority | Suggested test |
|---|---|---|---|---|
| createTopics partial failure / TopicExists / per-result error | Partial — per-topic errorCode returned, mixed batch untested | `KafkaAdminClientTest.testCreateTopicsPartialResponse` | High | Create existing → TopicAlreadyExists; mixed batch |
| createTopics validateOnly | Good — `integration/client/topics.test.ts` | `KafkaAdminClientTest.testCreateTopicsValidateOnly` | Low | — |
| createTopics/deleteTopics **retry on NOT_CONTROLLER** | None — single shot to `getControllerBroker()` | `KafkaAdminClientTest` NotController retry | High | Controller returns NotController → re-discover + retry |
| createPartitions | None — not implemented | `KafkaAdminClientTest.testCreatePartitions` | Med | Implement + test |
| describeConfigs / alterConfigs / incrementalAlterConfigs | None — not implemented | `KafkaAdminClientTest.testIncrementalAlterConfigs` | High | Implement + test config read & alter |
| ACLs create/describe/delete + filters (LITERAL/PREFIXED/ANY) | Good — `acls.test.ts` | `KafkaAdminClientTest.testDescribeAcls` | Low | — |
| ACL describe/delete error-per-result + top-level error | Partial — top-level only logged | `KafkaAdminClientTest.testDescribeAclsNonRetriableError` | Med | describeAcls SecurityDisabled top-level error |
| list/describe/delete consumer groups | Good — `groups.test.ts` | `KafkaAdminClientTest.testListConsumerGroups` | Low | — |
| describeGroups partial failure (per-group error, missing coordinator) | Partial — errors swallowed, group dropped silently | `KafkaAdminClientTest.testDescribeConsumerGroups` | Med | One bad + one good → bad surfaced not dropped |
| listOffsets (earliest/latest, read_committed LSO) | Partial — implemented, no admin test | `KafkaAdminClientTest.testListOffsets` | Med | Produce N → earliest/latest; committed vs uncommitted LSO |
| deleteRecords | None — not implemented | `KafkaAdminClientTest.testDeleteRecords` | Med | Implement + test low-watermark advance |
| describeCluster | Partial — `cluster.test.ts`, weak assertions | `KafkaAdminClientTest.testDescribeCluster` | Low | Assert controllerId, broker count, clusterId |
| Request fan-out (listGroups, describeGroups by coordinator) | Partial — logic exists, only happy-path integration | `KafkaAdminClientTest.testListConsumerGroupsWithStates` | Med | One broker throws → others still aggregated |
| listGroups statesFilter | Partial — plumbed, not asserted | `KafkaAdminClientTest.testListConsumerGroupsWithStates` | Low | Filter by Stable/Empty |

**Summary (Transactions).** The state machine in `producer.ts` is functional but coarse: it
models only `idle/in_transaction/committing/aborting` with no fatal-vs-abortable distinction
and no epoch bumping — the backbone of `TransactionManagerTest`. Strongest coverage:
`producer-txn-commit-retry.test.ts` (TxnOffsetCommit retries, commit→abort fallback),
`producer-init-coordinator-load.test.ts` (load retry + fail-fast), and
`producer-txn-roundrobin.test.ts`. The biggest gaps are **fencing semantics on coordinator
RPCs** (ProducerFenced/InvalidProducerEpoch/InvalidTxnState during AddPartitions/AddOffsets/
EndTxn are treated as generic non-retriable errors with no fatal classification and no epoch
bump — see `abortTransaction` and `addPartitionsToTransaction`), AddPartitions dedup /
CONCURRENT_TRANSACTIONS retry, and forwarding of **consumer group metadata** (gen/member/
groupInstanceId) in TxnOffsetCommit. The abort path is weaker than commit (single EndTxn, no
COORDINATOR_LOADING/NOT_COORDINATOR retry). Integration coverage is good for visibility and
timeout but does not exercise crash/fence recovery or zombie fencing across producer instances.

**Summary (Admin).** `admin.ts` implements topics, groups, ACLs, describeCluster, and
listOffsets, and the ACL/group integration tests are genuinely solid. But several Java-covered
APIs are entirely **unimplemented and untested**: `createPartitions`, `describeConfigs`/
`alterConfigs`/`incrementalAlterConfigs`, and `deleteRecords`. There are **no admin unit tests
at all**, so partial-failure handling and controller/coordinator fan-out only get hit on happy
paths. Two correctness gaps stand out: `createTopics`/`deleteTopics` send a single request to
`getControllerBroker()` with **no NOT_CONTROLLER retry** even though that code is retriable,
and partial failures (TopicAlreadyExists, a failing group in `describeGroups`, top-level ACL
errors) are silently logged/dropped rather than surfaced. Adding mocked-broker unit tests
(mirroring the producer `_helpers.ts` pattern) would close most admin gaps cheaply.

---

## Share Consumer & Flow (Streams)

### Share Consumer

| Scenario | kafkats coverage | Java reference test | Priority | Suggested test |
|---|---|---|---|---|
| Acquire → ACCEPT/RELEASE/REJECT lifecycle end-to-end | Partial — unit proves the right wire type is enqueued + `already-handled` guard; no test that RELEASE redelivers or REJECT does not | `ShareConsumerTest.testReleaseAcknowledgement`, `testRejectAcknowledgement` | High | Integration: produce 1, `release()` → re-delivered with `deliveryCount`+1; `reject()` → never seen again |
| Acquisition-lock timeout → redelivery | None — `acquisitionLockTimeoutMs` getter exists, never exercised | `ShareConsumerTest.testAcquisitionLockTimeout` / `SharePartitionTest` | High | Low `record.lock.duration.ms`, handler never acks → record redelivers after timeout |
| Delivery count & max delivery attempts (archived after N) | Partial — `deliveryCount` plumbed & filtered (`share-acquired-records.test.ts`), no increment-across-redelivery or archival test | `ShareConsumerTest.testMaxDeliveryCountReached` | High | Release repeatedly → `deliveryCount` increments then delivery stops |
| Share session epoch progression (0→1→…, reset, wrap) | Good — `share-session-epoch.test.ts`, `share-fixes.test.ts` | `ShareSessionTest`, `ShareConsumeRequestManagerTest.testEpoch` | Low | Direct `nextShareSessionEpoch(MAX)→1` and `-1` assertion |
| forgottenTopicsData on revoke / forget-only fetch | Good — `share-forgotten-topics.test.ts` | `ShareSessionTest` / `testForget` | Low | — |
| Explicit vs implicit (auto) ack | Partial — `auto-ack.test.ts` (failure path); `stream()` implicit finalize not unit-tested | `ShareConsumerTest.testImplicitAcknowledgement` | Med | `stream()` acks previous on advance, releases in-flight on break |
| Batch ack ordering / coalescing / dedupe / RENEW (KIP-1222) | Good — `ack-manager.test.ts` | `ShareConsumeRequestManagerTest` ack-batching | Low | — |
| ShareGroupHeartbeat membership (join, assign, fence/rejoin, leave) | Partial — fencing→rejoin covered; join/leave/assignment-diff not directly unit-tested | `ShareConsumerTest.testRebalance`, `ShareMembershipManagerTest` | Med | `applyAssignment` emits correct assigned/revoked; `leaveGroup` sends memberEpoch=-1 |
| Error/resilience (broker death, leader move, fatal vs retriable) | Good — `share-fetch-resilience.test.ts`, `share-fixes.test.ts` | `ShareFetchCollector.handleInitializeErrors` | Low | — |
| Protocol codec v1/v2 (acquireMode, isRenewAck, version-guard) | Good — `share-messages.test.ts` | Java `*RequestTest`/`*ResponseTest` | Low | — |
| Concurrency / multi-member queue distribution | Partial — one integration test; no concurrency>1 ordering/starvation test | `ShareConsumerTest.testMultipleConsumers` | Med | `concurrency: N` asserting no duplicate delivery under parallelism |

### Flow / Streams

| Scenario | kafkats coverage | Java reference test | Priority | Suggested test |
|---|---|---|---|---|
| Tumbling/hopping window correctness (multi-window, advance>size guard) | Good — `hopping-windows.test.ts` | `TimeWindowedKStreamImplTest`, `KStreamWindowAggregateTest` | Low | — |
| Sliding windows | None (intentional) — `sliding-windows-throw.test.ts` asserts "not implemented" | `SlidingWindowedKStreamImplTest` | Low | Feature gap — implement then test |
| Session windows: merge, bridge, retraction, in-place update | Good — `session-merge-retraction.test.ts`, `reduce-session-expiry.test.ts` | `SessionWindowedKStreamImplTest` | Low | — |
| Grace period / explicit late-record handling | Partial — late dropped by retention cutoff (`windowed-late-record.test.ts`); no explicit `grace()` API | `KStreamWindowAggregateTest` grace cases | Med | Distinct grace-vs-retention boundary once a grace API exists |
| Window/session retention via stream-time (not wall-clock) | Good — `window-stream-time.test.ts`, `stream-join-expiry.test.ts` | `KStreamWindowAggregateTest`, `AbstractWindowBytesStoreTest` | Low | — |
| Stream-stream windowed join (inner/left/outer, bounds, both-side expiry) | Good — `stream-join-expiry.test.ts` + integration | `KStreamKStreamJoinTest` | Low | Asymmetric before/after bounds once supported |
| Stream-table join (inner/left, table-update visibility, tombstone) | Good — `flow.test.ts` + integration | `KStreamKTableJoinTest` | Low | — |
| Table-table join (inner/left/outer, precise retraction) | Good — extensive in `flow.test.ts` | `KTableKTableInnerJoinTest` etc. | Low | — |
| Foreign-key join | None (feature absent) | `KTableKTableForeignKeyJoinIntegrationTest` | Low | Feature gap |
| Aggregation/reduce/count (KGroupedStream & KGroupedTable, delta, group-empty tombstone) | Good — `flow.test.ts`, `delta-tombstone.test.ts`, `table-groupby-ordering.test.ts` | `KGroupedStreamImplTest`, `KTableAggregateTest` | Low | — |
| Suppression (until-window-close / emit-final) | None (feature absent) | `SuppressScenarioTest`, `KTableSuppressProcessorTest` | Med | Feature gap — common need |
| KTable.filter retraction on predicate true→false | Partial — join/groupBy retraction tested, not `filter` flip | `KTableFilterTest` | Med | Value passes filter then fails → assert tombstone emitted |
| Changelog write (local-first ordering, tombstones, transactional) | Good — `changelog.test.ts` | `ChangeLoggingKeyValueBytesStoreTest` | Low | — |
| Changelog restore / checkpoints / aborted-tx skip / rebalance restore | Good — integration suite + `checkpoint-error-surface.test.ts` | `StoreChangelogReaderTest`, `RestoreIntegrationTest` | Low | — |
| Exactly-once (transactional batch + sendOffsets) | Partial — transaction shape verified; no crash-between-write-and-commit idempotency test | `EosIntegrationTest`, `EosV2Test` | Med | Kill between state-write and commit → no double-count on replay |
| Repartitioning (`through`, re-key, restore-restriction) | Partial — `through` + re-key threading tested; no test re-keyed data lands on correct partitions | `RepartitionTopicTest`, `KStreamRepartitionTest` | Low | — |
| Punctuation / scheduled callbacks | None (feature absent) — expiry is record-driven only | `PunctuationTest` | Med | Feature gap |
| Serde/codec round-trip (windowed-key, signed-time ordering) | Good — `codec.test.ts`, `changelog.test.ts`, `lmdb-signed-time-ordering.test.ts` | `WindowKeySchemaTest`, `SerdesTest` | Low | — |
| State store contracts (KV/window/session: range, bounds, expiry) | Good — `state.test.ts` + `flow-state-lmdb/tests/*` | `AbstractKeyValueStoreTest`, `AbstractWindowBytesStoreTest` | Low | — |
| TopologyTestDriver-style deterministic harness | Good — `testing.ts` `TestDriver`, `testing-example.test.ts` | `TopologyTestDriver` | Low | — |

**Summary.** Both domains have a solid, well-commented unit layer exercising the *plumbing* —
wire codecs, epoch/forgotten-topic bookkeeping, ack coalescing, changelog ordering, and
window/session expiry math — and the flow package has unusually thorough table-join retraction
and changelog-restore coverage (`flow.test.ts`, `changelog.test.ts`, the integration suite).
The most important share-consumer gaps are *behavioral lifecycle* gaps the current unit mocks
deliberately stub out: nothing tests that RELEASE actually redelivers, that REJECT suppresses
redelivery, that an acquisition-lock timeout re-queues a record, or that delivery-count /
max-attempts archival works — all of which Java `ShareConsumerTest` covers against a real
broker and which kafkats could add to its `share-consumer.test.ts` integration file (it already
spins up Kafka 4.1 with `share.version=1`). Membership-manager mechanics (`applyAssignment`
diffing, `leaveGroup` epoch -1) and concurrency>1 distribution are secondary share gaps. On the
flow side the gaps are mostly *unimplemented features* rather than untested code — sliding
windows, an explicit grace API distinct from retention, suppression/emit-final, punctuation,
and foreign-key joins — so the priority there is the feature work, after which the
`SuppressScenarioTest`/`SlidingWindowedKStreamImplTest`/`PunctuationTest` analogs become
writable. The two genuine flow *test* gaps worth closing now: a direct `KTable.filter`
retraction test and an EOS crash-idempotency integration test, since the EOS machinery exists
but its failure-recovery guarantee is asserted only structurally.

---

## Cross-Cutting Recommendations (Priority-Ordered)

These aggregate the **High**-priority, tractable items across all domains — the suggested
order for closing the gap with the Java suites without first shipping new features.

### Tier 1 — deterministic unit tests for already-implemented behavior

1. **Coordinator membership state machine** (`consumer-group.ts`): leader-vs-follower
   assignment, MEMBER_ID_REQUIRED two-round join, Sync-path REBALANCE_IN_PROGRESS rejoin,
   UNKNOWN_MEMBER_ID / ILLEGAL_GENERATION reset on Join/Sync, session-timeout / poll-interval
   expiry. *(mirrors ConsumerCoordinatorTest / AbstractCoordinatorTest)*
2. **Transaction fencing & epoch semantics** (`producer.ts`): ProducerFenced / InvalidProducerEpoch
   / InvalidTxnState on coordinator RPCs → fatal classification; epoch bump on abort and on
   reinit; group-metadata forwarding in TxnOffsetCommit; CONCURRENT_TRANSACTIONS retry.
   *(mirrors TransactionManagerTest)*
3. **Idempotent sequence machinery** (`producer.ts`): reserve/commit/rollback, 2³¹-1 wrap split,
   UNKNOWN_PRODUCER_ID recovery, and a deterministic `maxInFlight` per-partition muting test.
   *(mirrors SenderTest)*
4. **Network correlation & ordering** (`request-queue.ts`, `connection.ts`): interleaved/out-of-
   order response correlation and FIFO send ordering under pipelining. *(NetworkClientTest /
   InFlightRequestsTest)*
5. **SCRAM RFC 5802 vectors & KIP-368 reauth** (`scram.ts`, `connection.ts`): proof/server-
   signature against fixed vectors, nonce-mismatch rejection, username escaping; reauth timer
   firing/clamping/blocking. *(ScramFormatterTest / SaslAuthenticatorTest)*

### Tier 2 — wire-compatibility & codec

6. **Real compression per codec** (snappy/lz4/zstd through `encode/decodeRecordBatch`) and
   **golden Java/broker byte vectors** for record batches. *(MemoryRecordsTest / DefaultRecordBatchTest)*
7. **Per-message-type ser/de** for Produce, Fetch, Metadata, ListOffsets, ApiVersions across
   flexible and non-flexible versions. *(RequestResponseTest)*
8. **Admin partial-failure unit tests** with mocked brokers: createTopics TopicExists / mixed
   batch, NOT_CONTROLLER retry, describeGroups one-bad-one-good.

### Tier 3 — feature decisions (implement-and-test, or document as out of scope)

These are behaviors the Java client tests but kafkats does not implement. Each needs a
product decision before a test can exist:

- Fetch-session / incremental fetch; leader-epoch tracking & truncation detection.
- `max.poll.records`; `fetch.max.bytes` / `max.partition.fetch.bytes` enforcement.
- Buffer-pool / `buffer.memory` backpressure; batch-splitting on MESSAGE_TOO_LARGE.
- Throttle (`throttle_time_ms`) handling; ApiVersions UNSUPPORTED_VERSION fallback.
- Control-record (`ControlRecordType`) parsing; `HAS_DELETE_HORIZON_MASK` decode.
- Public positional consumer APIs (`beginningOffsets`/`endOffsets`/`offsetsForTimes`/
  `seekToBeginning`/`seekToEnd`); RoundRobin assignor.
- Admin `createPartitions`, config (describe/alter/incremental), `deleteRecords`.
- SASLprep normalization; SASL_SSL / TLS transport tests.
