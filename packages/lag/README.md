# @kafkats/lag

Kafka consumer-group **time lag** exporter for Prometheus: how many seconds behind each consumer group is, per
partition.

It is designed to run **alongside [kafka_exporter](https://github.com/danielqsj/kafka_exporter)**, not replace it.
kafka_exporter gives you offsets, offset lag, and cluster topology; this adds the one number it cannot compute,
because it requires reading the record at each committed offset: the age of the oldest unconsumed message. None of
the series kafka_exporter emits are duplicated, so both can be scraped into the same Prometheus without collisions,
and the shared `consumergroup` / `topic` / `partition` labels join cleanly in queries.

```bash
npx @kafkats/lag --brokers localhost:9092
```

```bash
docker run --rm -p 9464:9464 \
  -e KAFKA_BROKERS=host.docker.internal:9092 \
  ghcr.io/chrisrecalis/kafkats-lag:latest
```

Metrics are served on `/metrics`, port `9464` by default. Run `kafkats-lag --help` for Kafka authentication,
filtering, and collection options.

## Metrics

| Metric                                    | Labels                                | Meaning                                                                                                              |
| ----------------------------------------- | ------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `kafka_consumergroup_lag_seconds`         | `consumergroup`, `topic`, `partition` | Age in seconds of the oldest record the group has not consumed. `0` when caught up.                                  |
| `kafka_consumergroup_max_lag_seconds`     | `consumergroup`                       | Worst partition time lag in the group.                                                                               |
| `kafkats_lag_collection_success`          |                                       | `1` if the last collection had no errors.                                                                            |
| `kafkats_lag_collection_duration_seconds` |                                       | Seconds the last collection took.                                                                                    |
| `kafkats_lag_last_success_unixtime`       |                                       | When the last error-free collection finished.                                                                        |
| `kafkats_lag_partitions`                  | `source`                              | Partitions in the last collection by how lag was obtained: `caught_up`, `fetched`, `cached`, `estimated`, `unknown`. |
| `kafkats_lag_fetch_requests`              |                                       | Record fetches sent to brokers in the last collection. `kafkats_lag_fetch_requests_total` accumulates since start.   |
| `kafkats_lag_fetch_bytes`                 |                                       | Record bytes read from brokers in the last collection. `kafkats_lag_fetch_bytes_total` accumulates since start.      |

When the group is active, per-partition series also carry `consumer_id`, `client_id`, and `member_host` for the
member that owns the partition. Set `--cluster-name` to add a `cluster_name` label to everything.

If the record at the committed offset no longer exists (retention or log compaction), the age is measured from the
next surviving record and is a lower bound.

## How it works

Every `--interval` seconds the exporter lists consumer groups, fetches each group's committed offsets, and the log
start and end offsets of every partition they cover. How the time lag itself is measured depends on `--mode`:

| Mode              | Measurement                                                                                  | Cost per collection                                                      |
| ----------------- | -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `exact` (default) | Fetch one small batch at the committed offset and read the record's timestamp.               | One 64 KiB fetch per partition whose committed offset moved; exact.      |
| `estimate`        | Offset lag divided by the partition's produce rate over the last few collections.            | None beyond ListOffsets. An estimate; no value while warming up or idle. |
| `auto`            | `estimate`, and `exact` for the partitions where no estimate is possible (idle, warming up). | Fetches only on the idle partitions, which is where they are cheapest.   |

`exact` runs the equivalent of one extra lightweight consumer over every moving partition. `kafkats_lag_fetch_requests`,
`kafkats_lag_fetch_bytes`, and `kafkats_lag_partitions{source=...}` show what a collection actually cost and how much of
it was fetched, cached, or estimated, so the mode can be chosen from measurements. Fetch results are cached,
so a committed offset that has not moved costs no fetches until the cache expires (10 minutes by default), and record
payloads are skipped over, never decoded. On clusters with thousands of moving partitions prefer `auto`: it keeps the
exact answer where estimation cannot work and costs almost nothing everywhere else.

Exporter health is available at `/healthz`: 200 when a collection finished within the last three intervals, 503 when
collections have stalled. Per-group or per-topic errors do not make the exporter unhealthy (one denied group would
otherwise restart the pod); they are listed in the response body and reflected in `kafkats_lag_collection_success`.
