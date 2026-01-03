# @kafkats/benchmark

Performance benchmarks comparing @kafkats/client with kafkajs.

## Commands

```bash
pnpm bench:producer   # Run producer benchmarks
pnpm bench:consumer   # Run consumer benchmarks
pnpm bench:share      # Run ShareConsumer benchmarks (kafkats only)
pnpm bench:all        # Run all benchmarks
```

## Structure

```
src/
├── producer-benchmark.ts  # Producer throughput comparisons
├── consumer-benchmark.ts  # Consumer throughput comparisons
├── kafka-cluster.ts       # Testcontainers Kafka setup
├── utils.ts               # Benchmark utilities
└── index.ts               # Entry point for all benchmarks
```

## Running Benchmarks

Benchmarks use a persistent Docker Compose Kafka cluster. Start it once, then run benchmarks repeatedly:

```bash
KAFKA_ADVERTISED_HOST=localhost docker compose -f packages/benchmark/docker-compose.yml up -d
KAFKA_BROKERS=localhost:19292,localhost:19293,localhost:19294 pnpm -C packages/benchmark bench:all
#
# If running from a container/devcontainer, use:
# KAFKA_ADVERTISED_HOST=host.docker.internal docker compose -f packages/benchmark/docker-compose.yml up -d
# KAFKA_BROKERS=host.docker.internal:19292,host.docker.internal:19293,host.docker.internal:19294 pnpm -C packages/benchmark bench:all
```

### Share Groups

The benchmark cluster is configured for Kafka Share Groups (`share.version=1`), but Share Groups are also a **feature flag**. After the cluster is up, run this once:

```bash
docker exec kafkats-bench-kafka-1 /opt/kafka/bin/kafka-features.sh --bootstrap-server localhost:19092 upgrade --feature share.version=1
```

Share benchmarks only run `@kafkats/client` (no comparisons), since Share Groups are not widely supported in other clients.

### Flags

All `bench:*` scripts run the same runner (`src/index.ts`).

```bash
pnpm -C packages/benchmark bench:all -- --iterations 10 --warmup 1
pnpm -C packages/benchmark bench:producer -- --messageCount 50000 --messageSize 1024 --batchSize 500
pnpm -C packages/benchmark bench:consumer -- --messageCount 100000 --messageSize 1024
pnpm -C packages/benchmark bench:share -- --messageCount 100000 --messageSize 1024 --shareConsumers 2

# Optional diagnostics / trace + NDJSON output (paths are relative to packages/benchmark)
pnpm -C packages/benchmark bench:all -- --diagnostics --trace --json results/bench.ndjson
```

Results compare:

- **@kafkats/client**: This library
- **kafkajs**: Popular existing Kafka client

## Adding Benchmarks

When adding new benchmarks:

1. Use `kafka-cluster.ts` helpers for container setup
2. Use `utils.ts` for timing and statistics
3. Ensure fair comparison (same message sizes, batch settings, etc.)
4. Report messages/sec and MB/sec metrics
