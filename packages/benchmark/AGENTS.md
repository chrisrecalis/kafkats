# @kafkats/benchmark

Performance benchmarks comparing @kafkats/client with kafkajs.

## Commands

```bash
pnpm bench:producer   # Run producer benchmarks
pnpm bench:consumer   # Run consumer benchmarks
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
docker compose -f packages/benchmark/docker-compose.yml up -d
KAFKA_BROKERS=localhost:19292,localhost:19293,localhost:19294 pnpm -C packages/benchmark bench:all
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
