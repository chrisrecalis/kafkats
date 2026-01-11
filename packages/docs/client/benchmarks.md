# Benchmarks

The `@kafkats/benchmark` package runs throughput benchmarks comparing `@kafkats/client` against KafkaJS.

## Running

Benchmarks use a persistent Docker Compose Kafka cluster. Start it once:

```bash
# If running from a devcontainer:
KAFKA_ADVERTISED_HOST=host.docker.internal docker compose -f packages/benchmark/docker-compose.yml up -d

# If running directly on the host:
# KAFKA_ADVERTISED_HOST=localhost docker compose -f packages/benchmark/docker-compose.yml up -d
```

Then run the benchmarks:

```bash
KAFKA_BROKERS=host.docker.internal:19292,host.docker.internal:19293,host.docker.internal:19294 pnpm -C packages/benchmark bench:producer -- --iterations 10 --warmup 2
KAFKA_BROKERS=host.docker.internal:19292,host.docker.internal:19293,host.docker.internal:19294 pnpm -C packages/benchmark bench:consumer -- --iterations 10 --warmup 2
```

## Sample Results

These results were collected with:

- Kafka cluster: 3 brokers via `packages/benchmark/docker-compose.yml`
- Payload: 10,000 messages, 1 KB message size
- Producer batch size: 100
- Iterations: 10 (+2 warmup)

### Producer

| Library               | Mean throughput |
| --------------------- | --------------- |
| `@kafkats/client`     | 39,545 msg/s    |
| `kafkajs`             | 22,333 msg/s    |
| `@platformatic/kafka` | 37,636 msg/s    |

`@kafkats/client` vs KafkaJS: **1.77x** mean throughput.

### Consumer

| Library               | Mean throughput |
| --------------------- | --------------- |
| `@kafkats/client`     | 107,075 msg/s   |
| `kafkajs`             | 62,666 msg/s    |
| `@platformatic/kafka` | 73,124 msg/s    |

`@kafkats/client` vs KafkaJS: **1.71x** mean throughput.

::: tip Notes
Absolute numbers vary by hardware, Docker/VM networking, and tuning. For reproducible comparisons, run the suite on your target environment and compare ratios rather than raw msg/s.
:::
