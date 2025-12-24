# Examples

Complete, working examples to help you get started with kafkats.

## Basic Examples

| Example                                      | Description                         |
| -------------------------------------------- | ----------------------------------- |
| [Simple Producer](/examples/simple-producer) | Send messages to Kafka              |
| [Consumer Group](/examples/consumer-group)   | Read messages with a consumer group |

## Stream Processing

| Example                                          | Description                         |
| ------------------------------------------------ | ----------------------------------- |
| [Stream Processing](/examples/stream-processing) | Basic stream transformations        |
| [Word Count](/examples/word-count)               | Classic word count with aggregation |

## Running the Examples

1. Start Kafka locally:

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_CFG_NODE_ID=0 \
  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9093 \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  bitnami/kafka:latest
```

2. Install dependencies:

```bash
pnpm add @kafkats/client @kafkats/flow
```

3. Run the example:

```bash
npx tsx example.ts
```
