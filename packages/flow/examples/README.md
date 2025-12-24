# @kafkats/flow Examples

Real-world examples demonstrating streaming topologies with `@kafkats/flow`.

## Prerequisites

- Docker & Docker Compose
- Node.js 18+
- pnpm

## Quick Start

### 1. Start Kafka

```bash
docker compose up -d
```

Wait for Kafka to be healthy:

```bash
docker compose ps
# Should show "healthy" status
```

### 2. Install Dependencies

From the repository root:

```bash
pnpm install
```

### 3. Run the Clickstream Example

```bash
# Terminal 1: Start the streaming topology
pnpm -C packages/flow/examples clickstream

# Terminal 2: Generate simulated web events
pnpm -C packages/flow/examples clickstream:produce
```

### 4. Observe Output

Use Kafka console consumer to view results:

```bash
# View user sessions
docker exec kafkats-examples-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic user-sessions \
  --from-beginning

# View page metrics
docker exec kafkats-examples-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic page-metrics \
  --from-beginning

# View premium user events
docker exec kafkats-examples-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic premium-events \
  --from-beginning
```

### 5. Inspect Changelog Topics

Stateful operations (aggregations, tables) are backed by changelog topics for fault tolerance. These topics follow the naming pattern `{applicationId}-{storeName}-changelog`.

```bash
# List all changelog topics
docker exec kafkats-examples-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list | grep changelog

# Example output:
# clickstream-analytics-table-user-profiles-0-changelog
```

View the contents of a changelog topic:

```bash
# View the user-profiles table changelog
docker exec kafkats-examples-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic clickstream-analytics-table-user-profiles-0-changelog \
  --from-beginning \
  --property print.key=true
```

Changelog topics use log compaction to retain only the latest value per key, enabling efficient state restoration on restart.

### 6. Stop

```bash
# Stop the topology (Ctrl+C in both terminals)

# Stop Kafka
docker compose down
```

---

## Clickstream Analytics Example

A web analytics platform that processes user events in real-time.

### Architecture

```
web-events (input)
    |
    +--[left join user-profiles]-->
    |     |
    |     +--[branch by tier]-->  premium-events
    |     |                       free-events
    |     |
    |     +--[session windows 30s]-->  user-sessions
    |
    +--[filter page_view]--[tumbling window 1m]-->  page-metrics
```

### Topics

| Topic            | Type      | Description                                  |
| ---------------- | --------- | -------------------------------------------- |
| `web-events`     | Input     | Raw page views and clicks from web clients   |
| `user-profiles`  | Table     | User data for enrichment (tier, country)     |
| `user-sessions`  | Output    | Detected user sessions with activity summary |
| `page-metrics`   | Output    | Page view counts per minute                  |
| `premium-events` | Output    | Events from premium tier users               |
| `free-events`    | Output    | Events from free tier users                  |
| `*-changelog`    | Changelog | Internal state store backing topics          |

### Flow Features Demonstrated

| Feature                      | Usage                              |
| ---------------------------- | ---------------------------------- |
| `flow()`                     | Create streaming application       |
| `stream()`                   | Define input stream source         |
| `table()`                    | Define changelog table for lookups |
| `leftJoin()`                 | Enrich events with user data       |
| `branch()`                   | Route events by user tier          |
| `groupByKey()`               | Group by user for sessions         |
| `groupBy()`                  | Group by page for metrics          |
| `windowedBy(SessionWindows)` | Detect user sessions               |
| `windowedBy(TimeWindows)`    | 1-minute tumbling windows          |
| `aggregate()`                | Build session summaries            |
| `count()`                    | Count page views                   |
| `filter()`                   | Select page_view events            |
| `map()`                      | Transform windowed results         |
| `to()`                       | Write to output topics             |

### Configuration

Environment variables:

| Variable            | Default          | Description                 |
| ------------------- | ---------------- | --------------------------- |
| `KAFKA_BROKERS`     | `localhost:9092` | Comma-separated broker list |
| `EVENTS_PER_SECOND` | `5`              | Event generation rate       |

Example:

```bash
KAFKA_BROKERS=broker1:9092,broker2:9092 pnpm clickstream
EVENTS_PER_SECOND=20 pnpm clickstream:produce
```

### Event Types

**PageViewEvent**

```json
{
	"type": "page_view",
	"userId": "user-1",
	"page": "/products",
	"referrer": "/home",
	"timestamp": 1703001234567
}
```

**ClickEvent**

```json
{
	"type": "click",
	"userId": "user-1",
	"elementId": "add-to-cart",
	"page": "/products",
	"timestamp": 1703001234567
}
```

### Output Examples

**SessionSummary** (user-sessions topic)

```json
{
	"userId": "user-1",
	"sessionStart": 1703001234567,
	"sessionEnd": 1703001294567,
	"pageViews": 5,
	"clicks": 3,
	"pages": ["/home", "/products", "/checkout"]
}
```

**PageStats** (page-metrics topic)

```json
{
	"page": "/products",
	"windowStart": 1703001200000,
	"viewCount": 42
}
```

---

## Adding New Examples

1. Create a new folder under `examples/`
2. Add types, topology, and entry point
3. Add npm scripts to `package.json`
4. Document in this README
