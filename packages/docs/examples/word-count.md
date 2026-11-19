# Word Count

The classic word count example using @kafkats/flow.

## Code

```typescript
import { flow, topic, string, json, TimeWindows } from '@kafkats/flow'

// Input: lines of text
const lines = topic('lines', { key: string(), value: string() })

// Output: word counts per minute
interface WordCount {
	word: string
	count: number
	windowStart: number
	windowEnd: number
}

async function main() {
	const app = flow({
		applicationId: 'word-count',
		client: { clientId: 'word-count', brokers: ['localhost:9092'] },
	})

	app.stream(lines)
		// Split lines into words
		.flatMapValues(line =>
			(line ?? '')
				.toLowerCase()
				.split(/\s+/)
				.filter(word => word.length > 0)
		)

		// Rekey by word
		.selectKey((_, word) => word!)

		// Group by word: the re-keyed stream is repartitioned through word-count-words-repartition
		.groupByKey({ key: string(), value: string(), name: 'words' })

		// Count in 1-minute windows (no late records accepted)
		.windowedBy(TimeWindows.ofSizeWithNoGrace('1m'))
		.count({ storeName: 'word-counts' })

		// Convert to output format
		.toStream()
		.map(
			(windowed, count) =>
				[
					windowed!.key,
					{
						word: windowed!.key,
						count: count ?? 0,
						windowStart: windowed!.window.start,
						windowEnd: windowed!.window.end,
					},
				] as const
		)

		// Write results
		.to('word-counts', { key: string(), value: json<WordCount>() })

	// Handle shutdown
	process.on('SIGTERM', () => void app.close())

	await app.start()
	console.log('Word count processor started')
}

main().catch(console.error)
```

## How It Works

1. **Read** lines of text from input topic
2. **FlatMapValues** - Split each line into words
3. **SelectKey** - Rekey by the word itself
4. **GroupByKey** - Group all occurrences of each word (through a repartition topic, so that each word is counted in one task)
5. **WindowedBy** - Apply 1-minute time windows
6. **Count** - Count occurrences per word per window
7. **Map** - Transform to output format
8. **To** - Write to output topic

## Topology Visualization

```
lines
  │
  ├──► flatMapValues (split into words)
  │
  ├──► selectKey (key by word)
  │
  ├──► groupByKey
  │
  ├──► windowedBy (1 minute)
  │
  ├──► count
  │
  ├──► toStream
  │
  ├──► map (format output)
  │
  └──► word-counts
```

## Testing

1. Start the word count processor:

```bash
npx tsx word-count.ts
```

2. Send some text:

```typescript
import { KafkaClient } from '@kafkats/client'

const client = new KafkaClient({
	clientId: 'producer',
	brokers: ['localhost:9092'],
})

const producer = client.producer()

await producer.send('lines', [
	{ key: 'doc1', value: 'hello world hello' },
	{ key: 'doc2', value: 'hello kafka streams' },
	{ key: 'doc3', value: 'kafka is great kafka' },
])

await producer.disconnect()
```

3. Check results:

```bash
kafka-console-consumer --topic word-counts --from-beginning
```

Expected output (every update of a window is forwarded; the order across words may differ):

```json
{"word":"hello","count":1,"windowStart":1703001200000,"windowEnd":1703001260000}
{"word":"world","count":1,"windowStart":1703001200000,"windowEnd":1703001260000}
{"word":"hello","count":2,"windowStart":1703001200000,"windowEnd":1703001260000}
{"word":"hello","count":3,"windowStart":1703001200000,"windowEnd":1703001260000}
{"word":"kafka","count":1,"windowStart":1703001200000,"windowEnd":1703001260000}
{"word":"streams","count":1,"windowStart":1703001200000,"windowEnd":1703001260000}
{"word":"kafka","count":2,"windowStart":1703001200000,"windowEnd":1703001260000}
{"word":"is","count":1,"windowStart":1703001200000,"windowEnd":1703001260000}
{"word":"great","count":1,"windowStart":1703001200000,"windowEnd":1703001260000}
{"word":"kafka","count":3,"windowStart":1703001200000,"windowEnd":1703001260000}
```

## Variations

### Global Count (No Window)

Count all time:

```typescript
app.stream(lines)
	.flatMapValues(line => (line ?? '').toLowerCase().split(/\s+/))
	.selectKey((_, word) => word!)
	.groupByKey({ key: string(), value: string(), name: 'words' })
	.count() // No windowing
	.toStream()
	.to('word-totals') // string keys, counts as 8-byte big-endian integers
```

### Top N Words

Find top 10 words per window:

```typescript
// Would require additional processing:
// 1. Collect all word counts
// 2. Sort by count
// 3. Take top 10
```

### Stop Words Filtering

Filter common words:

```typescript
const stopWords = new Set(['the', 'a', 'an', 'is', 'are', 'was', 'were'])

app.stream(lines).flatMapValues(line =>
	(line ?? '')
		.toLowerCase()
		.split(/\s+/)
		.filter(word => word.length > 0 && !stopWords.has(word))
)
// ... rest of pipeline
```
