# Word Count

The classic word count example using @kafkats/flow.

## Code

```typescript
import { flow, topic, TimeWindows } from '@kafkats/flow'
import { string, json } from '@kafkats/client'

// Input: lines of text
const lines = topic('lines', {
	key: string(),
	value: string(),
})

// Output: word counts
interface WordCount {
	word: string
	count: number
	windowStart: number
	windowEnd: number
}

const wordCounts = topic('word-counts', {
	key: string(),
	value: json<WordCount>(),
})

async function main() {
	const app = flow({
		applicationId: 'word-count',
		client: { clientId: 'word-count', brokers: ['localhost:9092'] },
	})

	app.stream(lines)
		// Split lines into words
		.flatMapValues(line =>
			line
				.toLowerCase()
				.split(/\s+/)
				.filter(word => word.length > 0)
		)

		// Rekey by word
		.selectKey((_, word) => word)

		// Group by word
		.groupByKey()

		// Count in 1-minute windows
		.windowedBy(TimeWindows.of('1m'))
		.count()

		// Convert to output format
		.toStream()
		.map((windowedKey, count) => ({
			key: windowedKey.key,
			value: {
				word: windowedKey.key,
				count,
				windowStart: windowedKey.window.start,
				windowEnd: windowedKey.window.end,
			},
		}))

		// Write results
		.to(wordCounts)

	// Handle shutdown
	process.on('SIGTERM', async () => {
		await app.close()
	})

	console.log('Word count processor started')
	await app.start()
}

main().catch(console.error)
```

## How It Works

1. **Read** lines of text from input topic
2. **FlatMapValues** - Split each line into words
3. **SelectKey** - Rekey by the word itself
4. **GroupByKey** - Group all occurrences of each word
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

Expected output:

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
	.flatMapValues(line => line.toLowerCase().split(/\s+/))
	.selectKey((_, word) => word)
	.groupByKey()
	.count() // No windowing
	.toStream()
	.to(wordCounts)
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
	line
		.toLowerCase()
		.split(/\s+/)
		.filter(word => word.length > 0 && !stopWords.has(word))
)
// ... rest of pipeline
```
