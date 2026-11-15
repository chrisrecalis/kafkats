import { metrics, trace, type MeterProvider, type TracerProvider } from '@opentelemetry/api'

import { measureClients } from './metrics.js'
import { traceMessages } from './tracing.js'

export { ATTR, KAFKATS_METRIC, METRIC } from './attributes.js'

const NAME = '@kafkats/opentelemetry'

export interface InstrumentKafkaOptions {
	/** Where spans go. Defaults to the global tracer provider. */
	tracerProvider?: TracerProvider
	/** Where metrics go. Defaults to the global meter provider. */
	meterProvider?: MeterProvider
	/** Set to false to create no spans (and write no trace headers). */
	traces?: boolean
	/** Set to false to record no metrics. */
	metrics?: boolean
}

/**
 * Report what `@kafkats/client` does to OpenTelemetry: spans for sends and handler calls,
 * connected across topics through message headers, and metrics of the client's own work.
 *
 * Call it once, before creating producers and consumers. It listens to the diagnostics the
 * client publishes; nothing in the client is patched. Returns the function that stops it.
 */
export function instrumentKafka(options: InstrumentKafkaOptions = {}): () => void {
	const stop: Array<() => void> = []
	if (options.traces !== false) {
		stop.push(traceMessages((options.tracerProvider ?? trace).getTracer(NAME)))
	}
	if (options.metrics !== false) {
		stop.push(measureClients((options.meterProvider ?? metrics).getMeter(NAME)))
	}
	return () => {
		for (const stopOne of stop) stopOne()
	}
}
