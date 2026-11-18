import { metrics, trace, type MeterProvider, type TracerProvider } from '@opentelemetry/api'

import { traceFlow } from './flow.js'
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
 * Report what `@kafkats/client` does to OpenTelemetry: spans for sends, handler calls and the
 * records a `@kafkats/flow` application processes, connected across topics through message
 * headers, and metrics of the client's own work.
 *
 * Call it once, before creating producers and consumers. It listens to the diagnostics the
 * client and flow publish; nothing in them is patched. Returns the function that stops it.
 */
export function instrumentKafka(options: InstrumentKafkaOptions = {}): () => void {
	const stop: Array<() => void> = []
	if (options.traces !== false) {
		const tracer = (options.tracerProvider ?? trace).getTracer(NAME)
		stop.push(traceMessages(tracer), traceFlow(tracer))
	}
	if (options.metrics !== false) {
		stop.push(measureClients((options.meterProvider ?? metrics).getMeter(NAME)))
	}
	return () => {
		for (const stopOne of stop) stopOne()
	}
}
