// Sets up OpenTelemetry the way an application would, by OTEL_MODE:
//   off | metrics | traces | both | sampled (both, 1% of traces sampled)
// Spans and metrics go to processors that drop them: this measures instrumentation, not export.
import { context, propagation } from '@opentelemetry/api'
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks'
import { W3CTraceContextPropagator } from '@opentelemetry/core'
import {
	MeterProvider,
	PeriodicExportingMetricReader,
	InMemoryMetricExporter,
	AggregationTemporality,
} from '@opentelemetry/sdk-metrics'
import {
	BasicTracerProvider,
	TraceIdRatioBasedSampler,
	ParentBasedSampler,
	AlwaysOnSampler,
} from '@opentelemetry/sdk-trace-base'
import { instrumentKafka } from '../src/index.js'

export const mode = process.env.OTEL_MODE ?? 'off'
if (mode !== 'off') {
	const traces = mode === 'traces' || mode === 'both' || mode === 'sampled'
	const metrics = mode === 'metrics' || mode === 'both' || mode === 'sampled'
	if (traces) {
		context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable())
		propagation.setGlobalPropagator(new W3CTraceContextPropagator())
	}
	const noopProcessor = { onStart() {}, onEnd() {}, forceFlush: async () => {}, shutdown: async () => {} }
	const tracerProvider = new BasicTracerProvider({
		sampler:
			mode === 'sampled'
				? new ParentBasedSampler({ root: new TraceIdRatioBasedSampler(0.01) })
				: new AlwaysOnSampler(),
		spanProcessors: [noopProcessor],
	})
	const meterProvider = new MeterProvider({
		readers: [
			new PeriodicExportingMetricReader({
				exporter: new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE),
				exportIntervalMillis: 1000,
			}),
		],
	})
	instrumentKafka({ tracerProvider, meterProvider, traces, metrics })
}
