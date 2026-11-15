import { context, propagation } from '@opentelemetry/api'
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks'
import { W3CTraceContextPropagator } from '@opentelemetry/core'
import {
	AggregationTemporality,
	InMemoryMetricExporter,
	MeterProvider,
	PeriodicExportingMetricReader,
	type DataPoint,
	type Histogram,
} from '@opentelemetry/sdk-metrics'
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'

/** What an application sets up once: how context follows async work, and the header format. */
export function useW3CContext(): void {
	context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable())
	propagation.setGlobalPropagator(new W3CTraceContextPropagator())
}

/** A tracer provider that keeps finished spans in memory. */
export function spanRecorder() {
	const exporter = new InMemorySpanExporter()
	const tracerProvider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] })
	return { tracerProvider, spans: () => exporter.getFinishedSpans() }
}

/** A meter provider whose metrics are read on demand. */
export function metricRecorder() {
	const reader = new PeriodicExportingMetricReader({
		exporter: new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE),
		exportIntervalMillis: 3_600_000,
	})
	const meterProvider = new MeterProvider({ readers: [reader] })
	/** The data points of every metric collected now, by metric name. */
	const collect = async (): Promise<Map<string, Array<DataPoint<number | Histogram>>>> => {
		const { resourceMetrics } = await reader.collect()
		const points = new Map<string, Array<DataPoint<number | Histogram>>>()
		for (const scope of resourceMetrics.scopeMetrics) {
			for (const metric of scope.metrics) {
				points.set(metric.descriptor.name, metric.dataPoints as Array<DataPoint<number | Histogram>>)
			}
		}
		return points
	}
	return { meterProvider, collect }
}
