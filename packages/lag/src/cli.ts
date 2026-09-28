#!/usr/bin/env node
/**
 * Standalone Prometheus time-lag exporter. Complements kafka_exporter: it emits only what kafka_exporter
 * cannot (seconds behind, records lost to retention) and none of the offset/topology series.
 *
 *   kafkats-lag --brokers localhost:9092 --groups '/^orders-/' --port 9464
 *
 * Every flag can also be set through the environment variable named in `--help`.
 */
import { readFileSync } from 'node:fs'
import { createServer, type Server } from 'node:http'
import { parseArgs } from 'node:util'
import { KafkaClient, createLogger, type KafkaClientConfig, type LogLevel } from '@kafkats/client'
import { parseNameFilter } from './filter.js'
import { LagMetrics } from './metrics.js'
import { LagMonitor } from './monitor.js'
import type { LagMode } from './types.js'

type SaslConfig = NonNullable<KafkaClientConfig['sasl']>
type TlsConfig = NonNullable<KafkaClientConfig['tls']>

const HELP = `kafkats-lag - Kafka consumer group time lag exporter (seconds behind, per partition)

Usage: kafkats-lag [options]

Kafka
  --brokers <list>          Comma-separated bootstrap servers        [KAFKA_BROKERS] (required)
  --client-id <id>          Client ID                                 [KAFKA_CLIENT_ID] (default: kafkats-lag)
  --cluster-name <name>     Value of the cluster_name metric attribute [KAFKA_CLUSTER_NAME]
  --sasl-mechanism <m>      PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512     [KAFKA_SASL_MECHANISM]
  --sasl-username <user>                                              [KAFKA_SASL_USERNAME]
  --sasl-password <pass>                                              [KAFKA_SASL_PASSWORD]
  --tls                     Enable TLS                                [KAFKA_TLS=true]
  --tls-ca <file>           CA certificate file                       [KAFKA_TLS_CA]
  --tls-cert <file>         Client certificate file (mTLS)            [KAFKA_TLS_CERT]
  --tls-key <file>          Client key file (mTLS)                    [KAFKA_TLS_KEY]
  --tls-insecure            Skip server certificate verification      [KAFKA_TLS_INSECURE=true]

Selection
  --groups <filter>         Groups to monitor: a,b,c or /regex/       [KAFKA_LAG_GROUPS] (default: all)
  --exclude-groups <filter> Groups to skip                            [KAFKA_LAG_EXCLUDE_GROUPS]
  --topics <filter>         Topics to report                          [KAFKA_LAG_TOPICS] (default: all)
  --exclude-topics <filter> Topics to skip                            [KAFKA_LAG_EXCLUDE_TOPICS]

Collection
  --interval <seconds>      Collection interval                       [KAFKA_LAG_INTERVAL] (default: 30)
  --mode <mode>             exact | estimate | auto                   [KAFKA_LAG_MODE] (default: exact)
                              exact: read the timestamp of the record at the committed offset
                              estimate: offset lag / recent produce rate; no fetches, no value while idle
                              auto: estimate, fetching only where no estimate is possible
  --read-committed          Measure lag against the last stable offset [KAFKA_LAG_READ_COMMITTED=true]
  --max-fetches <n>         Concurrent timestamp fetches              [KAFKA_LAG_MAX_FETCHES] (default: 8)

Export
  --port <port>             Prometheus listen port                    [KAFKA_LAG_PORT] (default: 9464)
  --host <host>             Prometheus listen host                    [KAFKA_LAG_HOST] (default: 0.0.0.0)

  --log-level <level>       silent | error | warn | info | debug      [KAFKA_TS_LOG_LEVEL] (default: info)
  --help
`

interface Options {
	brokers: string[]
	clientId: string
	clusterName: string | undefined
	sasl: SaslConfig | undefined
	tls: TlsConfig | undefined
	groups: string | undefined
	excludeGroups: string | undefined
	topics: string | undefined
	excludeTopics: string | undefined
	intervalMs: number
	mode: LagMode
	readCommitted: boolean
	maxFetches: number
	port: number
	host: string
	logLevel: LogLevel
}

function parseOptions(argv: string[]): Options | null {
	const { values } = parseArgs({
		args: argv,
		options: {
			brokers: { type: 'string' },
			'client-id': { type: 'string' },
			'cluster-name': { type: 'string' },
			'sasl-mechanism': { type: 'string' },
			'sasl-username': { type: 'string' },
			'sasl-password': { type: 'string' },
			tls: { type: 'boolean' },
			'tls-ca': { type: 'string' },
			'tls-cert': { type: 'string' },
			'tls-key': { type: 'string' },
			'tls-insecure': { type: 'boolean' },
			groups: { type: 'string' },
			'exclude-groups': { type: 'string' },
			topics: { type: 'string' },
			'exclude-topics': { type: 'string' },
			interval: { type: 'string' },
			mode: { type: 'string' },
			'read-committed': { type: 'boolean' },
			'max-fetches': { type: 'string' },
			port: { type: 'string' },
			host: { type: 'string' },
			'log-level': { type: 'string' },
			help: { type: 'boolean', short: 'h' },
		},
		strict: true,
	})

	if (values.help) return null

	const env = process.env
	const str = (flag: string | undefined, envName: string): string | undefined => flag ?? env[envName]
	const bool = (flag: boolean | undefined, envName: string): boolean => flag ?? env[envName] === 'true'
	const num = (flag: string | undefined, envName: string, fallback: number): number => {
		const raw = str(flag, envName)
		if (raw === undefined) return fallback
		const n = Number(raw)
		if (!Number.isFinite(n) || n <= 0) fail(`${envName}: expected a positive number, got "${raw}"`)
		return n
	}

	const brokersRaw = str(values.brokers, 'KAFKA_BROKERS')
	if (!brokersRaw) fail('--brokers (or KAFKA_BROKERS) is required')
	const brokers = brokersRaw
		.split(',')
		.map(b => b.trim())
		.filter(Boolean)

	const mechanism = str(values['sasl-mechanism'], 'KAFKA_SASL_MECHANISM')
	let sasl: SaslConfig | undefined
	if (mechanism) {
		const username = str(values['sasl-username'], 'KAFKA_SASL_USERNAME')
		const password = str(values['sasl-password'], 'KAFKA_SASL_PASSWORD')
		if (!username || password === undefined)
			fail('--sasl-username and --sasl-password are required with --sasl-mechanism')
		if (mechanism !== 'PLAIN' && mechanism !== 'SCRAM-SHA-256' && mechanism !== 'SCRAM-SHA-512') {
			fail(`unsupported SASL mechanism "${mechanism}" (use the library API for OAUTHBEARER)`)
		}
		sasl = { mechanism, username, password }
	}

	let tls: TlsConfig | undefined
	const tlsEnabled = bool(values.tls, 'KAFKA_TLS') || !!str(values['tls-ca'], 'KAFKA_TLS_CA')
	if (tlsEnabled) {
		const ca = str(values['tls-ca'], 'KAFKA_TLS_CA')
		const cert = str(values['tls-cert'], 'KAFKA_TLS_CERT')
		const key = str(values['tls-key'], 'KAFKA_TLS_KEY')
		tls = {
			enabled: true,
			ca: ca ? readFileSync(ca) : undefined,
			cert: cert ? readFileSync(cert) : undefined,
			key: key ? readFileSync(key) : undefined,
			rejectUnauthorized: !bool(values['tls-insecure'], 'KAFKA_TLS_INSECURE'),
		}
	}

	const logLevel = (str(values['log-level'], 'KAFKA_TS_LOG_LEVEL') ?? 'info') as LogLevel
	if (!['silent', 'error', 'warn', 'info', 'debug'].includes(logLevel)) {
		fail(`KAFKA_TS_LOG_LEVEL: unsupported log level "${logLevel}"`)
	}

	return {
		brokers,
		clientId: str(values['client-id'], 'KAFKA_CLIENT_ID') ?? 'kafkats-lag',
		clusterName: str(values['cluster-name'], 'KAFKA_CLUSTER_NAME'),
		sasl,
		tls,
		groups: str(values.groups, 'KAFKA_LAG_GROUPS'),
		excludeGroups: str(values['exclude-groups'], 'KAFKA_LAG_EXCLUDE_GROUPS'),
		topics: str(values.topics, 'KAFKA_LAG_TOPICS'),
		excludeTopics: str(values['exclude-topics'], 'KAFKA_LAG_EXCLUDE_TOPICS'),
		intervalMs: num(values.interval, 'KAFKA_LAG_INTERVAL', 30) * 1000,
		mode: parseMode(str(values.mode, 'KAFKA_LAG_MODE')),
		readCommitted: bool(values['read-committed'], 'KAFKA_LAG_READ_COMMITTED'),
		maxFetches: num(values['max-fetches'], 'KAFKA_LAG_MAX_FETCHES', 8),
		port: num(values.port, 'KAFKA_LAG_PORT', 9464),
		host: str(values.host, 'KAFKA_LAG_HOST') ?? '0.0.0.0',
		logLevel,
	}
}

function parseMode(raw: string | undefined): LagMode {
	if (raw === undefined) return 'exact'
	if (raw === 'exact' || raw === 'estimate' || raw === 'auto') return raw
	return fail(`--mode: expected exact, estimate or auto, got "${raw}"`)
}

function fail(message: string): never {
	process.stderr.write(`kafkats-lag: ${message}\n`)
	process.exit(2)
}

async function main(): Promise<void> {
	let options: Options | null
	try {
		options = parseOptions(process.argv.slice(2))
	} catch (error) {
		fail((error as Error).message)
	}
	if (!options) {
		process.stdout.write(HELP)
		return
	}

	const logger = createLogger(options.logLevel, { component: 'kafkats-lag' })
	const metrics = new LagMetrics({ clusterName: options.clusterName })

	const client = new KafkaClient({
		brokers: options.brokers,
		clientId: options.clientId,
		sasl: options.sasl,
		tls: options.tls,
		logLevel: options.logLevel,
	})

	const monitor = new LagMonitor({
		client,
		groups: parseNameFilter(options.groups),
		excludeGroups: parseNameFilter(options.excludeGroups),
		topics: parseNameFilter(options.topics),
		excludeTopics: parseNameFilter(options.excludeTopics),
		intervalMs: options.intervalMs,
		mode: options.mode,
		maxConcurrentFetches: options.maxFetches,
		isolationLevel: options.readCommitted ? 'read_committed' : 'read_uncommitted',
		logger,
	})

	monitor.on('snapshot', snapshot => {
		metrics.update(snapshot, monitor.lastSuccessAt)
		const partitions = snapshot.groups.reduce((n, g) => n + g.partitions.length, 0)
		logger.info('lag collected', {
			groups: snapshot.groups.length,
			partitions,
			durationMs: snapshot.durationMs,
			errors: snapshot.errors.length,
			fetches: snapshot.stats.fetchRequests,
			fetchBytes: snapshot.stats.fetchBytes,
			...snapshot.stats.partitions,
		})
		for (const error of snapshot.errors) {
			logger.warn('lag collection error', { scope: error.scope, message: error.message })
		}
	})

	const server = createMetricsServer(metrics, monitor, options.intervalMs)
	await listen(server, options.port, options.host)
	logger.info('prometheus endpoint ready', {
		metricsUrl: `http://${options.host}:${options.port}/metrics`,
		healthUrl: `http://${options.host}:${options.port}/healthz`,
	})

	let shuttingDown = false
	const shutdown = async (signal: string): Promise<void> => {
		if (shuttingDown) return
		shuttingDown = true
		logger.info('shutting down', { signal })
		await monitor.stop()
		await closeServer(server)
		await client.disconnect().catch(() => {})
		process.exit(0)
	}
	process.on('SIGINT', () => void shutdown('SIGINT'))
	process.on('SIGTERM', () => void shutdown('SIGTERM'))

	logger.info('connecting', { brokers: options.brokers })
	await monitor.start()
}

function createMetricsServer(metrics: LagMetrics, monitor: LagMonitor, intervalMs: number): Server {
	return createServer((request, response) => {
		let pathname: string
		try {
			pathname = new URL(request.url ?? '/', 'http://localhost').pathname
		} catch {
			response.writeHead(400).end('Bad Request')
			return
		}

		if (pathname === '/metrics') {
			metrics.registry
				.metrics()
				.then(body => {
					response.writeHead(200, { 'content-type': metrics.registry.contentType })
					response.end(body)
				})
				.catch(() => response.writeHead(500).end())
			return
		}
		if (pathname === '/healthz') {
			// Liveness: a collection finished recently. Per-group/topic errors are reported in the body and
			// via kafkats_lag_collection_success, not as unhealthy, or one denied group would restart the pod.
			const latest = monitor.latest
			const staleAfterMs = 3 * intervalMs
			const healthy = latest !== null && Date.now() - (latest.collectedAt + latest.durationMs) < staleAfterMs
			response.writeHead(healthy ? 200 : 503, { 'content-type': 'application/json' })
			response.end(
				JSON.stringify({
					status: healthy ? 'ok' : 'unhealthy',
					lastCollectedAt: latest?.collectedAt ?? null,
					lastSuccessAt: monitor.lastSuccessAt,
					errors: latest?.errors ?? [],
				})
			)
			return
		}

		response.writeHead(404).end()
	})
}

function listen(server: Server, port: number, host: string): Promise<void> {
	return new Promise((resolve, reject) => {
		server.once('error', reject)
		server.listen(port, host, resolve)
	})
}

function closeServer(server: Server): Promise<void> {
	return new Promise(resolve => server.close(() => resolve()))
}

main().catch(error => {
	process.stderr.write(`kafkats-lag: ${(error as Error).stack ?? String(error)}\n`)
	process.exit(1)
})
