/**
 * Client-level probe (no flow): does a plain @kafkats/client consumer.runEach survive `docker restart kafka-chaos` / pause?
 * Run (from packages/flow): pnpm exec tsx tests/chaos/client-restart-probe.ts [restart|pause]
 * Prints consumer 'error' events, whether runEach() rejects (and its stack), and whether consumption resumes.
 */
import { execSync } from 'node:child_process'
import { makeClient, createTopics, uid, sleep, CONTAINER } from './common.js'

const fault = process.argv[2] ?? 'restart'
const client = await makeClient()
const topic = uid('probe')
await createTopics(client, [topic], 2)
const consumer = client.consumer({ groupId: uid('g'), autoOffsetReset: 'earliest', maxWaitMs: 100, sessionTimeoutMs: 6000 })
let got = 0
const T0 = Date.now()
const ev = (m: string) => console.log(`[+${((Date.now() - T0) / 1000).toFixed(1)}s] ${m}`)
consumer.on('error', (e: any) => ev(`consumer 'error' event: ${e?.constructor?.name}: ${e?.message}`))
let runDone: string | null = null
const run = consumer.runEach([topic], async () => { got++ }).then(() => { runDone = 'resolved' }, (e: any) => { runDone = `REJECTED: ${e?.message}\n${e?.stack?.split('\n').slice(0, 8).join('\n')}` })
const p = client.producer({ lingerMs: 0 })
const stop = { v: false }
;(async () => { let i = 0; while (!stop.v) { try { await p.send(topic, [{ key: 'k' + (i % 4), value: Buffer.from('v' + i) }]); i++ } catch {} await sleep(100) } })()
await sleep(3000); ev(`got=${got}`)
if (fault === 'restart') execSync(`docker restart ${CONTAINER}`); else { execSync(`docker pause ${CONTAINER}`); await sleep(25000); execSync(`docker unpause ${CONTAINER}`) }
ev('fault over')
const before = got
await sleep(30000)
ev(`got=${got} (progress after fault: ${got - before}) runEach=${runDone ?? 'still running'}`)
stop.v = true
consumer.stop()
await Promise.race([run, sleep(10000)])
process.exit(0)
