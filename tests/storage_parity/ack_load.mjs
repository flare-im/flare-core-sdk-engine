// Closed-loop, one outstanding logical send per connected native SDK. No psql in the timed path.
import assert from 'node:assert/strict';
import {createReadStream} from 'node:fs';
import {stat} from 'node:fs/promises';
import {createInterface} from 'node:readline';
import {join} from 'node:path';
import {stripVTControlCharacters} from 'node:util';
import {withBindings, sample} from './clients.mjs';
import {assertOwnedDatabase, receiptMetrics} from './archive_receipts.mjs';
import {ackLossRelay, brokerAccepted} from './fault_wire.mjs';
import {connect, getOne, prepare, queryServer, runId} from './live_stack.mjs';
import {stageOffsets, readStages, summarizeStages} from './ack_stages.mjs';
import {sendSample} from './session.mjs';

const levels = (process.env.ACK_BENCH_LEVELS ?? '1,4,16,32').split(',').map(Number);
const messages = Number(process.env.ACK_BENCH_MESSAGES ?? 1024);
const rounds = Number(process.env.ACK_BENCH_ROUNDS ?? 2);
assert.ok(levels.length && new Set(levels).size === levels.length);
assert.ok(levels.every(n => Number.isInteger(n) && n >= 1 && n <= 64));
assert.ok(Number.isSafeInteger(messages) && messages >= Math.max(...levels));
assert.ok(Number.isSafeInteger(rounds) && rounds >= 1);

function sqlStats() {
  return queryServer('', `WITH stats AS (
      SELECT calls, total_exec_time, query LIKE 'SELECT m.server_id, m.seq FROM message_idempotency_ledger l%' AS receipt
      FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
        AND (query LIKE 'SELECT m.server_id, m.seq FROM message_idempotency_ledger l%'
          OR query LIKE 'SELECT conversation_id, server_id, conversation_seq, durability%FROM message_idempotency_ledger%'))
    SELECT json_build_object('calls', coalesce(sum(calls) FILTER (WHERE receipt),0),
      'execMs', coalesce(sum(total_exec_time) FILTER (WHERE receipt),0),
      'historicalCalls', coalesce(sum(calls) FILTER (WHERE NOT receipt),0)) FROM stats`);
}

function delta(after, before) {
  return Object.fromEntries(Object.entries(after).map(([k, v]) => [k, v - (before[k] ?? 0)]));
}

function addCounts(target, changes) {
  for (const [key, count] of Object.entries(changes)) target[key] = (target[key] ?? 0) + count;
}

function percentile(sorted, q) {
  return sorted[Math.ceil(sorted.length * q) - 1];
}

// Read only bytes appended during this batch. The service records elapsed time before
// writing its trace; the independent Prometheus count and sum check the sample window.
const ingestLog = join(process.env.LIVE_SMOKE_DIR, 'logs', 'message-ingest.log');
async function measuredDurations(offset) {
  const end = (await stat(ingestLog)).size;
  assert.ok(end > offset, 'enable LIVE_STACK_ACK_PROFILE and rebuild message-ingest');
  const stream = createReadStream(ingestLog, {start: offset, end: end - 1});
  const samples = [];
  for await (const line of createInterface({input: stream})) {
    if (!line.includes('archive receipt duration')) continue;
    const match = /elapsed_us[=":\s]+(\d+)/.exec(stripVTControlCharacters(line));
    assert.ok(match, 'missing archive receipt elapsed_us');
    samples.push(Number(match[1]) / 1000);
  }
  return samples;
}

function report(windows) {
  const elapsed = windows.reduce((sum, w) => sum + w.elapsed, 0);
  const calls = windows.reduce((sum, w) => sum + w.calls, 0);
  const attempts = windows.reduce((sum, w) => sum + w.attempts, 0);
  const outcomes = {};
  for (const w of windows) addCounts(outcomes, w.outcomes);
  const stageMs = windows.flatMap(w => w.stageMs).sort((a,b) => a-b);
  const sorted = key => windows.flatMap(w => w[key]).sort((a, b) => a - b);
  const wire = sorted('firstAckMs'), sdk = sorted('sdkMs'), durable = sorted('durableWireMs');
  const n = sdk.length;
  const perSecond = count => +(count * 1000 / elapsed).toFixed(2);
  return {...summarizeStages(windows.flatMap(w => w.stages)), messages: n, attempts, elapsedMs: +elapsed.toFixed(1), sdkPersistedPerSec: perSecond(n),
    wirePersistedPerSec: perSecond(durable.length), syncOnly: n - durable.length, verifyPerSec: perSecond(attempts),
    firstAckP95Ms: +percentile(wire, .95).toFixed(2), firstAckP99Ms: +percentile(wire, .99).toFixed(2),
    sdkP95Ms: +percentile(sdk, .95).toFixed(2), sdkP99Ms: +percentile(sdk, .99).toFixed(2),
    wirePersistedP95Ms: durable.length ? +percentile(durable, .95).toFixed(2) : null,
    wirePersistedP99Ms: durable.length ? +percentile(durable, .99).toFixed(2) : null,
    verifyP95Ms: +percentile(stageMs, .95).toFixed(3),
    verifyP99Ms: +percentile(stageMs, .99).toFixed(3), sqlCalls: calls, historicalCalls: windows.reduce((sum,w) => sum+w.historicalCalls,0), sqlPerMessage: +(calls/n).toFixed(3),
    sqlExecMs: +windows.reduce((sum,w) => sum+w.execMs,0).toFixed(2), outcomes,
    provisional: windows.reduce((sum,w) => sum+w.provisional,0)};
}

function maybeComplete(active) {
  // A storage-backed sync can settle the SDK after admission while the gateway
  // is recovering. Count that completion separately from a gateway Persisted.
  if (active.sdk && active.attempts > 0 && active.frames.length === active.attempts) {
    active.completion.resolve();
  }
}

async function send(side, item) {
  const completion = Promise.withResolvers();
  const pending = {id: item.clientMsgId, attempts: 0, frames: [], first: null, completion};
  side.active = pending;
  const timeout = setTimeout(() => completion.reject(Error('send timeout ' + pending.id +
    ' attempts=' + pending.attempts + ' responses=' + pending.frames.length + ' sdk=' + Boolean(pending.sdk))), 60000);
  try {
    await Promise.all([sendSample(side.device.invoke, side.cid, item), completion.promise]);
    const expected = [side.cid, pending.sdk.serverId, pending.sdk.seq];
    assert.ok(pending.sdk.success && pending.sdk.conversationId === side.cid && pending.sdk.serverId && pending.sdk.seq > 0);
    assert.ok(pending.first !== null && pending.sdkAt >= pending.first);
    for (const frame of pending.frames) {
      assert.equal(frame.cid, side.cid);
      if (frame.persisted) assert.deepEqual([frame.cid, frame.serverId, frame.seq], expected);
    }
    const durable = pending.frames.find(f => f.persisted);
    return {id: pending.id, frames: pending.sentFrames, startUnixMs: pending.startUnixMs, serverId: pending.sdk.serverId, seq: pending.sdk.seq, sdkMs: pending.sdkAt - pending.first,
      firstAckMs: pending.frames[0].at - pending.first, durableWireMs: durable ? durable.at - pending.first : null,
      attempts: pending.attempts, provisional: pending.frames.filter(f => f.durability === brokerAccepted).length};
  } finally {
    clearTimeout(timeout);
    side.active = null;
  }
}

async function measure(sides, round, concurrency) {
  const before = await receiptMetrics(), startDb = sqlStats();
  const logOffset = (await stat(ingestLog)).size;
  const offsets = process.env.ACK_BENCH_STAGES === '1' ? await stageOffsets() : null;
  let next = 0;
  const start = performance.now();
  const perSide = await Promise.all(sides.map(async side => {
    const rows = [];
    while (next < messages) {
      const index = next++;
      const original = sample.messages[index % sample.messages.length];
      rows.push(await send(side, {...original, clientMsgId: original.clientMsgId + '-' + round + '-' + concurrency + '-' + index}));
    }
    return rows;
  }));
  const elapsed = performance.now() - start;
  const endDb = sqlStats(), after = await receiptMetrics();
  const rows = perSide.flat(), outcomes = delta(after.outcomes, before.outcomes);
  const attempts = rows.reduce((sum,r) => sum+r.attempts,0);
  const observations = Object.values(outcomes).reduce((a,b) => a+b,0);
  assert.equal(attempts, observations, 'wire attempts and isolated archive_receipt observations must agree');
  const stageMs = await measuredDurations(logOffset);
  assert.equal(stageMs.length, observations, 'one trace duration per counted archive receipt');
  const traceSeconds = stageMs.reduce((sum,ms) => sum + ms / 1000, 0);
  assert.ok(Math.abs(traceSeconds - (after.seconds - before.seconds)) <= observations * 1e-6 + 1e-7,
    'trace and histogram durations must agree to microsecond truncation');
  assert.equal(rows.length, messages);
  assert.ok(endDb.calls > startDb.calls);
  // Audit all identities after the timed window, including absence of duplicate rows.
  for (const [index, side] of sides.entries()) {
    const actual = queryServer(side.cid, `SELECT coalesce(json_agg(r),'[]') FROM
      (SELECT client_msg_id AS id, sender_id, server_id, seq FROM messages
       WHERE tenant_id=:'tenant' AND conversation_id=:'cid' AND client_msg_id=ANY(string_to_array(:'client_id',','))) r`,
      perSide[index].map(r => r.id).join(','));
    const byId = new Map(actual.map(r => [r.id, r]));
    assert.equal(byId.size, actual.length);
    for (const receipt of perSide[index]) {
      const stored = byId.get(receipt.id);
      assert.deepEqual([stored?.sender_id, stored?.server_id, stored?.seq], [side.user, receipt.serverId, receipt.seq]);
    }
  }
  const stages = offsets ? await readStages(offsets, rows, endDb.calls - startDb.calls, endDb.historicalCalls - startDb.historicalCalls) : [];
  return {elapsed, stages, historicalCalls: endDb.historicalCalls-startDb.historicalCalls, calls: endDb.calls-startDb.calls, execMs: endDb.execMs-startDb.execMs, outcomes,
    stageMs, sdkMs: rows.map(r => r.sdkMs), firstAckMs: rows.map(r => r.firstAckMs),
    durableWireMs: rows.map(r => r.durableWireMs).filter(ms => ms !== null), attempts,
    provisional: rows.reduce((sum,r) => sum+r.provisional,0)};
}

assertOwnedDatabase();
queryServer('', "CREATE EXTENSION IF NOT EXISTS pg_stat_statements; SELECT true::text");
sqlStats(); // Fail before starting clients if the server didn't preload the extension.
await withBindings(process.argv[2], process.argv[3], async ({native}) => {
  const sides = [], results = new Map(levels.map(n => [n, []]));
  try {
    for (let index = 0; index < Math.max(...levels); index++) {
      const user = 'load-' + runId + '-' + index, side = {user};
      side.wire = await ackLossRelay(ack => {
        const active = side.active;
        if (active?.id !== ack.clientId) return;
        active.frames.push(ack);
        maybeComplete(active);
      }, f => {
        if (side.active?.id !== f.clientId) return;
        assert.equal(f.cid, side.cid);
        if (side.active.first === null) {
          side.active.first = f.at;
          side.active.startUnixMs = performance.timeOrigin + f.at;
          side.active.sentFrames = [];
        }
        side.active.sentFrames.push(f.frameId);
        side.active.attempts++;
      });
      sides.push(side);
      side.device = await native(user, (ack, at) => {
        const active = side.active;
        if (active?.id !== ack.clientMsgId || ack.stage !== 'persisted' || active.sdk) return;
        active.sdk = ack;
        active.sdkAt = at;
        maybeComplete(active);
      });
      await prepare(side.device, user, side.wire.url);
      await connect(side.device, user);
      side.cid = (await getOne(side.device, user + '-peer')).conversationId;
      await send(side, {...sample.messages[0], clientMsgId: 'warm-' + index});
    }
    for (let round = 1; round <= rounds; round++) {
      for (const n of round % 2 ? levels : [...levels].reverse()) {
        const window = await measure(sides.slice(0,n), round, n);
        results.get(n).push(window);
        console.log(JSON.stringify({round, concurrency: n, ...report([window])}));
      }
    }
    for (const [concurrency, windows] of results) {
      console.log(JSON.stringify({pooled: true, concurrency, ...report(windows)}));
    }
  } finally {
    await Promise.all(sides.map(side => side.wire.close()));
  }
});
