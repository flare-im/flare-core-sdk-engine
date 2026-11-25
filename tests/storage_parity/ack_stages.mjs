import assert from 'node:assert/strict';
import {open, stat} from 'node:fs/promises';
import {join} from 'node:path';
import {stripVTControlCharacters} from 'node:util';

const files = ['gw-a', 'storage-writer', 'message-ingest'];
const log = service => join(process.env.LIVE_SMOKE_DIR, 'logs', service + '.log');

export async function stageOffsets() {
  return Promise.all(files.map(async name => (await stat(log(name))).size));
}

async function readWindow(service, offset, visit) {
  const file = await open(log(service));
  try {
    const size = (await file.stat()).size;
    assert.ok(size > offset, 'missing profile output from ' + service);
    for await (const line of file.readLines({start: offset, end: size - 1, autoClose: false})) {
      if (!line.includes(' duration') && !line.includes('archive transaction committed') && !line.includes('inbound message dispatched')) continue;
      visit(stripVTControlCharacters(line));
    }
  } finally {
    await file.close();
  }
}

function field(line, key) {
  return new RegExp('\\b' + key + '="?([^"\\s,}]+)').exec(line)?.[1];
}

function ms(line, key) {
  const value = Number(field(line, key));
  assert.ok(Number.isFinite(value), 'missing timing ' + key);
  return value / 1000;
}

// Window-local client IDs are unique, including across senders. Transport frame IDs
// correlate the queue sample independently of the IM envelope. SQL round trips in the
// same send are summed before computing percentiles. Writer times overlap verification.
export async function readStages(offsets, rows, receiptCalls, historicalCalls) {
  const byId = new Map(rows.map(r => [r.id, {
    queue: [], archive: [], admission: [], transaction: [], commit: [], wireToCommit: [],
    historicalPool: 0, historicalQuery: 0, receiptPool: 0, receiptQuery: 0,
    wire: r.durableWireMs, startUnixMs: r.startUnixMs, frames: r.frames.length,
  }]));
  const queueByFrame = new Map();
  const wireFrames = new Map(rows.map(r => [r.id, r.frames]));
  assert.equal(byId.size, rows.length);
  const calls = {receipt: 0, historical: 0};
  await Promise.all(files.map((service, index) => readWindow(service, offsets[index], line => {
    if (service === 'gw-a') {
      const frameId = field(line, 'frame_id');
      const key = field(line, 'connection_id') + '/' + frameId;
      if (line.includes('inbound queue duration')) {
        queueByFrame.set(key, ms(line, 'queue_us'));
      } else if (line.includes('inbound message dispatched')) {
        const id = field(line, 'client_id');
        if (!byId.has(id)) return;
        assert.ok(wireFrames.get(id).includes(frameId));
        assert.ok(queueByFrame.has(key), 'missing connection-scoped queue entry');
        byId.get(id).queue.push(queueByFrame.get(key));
        queueByFrame.delete(key);
      }
      return;
    }
    const r = byId.get(field(line, 'client_id'));
    if (!r) return;
    if (line.includes('archive transaction committed')) {
      r.transaction.push(ms(line, 'transaction_us'));
      r.commit.push(ms(line, 'commit_us'));
      r.wireToCommit.push(ms(line, 'observed_unix_us') - r.startUnixMs);
    } else if (line.includes('archive receipt duration')) {
      r.archive.push(ms(line, 'elapsed_us'));
    } else if (line.includes('ingest admission duration')) {
      r.admission.push(ms(line, 'admission_us'));
    } else if (line.includes('ledger query duration')) {
      const kind = field(line, 'kind');
      assert.ok(Object.hasOwn(calls, kind));
      calls[kind]++;
      r[kind + 'Pool'] += ms(line, 'pool_us');
      r[kind + 'Query'] += ms(line, 'query_us');
    }
  })));
  assert.equal(calls.historical, historicalCalls);
  // A receipt SQL canceled during a deadline may be absent from either counter.
  const complete = rows.every(r => r.attempts === 1 && r.provisional === 0 && r.durableWireMs !== null);
  if (complete) assert.equal(calls.receipt, receiptCalls);
  return Array.from(byId.values(), r => {
    assert.equal(r.queue.length, r.frames, 'one queue sample per uplink');
    assert.equal(r.archive.length, r.frames, 'one verification sample per uplink');
    assert.equal(r.admission.length, r.frames, 'one ingest admission per uplink');
    assert.equal(r.transaction.length, 1, 'one committed archive transaction per send');
    const sum = values => values.reduce((a, b) => a + b, 0);
    return {wire: r.wire, queue: sum(r.queue), admission: sum(r.admission), archive: sum(r.archive),
      historicalPool: r.historicalPool, historicalQuery: r.historicalQuery,
      receiptPool: r.receiptPool, receiptQuery: r.receiptQuery, transaction: r.transaction[0],
      commit: r.commit[0], wireToCommit: r.wireToCommit[0],
      commitToAck: r.wire === null ? null : r.wire - r.wireToCommit[0]};
  });
}

export function summarizeStages(samples) {
  if (!samples.length) return {};
  const durable = samples.filter(s => s.wire !== null).sort((a, b) => b.wire - a.wire);
  const slowest = durable.slice(0, Math.ceil(durable.length / 100));
  const stages = {};
  for (const name of Object.keys(samples[0])) {
    const sorted = samples.map(s => s[name]).filter(n => n !== null).sort((a, b) => a - b);
    if (!sorted.length) continue;
    const rank = q => +sorted[Math.ceil(sorted.length * q) - 1].toFixed(2);
    stages[name] = {p95: rank(.95), p99: rank(.99), slowestWire1pctMean: slowest.length
      ? +(slowest.reduce((s, row) => s + row[name], 0) / slowest.length).toFixed(2) : null};
  }
  return {stages};
}
