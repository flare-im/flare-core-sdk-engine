import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {createInterface} from 'node:readline';
import {queryServer} from './live_stack.mjs';

export function assertOwnedDatabase() {
  const prefix = process.env.LIVE_STACK_PREFIX;
  assert.ok(prefix);
  assert.equal(queryServer('', "SELECT to_json(shobj_description(oid, 'pg_database')) FROM pg_database WHERE datname=current_database()"),
    prefix + '/v1', 'requires the suite-owned database');
}

// SHARE pauses the writer. ACCESS EXCLUSIVE also pauses the archive receipt SELECT.
// PostgreSQL releases both locks on disconnect or idle transaction timeout.
export async function holdArchive(blockLookups = false) {
  const db = new URL(process.env.LIVE_SMOKE_CORE_DB);
  assert.equal(db.hostname, '127.0.0.1');
  assertOwnedDatabase();
  const idleLimit = blockLookups ? '75s' : '20s';
  const mode = blockLookups ? "ACCESS EXCLUSIVE" : "SHARE";
  const p = spawn('psql', ['-XAtqw', '-v', 'ON_ERROR_STOP=1'], {stdio: ['pipe','pipe','inherit'],
    env: {...process.env, PGAPPNAME: 'flare-ack-archive-lock', PGHOST: db.hostname, PGPORT: db.port, PGDATABASE: db.pathname.slice(1),
      PGUSER: decodeURIComponent(db.username), PGPASSWORD: decodeURIComponent(db.password)}});
  const exited = once(p, 'close');
  const lines = createInterface({input: p.stdout})[Symbol.asyncIterator]();
  p.stdin.write(`BEGIN; SET LOCAL lock_timeout='3s'; SET LOCAL idle_in_transaction_session_timeout='${idleLimit}'; LOCK TABLE messages IN ${mode} MODE; SELECT 'locked';\n`);
  assert.equal((await lines.next()).value, 'locked');
  return async () => {
    p.stdin.end('ROLLBACK;\n');
    assert.deepEqual(await exited, [0, null]);
  };
}


// Read the service's cumulative histogram. Snapshot differences isolate each test window
// without resetting shared counters or timing the instrumentation on the send path.
export async function receiptMetrics() {
  const url = new URL(process.env.LIVE_SMOKE_INGEST_METRICS);
  assert.equal(url.hostname, '127.0.0.1');
  const response = await fetch(url, {signal: AbortSignal.timeout(5000)});
  assert.equal(response.status, 200);
  const outcomes = {};
  let seconds = 0;
  for (const line of (await response.text()).split('\n')) {
    const match = /^message_orchestrator_send_stage_duration_seconds_(count|sum)\{([^}]+)\} (\S+)$/.exec(line);
    if (!match) continue;
    const labels = Object.fromEntries([...match[2].matchAll(/(\w+)="([^"]*)"/g)].map(m => [m[1], m[2]]));
    if (labels.stage !== 'archive_receipt') continue;
    const value = Number(match[3]);
    if (match[1] === 'count') outcomes[labels.outcome] = value;
    if (match[1] === 'sum') seconds += value;
  }
  return {outcomes, seconds};
}
