import assert from 'node:assert/strict';
import {execFileSync, spawn} from 'node:child_process';
import {once} from 'node:events';
import {mkdtemp, readFile, rm} from 'node:fs/promises';
import {createServer} from 'node:http';
import {createRequire} from 'node:module';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {createInterface} from 'node:readline';
import {fileURLToPath} from 'node:url';

const dir = path.dirname(fileURLToPath(import.meta.url));
const workspace = path.resolve(dir, '../../..');
const sdk = path.join(workspace, 'flare-im-core-sdk');
export const webRequire = createRequire(path.join(workspace, 'flare-im-core-examples/apps/flare-core-web-app/package.json'));
const {chromium} = webRequire('playwright');
const {build} = webRequire('esbuild');
export const sample = JSON.parse(await readFile(path.join(dir, 'send.json')));

function nativeOutbox(dataRoot) {
  const query = `
import json, sqlite3, sys
from pathlib import Path
db_path, = Path(sys.argv[1]).rglob("flare_im_sdk.db")
with sqlite3.connect(db_path.as_uri() + "?mode=ro", uri=True) as db:
    print(json.dumps(db.execute("SELECT client_msg_id, conversation_id FROM pending_sends ORDER BY enqueued_at_ms, rowid").fetchall()))
`;
  return JSON.parse(execFileSync('python3', ['-c', query, dataRoot]));
}

// One reply line per request: the result JSON, or {"__error": details} from the driver.
export function sdkError(details) {
  return Object.assign(Error(details.message ?? details.code), {sdk: details});
}
function lineReply(line) {
  const value = JSON.parse(line);
  if (value && typeof value === 'object' && '__error' in value) throw sdkError(value.__error);
  return value;
}

export async function withBindings(libraryArg, pkgArg, run) {
  if (!libraryArg || !pkgArg) throw Error('Expected the C shared library and release wasm-pack pkg directory');
  const library = path.resolve(libraryArg);
  const pkg = path.resolve(pkgArg);
  const root = await mkdtemp(path.join(tmpdir(), 'flare-parity-'));
  const driver = process.env.ACK_NATIVE_DRIVER || path.join(root, 'native-driver');
  let server;
  const clients = new Set();
  const storageScopes = new Map();
  function track(client) {
    clients.add(client);
    const close = client.close;
    client.close = async () => {
      if (clients.delete(client)) await close();
    };
    return client;
  }
  async function native(name = 'native', onAck) {
    const dataUrl = path.join(root, name);
    const child = spawn(driver, [], {stdio: ['pipe', 'pipe', 'inherit', 'pipe']});
    const exit = once(child, 'exit');
    let killedDuringCommit = false;
    const lines = createInterface({input: child.stdout})[Symbol.asyncIterator]();
    const acks = [];
    const syncs = [];
    createInterface({input: child.stdio[3]}).on('line', line => {
      const event = JSON.parse(line);
      if (event.ack) {
        acks.push(event.ack);
        onAck?.(event.ack, performance.now());
      } else syncs.push(event);
    });
    async function raw(op, body) {
      child.stdin.write(op + '\t' + body + '\n');
      const watchdog = setTimeout(() => child.kill(), 45000);
      try {
        const line = await lines.next();
        if (line.done) throw Error('C driver exited during ' + op);
        return line.value;
      } finally { clearTimeout(watchdog); }
    }
    return track({
      dataUrl, acks, syncs, raw, outbox: () => nativeOutbox(dataUrl),
      storedReceive: (user, cid) => JSON.parse(execFileSync('python3',
        [path.join(dir, 'stored_receive.py'), dataUrl, user, cid])),
      async invoke(op, params) {
        let body = JSON.stringify(params);
        if (op === 'sdk.prepare') body = params.userId;
        if (op === 'sdk.connect') body = params.userId + '\t' + params.token;
        return lineReply(await raw(op, body));
      },
      async crashDuringCommit(point, id, send, target = {}) {
        const observer = spawn('python3', [path.join(dir, 'crash_sqlite.py'), dataUrl, String(child.pid), id, point, JSON.stringify(target)],
          {stdio: ['ignore', 'pipe', 'inherit'], timeout: 25000});
        const observedExit = once(observer, 'exit');
        const output = createInterface({input: observer.stdout})[Symbol.asyncIterator]();
        try {
          assert.equal((await output.next()).value, 'armed');
          const submitted = point !== 'outgoing' ? send() :
            assert.rejects(send(), /C driver exited during message.send/);
          // Register the rejection handler immediately in case the fault fails to fire.
          await Promise.all([submitted, (async () => {
            assert.equal((await output.next()).value, 'uncommitted WAL frame observed; writer killed');
            assert.deepEqual(await observedExit, [0, null]);
          })()]);
          assert.deepEqual(await exit, [null, 'SIGKILL']);
          killedDuringCommit = true;
        } finally {
          if (observer.exitCode === null && !observer.signalCode) observer.kill();
        }
      },
      async close() {
        if (killedDuringCommit) return;
        child.stdin.end();
        const timer = setTimeout(() => child.kill(), 5000);
        try {
          const [code] = await exit;
          assert.equal(code, 0, 'C driver exit');
        } finally { clearTimeout(timer); }
      },
    });
  }
  async function web(name = 'browser') {
    const context = await chromium.launchPersistentContext(path.join(root, name), {
      headless: true, executablePath: process.env.CHROME_PATH || undefined,
    });
    try {
      const page = await context.newPage();
      const acks = [];
      const syncs = [];
      await page.exposeFunction('recordScope', scope => storageScopes.set(name, scope));
      await page.exposeFunction('recordAck', ack => acks.push(ack));
      await page.exposeFunction('recordSync', run => syncs.push(run));
      await page.goto('http://127.0.0.1:' + server.address().port);
      await page.evaluate(async () => {
        const {createWasmIndexedDbStorageHost} = await import('/host.mjs');
        const wasm = await import('/pkg/flare_im_core_sdk.js');
        await wasm.default();
        const runtime = wasm.createWasmRuntime();
        const host = createWasmIndexedDbStorageHost();
        let account;
        const load = host.loadSnapshot.bind(host);
        host.loadSnapshot = async args => {
          account = args;
          await window.recordScope(args.userId);
          return load(args);
        };
        runtime.setStorageHost(host);
        runtime.setEventCallback(({channel, payload}) => {
          if (channel === 'im://send_ack') window.recordAck(payload.ack);
          if (channel === 'im://sync_readiness') window.recordSync(payload);
        });
        // Raw JSON in, raw JSON out: u64 values beyond 2^53 reach the core unrounded,
        // and the reply text shows exactly what the binding produced.
        window.callSdkRaw = async (op, text) => {
          const big = (_, value) => typeof value === 'bigint' ? value.toString() : value;
          try {
            return JSON.stringify(await runtime.invoke(op, text) ?? null, big);
          } catch (error) {
            const {code, reason, retryable, message} = error ?? {};
            return JSON.stringify({__error: {code, reason, retryable, message}});
          }
        };
        window.callSdk = (op, req) => new Promise((resolve, reject) => {
          const timer = setTimeout(() => reject(Error('WASM timeout during ' + op)), 45000);
          runtime.invoke(op, JSON.stringify(req)).then(resolve, reject).finally(() => clearTimeout(timer));
        });
        // A fresh browser can inspect the previously opened storage namespace before sdk.prepare.
        window.readStoredReceive = async (scope, user, cid) => {
          const request = indexedDB.open('flare-core-web-app');
          const result = request => new Promise((resolve, reject) => {
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
          });
          request.onupgradeneeded = () => request.transaction.abort();
          const db = await result(request);
          const stores = ['wasmMessages', 'wasmConversations', 'wasmCursors', 'wasmPendingSends'];
          let rows;
          try {
            const tx = db.transaction(stores, 'readonly');
            rows = await Promise.all(stores.map(store =>
              result(tx.objectStore(store).index('userId').getAll(scope))));
          } finally { db.close(); }
          const [messages, summaries, cursors, pending] = rows;
          const c = summaries.find(row => row.conversation.conversationId === cid).conversation;
          const cursor = cursors.find(row => row.cursorKey === user + ':' + cid)?.value.split(':').map(Number);
          return {
            messages: messages.map(row => row.message).filter(m => m.conversationId === cid)
              .sort((a, b) => a.conversationSeq - b.conversationSeq)
              .map(m => [m.clientMsgId, m.serverId, m.conversationSeq]),
            lastId: c.lastMessageId, lastSender: c.lastSenderId, lastAt: c.lastMessageAt, preview: c.lastMessagePreview,
            unread: c.unreadCount, readSeq: c.lastReadSeq, maxSeq: c.maxSeq,
            pendingRead: c.ext?.['_local.pendingReadSeq'] ?? null,
            receipt: JSON.parse(c.ext?.['_local.messageProjectionRanges'] ?? 'null'),
            cursor: cursor ? {seq: cursor[0], syncedAt: cursor[1]} : null,
            outbox: pending.map(row => row.entry).sort((a, b) => a.enqueued_at_ms - b.enqueued_at_ms)
              .map(row => [row.client_msg_id, row.conversation_id]),
          };
        };
        window.readOutbox = async () => JSON.parse(await load(account)).pendingSends
          .sort((a,b) => a.enqueued_at_ms - b.enqueued_at_ms).map(row => [row.client_msg_id, row.conversation_id]);
      });
      return track({acks, syncs, outbox: () => page.evaluate(() => window.readOutbox()),
        raw: (op, text) => page.evaluate(([op, text]) => window.callSdkRaw(op, text), [op, text]),
        storedReceive: (user, cid) => page.evaluate(args => window.readStoredReceive(...args),
          [storageScopes.get(name), user, cid]),
        invoke: (op, req) => page.evaluate(([op,req]) => window.callSdk(op, req), [op, req]),
        async crashDuringCommit(point, id, send, target = {}) {
          // The production host owns the transaction. Observe the targeted put success,
          // then keep it live with reads until Chromium terminates the renderer.
          await page.evaluate(([point, id, target, scope]) => {
            const put = IDBObjectStore.prototype.put;
            function matches(store, row) {
              switch (point) {
                case 'outgoing': return store === 'wasmPendingSends' && row.entry?.client_msg_id === id;
                case 'incoming': return store === 'wasmMessages' && row.message?.clientMsgId === id;
                case 'before-projection': {
                  if (store !== 'wasmConversations' || row.userId !== scope ||
                      row.conversation.conversationId !== target.cid) return false;
                  const receipt = row.conversation.ext?.['_local.messageProjectionRanges'];
                  return receipt && JSON.parse(receipt).materializedSeq === target.seq;
                }
                case 'before-cursor': return store === 'wasmCursors' && row.userId === scope &&
                  row.cursorKey === target.user + ':' + target.cid && Number(row.value.split(':')[0]) === target.seq;
                default: throw Error('Unknown crash point ' + point);
              }
            }
            IDBObjectStore.prototype.put = function (row, ...args) {
              const request = put.call(this, row, ...args);
              if (matches(this.name, row)) {
                request.addEventListener('success', () => {
                  const keepActive = () => { this.get(row.key).onsuccess = keepActive; };
                  keepActive();
                  window.targetPutStaged = true;
                });
              }
              return request;
            };
          }, [point, id, target, storageScopes.get(name)]);
          const cdp = await context.newCDPSession(page);
          const submitted = send().then(() => 'completed', error => error);
          await page.waitForFunction(() => window.targetPutStaged, null, {timeout: 15000});
          if (point === 'incoming') await new Promise(resolve => setTimeout(resolve, 500));
          const crash = page.waitForEvent('crash', {timeout: 10000});
          void cdp.send('Page.crash').catch(() => {});
          await crash;
          // A crashed target can stay attached in CDP. Closing it releases pending evaluate calls.
          await page.close({runBeforeUnload: false});
          const result = await submitted;
          if (point !== 'outgoing') assert.equal(result, 'completed', 'peer submitted message');
          else assert.ok(result instanceof Error, 'WASM send was interrupted inside the active transaction');
        },
        close: () => context.close()});
    } catch (e) { await context.close(); throw e; }
  }
  // The Tauri command path without a window: bindings/tauri/examples/parity_driver.rs.
  let tauriDriver = process.env.TAURI_PARITY_DRIVER;
  async function tauri(name = 'tauri') {
    if (!tauriDriver) {
      const engine = path.resolve(dir, '../..');
      execFileSync('cargo', ['build', '--locked', '-p', 'flare-im-core-sdk-tauri', '--example', 'parity_driver'],
        {cwd: engine, stdio: 'inherit', env: {...process.env, RUSTC_WRAPPER: ''}});
      const {target_directory: target} = JSON.parse(execFileSync('cargo',
        ['metadata', '--no-deps', '--format-version', '1'], {cwd: engine, maxBuffer: 64 << 20}));
      tauriDriver = path.join(target, 'debug/examples/parity_driver');
    }
    const child = spawn(tauriDriver, [], {stdio: ['pipe', 'pipe', 'inherit']});
    const exit = once(child, 'exit');
    const lines = createInterface({input: child.stdout})[Symbol.asyncIterator]();
    async function raw(op, body) {
      child.stdin.write(op + '\t' + body + '\n');
      const watchdog = setTimeout(() => child.kill(), 45000);
      try {
        const line = await lines.next();
        if (line.done) throw Error('Tauri driver exited during ' + op);
        return line.value;
      } finally { clearTimeout(watchdog); }
    }
    return track({
      dataUrl: path.join(root, name),
      raw,
      async invoke(op, params) {
        return lineReply(await raw(op, op === 'sdk.prepare' ? params.userId : JSON.stringify(params)));
      },
      async close() {
        child.stdin.end();
        const timer = setTimeout(() => child.kill(), 5000);
        try {
          const [code] = await exit;
          assert.equal(code, 0, 'Tauri driver exit');
        } finally { clearTimeout(timer); }
      },
    });
  }
  let runError;
  try {
    if (!process.env.ACK_NATIVE_DRIVER) {
      execFileSync(process.env.CC || 'cc', [path.join(dir,'native.c'), '-I', path.join(sdk,'native/abi'),
        library, '-pthread', '-Wl,-rpath,' + path.dirname(library), '-o', driver]);
    }
    const host = await build({entryPoints: [path.join(sdk,
      'packages/flare-core-typescript-sdk/src/adapters/web/idbWasmStorageHost.ts')], bundle:true, format:'esm', write:false});
    server = createServer(async (req,res) => {
      try {
        const name = new URL(req.url,'http://localhost').pathname;
        if (name === '/') return res.end('<!doctype html><title>Storage parity</title>');
        res.setHeader('Content-Type',name.endsWith('.wasm')?'application/wasm':'text/javascript');
        if (name === '/host.mjs') return res.end(host.outputFiles[0].contents);
        if (name.startsWith('/pkg/')) return res.end(await readFile(path.join(pkg,name.slice(5))));
      } catch { /* A missing package asset returns 404 below. */ }
      res.writeHead(404).end();
    });
    server.listen(0, '127.0.0.1');
    await once(server,'listening');
    return await run({native, web, tauri});
  } catch (error) {
    runError = error;
    throw error;
  } finally {
    const closed = await Promise.allSettled([...clients].map(c => c.close()));
    if (server?.listening) await new Promise(resolve => server.close(resolve));
    await rm(root, {recursive: true, force: true});
    if (!runError) {
      const failure = closed.find(result => result.status === 'rejected');
      if (failure) throw failure.reason;
    }
  }
}
