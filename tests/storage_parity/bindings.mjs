// Stage 2.3: the same commands give the same observable results through the C ABI, the
// production WASM runtime and the Tauri command path. Offline; usage in storage/README.md.
//
//   node tests/storage_parity/bindings.mjs <libflare_im_core_sdk_ffi> <wasm-pack pkg>
//
// Covered per binding: structured errors (before prepare, invalid input, unknown
// operation), the largest JS-safe u64 and byte arrays through a stored message, view revisions
// before and after a local send, and cancellation / timeout outcomes.
import assert from 'node:assert/strict';
import {withBindings} from './clients.mjs';

const TENANT = 'parity23-tenant';
const USER = 'parity23-user';
const CID = 'group-parity23';
// 2^53 - 1: the largest u64 the contract guarantees at JavaScript hosts (manifest rules).
const BIG = '9007199254740991';

// Integers keep their source text, so a rounded or re-encoded u64 shows up as a diff.
function parseExact(text) {
  return JSON.parse(text, (_, value, context) =>
    typeof value === 'number' && !Number.isSafeInteger(value) && context?.source ? context.source : value);
}

async function settle(client, op, body) {
  const reply = parseExact(await client.raw(op, typeof body === 'string' ? body : JSON.stringify(body)));
  if (reply && typeof reply === 'object' && '__error' in reply) {
    const {code, reason, retryable, message} = reply.__error;
    return {error: {code, reason, retryable, message}};
  }
  return {ok: reply};
}

async function probe(client) {
  const seen = {};
  seen.beforePrepare = await settle(client, 'message.get_raw', {messageId: 'parity23-none'});
  const sdkConfig = {
    tenantId: TENANT, deviceId: 'parity23-device', dataUrl: client.dataUrl,
    endpoints: {baseUrl: 'http://127.0.0.1:9', websocketUrl: 'ws://127.0.0.1:9'},
  };
  if (!client.dataUrl) delete sdkConfig.dataUrl;
  assert.deepEqual(await settle(client, 'sdk.init', {environment: 'development', sdkConfig}), {ok: null});
  // Each driver frames sdk.prepare its own way (C and Tauri take the bare user ID).
  await client.invoke('sdk.prepare', {userId: USER});

  seen.invalidKey = await settle(client, 'message.get_by_key', {conversationId: '', senderId: USER, clientMsgId: 'x'});
  seen.unknownOperation = await settle(client, 'message.parity23_missing', {});

  const opened = await settle(client, 'view.timeline.open', {conversationId: CID, messageLimit: 20});
  const viewId = opened.ok?.viewId;
  seen.viewOpened = opened.error ?? {revision: opened.ok.revision};

  const draft = (await settle(client, 'message.create_text', {conversationId: CID, text: 'u64 · bytes'})).ok;
  draft.clientMsgId = 'parity23-client-1';
  draft.createdAt = draft.clientCreatedAt = 1780000000100;
  draft.extensions = {bin: [0, 1, 127, 128, 254, 255]};
  // Splice the u64 into the request text so it never passes through a JS number.
  const body = JSON.stringify({...draft, localState: {...draft.localState, sortTs: 0}})
    .replace('"sortTs":0', '"sortTs":' + BIG);
  seen.send = await settle(client, 'message.send', body);
  const stored = await settle(client, 'message.get_raw', {messageId: draft.clientMsgId});
  seen.stored = stored.error ?? {sortTs: stored.ok.localState.sortTs, extensions: stored.ok.extensions};

  if (viewId) {
    // view.updated is pushed asynchronously; the snapshot is the authoritative revision.
    await new Promise(resolve => setTimeout(resolve, 300));
    const snapshot = await settle(client, 'view.timeline.snapshot', {viewId});
    seen.viewAfterSend = snapshot.error ?? {
      revision: snapshot.ok.revision,
      rows: snapshot.ok.snapshot.data.messages.map(row => row.clientMsgId),
    };
    seen.viewClosed = await settle(client, 'view.close', {viewId});
  }

  seen.cancelMissing = await settle(client, 'media.cancel_transfer', {transferId: 'parity23-missing'});
  seen.confirmTimeout = await settle(client, 'message.await_confirmation',
    {conversationId: CID, senderId: USER, clientMsgId: draft.clientMsgId, timeoutMs: 50});
  return seen;
}

await withBindings(process.argv[2], process.argv[3], async ({native, web, tauri}) => {
  const results = {};
  for (const [name, open] of Object.entries({c: native, wasm: web, tauri})) {
    const client = await open('parity23-' + name);
    try {
      results[name] = await probe(client);
    } finally {
      await client.close();
    }
  }
  // Every binding must match C exactly, errors included.
  assert.deepEqual(results.wasm, results.c, 'WASM differs from C');
  assert.deepEqual(results.tauri, results.c, 'Tauri differs from C');

  const c = results.c;
  assert.equal(c.beforePrepare.error?.code, 'NOT_CONNECTED');
  assert.equal(c.invalidKey.error?.code, 'INVALID_PARAMETER');
  assert.equal(c.unknownOperation.error?.code, 'OPERATION_NOT_SUPPORTED');
  assert.equal(String(c.stored.sortTs), BIG, 'the JS-safe u64 bound survives every binding exactly');
  assert.deepEqual(c.stored.extensions, {bin: [0, 1, 127, 128, 254, 255]});
  assert.ok(c.viewAfterSend.revision > c.viewOpened.revision, 'a local send advances the view revision');
  assert.deepEqual(c.viewAfterSend.rows, ['parity23-client-1']);
  assert.deepEqual(c.cancelMissing, {ok: {cancelled: false}});
  assert.equal(c.confirmTimeout.error?.code, 'OPERATION_TIMEOUT');
  console.log('C / production WASM / Tauri matched:', JSON.stringify(c));
});
