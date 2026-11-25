// Run against the isolated Core live stack. The relay only forwards bytes or drops sockets.
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {randomUUID} from 'node:crypto';
import {once} from 'node:events';
import net from 'node:net';
import {setTimeout as delay} from 'node:timers/promises';
import {sample} from './clients.mjs';

const api = new URL(process.env.LIVE_SMOKE_CORE_API);
export const gateway = new URL(process.env.LIVE_SMOKE_WS_A);
const db = new URL(process.env.LIVE_SMOKE_CORE_DB);
export const tenantId = process.env.LIVE_SMOKE_TENANT;
for (const url of [api, gateway, db]) assert.equal(url.hostname, '127.0.0.1', 'isolated local stack');
assert.ok(tenantId);
export const runId = randomUUID().slice(0, 8);
export const projectionKey = '_local.messageProjectionRanges';
assert.equal(sample.messages.length, 2, 'this scenario sends one sample online and one offline');
export const [online, offline] = sample.messages;
export const missed = {clientMsgId: 'phase13-peer', text: '断线期间的对端消息', at: offline.at + 1};

export async function eventually(description, read, matches) {
  const deadline = Date.now() + 60000;
  let value;
  do {
    value = await read();
    if (matches(value)) return value;
    await delay(200);
  } while (Date.now() < deadline);
  throw Error(description + ' timed out: ' + JSON.stringify(value));
}

export async function relay() {
  let open = true, connections = 0;
  const sockets = new Set();
  function cut() {
    open = false;
    for (const s of sockets) s.destroy();
  }
  const server = net.createServer(client => {
    if (!open) return client.destroy();
    const upstream = net.connect(Number(gateway.port), gateway.hostname);
    connections++;
    for (const [side, other] of [[client, upstream], [upstream, client]]) {
      sockets.add(side);
      side.on('error', () => side.destroy());
      side.once('close', () => { sockets.delete(side); other.destroy(); });
      side.pipe(other);
    }
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const url = new URL(gateway);
  url.port = String(server.address().port);
  return {url: url.href, cut, restore: () => { open = true; }, count: () => connections,
    close: () => { cut(); return new Promise(resolve => server.close(resolve)); }};
}

export async function prepare(client, userId, ws, deviceId = userId + '-device', ackTimeoutSecs) {
  const sdkConfig = {tenantId, deviceId, ackTimeoutSecs,
    endpoints: {baseUrl: api.origin, websocketUrl: ws}};
  if (client.dataUrl) sdkConfig.dataUrl = client.dataUrl;
  await client.invoke('sdk.init', {environment: 'development', sdkConfig});
  await client.invoke('sdk.prepare', {userId});
}
export const status = c => c.invoke('connection.get_status', {});
export async function connect(client, userId, deviceId = userId + '-device') {
  const response = await fetch(new URL('/api/v1/auth/tokens', api), {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({tenantId, userId, deviceId}),
    signal: AbortSignal.timeout(10000),
  });
  assert.ok(response.ok, 'token service HTTP ' + response.status);
  const {data} = await response.json();
  assert.ok(data?.token, 'local token issuer returned a token');
  await client.invoke('sdk.connect', {userId, token: data.token});
  await eventually('online ' + userId, () => status(client), s => s.phase === 'online');
}
export const getOne = (c, sourceId) => c.invoke('conversation.get_one', {sourceId, conversationType: 'single'});
export const raw = (c, id) => c.invoke('message.get_raw', {messageId: id});

export function serverRows(cid) {
  const sql = `SELECT coalesce(json_agg(r ORDER BY seq), '[]') FROM
      (SELECT client_msg_id AS id, server_id, seq, sender_id,
              (extract(epoch FROM timestamp) * 1000)::bigint AS message_ms,
              extra->>'request_context' AS request_context, extra->>'conversation_id' AS routed_cid
         FROM messages
       WHERE tenant_id = :'tenant' AND conversation_id = :'cid') r;`;
  return queryServer(cid, sql);
}

export function receiptEvents(cid) {
  return queryServer(cid, `SELECT coalesce(json_agg(r ORDER BY seq), '[]') FROM
    (SELECT seq, event_type, operator_id FROM events
       WHERE tenant_id = :'tenant' AND conversation_id = :'cid') r;`);
}

export function queryServer(cid, sql, clientId = '') {
  return JSON.parse(execFileSync('psql', ['-XqAtw', '-v', 'ON_ERROR_STOP=1', '-v', 'tenant=' + tenantId,
    '-v', 'cid=' + cid, '-v', 'client_id=' + clientId], {input: sql, encoding: 'utf8', timeout: 6000, env: {...process.env,
    PGHOST: db.hostname, PGPORT: db.port, PGUSER: decodeURIComponent(db.username),
    PGPASSWORD: decodeURIComponent(db.password), PGDATABASE: db.pathname.slice(1)}}));
}
export async function durable(client, item, cid) {
  const events = () => client.acks.filter(a => a.clientMsgId === item.clientMsgId);
  const acks = await eventually('persisted ACK for ' + item.clientMsgId, events,
    list => list.some(a => a.stage === 'persisted'));
  const ack = acks.find(a => a.stage === 'persisted');
  assert.equal(ack.success, true);
  assert.equal(ack.conversationId, cid);
  assert.ok(ack.seq > 0 && ack.serverId);
  const rows = serverRows(cid).filter(r => r.id === item.clientMsgId);
  assert.equal(rows.length, 1, 'one durable server row');
  assert.equal(ack.seq, rows[0].seq);
  assert.equal(ack.serverId, rows[0].server_id);
  return ack.seq;
}

export async function snapshot(client, cid, user, peer, rows, expectedReadSeq = 1) {
  const {messages} = await client.invoke('message.list', {conversationId: cid, beforeSeq: 0, limit: 10});
  const conversation = await client.invoke('conversation.get', {conversationId: cid});
  const outbox = await client.outbox();
  const items = [online, missed, offline];
  assert.deepEqual(rows.map(r => r.id), items.map(m => m.clientMsgId));
  assert.deepEqual(rows.map(r => r.seq), [1, 2, 3]);
  assert.deepEqual(messages.map(m => m.clientMsgId), rows.map(r => r.id).reverse());
  const details = [];
  for (const [i, item] of items.entries()) {
    const m = await raw(client, item.clientMsgId), row = rows[i];
    assert.equal(m.serverId, row.server_id);
    assert.equal(m.conversationId, cid);
    assert.equal(m.senderId, row.sender_id);
    assert.equal(m.senderId, i === 1 ? peer : user);
    assert.equal(m.conversationSeq, row.seq);
    assert.equal(m.content.text, item.text);
    assert.equal(m.attributes.tenant_id, tenantId);
    assert.equal(m.attributes.request_context, row.request_context);
    assert.equal(m.attributes.conversation_id, row.routed_cid);
    assert.equal(m.createdAt, row.message_ms);
    assert.ok([item.at, row.message_ms].includes(m.clientCreatedAt), 'client creation time has a known source');
    assert.equal(m.localState.sending, false);
    assert.equal(m.localState.failed, false);
    assert.equal(m.receipt.status, 'sent');
    assert.equal(m.timelineKey, 'server:' + row.server_id);
    assert.equal(m.timelineSortTs, m.createdAt);
    assert.ok(m.localState.sortTs > 0);
    details.push(m);
  }
  assert.equal(conversation.conversationId, cid);
  assert.equal(conversation.maxSeq, rows.at(-1).seq);
  assert.equal(conversation.lastMessageId, rows.at(-1).server_id);
  assert.equal(conversation.lastMessageAt, details.at(-1).createdAt);
  assert.ok(conversation.updatedAtTs >= conversation.lastMessageAt,
    'conversation sort timestamp must include the last message: ' + conversation.updatedAtTs +
    ' < ' + conversation.lastMessageAt);
  assert.equal(conversation.businessType, 'single');
  assert.equal(conversation.participantVersion, 2);
  assert.equal(conversation.membersCount, 2);
  assert.equal(conversation.unreadCount, 1);
  assert.equal(conversation.lastReadSeq, expectedReadSeq);
  assert.equal(JSON.parse(conversation.lastMessagePreview).a.t, offline.text);
  assert.equal(conversation.lastSenderId, user);
  const receipt = JSON.parse(conversation.ext[projectionKey]);
  assert.deepEqual(receipt.ranges, [{from: rows[0].seq, to: rows.at(-1).seq}]);
  assert.equal(receipt.materializedSeq, conversation.maxSeq);
  assert.ok(Number.isInteger(receipt.summarySeq) && receipt.summarySeq >= 0 &&
    receipt.summarySeq <= rows[1].seq);
  assert.deepEqual(outbox, []);
  return {details, messages, conversation, outbox};
}

// Each binding talks to a fresh real conversation. Map run-specific identities and clocks
// only after checking their relationships against the gateway ACKs, database and local DTOs.
export function comparable(snapshot, {user, peer, cid, rows}) {
  const ids = [[peer, '<peer>'], [user, '<self>'], [cid, '<conversation>'],
    ...rows.map(r => [r.server_id, '<server:' + r.seq + '>'])];
  const drafts = new Map([online, offline, missed].map(item => [item.clientMsgId, item.at]));
  const clocks = new Set(['createdAt', 'sortTs', 'timelineSortTs',
    'lastMessageAt', 'time', 'updatedAt', 'updatedAtTs']);
  return JSON.parse(JSON.stringify(snapshot, function (key, value) {
    // This device-local watermark records how far a server summary had already counted.
    // The range, materialized cursor and unread result must still agree across delivery orders.
    if (key === projectionKey) return {...JSON.parse(value), summarySeq: '<observed summary>'};
    if (key === 'clientCreatedAt') return value === drafts.get(this.clientMsgId) ? '<draft-time>' : '<server-time>';
    if (typeof value === 'string') {
      for (const [actual, alias] of ids) value = value.replaceAll(actual, alias);
    }
    if (clocks.has(key) && typeof value === 'number' && value > 0) return '<timestamp>';
    return value;
  }));
}

export async function readReceivedState(device, cid, peer, rows, projected = 2) {
  const {messages} = await device.invoke('message.list', {conversationId: cid, beforeSeq: 0, limit: 10});
  assert.deepEqual(messages.map(m => m.clientMsgId), [offline.clientMsgId, online.clientMsgId]);
  const details = [];
  const samples = [online, offline];
  for (const [i, row] of rows.entries()) {
    const sample = samples[i];
    const m = await raw(device, row.id);
    assert.deepEqual([m.serverId, m.conversationSeq, m.senderId, m.conversationId, m.content.text, m.createdAt],
      [row.server_id, row.seq, peer, cid, sample.text, row.message_ms]);
    assert.ok([sample.at, row.message_ms].includes(m.clientCreatedAt));
    assert.equal(m.timelineSortTs, m.createdAt);
    assert.equal(m.localState.sending, false);
    details.push(m);
  }
  const conversation = await device.invoke('conversation.get', {conversationId: cid});
  assert.deepEqual([conversation.unreadCount, conversation.lastReadSeq, conversation.maxSeq,
    conversation.lastMessageId], [projected, 0, 2, rows[1].server_id]);
  assert.equal(conversation.lastMessageAt, details[1].createdAt);
  assert.equal(conversation.lastSenderId, peer);
  assert.equal(JSON.parse(conversation.lastMessagePreview).a.t, offline.text);
  assert.ok(conversation.updatedAtTs >= conversation.lastMessageAt);
  const {conversations} = await device.invoke('conversation.list', {});
  const listed = conversations.find(row => row.conversationId === cid);
  assert.ok(listed);
  for (const field of ['lastMessageId', 'lastMessagePreview', 'lastMessageAt',
    'lastSenderId', 'maxSeq', 'unreadCount', 'lastReadSeq']) {
    assert.deepEqual(listed[field], conversation[field], field);
  }
  const receipt = JSON.parse(conversation.ext[projectionKey]);
  assert.deepEqual(receipt.ranges, [{from: 1, to: projected}]);
  assert.equal(receipt.materializedSeq, projected);
  assert.ok(Number.isInteger(receipt.summarySeq) && receipt.summarySeq >= 0 && receipt.summarySeq <= 2);
  const outbox = await device.outbox();
  assert.deepEqual(outbox, []);
  return {messages, details, conversation, outbox};
}
