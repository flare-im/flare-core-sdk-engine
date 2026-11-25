import assert from 'node:assert/strict';
import {once} from 'node:events';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {gunzipSync} from 'node:zlib';
import {webRequire} from './clients.mjs';
import {eventually, gateway, tenantId} from './live_stack.mjs';

const {WebSocket, WebSocketServer} = webRequire('ws');
const protobuf = webRequire('protobufjs');
const proto = file => fileURLToPath(new URL('../../../' + file, import.meta.url));
const wire = protobuf.loadSync(proto('flare-transport/proto/frame.proto'));
const im = protobuf.loadSync(['ack.proto', 'topic_envelope.proto'].map(f => proto('flare-protocol/proto/' + f)));
const frameType = wire.lookupType('flare.core.Frame');
const commands = wire.lookupEnum('flare.core.commands.PayloadCommand.Type').values;
const message = im.lookupType('flare.common.v1.Message');
const push = im.lookupType('flare.common.v1.MessagePush');
const events = im.lookupType('flare.common.v1.EventEnvelope');
const sendAck = im.lookupType('flare.common.v1.Ack');
export const brokerAccepted = im.lookupEnum('flare.common.v1.SendAckDurability').values.SEND_ACK_DURABILITY_BROKER_ACCEPTED;
export const persisted = im.lookupEnum('flare.common.v1.SendAckDurability').values.SEND_ACK_DURABILITY_PERSISTED;
const envelope = im.lookupType('flare.common.v1.PushTaskEnvelope');
const loud = im.lookupEnum('flare.common.v1.PushDisposition').values.PUSH_DISPOSITION_LOUD;

function unpack(bytes) {
  return bytes[0] === 0x1f && bytes[1] === 0x8b ? gunzipSync(bytes, {maxOutputLength: 8 * 1024 * 1024}) : bytes;
}

// Hold the authenticated sender's downlink only. Keep its uplink live, including the send.
// Retain the matching ACK until the caller independently confirms COMMIT, then sever the socket.
export async function ackLossRelay(observeSendAck, observeSend) {
  const server = new WebSocketServer({host: '127.0.0.1', port: 0});
  await once(server, 'listening');
  const connections = new Set();
  let blocked = false, target, held, decodeError, opened = 0;
  const downlinks = new Map();
  const withheldReceipts = [];
  let receiptConversation;
  server.on('connection', client => {
    if (blocked) return client.terminate();
    const upstream = new WebSocket(gateway, {perMessageDeflate: false});
    const connection = ++opened;
    const pair = {client, upstream};
    connections.add(pair);
    const close = () => {
      connections.delete(pair);
      upstream.terminate();
      client.terminate();
    };
    for (const socket of [upstream, client]) {
      socket.on('error', close);
      socket.once('close', close);
    }
    const early = [];
    client.on('message', (bytes, binary) => {
      if (observeSend) {
        const frame = unpack(bytes);
        const cmd = frame[0] === 0x7b ? undefined : frameType.decode(frame).command?.payload;
        if (cmd?.type === commands.MESSAGE) {
          const msg = message.decode(unpack(cmd.payload));
          const attempt = {clientId: msg.clientMsgId, cid: msg.conversationId, frameId: cmd.messageId, connection, at: performance.now()};
          const resume = observeSend(attempt);
          if (resume instanceof Promise) {
            // Forward the SDK's own bytes after the test releases its archive barrier.
            resume.then(() => {
              if (upstream.readyState === WebSocket.OPEN) {
                attempt.forwardedAt = performance.now();
                upstream.send(bytes, {binary});
              }
            });
            return;
          }
        }
      }
      if (receiptConversation) {
        const cmd = frameType.decode(unpack(bytes)).command?.payload;
        if (cmd?.type === commands.ACK) {
          const ack = sendAck.decode(unpack(cmd.payload));
          const receipts = [ack.conversation, ...(ack.batch?.conversationAcks ?? [])]
            .filter(r => r?.conversationId === receiptConversation && Number(r.lastDeliveredSeq) > 0);
          if (receipts.length) {
            withheldReceipts.push(...receipts.map(r => Number(r.lastDeliveredSeq)));
            return;
          }
        }
      }
      if (upstream.readyState === WebSocket.OPEN) upstream.send(bytes, {binary});
      else early.push([bytes, {binary}]);
    });
    upstream.once('open', () => { for (const args of early) upstream.send(...args); });
    upstream.on('message', (bytes, binary) => {
      if (!target) {
        if (observeSendAck) {
          // The negotiation response uses JSON before the protobuf data channel opens.
          const frame = unpack(bytes);
          const cmd = frame[0] === 0x7b ? undefined : frameType.decode(frame).command?.payload;
          if (cmd?.type === commands.ACK) {
            const ack = sendAck.decode(unpack(cmd.payload)).send;
            if (ack?.accepted) observeSendAck({
              clientId: ack.clientMsgId, cid: ack.conversationId, serverId: ack.accepted.serverMsgId, at: performance.now(),
              seq: Number(ack.accepted.conversationSeq), durability: ack.accepted.durability,
              persisted: ack.accepted.durability === persisted,
            });
          }
        }
        if (client.readyState === WebSocket.OPEN) client.send(bytes, {binary});
        return;
      }
      try {
        const cmd = frameType.decode(unpack(bytes)).command?.payload;
        if (cmd?.type === commands.MESSAGE || cmd?.type === commands.EVENT) {
          const body = unpack(cmd.payload);
          // Gateway versions use MessagePush or an inline EventEnvelope.
          for (const read of [() => push.decode(body).messages,
            () => events.decode(body).events.map(e => e.message)]) {
            try {
              for (const m of read()) {
                if (m?.clientMsgId && m.serverId) downlinks.set(m.clientMsgId,
                  {serverId: m.serverId, cid: m.conversationId, seq: Number(m.conversationSeq)});
              }
            } catch { /* alternative protobuf shape */ }
          }
          return;
        }
        if (cmd?.type !== commands.ACK) return;
        const ack = sendAck.decode(unpack(cmd.payload)).send;
        if (ack?.clientMsgId === target && ack.accepted?.durability === persisted) held = ack;
      } catch (error) { decodeError = String(error); }
    });
  });
  function cut() {
    blocked = true;
    for (const {upstream, client} of connections) { upstream.terminate(); client.terminate(); }
    receiptConversation = undefined;
    held = undefined;
    target = undefined;
  }
  return {
    url: 'ws://127.0.0.1:' + server.address().port,
    arm(id) { assert.ok(connections.size); target = id; },
    dropDeliveryReceipts(cid) { receiptConversation = cid; },
    withheldReceipts,
    async waitMessage(id) {
      return eventually('withheld realtime message ' + id, () => downlinks.get(id), Boolean);
    },
    async waitAck() {
      const {ack} = await eventually('held gateway persisted ACK', () => ({ack: held, decodeError}), x => x.ack);
      return {clientId: ack.clientMsgId, cid: ack.conversationId, serverId: ack.accepted.serverMsgId,
        seq: Number(ack.accepted.conversationSeq)};
    },
    cut, restore: () => { blocked = false; },
    close: () => { cut(); return new Promise(resolve => server.close(resolve)); },
  };
}

function redis(args) {
  const endpoint = new URL('redis://' + process.env.LIVE_SMOKE_PUSH_REDIS);
  assert.equal(endpoint.hostname, '127.0.0.1');
  return JSON.parse(execFileSync('redis-cli', ['--json', '-h', endpoint.hostname, '-p', endpoint.port,
    '-n', endpoint.pathname.slice(1), ...args], {encoding: 'utf8', timeout: 5000}));
}

// Return binary envelopes as hex so that redis-cli's text encoding cannot alter protobuf bytes.
// Limit the scan to the isolated stack and this test window.
const readTasks = `
local result = {}
local rows = redis.call('XRANGE', KEYS[1], '(' .. ARGV[1], '+', 'COUNT', 257)
if #rows > 256 then return redis.error_reply('push test window exceeded 256 entries') end
for _, row in ipairs(rows) do
  local f = {}
  for i = 1, #row[2], 2 do f[row[2][i]] = row[2][i+1] end
  if f.tenant_id == ARGV[2] and f.user_id == ARGV[3] and f.message_id == ARGV[4] then
    local hex = f.envelope:gsub('.', function(c) return string.format('%02x', string.byte(c)) end)
    table.insert(result, hex)
  end
end
return result`;

export function pushWindow() {
  const stream = process.env.LIVE_SMOKE_PUSH_OUTBOX;
  assert.ok(stream);
  const from = redis(['EVAL_RO', "local r = redis.call('XREVRANGE', KEYS[1], '+','-','COUNT',1); return r[1] and r[1][1] or '0-0'", '1', stream]);
  return (user, deviceId, cid, row) => {
    const frames = redis(['EVAL_RO', readTasks, '1', stream, from, tenantId, user, row.server_id]);
    const tasks = frames.map(hex => envelope.decode(Buffer.from(hex, 'hex')));
    for (const task of tasks) {
      assert.equal(task.tenantId, tenantId);
      assert.equal(task.userId, user);
      assert.equal(task.deviceId, deviceId);
      assert.equal(task.conversationId, cid);
      assert.equal(task.messageId, row.server_id);
      assert.equal(Number(task.conversationSeq), row.seq);
      assert.equal(task.disposition, loud);
    }
    return tasks.length;
  };
}

export function pushTrace(user, messageId) {
  const key = 'push:trace:' + tenantId + ':' + user + ':' + messageId;
  return redis(['LRANGE', key, '0', '-1']).map(entry => JSON.parse(entry));
}
