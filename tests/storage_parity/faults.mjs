import assert from 'node:assert/strict';
import {withBindings} from './clients.mjs';
import {sendSample} from './session.mjs';
import {
  comparable, connect, durable, eventually, gateway, getOne, missed, offline, online,
  prepare, raw, readReceivedState, receiptEvents, runId, serverRows, snapshot, status,
} from './live_stack.mjs';
import {ackLossRelay, pushTrace, pushWindow} from './fault_wire.mjs';

async function registerDevice(client, user) {
  await client.invoke('sdk.set_push_permission', {allowed: true});
  await client.invoke('sdk.update_push_token', {provider: 'fcm', platform: 'android',
    token: 'fixture-' + user, appBundle: 'im.flare.acceptance', capabilities: ['dataMessages']});
  await eventually('device push binding', () => client.invoke('sdk.push_status', {}), s => s.state === 'registered');
}

async function faults(label, open, native) {
  const user = 'fault-' + label + '-' + runId, peer = user + '-peer';
  const wire = await ackLossRelay();
  const pushWire = label === 'c' ? wire : await ackLossRelay();
  try {
    const pushTasks = pushWindow();
    const client = await open(user), other = await native(peer);
    await prepare(client, user, wire.url);
    await connect(client, user);
    let pushDevice = user + '-device';
    if (label === 'c') {
      await registerDevice(client, user);
    } else {
      assert.equal((await client.invoke('sdk.push_status', {})).state, 'disabled');
      // OS push is native-only. Register a real companion device on this account and observe its independent gateway link.
      pushDevice = user + '-phone';
      const phone = await native(pushDevice);
      await prepare(phone, user, pushWire.url, pushDevice);
      await connect(phone, user, pushDevice);
      await registerDevice(phone, user);
      pushWire.arm(missed.clientMsgId);
    }
    await prepare(other, peer, gateway.href);
    await connect(other, peer);
    const {conversationId: cid} = await getOne(client, peer);
    assert.equal((await getOne(other, user)).conversationId, cid);

    wire.arm(online.clientMsgId);
    await sendSample(client.invoke, cid, online);
    const held = await wire.waitAck();
    const [first] = await eventually('first message committed on server', () => serverRows(cid), r => r.length === 1);
    assert.deepEqual(held, {clientId: online.clientMsgId, cid, serverId: first.server_id, seq: 1});
    assert.deepEqual(await client.outbox(), [[online.clientMsgId, cid]]);
    assert.ok(!client.acks.some(a => a.clientMsgId === online.clientMsgId && a.stage === 'persisted'));
    await sendSample(other.invoke, cid, missed);
    await durable(other, missed, cid);
    const committed = serverRows(cid);
    assert.deepEqual(committed.map(r => r.id), [online.clientMsgId, missed.clientMsgId]);
    const witness = committed[1];
    const delivered = await pushWire.waitMessage(missed.clientMsgId);
    assert.deepEqual(delivered, {serverId: witness.server_id, seq: witness.seq, cid});
    assert.equal(pushTasks(user, pushDevice, cid, witness), 0);
    if (pushWire !== wire) pushWire.cut();
    wire.cut();
    await eventually('loss of ACK connection', () => status(client), s => !['online','syncing'].includes(s.phase));
    console.log(label, 'lost gateway persisted ACK after database commit', first.server_id);

    const before = await client.invoke('conversation.get', {conversationId: cid});
    const pending = await raw(client, online.clientMsgId);
    assert.ok(pending.localState.sending);
    assert.equal(pending.conversationSeq, 0);
    await client.crashDuringCommit('outgoing', offline.clientMsgId, () => sendSample(client.invoke, cid, offline));
    await client.close();
    console.log(label, 'terminated inside the local outgoing transaction');

    await eventually('worker took over offline delivery', () => pushTasks(user, pushDevice, cid, witness), n => n === 1);

    const trace = await eventually('unconfirmed delivery handed to push', () => pushTrace(user, witness.server_id),
      stages => stages.some(s => s.st === 'handoff' && s.dev === pushDevice && s.f.cause === 'link_lost' && Number(s.f.pending) > 0));
    console.log(label, 'push handoff', trace.find(s => s.st === 'handoff' && s.dev === pushDevice).f);
    const recovered = await open(user);
    await prepare(recovered, user, wire.url);
    assert.equal(await raw(recovered, offline.clientMsgId), null);
    assert.equal(await raw(recovered, missed.clientMsgId), null);
    assert.deepEqual(await raw(recovered, online.clientMsgId), pending);
    assert.deepEqual(await recovered.invoke('conversation.get', {conversationId: cid}), before);
    assert.deepEqual(await recovered.outbox(), [[online.clientMsgId, cid]]);
    wire.restore();
    await connect(recovered, user);
    await eventually('lost ACK repaired', () => raw(recovered, online.clientMsgId),
      m => m?.serverId === first.server_id && m.conversationSeq === 1 && !m.localState.sending);
    await eventually('offline witness synchronized', () => raw(recovered, missed.clientMsgId),
      m => m?.serverId === witness.server_id && m.conversationSeq === 2);
    await eventually('recovered outbox drained', () => recovered.outbox(), q => !q.length);

    // The interrupted call never committed locally. Retry that caller intent with its original ID.
    await sendSample(recovered.invoke, cid, offline);
    await durable(recovered, offline, cid);
    await eventually('retry removed from outbox', () => recovered.outbox(), q => !q.length);
    const rows = serverRows(cid);
    assert.deepEqual(receiptEvents(cid).map(r => [r.seq, r.event_type]), [[1, 1], [2, 1], [3, 1]]);
    const state = await snapshot(recovered, cid, user, peer, rows, 0);
    assert.equal(pushTasks(user, pushDevice, cid, witness), 1);
    wire.cut();
    await recovered.close();
    const cold = await open(user);
    await prepare(cold, user, wire.url);
    assert.deepEqual(await snapshot(cold, cid, user, peer, rows, 0), state);
    await cold.close();
    await other.close();
    console.log(label, JSON.stringify({lostAck: first.server_id, offlinePush: witness.server_id,
      seqs: rows.map(r => r.seq), unread: state.conversation.unreadCount,
      readSeq: state.conversation.lastReadSeq, outbox: state.outbox.length}));
    return comparable(state, {user, peer, cid, rows});
  } finally {
    if (pushWire !== wire) await pushWire.close();
    await wire.close();
  }
}

// Exercise the opposite ACK direction. Both fixture messages originate on the peer.
// A committed first receive loses its delivery receipt; the second receive dies inside its write.
async function receiveFaults(label, open, native) {
  const user = 'in-' + label + '-' + runId, peer = user + '-peer';
  const wire = await ackLossRelay();
  try {
    const client = await open(user), other = await native(peer);
    await prepare(client, user, wire.url);
    await connect(client, user);
    await prepare(other, peer, gateway.href);
    await connect(other, peer);
    const {conversationId: cid} = await getOne(client, peer);
    assert.equal((await getOne(other, user)).conversationId, cid);
    wire.dropDeliveryReceipts(cid);
    await sendSample(other.invoke, cid, online);
    await durable(other, online, cid);
    await eventually('device delivery ACK withheld', () => wire.withheldReceipts, a => a.includes(1));
    const first = await raw(client, online.clientMsgId);
    const before = await client.invoke('conversation.get', {conversationId: cid});
    assert.equal(before.unreadCount, 1);
    assert.equal(before.lastMessageId, first.serverId);

    await client.crashDuringCommit('incoming', offline.clientMsgId, () => sendSample(other.invoke, cid, offline));
    wire.cut();
    assert.ok(!wire.withheldReceipts.some(seq => seq >= 2), 'no delivery ACK for interrupted receive');
    await durable(other, offline, cid);
    await client.close();
    const rows = serverRows(cid);
    assert.deepEqual(rows.map(r => [r.id, r.seq]), [[online.clientMsgId, 1], [offline.clientMsgId, 2]]);

    const recovered = await open(user);
    await prepare(recovered, user, wire.url);
    assert.deepEqual(await raw(recovered, online.clientMsgId), first);
    assert.equal(await raw(recovered, offline.clientMsgId), null);
    const initial = await recovered.invoke('conversation.get', {conversationId: cid});
    assert.deepEqual([initial.unreadCount, initial.lastMessageId, initial.maxSeq], [1, first.serverId, 1]);
    assert.deepEqual(await recovered.outbox(), []);
    wire.restore();
    await connect(recovered, user);
    await eventually('both incoming messages projected', () => recovered.invoke('conversation.get', {conversationId: cid}),
      c => c.unreadCount === 2 && c.maxSeq === 2 && c.lastMessageId === rows[1].server_id);

    const state = await readReceivedState(recovered, cid, peer, rows);
    wire.cut();
    await recovered.close();
    const cold = await open(user);
    await prepare(cold, user, wire.url);
    assert.deepEqual(await readReceivedState(cold, cid, peer, rows), state);
    await cold.close();
    await other.close();
    console.log(label, 'receive recovery: delivery ACK 1 withheld, no ACK 2, messages 2, unread 2, outbox 0');
    return comparable(state, {user, peer, cid, rows});
  } finally { await wire.close(); }
}

await withBindings(process.argv[2], process.argv[3], async ({native, web}) => {
  const c = await faults('c', native, native);
  const wasm = await faults('wasm', web, native);
  assert.deepEqual(wasm, c);
  const cReceived = await receiveFaults('c', native, native);
  const wasmReceived = await receiveFaults('wasm', web, native);
  assert.deepEqual(wasmReceived, cReceived);
  console.log('C / production WASM matched after lost ACK, transaction crash and offline push takeover.');
});
