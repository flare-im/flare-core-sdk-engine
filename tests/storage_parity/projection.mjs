import assert from 'node:assert/strict';
import {withBindings} from './clients.mjs';
import {sendSample} from './session.mjs';
import {
  comparable, connect, durable, eventually, gateway, getOne, offline, online,
  prepare, readReceivedState, relay, runId, serverRows,
} from './live_stack.mjs';

function verifyStored(state, rows, projected, cursor) {
  assert.deepEqual(state.messages, rows.map(r => [r.id, r.server_id, r.seq]));
  assert.deepEqual(state.receipt.ranges, [{from: 1, to: projected}]);
  assert.equal(state.receipt.materializedSeq, projected);
  assert.equal(state.unread, projected);
  assert.equal(state.readSeq, 0);
  assert.equal(state.cursor?.seq, cursor);
  assert.ok(Number.isSafeInteger(state.cursor.syncedAt) && state.cursor.syncedAt > 0);
  assert.deepEqual(state.outbox, []);
  const last = rows.at(-1);
  assert.deepEqual([state.lastId, state.lastSender, state.lastAt, state.maxSeq],
    [last.server_id, last.sender_id, last.message_ms, last.seq]);
  assert.equal(JSON.parse(state.preview).a.t, [online, offline][last.seq - 1].text);
}

async function recover(label, point, open, native) {
  const user = ['proj', label, point, runId].join('-'), peer = user + '-peer';
  const wire = await relay();
  try {
    const receiver = await open(user), sender = await native(peer);
    await prepare(receiver, user, wire.url);
    await connect(receiver, user);
    await prepare(sender, peer, gateway.href);
    await connect(sender, peer);
    const {conversationId: cid} = await getOne(receiver, peer);
    assert.equal((await getOne(sender, user)).conversationId, cid);

    await sendSample(sender.invoke, cid, online);
    await durable(sender, online, cid);
    const baseline = await eventually('first projection and cursor commit', () => receiver.storedReceive(user, cid),
      s => s.cursor?.seq === 1 && s.receipt?.materializedSeq === 1);
    verifyStored(baseline, serverRows(cid), 1, 1);

    await receiver.crashDuringCommit(point, offline.clientMsgId, () => sendSample(sender.invoke, cid, offline),
      {user, cid, seq: 2});
    wire.cut();
    await receiver.close();
    await durable(sender, offline, cid);
    const rows = serverRows(cid);
    assert.deepEqual(rows.map(r => [r.id, r.seq]), [[online.clientMsgId, 1], [offline.clientMsgId, 2]]);

    // Read the actual store in the new process/profile before the SDK can repair anything.
    const reopened = await open(user);
    const torn = await reopened.storedReceive(user, cid);
    const projected = point === 'before-projection' ? 1 : 2;
    verifyStored(torn, rows, projected, 1);
    assert.deepEqual(torn.cursor, baseline.cursor);
    const previewSeq = rows.find(r => r.server_id === torn.lastId).seq;
    console.log(label, point, 'cold store', {messages: torn.messages.length, previewSeq,
      projected, unread: torn.unread, cursor: torn.cursor.seq, outbox: torn.outbox.length});

    await prepare(reopened, user, wire.url);
    const offlineState = await readReceivedState(reopened, cid, peer, rows, projected);
    const prepared = await reopened.storedReceive(user, cid);
    verifyStored(prepared, rows, projected, 1);
    assert.deepEqual(prepared, torn, "offline prepare changed the committed state");
    console.log(label, point, 'offline SDK preview seq', offlineState.conversation.maxSeq);
    wire.restore();
    await connect(reopened, user);
    await eventually('cold login sync converged', () => reopened.syncs, events =>
      events.some(e => e.stage === 'Converged'));
    const repaired = await eventually('replay committed the projection and sync cursor', () => reopened.storedReceive(user, cid),
      s => s.cursor?.seq === 2 && s.receipt.materializedSeq === 2);
    verifyStored(repaired, rows, 2, 2);
    assert.ok(repaired.cursor.syncedAt >= baseline.cursor.syncedAt);
    const state = await readReceivedState(reopened, cid, peer, rows);
    wire.cut();
    await reopened.close();
    const cold = await open(user);
    const final = await cold.storedReceive(user, cid);
    verifyStored(final, rows, 2, 2);
    assert.deepEqual(final, repaired);
    await prepare(cold, user, wire.url);
    assert.deepEqual(await readReceivedState(cold, cid, peer, rows), state);
    assert.deepEqual(serverRows(cid), rows);
    await cold.close();
    await sender.close();
    console.log(label, point, 'recovered', {messages: final.messages.length, previewSeq: final.maxSeq,
      projected: final.receipt.materializedSeq, unread: final.unread, cursor: final.cursor.seq, outbox: final.outbox.length});
    return comparable({
      offline: {...offlineState, cursor: prepared.cursor.seq},
      recovered: {...state, cursor: final.cursor.seq},
    }, {user, peer, cid, rows});
  } finally { await wire.close(); }
}

await withBindings(process.argv[2], process.argv[3], async ({native, web}) => {
  for (const point of ['before-projection', 'before-cursor']) {
    const c = await recover('c', point, native, native);
    const wasm = await recover('wasm', point, web, native);
    assert.deepEqual(wasm, c, point);
  }
  console.log('C / production WASM matched offline and after compensation at both commit boundaries.');
});
