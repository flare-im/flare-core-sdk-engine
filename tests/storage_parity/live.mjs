import assert from 'node:assert/strict';
import {withBindings} from './clients.mjs';
import {sendSample} from './session.mjs';
import {
  comparable, connect, durable, eventually, gateway, getOne, missed, offline, online, prepare,
  projectionKey, raw, relay, runId, serverRows, snapshot, status,
} from './live_stack.mjs';

async function scenario(label, open, native) {
  const wire = await relay();
  const user = 'p13-' + label + '-' + runId, peer = user + '-peer';
  const client = await open(user), other = await native(peer);
  try {
    await prepare(client, user, wire.url);
    await connect(client, user);
    await prepare(other, peer, gateway.href);
    await connect(other, peer);
    const {conversationId: cid} = await getOne(client, peer);
    assert.equal((await getOne(other, user)).conversationId, cid);
    assert.ok(wire.count() > 0, 'client opened the monitored WS route');
    await sendSample(client.invoke, cid, online);
    const firstSeq = await durable(client, online, cid);
    console.log(label, 'online persisted ACK', firstSeq);
    wire.cut();
    await eventually('loss detected', () => status(client), s => s.phase !== 'online' && s.phase !== 'syncing');
    const before = wire.count();
    await sendSample(client.invoke, cid, offline);
    assert.deepEqual(await client.outbox(), [[offline.clientMsgId, cid]]);
    assert.ok(!client.acks.some(a => a.clientMsgId === offline.clientMsgId && a.stage === 'persisted'));
    const queued = await raw(client, offline.clientMsgId);
    assert.equal(queued.localState.sending, true);
    assert.equal(queued.localState.sortTs, offline.at);
    assert.equal(queued.conversationSeq, 0);
    await sendSample(other.invoke, cid, missed);
    await durable(other, missed, cid);
    assert.equal(await raw(client, missed.clientMsgId), null);
    assert.deepEqual(serverRows(cid).map(r => r.id), [online.clientMsgId, missed.clientMsgId]);
    assert.equal(wire.count(), before);
    const completed = client.syncs.length;
    wire.restore();
    await eventually('automatic reconnect', () => status(client), s => s.phase === 'online');
    assert.ok(wire.count() > before, 'new gateway socket after outage');
    await durable(client, offline, cid);
    const sync = await eventually('reconnect catch-up', () => client.syncs.slice(completed), events =>
      events.some(e => e.trigger === 'Reconnect' && e.stage === 'ForegroundFresh' &&
        events.some(end => end.runId === e.runId && end.stage === 'Converged')));
    const syncRunId = sync.find(e => e.trigger === 'Reconnect' && e.stage === 'ForegroundFresh').runId;
    await eventually('missed message in local storage', () => raw(client, missed.clientMsgId), Boolean);
    await eventually('outbox drained', () => client.outbox(), rows => rows.length === 0);
    const rows = serverRows(cid);
    const state = await snapshot(client, cid, user, peer, rows);
    wire.cut();
    await client.close();
    const reopened = await open(user);
    await prepare(reopened, user, wire.url);
    assert.deepEqual(await snapshot(reopened, cid, user, peer, rows), state);
    await reopened.close();
    await other.close();
    const {unreadCount, lastReadSeq, maxSeq, participantVersion} = state.conversation;
    const confirmed = rows.map(({id, server_id, seq}) => ({id, serverId: server_id, seq}));
    const projection = JSON.parse(state.conversation.ext[projectionKey]);
    console.log(label, JSON.stringify({sockets: wire.count(), syncRun: syncRunId, confirmed, projection, unreadCount, lastReadSeq,
      maxSeq, participantVersion, outbox: state.outbox}));
    return comparable(state, {user, peer, cid, rows});
  } finally {
    await wire.close();
  }
}

await withBindings(process.argv[2], process.argv[3], async ({native, web}) => {
  const c = await scenario('c', native, native);
  const wasm = await scenario('wasm', web, native);
  assert.deepEqual(wasm, c);
  console.log('C / production WASM gateway ACK, automatic reconnect, compensation and offline reopen matched.');
});
