import assert from 'node:assert/strict';
import {withBindings} from './clients.mjs';
import {sendSample} from './session.mjs';
import {connect, durable, eventually, gateway, getOne, offline, online, prepare, relay, runId} from './live_stack.mjs';

async function scenario(label, open, native) {
  const user = 'read-' + label + '-' + runId;
  const peer = user + '-sender';
  const wire = await relay();
  let receiver, sender;
  try {
    receiver = await open(user);
    sender = await native(peer);
    await prepare(receiver, user, wire.url);
    await connect(receiver, user);
    await prepare(sender, peer, gateway.href);
    await connect(sender, peer);
    const {conversationId} = await getOne(receiver, peer);
    assert.equal((await getOne(sender, user)).conversationId, conversationId);
    for (const item of [online, offline]) {
      await sendSample(sender.invoke, conversationId, item);
      await durable(sender, item, conversationId);
    }
    const stored = () => receiver.storedReceive(user, conversationId);
    const initial = await eventually('both messages committed', stored, s => s.unread === 2 && s.cursor?.seq === 2);
    wire.cut();
    await receiver.close();
    receiver = await open(user);
    await prepare(receiver, user, wire.url);
    await receiver.invoke('conversation.mark_read', {conversationId, readSeq: 1});
    const pending = await stored();
    assert.deepEqual([pending.readSeq, pending.unread, pending.pendingRead], [1, 1, '1']);
    assert.deepEqual(pending.messages, initial.messages);
    assert.deepEqual(pending.outbox, initial.outbox);
    await receiver.close();
    receiver = await open(user);
    assert.deepEqual(await stored(), pending);
    await prepare(receiver, user, wire.url);
    wire.restore();
    await connect(receiver, user);
    const confirmed = await eventually('server confirms persisted read watermark', stored,
      s => s.pendingRead === null && s.readSeq === 1);
    assert.deepEqual(confirmed.messages, initial.messages);
    assert.deepEqual(confirmed.outbox, []);
    assert.equal(confirmed.lastId, initial.lastId);
    return [confirmed.readSeq, confirmed.unread, confirmed.cursor.seq, confirmed.messages.map(m => m[0])];
  } finally {
    await Promise.allSettled([receiver?.close(), sender?.close(), wire.close()]);
  }
}

await withBindings(process.argv[2], process.argv[3], async ({native, web}) => {
  const c = await scenario('c', native, native);
  const wasm = await scenario('wasm', web, native);
  assert.deepEqual(c, [1, 1, 2, [online.clientMsgId, offline.clientMsgId]]);
  assert.deepEqual(wasm, c);
});
