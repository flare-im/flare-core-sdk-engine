// Usage and prerequisites are in storage/README.md.
import assert from 'node:assert/strict';
import {sample, withBindings} from './clients.mjs';
import {exercise} from './session.mjs';

await withBindings(process.argv[2], process.argv[3], async ({native: openNative, web: openWeb}) => {
  async function reopen(open) {
    const writer = await open();
    await exercise(writer.invoke, sample, true, writer.dataUrl);
    await writer.close();
    const reader = await open();
    const state = await exercise(reader.invoke, sample, false, reader.dataUrl);
    state.outbox = await reader.outbox();
    await reader.close();
    return state;
  }
  const native = await reopen(openNative);
  const web = await reopen(openWeb);
  // The complete binding DTOs must agree, including timestamps and default values.
  assert.deepEqual(web, native);
  assert.deepEqual(native.timeline, sample.messages.map(m => m.clientMsgId).reverse());
  assert.deepEqual(native.conversations, [sample.conversationId]);
  assert.deepEqual(native.outbox, sample.messages.map(m => [m.clientMsgId, sample.conversationId]));
  for (const [i, message] of native.messages.entries()) {
    const item = sample.messages[i];
    assert.equal(message.clientMsgId, item.clientMsgId);
    assert.equal(message.content.text, item.text);
    assert.equal(message.conversationId, sample.conversationId);
    assert.equal(message.senderId, sample.senderId);
    assert.equal(message.localState.sortTs, item.at);
    assert.equal(message.localState.sending, true);
    assert.equal(message.localState.failed, false);
    assert.equal(message.receipt.status, 'sending');
    assert.equal(message.conversationSeq, 0);
  }
  const last = sample.messages.at(-1);
  assert.equal(native.conversation.conversationId, sample.conversationId);
  assert.equal(native.conversation.lastMessageId, last.clientMsgId);
  assert.equal(JSON.parse(native.conversation.lastMessagePreview).a.t, last.text);
  assert.equal(native.conversation.createdAt, sample.messages[0].at);
  assert.equal(native.conversation.updatedAt, last.at);
  assert.equal(native.conversation.lastMessageAt, last.at);
  assert.equal(native.conversation.businessType, 'chat');
  assert.equal(native.conversation.unreadCount, 0);
  assert.equal(native.conversation.maxSeq, 0);
  console.log('C / production WASM reopen matched: 2 complete messages, 1 conversation, 2 ordered outbox entries.');
});
