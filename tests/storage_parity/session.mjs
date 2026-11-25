export async function sendSample(invoke, conversationId, item) {
  const draft = await invoke('message.create_text', {conversationId, text: item.text});
  draft.clientMsgId = item.clientMsgId;
  draft.createdAt = draft.clientCreatedAt = item.at;
  draft.localState.sortTs = item.at;
  await invoke('message.send', draft);
}

// Run the same contract calls in the native process and the browser.
export async function exercise(invoke, sample, write, dataUrl) {
  const sdkConfig = {
    tenantId: sample.tenantId, deviceId: 'phase12-device',
    endpoints: {baseUrl: 'http://127.0.0.1:9', websocketUrl: 'ws://127.0.0.1:9'},
  };
  if (dataUrl) sdkConfig.dataUrl = dataUrl;
  await invoke('sdk.init', {environment: 'development', sdkConfig});
  await invoke('sdk.prepare', {userId: sample.senderId});
  if (write) {
    for (const item of sample.messages) {
      await sendSample(invoke, sample.conversationId, item);
    }
    return;
  }
  const messages = [];
  for (const item of sample.messages)
    messages.push(await invoke('message.get_raw', {messageId: item.clientMsgId}));
  const conversation = await invoke('conversation.get', {conversationId: sample.conversationId});
  const {messages: timeline} = await invoke('message.list', {conversationId: sample.conversationId, beforeSeq: 0, limit: 10});
  const {conversations} = await invoke('conversation.list', {});
  return {messages, conversation, timeline: timeline.map(row => row.clientMsgId),
    conversations: conversations.map(row => row.conversationId)};
}
