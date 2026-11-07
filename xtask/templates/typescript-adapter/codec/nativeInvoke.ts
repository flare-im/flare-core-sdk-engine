// GENERATED. Do not edit by hand.
import { NativeBridge, NativeCallDescriptor } from '../../contract/bridge_contract';
import { FlareSdkException } from '../../bridge/flareSdkException';
import {
  Conversation,
  ConversationTimelineSnapshot,
  HomeTimelineSnapshot,
  LifecycleEventName,
  ListConversationsResponse,
  ListMessagesResponse,
  Message,
  SdkErrorPayload,
  SendMessageResponse,
} from '../../model';
import {
  conversationFromJson,
  conversationTimelineSnapshotFromJson,
  homeTimelineSnapshotFromJson,
  listConversationsResponseFromJson,
  listMessagesResponseFromJson,
  listOfMaps,
  messageFromJson,
  sendAckFromJson,
} from './wireCodec';

export interface AdapterLifecycleEmitter {
  emit(event: unknown): void;
}

function recordFromNative(value: unknown, operation: string): Record<string, unknown> {
  if (value instanceof Map) {
    return Object.fromEntries(value.entries());
  }
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  throw new FlareSdkException('invalidParameter', 'native response must be an object', operation, { expected: 'object' });
}

export async function invokeVoid(
  bridge: NativeBridge,
  descriptor: NativeCallDescriptor,
  request?: unknown,
): Promise<void> {
  await bridge.invoke<void>(descriptor, request);
}

export async function invokeMap(
  bridge: NativeBridge,
  descriptor: NativeCallDescriptor,
  request?: unknown,
): Promise<Record<string, unknown>> {
  return recordFromNative(await bridge.invoke<unknown>(descriptor, request), descriptor.operation);
}

export async function invokeBool(
  bridge: NativeBridge,
  descriptor: NativeCallDescriptor,
  request?: unknown,
): Promise<boolean> {
  const value = await bridge.invoke<unknown>(descriptor, request);
  if (typeof value === 'boolean') {
    return value;
  }
  throw new FlareSdkException('invalidParameter', 'native response must be a boolean', descriptor.operation, { expected: 'boolean' });
}

/**
 * A copy of `value` without null fields, recursively through nested objects and arrays. Core
 * DTOs serialize "no value" as null; the generated models express it as an absent field.
 */
export function withoutNullFields<T>(value: T): T {
  if (Array.isArray(value)) {
    return value.map((item) => withoutNullFields(item)) as unknown as T;
  }
  if (value && typeof value === 'object' && !(value instanceof Uint8Array)) {
    const out: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
      if (item !== null && item !== undefined) out[key] = withoutNullFields(item);
    }
    return out as T;
  }
  return value;
}

/** A response that is a bare JSON array of model objects (null fields left out). */
export async function invokeList(
  bridge: NativeBridge,
  descriptor: NativeCallDescriptor,
  request?: unknown,
): Promise<Record<string, unknown>[]> {
  const value = await bridge.invoke<unknown>(descriptor, request);
  if (!Array.isArray(value)) {
    throw new FlareSdkException('invalidParameter', 'native response must be an array', descriptor.operation, { expected: 'array' });
  }
  return value.map((item) => withoutNullFields(recordFromNative(item, descriptor.operation)));
}

export async function invokeMessage(
  bridge: NativeBridge,
  descriptor: NativeCallDescriptor,
  request?: unknown,
): Promise<Message> {
  const raw = await invokeMap(bridge, descriptor, request);
  return messageFromJson((raw["message"] as unknown) ?? raw);
}

export async function invokeSendAck(
  bridge: NativeBridge,
  descriptor: NativeCallDescriptor,
  request?: unknown,
): Promise<SendMessageResponse> {
  return sendAckFromJson(await invokeMap(bridge, descriptor, request));
}

export async function invokeListConversations(
  bridge: NativeBridge,
  descriptor: NativeCallDescriptor,
  request?: unknown,
): Promise<ListConversationsResponse> {
  return listConversationsResponseFromJson(await invokeMap(bridge, descriptor, request));
}

export async function invokeListMessages(
  bridge: NativeBridge,
  descriptor: NativeCallDescriptor,
  request?: unknown,
): Promise<ListMessagesResponse> {
  return listMessagesResponseFromJson(await invokeMap(bridge, descriptor, request));
}

export async function invokeHomeTimelineSnapshot(
  bridge: NativeBridge,
  descriptor: NativeCallDescriptor,
  request?: unknown,
): Promise<HomeTimelineSnapshot> {
  return homeTimelineSnapshotFromJson(await invokeMap(bridge, descriptor, request));
}

export async function invokeConversationTimelineSnapshot(
  bridge: NativeBridge,
  descriptor: NativeCallDescriptor,
  request?: unknown,
): Promise<ConversationTimelineSnapshot> {
  return conversationTimelineSnapshotFromJson(await invokeMap(bridge, descriptor, request));
}

export async function invokeConversation(
  bridge: NativeBridge,
  descriptor: NativeCallDescriptor,
  request?: unknown,
): Promise<Conversation> {
  const raw = await invokeMap(bridge, descriptor, request);
  const conversations = listOfMaps(raw["conversations"]);
  if (conversations.length > 0) {
    return conversationFromJson(conversations[0]);
  }
  return conversationFromJson(raw);
}

export function sdkErrorPayloadFromError(error: unknown, operation: string): SdkErrorPayload {
  const maybeError = error as Error;
  const message = maybeError?.message ?? `${error}`;
  return {
    code: 'internal',
    message,
    operation,
    retryable: false,
    details: {
      type: maybeError?.name ?? 'Error',
    },
  };
}

export function userIdFromRequest(request: unknown): string | undefined {
  const record = request as Record<string, unknown>;
  const value = record.userId;
  return typeof value === 'string' ? value : undefined;
}

export function emitLifecycleEvent(
  events: AdapterLifecycleEmitter,
  name: LifecycleEventName,
  operation: string,
  userId?: string,
  error?: SdkErrorPayload,
): void {
  events.emit({
    name,
    operation,
    ...(userId !== undefined ? { userId } : {}),
    ...(error !== undefined ? { error } : {}),
  });
}
