import { randomUUID } from "node:crypto";
import {
  defineBundledChannelEntry,
  type OpenClawPluginApi,
} from "openclaw/plugin-sdk/channel-entry-contract";
import {
  bindChildSessionToParent,
  clearChildSessionBinding,
  getSubagentDisplayForChild,
  getParentSessionKeyForChild,
  getPushSessionTargetForSessionOrChild,
  getWriter,
  getWriterForSessionOrChild,
  rememberSubagentDisplay,
  pushToolCallId,
  popToolCallId,
  type SubagentDisplayInfo,
} from "./src/tool-store.js";
import { sendPushEvent } from "./src/send.js";
import { getPushChannelRuntime } from "./src/runtime.js";
import { createSubagentEventsFromMessages } from "./src/subagent-transcript-events.js";

type StreamingToolHookApi = Pick<OpenClawPluginApi, "on"> &
  Partial<Pick<OpenClawPluginApi, "runtime" | "lifecycle">>;

const deliveredSubagentEventKeys = new Set<string>();
const streamedSubagentTextByMessageKey = new Map<string, string>();

function emitSubagentStreamChunks(params: {
  childSessionKey: string;
  display: SubagentDisplayInfo;
  delta: string;
  messageId?: string;
}): void {
  for (const chunk of Array.from(params.delta)) {
    deliverSubagentEvent(params.childSessionKey, {
      type: "subagent_stream",
      agentId: params.display.agentId,
      label: params.display.label,
      childSessionKey: params.childSessionKey,
      ...(params.messageId ? { messageId: params.messageId } : {}),
      content: chunk,
      delta: chunk,
    });
  }
}

function subagentEventKey(payload: Record<string, unknown>): string | undefined {
  const childSessionKey = typeof payload.childSessionKey === "string" ? payload.childSessionKey : "";
  const type = typeof payload.type === "string" ? payload.type : "";
  if (!childSessionKey || !type) {
    return undefined;
  }
  const identity =
    typeof payload.toolCallId === "string"
      ? payload.toolCallId
      : typeof payload.messageId === "string"
        ? payload.messageId
        : undefined;
  if (type === "subagent_start" || type === "subagent_end" || type === "subagent_error") {
    return `${childSessionKey}:${type}`;
  }
  return identity ? `${childSessionKey}:${type}:${identity}` : undefined;
}

function getSubagentDisplay(childSessionKey: string): SubagentDisplayInfo {
  const parsedAgentId = childSessionKey.startsWith("agent:")
    ? (childSessionKey.split(":")[1] ?? childSessionKey)
    : (childSessionKey.split(":subagent:").pop() ?? childSessionKey);
  return (
    getSubagentDisplayForChild(childSessionKey) ?? {
      agentId: parsedAgentId,
      label: parsedAgentId,
    }
  );
}

function deliverSubagentEvent(childSessionKey: string, payload: Record<string, unknown>): boolean {
  const eventKey = subagentEventKey(payload);
  const writer = getWriterForSessionOrChild(childSessionKey);
  if (writer) {
    writer(payload);
    if (eventKey) {
      deliveredSubagentEventKeys.add(eventKey);
    }
    return true;
  }

  const target = getPushSessionTargetForSessionOrChild(childSessionKey);
  if (!target) {
    return false;
  }

  sendPushEvent({
    middlewareUrl: target.middlewareUrl,
    agentId: target.agentId,
    sessionId: target.sessionId,
    event: payload,
  }).catch(() => {});
  if (eventKey) {
    deliveredSubagentEventKeys.add(eventKey);
  }
  return true;
}

function hasDeliveredSubagentEvent(payload: Record<string, unknown>): boolean {
  const eventKey = subagentEventKey(payload);
  return Boolean(eventKey && deliveredSubagentEventKeys.has(eventKey));
}

function clearDeliveredSubagentEvents(childSessionKey: string): void {
  const prefix = `${childSessionKey}:`;
  for (const key of deliveredSubagentEventKeys) {
    if (key.startsWith(prefix)) {
      deliveredSubagentEventKeys.delete(key);
    }
  }
  for (const key of streamedSubagentTextByMessageKey.keys()) {
    if (key.startsWith(prefix)) {
      streamedSubagentTextByMessageKey.delete(key);
    }
  }
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function readAssistantText(message: unknown): string | undefined {
  const record = asRecord(message);
  if (!record || record.role !== "assistant" || !Array.isArray(record.content)) {
    return undefined;
  }
  const chunks: string[] = [];
  for (const block of record.content) {
    const blockRecord = asRecord(block);
    if (blockRecord?.type === "text" && typeof blockRecord.text === "string") {
      chunks.push(blockRecord.text);
    }
  }
  const text = chunks.join("\n\n");
  return text ? text : undefined;
}

function emitSubagentStreamDelta(update: {
  childSessionKey: string;
  display: SubagentDisplayInfo;
  message?: unknown;
  messageId?: string;
}): void {
  const fullText = readAssistantText(update.message);
  if (!fullText) {
    return;
  }
  const messageKey = `${update.childSessionKey}:${update.messageId ?? "__latest"}`;
  const previousText = streamedSubagentTextByMessageKey.get(messageKey) ?? "";
  if (!fullText.startsWith(previousText)) {
    streamedSubagentTextByMessageKey.set(messageKey, fullText);
    emitSubagentStreamChunks({
      childSessionKey: update.childSessionKey,
      display: update.display,
      delta: fullText,
      messageId: update.messageId,
    });
    return;
  }
  const delta = fullText.slice(previousText.length);
  if (!delta) {
    return;
  }
  streamedSubagentTextByMessageKey.set(messageKey, fullText);
  emitSubagentStreamChunks({
    childSessionKey: update.childSessionKey,
    display: update.display,
    delta,
    messageId: update.messageId,
  });
}

async function replayChildTranscript(childSessionKey: string, display: SubagentDisplayInfo): Promise<void> {
  let messages: unknown[];
  try {
    const runtime = getPushChannelRuntime();
    const result = await runtime.subagent.getSessionMessages({ sessionKey: childSessionKey });
    messages = Array.isArray(result.messages) ? result.messages : [];
  } catch {
    return;
  }

  for (const event of createSubagentEventsFromMessages({
    messages,
    agentId: display.agentId,
    label: display.label,
    childSessionKey,
  })) {
    if (hasDeliveredSubagentEvent(event)) {
      continue;
    }
    deliverSubagentEvent(childSessionKey, event);
  }
}

function emitSubagentStart(
  event: { agentId?: unknown; label?: unknown; childSessionKey?: unknown },
  requesterSessionKey?: string,
): void {
  if (!requesterSessionKey) {
    return;
  }
  const agentId = typeof event.agentId === "string" && event.agentId ? event.agentId : "subagent";
  const label = typeof event.label === "string" && event.label ? event.label : agentId;
  const childSessionKey =
    typeof event.childSessionKey === "string" ? event.childSessionKey : undefined;
  if (childSessionKey) {
    rememberSubagentDisplay(childSessionKey, { agentId, label });
    const existingParentSessionKey = getParentSessionKeyForChild(childSessionKey);
    if (!existingParentSessionKey || !getWriter(existingParentSessionKey)) {
      bindChildSessionToParent(childSessionKey, requesterSessionKey);
    }
  }
  const payload = {
    type: "subagent_start",
    agentId,
    label,
    childSessionKey,
  };
  if (childSessionKey) {
    if (hasDeliveredSubagentEvent(payload)) {
      return;
    }
    deliverSubagentEvent(childSessionKey, payload);
    return;
  }
  getWriter(requesterSessionKey)?.(payload);
}

function emitSubagentTranscriptUpdate(update: {
  sessionKey?: string;
  message?: unknown;
  messageId?: string;
}): void {
  const childSessionKey = typeof update.sessionKey === "string" ? update.sessionKey : undefined;
  if (!childSessionKey || !getParentSessionKeyForChild(childSessionKey) || update.message === undefined) {
    return;
  }

  const display = getSubagentDisplay(childSessionKey);
  emitSubagentStreamDelta({
    childSessionKey,
    display,
    message: update.message,
    messageId: update.messageId,
  });
  for (const event of createSubagentEventsFromMessages({
    messages: [{ ...(update.messageId ? { id: update.messageId } : {}), message: update.message }],
    agentId: display.agentId,
    label: display.label,
    childSessionKey,
  })) {
    if (event.type === "subagent_message") {
      continue;
    }
    if (hasDeliveredSubagentEvent(event)) {
      continue;
    }
    deliverSubagentEvent(childSessionKey, event);
  }
}

function registerSubagentTranscriptUpdates(api: StreamingToolHookApi): void {
  const unsubscribe = api.runtime?.events.onSessionTranscriptUpdate((update) => {
    emitSubagentTranscriptUpdate(update);
  });
  if (!unsubscribe) {
    return;
  }
  api.lifecycle?.registerRuntimeLifecycle({
    id: "push-channel-subagent-transcript-updates",
    description: "Unsubscribe push-channel child session transcript update streaming.",
    cleanup: () => {
      unsubscribe();
    },
  });
}

export function registerStreamingToolHooks(api: StreamingToolHookApi): void {
  registerSubagentTranscriptUpdates(api);

  api.on("before_tool_call", (event, ctx) => {
    const sessionKey = ctx.sessionKey;
    if (!sessionKey) {
      return;
    }
    const parentSessionKey = getParentSessionKeyForChild(sessionKey);
    const writer = parentSessionKey ? undefined : getWriter(sessionKey);
    if (!writer) {
      if (!parentSessionKey) {
        return;
      }
    }

    const toolCallId = event.toolCallId ?? ctx.toolCallId ?? `tool-${randomUUID()}`;
    if (parentSessionKey) {
      const display = getSubagentDisplay(sessionKey);
      const payload = {
        type: "subagent_tool_call",
        agentId: display.agentId,
        label: display.label,
        childSessionKey: sessionKey,
        toolCallId,
        toolName: event.toolName,
        args: event.params ?? {},
      };
      if (hasDeliveredSubagentEvent(payload)) {
        pushToolCallId(sessionKey, toolCallId);
        return;
      }
      const delivered = deliverSubagentEvent(sessionKey, payload);
      if (delivered) {
        pushToolCallId(sessionKey, toolCallId);
      }
      return;
    }
    if (!writer) {
      return;
    }
    writer({
      type: "tool_call",
      toolCallId,
      toolName: event.toolName,
      args: event.params,
    });
    pushToolCallId(sessionKey, toolCallId);
  });

  api.on("tool_result_persist", (event, ctx) => {
    const sessionKey = ctx.sessionKey;
    if (!sessionKey) {
      return;
    }
    const parentSessionKey = getParentSessionKeyForChild(sessionKey);
    const writer = parentSessionKey ? undefined : getWriter(sessionKey);
    const pendingToolCallId = popToolCallId(sessionKey);
    const toolCallId = event.toolCallId ?? ctx.toolCallId ?? pendingToolCallId;
    if ((!writer && !parentSessionKey) || !toolCallId) {
      return;
    }
    const message = event.message as
      | { content?: unknown; isError?: boolean; details?: unknown }
      | undefined;
    if (parentSessionKey) {
      const display = getSubagentDisplay(sessionKey);
      const payload: Record<string, unknown> = {
        type: "subagent_tool_result",
        agentId: display.agentId,
        label: display.label,
        childSessionKey: sessionKey,
        toolCallId,
        toolName: event.toolName,
      };
      if (message?.content !== undefined) payload.content = message.content;
      if (message?.isError !== undefined) payload.isError = message.isError;
      if (message?.isError) {
        payload.message =
          typeof message.content === "string" ? message.content : "Subagent tool failed";
      }
      if (hasDeliveredSubagentEvent(payload)) {
        return;
      }
      deliverSubagentEvent(sessionKey, payload);
      return;
    }
    if (!writer) {
      return;
    }
    const payload: Record<string, unknown> = { type: "tool_result", toolCallId };
    if (message?.content !== undefined) payload.content = message.content;
    if (message?.isError !== undefined) payload.isError = message.isError;
    if (event.toolName) payload.toolName = event.toolName;
    writer(payload);
  });

  api.on("subagent_spawning", (event, ctx) => {
    emitSubagentStart(event, ctx.requesterSessionKey);
  });

  api.on("subagent_spawned", (event, ctx) => {
    emitSubagentStart(event, ctx.requesterSessionKey);
  });

  api.on("subagent_ended", async (event, ctx) => {
    const requesterSessionKey = ctx.requesterSessionKey ?? getParentSessionKeyForChild(event.targetSessionKey);
    if (!requesterSessionKey && !getParentSessionKeyForChild(event.targetSessionKey)) {
      return;
    }
    const display = getSubagentDisplayForChild(event.targetSessionKey);
    const subagentId =
      display?.agentId ?? event.targetSessionKey.split(":subagent:").pop() ?? event.targetSessionKey;
    const label = display?.label ?? subagentId;
    await replayChildTranscript(event.targetSessionKey, { agentId: subagentId, label });
    const endPayload = {
      type: "subagent_end",
      agentId: subagentId,
      label,
      childSessionKey: event.targetSessionKey,
      status: event.outcome === "ok" || event.reason === "subagent-complete" ? "success" : (event.outcome ?? event.reason),
    };
    if (!hasDeliveredSubagentEvent(endPayload)) {
      deliverSubagentEvent(event.targetSessionKey, endPayload);
    }
    clearDeliveredSubagentEvents(event.targetSessionKey);
    clearChildSessionBinding(event.targetSessionKey);
  });
}

export default defineBundledChannelEntry({
  id: "push-channel",
  name: "Push Channel",
  description: "Push channel plugin for OpenClaw",
  importMetaUrl: import.meta.url,
  plugin: {
    specifier: "./src/channel.js",
    exportName: "pushChannelPlugin",
  },
  runtime: {
    specifier: "./src/runtime.js",
    exportName: "setPushChannelRuntime",
  },
  registerFull: registerStreamingToolHooks,
});
