/**
 * Per-session store for SSE writers and pending tool call IDs.
 * Used by before_tool_call / tool_result_persist hooks to emit
 * tool_call / tool_result events into the active SSE stream.
 *
 * No AG-UI dependency — events use a plain JSON envelope.
 */

export type SseWriter = (event: Record<string, unknown>) => void;

export type PushSessionTarget = {
  middlewareUrl: string;
  agentId: string;
  sessionId?: string;
};

export type SubagentDisplayInfo = {
  agentId: string;
  label: string;
};

type PushChannelToolStoreState = {
  writerStore: Map<string, SseWriter>;
  pendingStacks: Map<string, string[]>;
  childParentSessions: Map<string, string>;
  pushSessionTargets: Map<string, PushSessionTarget>;
  subagentDisplays: Map<string, SubagentDisplayInfo>;
};

const PUSH_CHANNEL_TOOL_STORE_KEY = Symbol.for("openclaw.pushChannel.toolStore");

function getToolStoreState(): PushChannelToolStoreState {
  const globalStore = globalThis as Record<PropertyKey, unknown>;
  const existing = globalStore[PUSH_CHANNEL_TOOL_STORE_KEY];
  if (existing) {
    const state = existing as Partial<PushChannelToolStoreState>;
    state.childParentSessions ??= new Map<string, string>();
    state.pushSessionTargets ??= new Map<string, PushSessionTarget>();
    state.subagentDisplays ??= new Map<string, SubagentDisplayInfo>();
    return state as PushChannelToolStoreState;
  }
  const created: PushChannelToolStoreState = {
    writerStore: new Map<string, SseWriter>(),
    pendingStacks: new Map<string, string[]>(),
    childParentSessions: new Map<string, string>(),
    pushSessionTargets: new Map<string, PushSessionTarget>(),
    subagentDisplays: new Map<string, SubagentDisplayInfo>(),
  };
  globalStore[PUSH_CHANNEL_TOOL_STORE_KEY] = created;
  return created;
}

const state = getToolStoreState();
const writerStore = state.writerStore;
const pendingStacks = state.pendingStacks;
const childParentSessions = state.childParentSessions;
const pushSessionTargets = state.pushSessionTargets;
const subagentDisplays = state.subagentDisplays;

// --- SSE writer ---

export function setWriter(sessionKey: string, writer: SseWriter): void {
  writerStore.set(sessionKey, writer);
}

export function getWriter(sessionKey: string): SseWriter | undefined {
  return writerStore.get(sessionKey);
}

export function getParentSessionKeyForChild(childSessionKey: string): string | undefined {
  return childParentSessions.get(childSessionKey);
}

export function getWriterForSessionOrChild(sessionKey: string): SseWriter | undefined {
  return writerStore.get(sessionKey) ?? writerStore.get(childParentSessions.get(sessionKey) ?? "");
}

export function bindChildSessionToParent(childSessionKey: string, parentSessionKey: string): void {
  childParentSessions.set(childSessionKey, parentSessionKey);
}

export function rememberSubagentDisplay(
  childSessionKey: string,
  display: SubagentDisplayInfo,
): void {
  if (!childSessionKey || !display.agentId) {
    return;
  }
  subagentDisplays.set(childSessionKey, display);
}

export function getSubagentDisplayForChild(
  childSessionKey: string,
): SubagentDisplayInfo | undefined {
  return subagentDisplays.get(childSessionKey);
}

export function rememberPushSessionTarget(
  sessionKey: string,
  target: PushSessionTarget,
): void {
  if (!target.middlewareUrl || !target.agentId) {
    return;
  }
  pushSessionTargets.set(sessionKey, target);
}

export function getPushSessionTargetForSessionOrChild(
  sessionKey: string,
): PushSessionTarget | undefined {
  return (
    pushSessionTargets.get(sessionKey) ??
    pushSessionTargets.get(childParentSessions.get(sessionKey) ?? "")
  );
}

export function clearChildSessionBinding(childSessionKey: string): void {
  childParentSessions.delete(childSessionKey);
  pendingStacks.delete(childSessionKey);
  subagentDisplays.delete(childSessionKey);
}

export function clearWriter(sessionKey: string): void {
  writerStore.delete(sessionKey);
  pendingStacks.delete(sessionKey);
  childParentSessions.delete(sessionKey);
}

// --- Pending toolCallId stack ---
// before_tool_call pushes, tool_result_persist pops.

export function pushToolCallId(sessionKey: string, toolCallId: string): void {
  let stack = pendingStacks.get(sessionKey);
  if (!stack) {
    stack = [];
    pendingStacks.set(sessionKey, stack);
  }
  stack.push(toolCallId);
}

export function popToolCallId(sessionKey: string): string | undefined {
  const stack = pendingStacks.get(sessionKey);
  const id = stack?.pop();
  if (stack && stack.length === 0) {
    pendingStacks.delete(sessionKey);
  }
  return id;
}
