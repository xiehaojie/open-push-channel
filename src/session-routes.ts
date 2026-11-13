type PushChannelSessionRoute = {
  agentId: string;
};

type PushChannelSessionRouteState = {
  routes: Map<string, PushChannelSessionRoute>;
};

const PUSH_CHANNEL_SESSION_ROUTES_KEY = Symbol.for("openclaw.pushChannel.sessionRoutes");

function getSessionRouteState(): PushChannelSessionRouteState {
  const globalStore = globalThis as Record<PropertyKey, unknown>;
  const existing = globalStore[PUSH_CHANNEL_SESSION_ROUTES_KEY];
  if (existing) {
    return existing as PushChannelSessionRouteState;
  }
  const created: PushChannelSessionRouteState = {
    routes: new Map<string, PushChannelSessionRoute>(),
  };
  globalStore[PUSH_CHANNEL_SESSION_ROUTES_KEY] = created;
  return created;
}

function normalize(value: string | number | null | undefined): string | undefined {
  if (value == null) {
    return undefined;
  }
  const normalized = String(value).trim();
  return normalized ? normalized : undefined;
}

export function rememberPushChannelSessionRoute(params: {
  sessionId: string | number;
  agentId: string;
}): void {
  const sessionId = normalize(params.sessionId);
  const agentId = normalize(params.agentId);
  if (!sessionId || !agentId) {
    return;
  }
  getSessionRouteState().routes.set(sessionId, { agentId });
}

export function resolvePushChannelAgentIdForSession(
  sessionId: string | number | null | undefined,
): string | undefined {
  const normalized = normalize(sessionId);
  if (!normalized) {
    return undefined;
  }
  return getSessionRouteState().routes.get(normalized)?.agentId;
}

export function clearPushChannelSessionRouteForTest(): void {
  getSessionRouteState().routes.clear();
}
