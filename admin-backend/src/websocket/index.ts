import { randomUUID } from "node:crypto";
import { WebSocketServer } from "ws";
import type WebSocket from "ws";
import type { RawData } from "ws";
import type { Server } from "node:http";
import { PROTOCOL_VERSION } from "../../../protocol/index.ts";
import type { Mention, PushEvent } from "../../../protocol/index.ts";
import type { PushGateway, GatewayOptions, RunReceipt } from "../gateway/gateway.ts";
import type {
  GatewayAuth,
  GatewaySession,
  PushPayload,
  RuntimeTarget,
} from "../gateway/types.ts";
import { createDefaultGateway } from "../gateway/default.ts";
import { createClientProjector } from "../gateway/client-events.ts";
import { authenticate, isLocalGatewayRequest } from "../gateway/auth.ts";

type ManagedSocket = WebSocket & { isAlive?: boolean };
type GatewaySocket = ManagedSocket & {
  agentId?: string;
  targets?: RuntimeTarget[];
  sessionIds?: Set<string>;
  sessionRuntimes?: Map<string, string>;
  protocolVersion?: typeof PROTOCOL_VERSION;
};
type RegisteredSocket = GatewaySocket & {
  agentId: string;
  sessionIds: Set<string>;
  sessionRuntimes: Map<string, string>;
};

type PublishPushInput = PushPayload & {
  agentId: string;
  sessionId?: string;
  runtimeId?: string;
  clientId?: string;
};

export type TaskPayload = {
  clientId: string;
  sessionId: string;
  eventId: string;
  content: string;
  runtimeId?: string;
  agentId?: string;
};

export type WebSocketOptions = Partial<Pick<GatewayOptions, "adapters" | "store">> & {
  runtimeId?: string;
  auth?: GatewayAuth;
};

export type GatewayServer = Server & {
  submitTask(payload: TaskPayload): RunReceipt | undefined;
  shutdown(): Promise<void>;
};

type ClientPayload = Record<string, unknown>;
type Projector = ReturnType<typeof createClientProjector>;
const connections = new Map<string, Set<RegisteredSocket>>();
const projectors = new Map<string, Projector>();
const terminalTypes = new Set(["run.completed", "run.failed", "run.cancelled", "run.deferred"]);
let gateway: PushGateway;
let defaultRuntimeId: string | undefined;

function trimToNull(value: unknown) {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function sendJson(socket: WebSocket, payload: object) {
  if (socket.readyState === socket.OPEN) {
    socket.send(JSON.stringify(payload));
  }
}

function publishEvent(event: PushEvent) {
  let project = projectors.get(event.runId);
  if (!project) {
    project = createClientProjector();
    projectors.set(event.runId, project);
  }
  const clientMessages = project(event).map((payload, index) =>
    Object.assign(payload, {
      eventId: event.eventId,
      cursor: event.cursor,
      deliveryId: `${event.eventId}:${index}`,
    }),
  );
  if (terminalTypes.has(event.type)) {
    projectors.delete(event.runId);
  }
  const targets = event.clientId
    ? (connections.get(event.clientId) ?? [])
    : Array.from(connections.values()).flatMap((sockets) => Array.from(sockets));
  for (const socket of targets) {
    if (
      socket.targets &&
      !socket.targets.some(
        (target) => target.runtimeId === event.runtimeId && target.agentId === event.agentId,
      )
    ) {
      continue;
    }
    const session = gateway.session(socket.agentId, event.conversationId);
    const subscribedRuntime = socket.sessionRuntimes?.get(event.conversationId) ?? defaultRuntimeId;
    if (event.audience !== "agent" && !socket.sessionIds?.has(event.conversationId)) {
      continue;
    }
    if (event.audience !== "agent" && subscribedRuntime !== event.runtimeId) {
      continue;
    }
    if (event.audience !== "agent" && session && session.clientId !== socket.agentId) {
      continue;
    }
    if (event.audience === "agent") {
      const matches = [...gateway.sessions.values()].some(
        (item) =>
          item.clientId === socket.agentId &&
          item.agentId === event.agentId &&
          item.runtimeId === event.runtimeId,
      );
      if (!matches && !(socket.agentId === event.agentId && event.runtimeId === defaultRuntimeId)) {
        continue;
      }
    }
    for (const payload of socket.protocolVersion === PROTOCOL_VERSION ? [event] : clientMessages) {
      sendJson(socket, payload);
    }
  }
}

function getGateway() {
  gateway ??= createDefaultGateway({ publish: publishEvent });
  defaultRuntimeId ??= gateway.adapters.keys().next().value;
  return gateway;
}

function isRegistered(socket: GatewaySocket): socket is RegisteredSocket {
  return Boolean(socket.agentId);
}

function rawText(raw: RawData) {
  if (Array.isArray(raw)) {
    return Buffer.concat(raw).toString("utf8");
  }
  return (Buffer.isBuffer(raw) ? raw : Buffer.from(raw)).toString("utf8");
}

function removeConnection(socket: GatewaySocket) {
  if (!isRegistered(socket)) return;
  const sockets = connections.get(socket.agentId);
  sockets?.delete(socket);
  if (sockets?.size === 0) {
    connections.delete(socket.agentId);
  }
}

function subscribe(socket: RegisteredSocket, sessionId: string) {
  socket.sessionIds.add(sessionId);
}

function startSession(socket: RegisteredSocket, data: ClientPayload, mentions?: Mention[]) {
  const conversationId = trimToNull(data.sessionId) || socket.agentId;
  const existing = getGateway().session(socket.agentId, conversationId);
  const runtimeId = trimToNull(data.runtimeId) ?? existing?.runtimeId ?? defaultRuntimeId;
  const agentId = trimToNull(data.agentId) ?? existing?.agentId ?? socket.agentId;
  if (
    socket.targets &&
    !socket.targets.some((target) => target.runtimeId === runtimeId && target.agentId === agentId)
  ) {
    throw new Error("Target is not authorized for this client");
  }
  const targets = socket.targets;
  if (
    targets &&
    mentions?.some(
      (mention) =>
        !targets.some(
          (target) => target.runtimeId === runtimeId && target.agentId === mention.agentId,
        ),
    )
  ) {
    throw new Error("Mention target is not authorized for this client and runtime");
  }
  const session = getGateway().startSession({
    clientId: socket.agentId,
    conversationId,
    runtimeId,
    agentId,
  });
  subscribe(socket, conversationId);
  socket.sessionRuntimes.set(conversationId, session.runtimeId);
  return session;
}

function replay(socket: GatewaySocket, session: GatewaySession, afterCursor: unknown = 0) {
  if (typeof afterCursor !== "number" || !Number.isSafeInteger(afterCursor) || afterCursor < 0) {
    throw new Error("Invalid event cursor");
  }
  const authorized =
    !socket.targets ||
    socket.targets.some(
      (target) => target.runtimeId === session.runtimeId && target.agentId === session.agentId,
    );
  if (!authorized) {
    return;
  }
  const folds = new Map<string, Projector>();
  // Rebuild presentation state from the canonical journal, then resend the last
  // cursor inclusively so a disconnect cannot lose sibling legacy projections.
  for (const event of gateway.store.events(session.clientId, session.conversationId)) {
    // Agent-wide feeds and old unbound routes may share a literal conversation
    // name, but never its binding or replay authorization. Filter before folding.
    if (
      event.audience !== "conversation" ||
      event.runtimeId !== session.runtimeId ||
      event.agentId !== session.agentId
    ) {
      continue;
    }
    let fold = folds.get(event.runId);
    if (!fold) {
      fold = createClientProjector();
      folds.set(event.runId, fold);
    }
    const messages = fold(event);
    if (terminalTypes.has(event.type)) {
      folds.delete(event.runId);
    }
    if (event.cursor < afterCursor) {
      continue;
    }
    if (socket.protocolVersion === PROTOCOL_VERSION) {
      sendJson(socket, event);
    } else {
      messages.forEach((payload, index) =>
        sendJson(socket, {
          ...payload,
          eventId: event.eventId,
          cursor: event.cursor,
          deliveryId: `${event.eventId}:${index}`,
        }),
      );
    }
  }
}

function normalizeMentions(value: unknown): Mention[] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }
  const mentions = value.flatMap((item: unknown) => {
    const mention = item as { agentId?: unknown; label?: unknown } | null | undefined;
    const agentId = trimToNull(mention?.agentId);
    const label = trimToNull(mention?.label);
    return agentId ? [{ agentId, ...(label ? { label } : {}) }] : [];
  });
  return mentions.length ? mentions : undefined;
}

function acknowledgeSession(socket: RegisteredSocket, session: GatewaySession) {
  sendJson(socket, {
    type: "session.started",
    sessionId: session.conversationId,
    runtimeId: session.runtimeId,
    agentId: session.agentId,
    capabilities: getGateway().capabilities(session.runtimeId),
  });
}

function forwardMessage(socket: RegisteredSocket, data: ClientPayload, signal: AbortSignal) {
  if (!socket.agentId || typeof data.content !== "string" || !data.content) {
    return Promise.resolve();
  }
  const conversationId = trimToNull(data.sessionId) || socket.agentId;
  const acknowledged = socket.sessionRuntimes.has(conversationId);
  const mentions = normalizeMentions(data.mentions);
  const session = startSession(socket, data, mentions);
  // Implicit first-message admission needs the same acknowledgment as session.start;
  // reconnecting SDKs restore only bindings the gateway has confirmed.
  if (!acknowledged) {
    acknowledgeSession(socket, session);
  }
  const request = {
    ...session,
    content: data.content,
    mentions,
    requestId: trimToNull(data.queryMessageId) ?? undefined,
    replyId: trimToNull(data.answerMessageId) ?? undefined,
  };
  return getGateway()
    .run(request, { signal })
    .then((result) => {
      if (result?.duplicate) {
        sendJson(socket, {
          type: "request_duplicate",
          sessionId: session.conversationId,
          queryMessageId: request.requestId,
          runId: result.runId,
          status: result.status,
        });
        replay(socket, session, data.afterCursor ?? 0);
      }
    });
}

function publishPush({ agentId, sessionId, runtimeId, clientId, ...payload }: PublishPushInput) {
  const currentGateway = getGateway();
  const selectedSession = trimToNull(sessionId);
  const addressed = selectedSession ? currentGateway.sessionsAt(selectedSession) : [];
  const candidates = addressed.filter(
    (item) =>
      (!clientId || item.clientId === clientId) &&
      (!runtimeId || item.runtimeId === runtimeId) &&
      [item.agentId, item.clientId, selectedSession].includes(agentId),
  );
  // Legacy pushes resolve only a unique binding. An addressed but mismatched
  // binding must reject, never fall through to the unbound legacy route below.
  if (addressed.length && candidates.length !== 1) {
    return false;
  }
  const session = candidates[0];
  if (session) {
    currentGateway.push({ ...session, replyId: `push-${randomUUID()}` }, payload);
    return true;
  }
  const selectedRuntimeId = runtimeId ?? defaultRuntimeId!;
  if (!selectedSession) {
    const owners = new Set(
      [...currentGateway.sessions.values()]
        .filter(
          (item) =>
            item.agentId === agentId &&
            item.runtimeId === selectedRuntimeId &&
            connections.has(item.clientId) &&
            (!clientId || item.clientId === clientId),
        )
        .map((item) => item.clientId),
    );
    if (connections.has(agentId) && (!clientId || clientId === agentId)) {
      owners.add(agentId);
    }
    if (!owners.size) {
      return false;
    }
    for (const ownerId of owners) {
      currentGateway.push(
        {
          clientId: ownerId,
          runtimeId: selectedRuntimeId,
          agentId,
          conversationId: agentId,
          audience: "agent",
          replyId: `push-${agentId}-${randomUUID()}`,
        },
        payload,
      );
    }
    return true;
  }
  const conversationId = selectedSession || agentId;
  const hasTarget = (id: string) =>
    (!clientId || clientId === id) &&
    [...(connections.get(id) ?? [])].some(
      (socket) =>
        (socket.sessionRuntimes?.get(conversationId) ?? defaultRuntimeId) === selectedRuntimeId &&
        (!selectedSession || socket.sessionIds?.has(conversationId)),
    );
  let targetAgentId = agentId;
  if (!hasTarget(targetAgentId)) {
    // The existing outbound route can address a conversation as its target.
    // Resolve it only when exactly one subscribed owner exists; never pick an arbitrary user.
    if (!selectedSession || agentId !== selectedSession) {
      return false;
    }
    const owners = [...connections.keys()].filter(hasTarget);
    if (owners.length !== 1) {
      return false;
    }
    targetAgentId = owners[0];
  }
  currentGateway.push(
    {
      runtimeId: selectedRuntimeId,
      agentId: targetAgentId,
      conversationId,
      // sendPushMessage's existing optional sessionId contract permits an
      // agent-wide notification. Only that explicit case bypasses conversation filtering.
      audience: selectedSession ? "conversation" : "agent",
      replyId: `push-${targetAgentId}:${conversationId}-${randomUUID()}`,
    },
    payload,
  );
  return true;
}

function initWebSocket(server: Server, { adapters, runtimeId, store, auth }: WebSocketOptions = {}) {
  gateway = createDefaultGateway({ publish: publishEvent, adapters, store, auth });
  defaultRuntimeId = runtimeId ?? gateway.adapters.keys().next().value;
  gateway.adapter(defaultRuntimeId);
  const wss = new WebSocketServer({ noServer: true });
  const plugins = new WebSocketServer({ noServer: true, maxPayload: 4 * 1024 * 1024 });
  // Plugin credentials and lifecycle are a separate trust boundary. Never let
  // a plugin socket register as a browser or inherit its subscription identity.
  server.on("upgrade", (request, socket, head) => {
    if (!auth && !isLocalGatewayRequest(request)) {
      socket.once("error", () => socket.destroy());
      socket.end("HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
      return;
    }
    let url;
    try {
      url = new URL(String(request.url), "http://localhost");
    } catch {
      socket.destroy();
      return;
    }
    if (url.pathname === "/plugins" && url.search) {
      socket.destroy();
      return;
    }
    const target = url.pathname === "/plugins" ? plugins : wss;
    target.handleUpgrade(request, socket, head, (peer) => target.emit("connection", peer));
  });
  const shutdown = new AbortController();
  plugins.on("connection", (socket: ManagedSocket) => {
    socket.isAlive = true;
    socket.on("pong", () => {
      socket.isAlive = true;
    });
    const timer = setTimeout(() => socket.close(4001, "Plugin handshake required"), 3000);
    timer.unref();
    socket.once("close", () => clearTimeout(timer));
    socket.once("error", () => {});
    socket.once("message", (raw) => {
      clearTimeout(timer);
      try {
        const hello = JSON.parse(rawText(raw));
        const adapter = gateway.adapter(hello.runtimeId);
        if (!adapter.attachPlugin) {
          throw new Error("Runtime does not accept plugins");
        }
        adapter.attachPlugin(socket, hello, (agentId, target, payload) => {
          const session = gateway.session(target.clientId, target.conversationId);
          if (!session || session.runtimeId !== adapter.id || session.agentId !== agentId) {
            throw new Error("Plugin notification target has no matching session binding");
          }
          const principal = auth?.clients.find((client) => client.id === session.clientId);
          if (
            auth &&
            !principal?.targets.some(
              (item) => item.runtimeId === session.runtimeId && item.agentId === session.agentId,
            )
          ) {
            throw new Error("Plugin notification target is not authorized");
          }
          const claim = gateway.push({ ...session, replyId: `push-${randomUUID()}` }, payload);
          return { runId: claim.runId, duplicate: !claim.fresh };
        });
      } catch {
        socket.close(4001, "Plugin handshake rejected");
      }
    });
  });
  wss.on("connection", (socket: GatewaySocket) => {
    socket.isAlive = true;
    socket.on("pong", () => {
      socket.isAlive = true;
    });
    socket.on("message", (raw) => {
      let data: ClientPayload;
      try {
        data = JSON.parse(rawText(raw));
      } catch {
        return;
      }
      if (!data || typeof data !== "object" || Array.isArray(data)) {
        return;
      }
      if (data.type === "ping") {
        return sendJson(socket, { type: "pong" });
      }
      if (data.type === "register") {
        const principal = auth ? authenticate(auth, data.token) : undefined;
        const agentId = auth ? principal?.id : trimToNull(data.agentId);
        if (!agentId) {
          return socket.close(4001, "Invalid agentId");
        }
        if (data.protocolVersion !== undefined && data.protocolVersion !== PROTOCOL_VERSION) {
          return socket.close(4002, "Unsupported protocol version");
        }
        removeConnection(socket);
        socket.agentId = agentId;
        socket.targets = principal?.targets;
        socket.protocolVersion = data.protocolVersion;
        socket.sessionIds = new Set();
        socket.sessionRuntimes = new Map();
        const registered = socket as RegisteredSocket;
        subscribe(registered, trimToNull(data.sessionId) || agentId);
        const sockets = connections.get(agentId) ?? new Set<RegisteredSocket>();
        sockets.add(registered);
        connections.set(agentId, sockets);
        sendJson(socket, {
          type: "registered",
          agentId,
          files: [],
          path: "",
          protocolVersion: PROTOCOL_VERSION,
          runtimes: getGateway()
            .catalog()
            .filter(
              (runtime) =>
                !socket.targets || socket.targets.some((target) => target.runtimeId === runtime.id),
            )
            .map((runtime) => {
              const targets = socket.targets;
              if (!targets || !runtime.agents) {
                return runtime;
              }
              const agents = runtime.agents.filter((targetAgentId) =>
                targets.some(
                  (target) => target.runtimeId === runtime.id && target.agentId === targetAgentId,
                ),
              );
              return Object.assign({}, runtime, { agents });
            }),
        });
      } else if (
        data.type === "session.start" ||
        data.type === "message" ||
        data.type === "cancel" ||
        data.type === "input.control"
      ) {
        const fail = (error: unknown) =>
          sendJson(socket, {
            type: data.type === "input.control" ? "input_control" : "error",
            ...(data.type === "input.control"
              ? {
                  controlId: trimToNull(data.controlId) ?? undefined,
                  runId: trimToNull(data.runId) ?? undefined,
                  status: "rejected",
                }
              : {}),
            message: (error as Error).message,
            sessionId: trimToNull(data.sessionId) || socket.agentId,
          });
        if (!isRegistered(socket)) {
          fail(new Error("Register before starting a session"));
          return;
        }
        try {
          if (data.type === "session.start") {
            const session = startSession(socket, data);
            acknowledgeSession(socket, session);
            replay(socket, session, data.afterCursor ?? 0);
            return;
          }
          if (data.type === "input.control") {
            const conversationId = trimToNull(data.sessionId) || socket.agentId;
            const session = gateway.session(socket.agentId, conversationId);
            if (!session || !socket.sessionRuntimes.has(conversationId)) {
              throw new Error("Unknown session owner");
            }
            if (
              (data.runtimeId !== undefined && data.runtimeId !== session.runtimeId) ||
              (data.agentId !== undefined && data.agentId !== session.agentId)
            ) {
              throw new Error("Control target does not match the bound session");
            }
            if (
              socket.targets &&
              !socket.targets.some(
                (target) =>
                  target.runtimeId === session.runtimeId && target.agentId === session.agentId,
              )
            ) {
              throw new Error("Target is not authorized for this client");
            }
            // Active controls use the acknowledged binding, never implicit
            // session creation or the normal queue that starts another run.
            void gateway
              .control({
                ...session,
                runId: trimToNull(data.runId),
                controlId: trimToNull(data.controlId),
                operation: data.operation,
                content: data.content,
              })
              .then((receipt) => {
                if (receipt.duplicate) {
                  sendJson(socket, {
                    ...receipt,
                    type: "input_control",
                    sessionId: conversationId,
                  });
                }
              })
              .catch(fail);
            return;
          }
          if (data.type === "cancel") {
            const session = gateway.session(socket.agentId, trimToNull(data.sessionId));
            if (!session || session.clientId !== socket.agentId) {
              throw new Error("Unknown session");
            }
            void gateway.cancel(session).catch(fail);
            return;
          }
          // Client disconnect is not task cancellation. The gateway owns accepted
          // work; only server shutdown stops observing the upstream stream.
          void forwardMessage(socket, data, shutdown.signal)?.catch(fail);
        } catch (error) {
          fail(error);
        }
      }
    });
    socket.on("close", () => removeConnection(socket));
    socket.on("error", () => removeConnection(socket));
  });
  const interval = setInterval(() => {
    for (const socket of [...wss.clients, ...plugins.clients]) {
      const managed = socket as ManagedSocket;
      if (!managed.isAlive) {
        removeConnection(socket);
        socket.terminate();
        continue;
      }
      managed.isAlive = false;
      socket.ping();
    }
  }, 30000);
  interval.unref();
  const currentGateway = gateway;
  const gatewayServer = server as GatewayServer;
  gatewayServer.submitTask = (payload) => {
    const session = currentGateway.session(payload.clientId, payload.sessionId);
    if (
      !session ||
      (payload.runtimeId !== undefined && payload.runtimeId !== session.runtimeId) ||
      (payload.agentId !== undefined && payload.agentId !== session.agentId)
    ) {
      return undefined;
    }
    const principal = auth?.clients.find((client) => client.id === session.clientId);
    if (
      auth &&
      !principal?.targets.some(
        (target) => target.runtimeId === session.runtimeId && target.agentId === session.agentId,
      )
    ) {
      throw Object.assign(new Error("Target is not authorized for this client"), { status: 403 });
    }
    const request = {
      ...session,
      content: payload.content,
      origin: "task" as const,
      requestId: `task:${payload.eventId}`,
    };
    const { receipt, completion } = currentGateway.submit(request, { signal: shutdown.signal });
    // Native failures are journaled as run.failed; a failed journal cannot be
    // sent to the client, but must not disappear after the HTTP acknowledgment.
    void completion.catch(() => console.error("Gateway background task observation failed"));
    return receipt;
  };
  gatewayServer.shutdown = async () => {
    shutdown.abort();
    for (const socket of [...wss.clients, ...plugins.clients]) {
      socket.terminate();
    }
    server.closeAllConnections();
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
    await currentGateway.close();
  };
  server.once("close", () => {
    wss.close();
    plugins.close();
    shutdown.abort();
    void currentGateway.close().catch(() => console.error("Gateway adapter cleanup failed"));
  });
  wss.once("close", () => clearInterval(interval));
  return connections;
}

const runtimeCatalog = () => getGateway().catalog();

export { initWebSocket, connections, publishPush, runtimeCatalog };
