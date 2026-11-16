import { randomUUID } from "node:crypto";
import { createEventWriter, validateAdapterSignal } from "../../../protocol/index.ts";
import { GatewayStore } from "./store.ts";
import type {
  AdapterSignal,
  ControlReceipt,
  InputControl,
  PushEvent,
  RunRequest,
  Signal,
} from "../../../protocol/index.ts";
import type {
  GatewayAdapter,
  GatewayContext,
  GatewaySession,
  JournalRequest,
  PushPayload,
  SessionAddress,
} from "./types.ts";

type Publish = (event: PushEvent) => void;
type EventWriter = ReturnType<typeof createEventWriter>;
type SubmitOptions = { signal?: AbortSignal };
type OwnedRun = RunRequest & SessionAddress & { runId: string };
type GatewayControl = ControlReceipt & { runId: string; duplicate?: true };
type ActiveRun = {
  request: OwnedRun;
  runId: string;
  emit: EventWriter;
  controls: Set<Promise<GatewayControl>>;
};

export type GatewayOptions = {
  adapters: Iterable<GatewayAdapter>;
  publish: Publish;
  store?: GatewayStore;
};

export type RunReceipt = { duplicate: boolean; runId: string; status: string };

type ControlInput = SessionAddress & Pick<RunRequest, "agentId" | "runtimeId"> & {
  [Field in keyof InputControl]: unknown;
};

class PushGateway {
  #sessionKeysByAddress = new Map<string, Set<string>>();
  readonly adapters = new Map<string, GatewayAdapter>();
  readonly store: GatewayStore;
  readonly deliver: Publish;
  readonly publish: Publish;
  readonly queues = new Map<string, Promise<void>>();
  readonly sessions = new Map<string, GatewaySession>();
  readonly activeRuns = new Map<string, ActiveRun>();
  private closing = false;
  private closeTask?: Promise<void>;

  constructor({ adapters, publish, store = new GatewayStore() }: GatewayOptions) {
    for (const adapter of adapters) {
      if (this.adapters.has(adapter.id)) {
        throw new Error(`Duplicate runtime: ${adapter.id}`);
      }
      this.adapters.set(adapter.id, adapter);
    }
    this.store = store;
    this.store.recoverInterrupted();
    this.deliver = publish;
    this.publish = (event) => publish(this.store.appendEvent(event));
    for (const session of this.store.sessions()) {
      this.#registerSession(session);
    }
  }

  sessionKey({ clientId, conversationId }: SessionAddress) {
    return JSON.stringify([clientId, conversationId]);
  }

  session(clientId: string, conversationId: string | null) {
    if (conversationId === null) {
      return undefined;
    }
    return this.sessions.get(this.sessionKey({ clientId, conversationId }));
  }

  #registerSession(session: GatewaySession) {
    const key = this.sessionKey(session);
    this.sessions.set(key, session);
    for (const address of [session.conversationId, session.nativeSessionId]) {
      let keys = this.#sessionKeysByAddress.get(address);
      if (!keys) {
        keys = new Set();
        this.#sessionKeysByAddress.set(address, keys);
      }
      keys.add(key);
    }
  }

  sessionsAt(address: string) {
    // Address bindings are immutable. Keep keys so lookups see the latest
    // session record after native admission replaces it in the primary map.
    const keys = this.#sessionKeysByAddress.get(address);
    return keys ? Array.from(keys, (key) => this.sessions.get(key)!) : [];
  }

  catalog() {
    return [...this.adapters.values()].map(({ id, agents }) =>
      Object.assign({ id, capabilities: this.capabilities(id) }, agents ? { agents } : {}),
    );
  }

  capabilities(runtimeId: string) {
    // Notification delivery belongs to the gateway, not to a native agent's RPC.
    return {
      steering: false,
      followUp: false,
      ...this.adapter(runtimeId).capabilities,
      proactivePush: true,
    };
  }

  startSession({ clientId, conversationId, runtimeId, agentId }: SessionAddress &
    Partial<Pick<RunRequest, "runtimeId" | "agentId">>) {
    if (this.closing) {
      throw new Error("Gateway is shutting down");
    }
    const existing = this.session(clientId, conversationId);
    if (existing) {
      if (
        (runtimeId && runtimeId !== existing.runtimeId) ||
        (agentId && agentId !== existing.agentId)
      ) {
        throw new Error("Session is already bound to a runtime and agent; start a new session");
      }
      this.adapter(existing.runtimeId).assertAgent?.(existing.agentId);
      return existing;
    }
    const selectedRuntime = runtimeId ?? this.adapters.keys().next().value;
    const adapter = this.adapter(selectedRuntime);
    const selectedAgent = agentId ?? clientId;
    adapter.assertAgent?.(selectedAgent);
    const session = {
      clientId,
      conversationId,
      runtimeId: selectedRuntime!,
      agentId: selectedAgent,
      nativeSessionId: randomUUID(),
      nativeStarted: false,
    };
    this.store.saveSession(session);
    this.#registerSession(session);
    return session;
  }

  async cancel(session: GatewaySession) {
    const adapter = this.adapter(session.runtimeId);
    if (!adapter.capabilities?.cancel || !adapter.cancel) {
      throw new Error("Runtime does not support cancellation");
    }
    const active = this.activeRuns.get(this.sessionKey(session));
    if (!active) {
      throw new Error("No active run to cancel");
    }
    await adapter.cancel(active.request);
  }

  async control(input: ControlInput): Promise<GatewayControl> {
    if (this.closing) {
      throw new Error("Gateway is shutting down");
    }
    if (
      typeof input.runId !== "string" ||
      !input.runId.trim() ||
      typeof input.controlId !== "string" ||
      !input.controlId.trim() ||
      (input.operation !== "steer" && input.operation !== "follow_up") ||
      typeof input.content !== "string" ||
      !input.content.trim()
    ) {
      throw new TypeError("Invalid native input control");
    }
    const session = this.session(input.clientId, input.conversationId);
    if (!session) {
      throw new Error("Unknown session owner");
    }
    if (input.runtimeId !== session.runtimeId || input.agentId !== session.agentId) {
      throw new Error("Control target does not match the bound session");
    }
    const adapter = this.adapter(session.runtimeId);
    const capability = input.operation === "steer" ? "steering" : "followUp";
    if (!adapter.capabilities?.[capability] || !adapter.control) {
      throw new Error("Runtime does not support this input control");
    }
    const active = this.activeRuns.get(this.sessionKey(session));
    const existing = this.store.controlReceipt(
      input.clientId,
      input.conversationId,
      input.controlId,
    );
    if (existing) {
      if (
        existing.runId !== input.runId ||
        existing.runtimeId !== input.runtimeId ||
        existing.agentId !== input.agentId ||
        existing.data.operation !== input.operation ||
        existing.data.content !== input.content
      ) {
        throw new Error("controlId conflict: input differs from the recorded control");
      }
      const lost = existing.data.status === "dispatching" && active?.runId !== input.runId;
      return {
        ...existing.data,
        status: lost ? "unknown" : existing.data.status,
        runId: existing.runId,
        duplicate: true,
      };
    }
    if (!active || active.runId !== input.runId) {
      throw new Error("Control must target the active run");
    }
    const data: Omit<ControlReceipt, "status" | "message"> = {
      controlId: input.controlId, operation: input.operation, content: input.content,
    };
    // Control dispatch is durable and belongs to the original run. It never
    // claims a new run or changes the parent request/reply correlation.
    active.emit({ type: "input.control", data: { ...data, status: "dispatching" } });
    const task = (async () => {
      let receipt: Pick<ControlReceipt, "status" | "message">;
      try {
        receipt = await adapter.control!(active.request, data);
        if (!["queued", "handled"].includes(receipt?.status)) {
          throw new Error("Invalid native control receipt");
        }
      } catch (error) {
        const rejected = typeof error === "object" && error !== null &&
          "rpcRejected" in error && error.rpcRejected;
        receipt = { status: rejected ? "rejected" : "unknown", message: (error as Error).message };
      }
      const final = { ...data, ...receipt };
      active.emit({ type: "input.control", data: final });
      return { ...final, runId: active.runId };
    })();
    active.controls.add(task);
    try {
      return await task;
    } finally {
      active.controls.delete(task);
    }
  }

  async close() {
    if (this.closeTask) {
      return this.closeTask;
    }
    this.closing = true;
    this.closeTask = (async () => {
      await Promise.all([...this.adapters.values()].map((adapter) => adapter.close?.()));
      await Promise.allSettled(this.queues.values());
      this.store.close();
    })();
    return this.closeTask;
  }

  adapter(runtimeId: string | undefined) {
    const adapter = runtimeId === undefined ? undefined : this.adapters.get(runtimeId);
    if (!adapter) {
      throw new Error(`Unknown runtime: ${runtimeId}`);
    }
    return adapter;
  }

  run(input: RunRequest, options: SubmitOptions = {}) {
    const { receipt, completion } = this.submit(input, options);
    return completion.then(() => receipt);
  }

  submit(input: RunRequest, options: SubmitOptions = {}): {
    receipt: RunReceipt; completion: Promise<void>;
  } {
    if (this.closing) {
      throw new Error("Gateway is shutting down");
    }
    const adapter = this.adapter(input.runtimeId);
    adapter.assertAgent?.(input.agentId);
    const request = { ...input, clientId: input.clientId ?? input.agentId };
    const { claim, emit } = this.admit(request, [
      { type: "input.accepted", data: { content: request.content } },
    ]);
    if (!claim.fresh) {
      return {
        receipt: { duplicate: true, runId: claim.runId, status: claim.status },
        completion: Promise.resolve(),
      };
    }
    // Runtime and agent are part of the lock identity: identical chat ids in
    // different runtimes must never share execution state.
    const key = JSON.stringify([
      request.clientId,
      request.runtimeId,
      request.agentId,
      request.conversationId,
    ]);
    const previous = this.queues.get(key) ?? Promise.resolve();
    const current = previous
      .catch(() => {})
      .then(() => this.execute(adapter, request, { ...options, emit, runId: claim.runId }));
    this.queues.set(key, current);
    const cleanup = () => {
      if (this.queues.get(key) === current) {
        this.queues.delete(key);
      }
    };
    void current.then(cleanup, cleanup);
    // HTTP triggers acknowledge committed admission without waiting for the model.
    // The queue still owns completion, including work accepted while offline.
    return {
      receipt: { duplicate: false, runId: claim.runId, status: "accepted" },
      completion: current,
    };
  }

  admit(request: JournalRequest, signals: Signal[]) {
    const runId = randomUUID();
    const events: PushEvent[] = [];
    let committed = false;
    const emit = this.writer(request, runId, (event) => {
      if (committed) {
        this.publish(event);
      } else {
        events.push(event);
      }
    });
    signals.forEach(emit);
    // Admission and replayable input/notification are one durable unit. Nothing
    // reaches clients or native tools until the entire transaction commits.
    const claim = this.store.claimRun(request, runId, events);
    if (claim.fresh) {
      committed = true;
      claim.events.forEach(this.deliver);
    }
    return { claim, emit };
  }

  writer(request: GatewayContext, runId: string = randomUUID(), publish = this.publish) {
    return createEventWriter(
      {
        runtimeId: request.runtimeId,
        agentId: request.agentId,
        conversationId: request.conversationId,
        runId,
        origin: request.origin ?? "prompt",
        audience: request.audience ?? "conversation",
        clientId: request.clientId ?? request.agentId,
        ...(request.requestId ? { requestId: request.requestId } : {}),
        ...(request.replyId ? { replyId: request.replyId } : {}),
      },
      publish,
    );
  }

  async execute(
    adapter: GatewayAdapter,
    input: RunRequest & SessionAddress,
    { signal, emit, runId }: SubmitOptions & { emit: EventWriter; runId: string },
  ) {
    let request = { ...input, runId };
    emit({ type: "run.started", data: {} });
    const key = this.sessionKey(request);
    const active: ActiveRun = { request, runId, emit, controls: new Set() };
    this.activeRuns.set(key, active);
    try {
      const session = this.session(request.clientId, request.conversationId);
      if (session) {
        request = {
          ...request,
          nativeSessionId: session.nativeSessionId,
          resume: session.nativeStarted,
        };
        active.request = request;
      }
      const result = await adapter.run(request, {
        signal,
        onDispatch: () => this.store.markDispatched(runId),
        onAccepted: () => {
          this.store.markDispatched(runId);
          if (!session || session.nativeStarted) {
            return;
          }
          const started = { ...session, nativeStarted: true };
          this.store.saveSession(started);
          this.sessions.set(key, started);
        },
        emit: (event) => {
          emit(validateAdapterSignal(event));
        },
      });
      // Native completion can race its control acknowledgment. Journal every
      // admitted receipt before the terminal event closes replay/presentation.
      await Promise.all(active.controls);
      if (result?.status === "cancelled") {
        emit({ type: "run.cancelled", data: {} });
      } else if (result?.status === "deferred") {
        emit({ type: "run.deferred", data: { message: result.message } });
      } else {
        emit({ type: "run.completed", data: {} });
      }
    } catch (error) {
      await Promise.allSettled(active.controls);
      emit({
        type: "run.failed",
        data: { message: error instanceof Error ? error.message : String(error) },
      });
    } finally {
      if (this.activeRuns.get(key) === active) {
        this.activeRuns.delete(key);
      }
    }
  }

  push(input: GatewayContext, payload: PushPayload) {
    if (this.closing) {
      throw new Error("Gateway is shutting down");
    }
    const adapter = this.adapter(input.runtimeId);
    const fields = (["content", "signal", "event"] as const).filter(
      (field) => payload[field] !== undefined,
    );
    if (fields.length !== 1) {
      throw new TypeError("Provide exactly one of content, signal or native event");
    }
    let signals: AdapterSignal[];
    if (payload.signal !== undefined) {
      signals = [validateAdapterSignal(payload.signal)];
    } else if (payload.event !== undefined) {
      if (!adapter.decodePushEvent) {
        throw new TypeError("Runtime has no native push event decoder; use content or signal");
      }
      signals = adapter.decodePushEvent(payload.event).map(validateAdapterSignal);
    } else {
      if (typeof payload.content !== "string" || !payload.content) {
        throw new TypeError("Invalid push content");
      }
      const messageId = randomUUID();
      signals = [
        { type: "message.started", data: { messageId } },
        { type: "message.delta", data: { messageId, delta: payload.content } },
        { type: "message.completed", data: { messageId } },
      ];
    }
    const request: JournalRequest = {
      ...input,
      clientId: input.clientId ?? input.agentId,
      origin: "push",
      content: payload.content,
      event: payload.event,
      signal: payload.signal,
      ...(payload.eventId ? { requestId: `push:${payload.eventId}` } : {}),
    };
    const { claim } = this.admit(request, [
      { type: "run.started", data: {} },
      ...signals,
      { type: "run.completed", data: {} },
    ]);
    return claim;
  }
}

export { PushGateway };
