const { randomUUID } = require("node:crypto");
const { createEventWriter, validateAdapterSignal } = require("../../../protocol/index.cjs");
const { GatewayStore } = require("./store");

class PushGateway {
  constructor({ adapters, publish, store = new GatewayStore() }) {
    this.adapters = new Map();
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
    this.queues = new Map();
    this.sessions = new Map(
      this.store.sessions().map((session) => [this.sessionKey(session), session]),
    );
    this.activeRuns = new Map();
  }

  sessionKey({ clientId, conversationId }) {
    return JSON.stringify([clientId, conversationId]);
  }

  session(clientId, conversationId) {
    return this.sessions.get(this.sessionKey({ clientId, conversationId }));
  }

  catalog() {
    return [...this.adapters.values()].map(({ id, agents }) =>
      Object.assign({ id, capabilities: this.capabilities(id) }, agents ? { agents } : {}),
    );
  }

  capabilities(runtimeId) {
    // Notification delivery belongs to the gateway, not to a native agent's RPC.
    return {
      steering: false,
      followUp: false,
      ...this.adapter(runtimeId).capabilities,
      proactivePush: true,
    };
  }

  startSession({ clientId, conversationId, runtimeId, agentId }) {
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
      runtimeId: selectedRuntime,
      agentId: selectedAgent,
      nativeSessionId: randomUUID(),
      nativeStarted: false,
    };
    this.store.saveSession(session);
    this.sessions.set(this.sessionKey(session), session);
    return session;
  }

  async cancel(session) {
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

  async control(input) {
    if (this.closing) {
      throw new Error("Gateway is shutting down");
    }
    if (
      ![input.runId, input.controlId].every((value) => typeof value === "string" && value.trim()) ||
      !["steer", "follow_up"].includes(input.operation) ||
      typeof input.content !== "string" ||
      !input.content.trim()
    ) {
      throw new TypeError("Invalid native input control");
    }
    const session = this.session(input.clientId, input.conversationId);
    if (!session) {
      throw new Error("Unknown session owner");
    }
    if (["runtimeId", "agentId"].some((field) => input[field] !== session[field])) {
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
        ["runId", "runtimeId", "agentId"].some((field) => existing[field] !== input[field]) ||
        ["operation", "content"].some((field) => existing.data[field] !== input[field])
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
    const data = { controlId: input.controlId, operation: input.operation, content: input.content };
    // Control dispatch is durable and belongs to the original run. It never
    // claims a new run or changes the parent request/reply correlation.
    active.emit({ type: "input.control", data: { ...data, status: "dispatching" } });
    const task = (async () => {
      let receipt;
      try {
        receipt = await adapter.control(active.request, data);
        if (!["queued", "handled"].includes(receipt?.status)) {
          throw new Error("Invalid native control receipt");
        }
      } catch (error) {
        receipt = { status: error.rpcRejected ? "rejected" : "unknown", message: error.message };
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

  adapter(runtimeId) {
    const adapter = this.adapters.get(runtimeId);
    if (!adapter) {
      throw new Error(`Unknown runtime: ${runtimeId}`);
    }
    return adapter;
  }

  run(input, options = {}) {
    const { receipt, completion } = this.submit(input, options);
    return completion.then(() => receipt);
  }

  submit(input, options = {}) {
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

  admit(request, signals) {
    const runId = randomUUID();
    const events = [];
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

  writer(request, runId = randomUUID(), publish = this.publish) {
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

  async execute(adapter, input, { signal, emit, runId }) {
    let request = { ...input, runId };
    emit({ type: "run.started", data: {} });
    const key = this.sessionKey(request);
    const active = { request, runId, emit, controls: new Set() };
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

  push(input, payload) {
    if (this.closing) {
      throw new Error("Gateway is shutting down");
    }
    const adapter = this.adapter(input.runtimeId);
    const fields = ["content", "signal", "event"].filter((field) => payload[field] !== undefined);
    if (fields.length !== 1) {
      throw new TypeError("Provide exactly one of content, signal or native event");
    }
    let signals;
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
    const request = {
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

module.exports = { PushGateway };
