import { randomUUID } from "node:crypto";
import path from "node:path";
import client from "../client.cjs";

export const name = "push-channel";
export const inject = ["agents", "subagents", "sessionPersistence", "loader"];
export const capabilities = Object.freeze({
  streaming: true,
  tools: true,
  reasoning: true,
  subagents: true,
  sessionRestore: true,
  steering: true,
  followUp: true,
  mentions: false,
  cancel: false,
});

function textOf(blocks = [], type = "text") {
  if (!Array.isArray(blocks)) {
    throw new Error("Invalid dsh content blocks");
  }
  return blocks
    .filter((block) => block.type === type)
    .map((block) => {
      if (typeof block.text !== "string") {
        throw new Error("Invalid dsh content text");
      }
      return block.text;
    })
    .join("\n");
}
const rejected = (message) => Object.assign(new Error(message), { rpcRejected: true });
const ownerKey = (request) =>
  JSON.stringify([request.clientId, request.runtimeId, request.agentId, request.conversationId]);
const inputMessage = (text) =>
  Object.freeze({
    id: randomUUID(),
    role: "user",
    source: Object.freeze({ kind: "user" }),
    content: Object.freeze([Object.freeze({ type: "text", text })]),
  });

/** Host-native bridge: all execution and persistence stay behind public ctx APIs. */
export function createDshBridge(ctx, { provider, model, cwd }) {
  if (
    ![provider, model].every((value) => typeof value === "string" && value.trim()) ||
    typeof cwd !== "string" ||
    !path.isAbsolute(cwd)
  ) {
    throw new Error("dsh plugin requires provider, model and an absolute cwd");
  }
  const sessions = new Map();
  const nodes = new Map();
  const activations = new Map();
  const disposers = [];
  const lifetime = new AbortController();
  let closing;

  const send = (node, type, data = {}) => node.job.emit({ type, data });
  const childIdentity = (node) => ({ agentId: node.agent.id, childSessionId: node.agent.id });
  const live = (node) => node && node.job.active && ctx.agents.get(node.agent.id) === node.agent;
  const queue = (job, extra = {}) => {
    // followup() synchronously publishes inbox insertion before returning.
    // Flush its native queue only after successful admission was acknowledged.
    if (!job.admitted) {
      return;
    }
    job.emit({
      type: "input.queue",
      data: {
        steering: job.agent.inbox.nextStep.map((message) => textOf(message.content)),
        followUp: job.agent.inbox.nextTurn.map((message) => textOf(message.content)),
        ...extra,
      },
    });
  };
  function fail(node, error) {
    node.job.failure ??= error instanceof Error ? error : new Error("Invalid dsh stream");
    node.job.poisoned = true;
    node.job.agent.cancel(
      { kind: "hook", reason: "Push Channel stream decoding failed" },
      { keepInbox: true },
    );
    void node.job.stop();
  }
  function observe(node, operation) {
    if (!node?.job.active) {
      return;
    }
    // Cordis contains notification-listener errors. Preserve the failure on the
    // owning run, or malformed output could otherwise become run.completed.
    try {
      operation();
    } catch (error) {
      fail(node, error);
    }
  }
  const listen = (event, handler) => disposers.push(ctx.on(event, handler));
  function startMessage(node, message) {
    if (message.started) {
      return;
    }
    message.started = true;
    if (!node.activation) {
      send(node, "message.started", { messageId: message.id });
    }
  }
  function reasoningEnd(node, message) {
    if (!message.reasoning) {
      return;
    }
    send(node, "reasoning.completed");
    message.reasoning = false;
  }
  function acceptFrame(node, frame) {
    if (
      !frame ||
      !Number.isSafeInteger(frame.revision) ||
      frame.revision <= node.revision ||
      typeof frame.attemptId !== "string"
    ) {
      throw new Error("Invalid dsh assistant stream revision");
    }
    node.revision = frame.revision;
    if (frame.type === "start") {
      if (node.attempt || ![frame.turn, frame.step].every(Number.isSafeInteger)) {
        throw new Error("Invalid dsh stream start");
      }
      const key = JSON.stringify([frame.turn, frame.step]);
      const message = node.messages.get(key) ?? {
        id: randomUUID(),
        started: false,
        reasoning: false,
      };
      if (message.completed) {
        throw new Error("Invalid repeated settled dsh message");
      }
      node.messages.set(key, message);
      node.attempt = { id: frame.attemptId, turn: frame.turn, step: frame.step, index: 0, message };
      return;
    }
    const attempt = node.attempt;
    if (!attempt || attempt.id !== frame.attemptId || frame.index !== attempt.index) {
      throw new Error("Invalid dsh stream chunk order");
    }
    const message = attempt.message;
    if (frame.type === "chunk") {
      if (
        !Number.isSafeInteger(frame.time) ||
        !frame.chunk ||
        typeof frame.chunk.type !== "string"
      ) {
        throw new Error("Invalid dsh stream chunk");
      }
      attempt.index++;
      const chunk = frame.chunk;
      if (chunk.type === "text-delta" || chunk.type === "reasoning-delta") {
        if (typeof chunk.text !== "string") {
          throw new Error("Invalid dsh stream text");
        }
        if (chunk.type === "text-delta") {
          startMessage(node, message);
          send(node, node.activation ? "subagent.delta" : "message.delta", {
            ...(node.activation ? childIdentity(node) : {}),
            messageId: message.id,
            delta: chunk.text,
          });
        } else if (!node.activation) {
          if (!message.reasoning) {
            send(node, "reasoning.started");
            message.reasoning = true;
          }
          send(node, "reasoning.delta", { delta: chunk.text });
        }
      } else if (
        !["block-start", "block-end", "tool-call-delta", "usage", "finish"].includes(chunk.type)
      ) {
        throw new Error("Invalid dsh stream chunk type");
      }
      return;
    }
    if (frame.type !== "end" || !frame.outcome || frame.outcome.kind !== "committed") {
      throw new Error("dsh assistant stream abandoned or invalid");
    }
    // session/event precedes committed end. Retain only this attempt's pending
    // settlement; synchronous Session history readers are deprecated upstream.
    const event = attempt.settlement;
    if (
      !event ||
      event.seq !== frame.outcome.seq ||
      event.type !== frame.outcome.eventType ||
      event.data.turn !== attempt.turn ||
      event.data.step !== attempt.step
    ) {
      throw new Error("Invalid dsh stream settlement");
    }
    if (event.type === "assistant/attempt") {
      // Failed provider attempts may have visible previews but no surfaced
      // message. A retry replaces that preview under the same turn/step id.
      if (message.started) {
        send(node, node.activation ? "subagent.message" : "message.snapshot", {
          ...(node.activation
            ? { ...childIdentity(node), content: "", replace: true }
            : { text: "" }),
          messageId: message.id,
        });
      }
      if (message.reasoning) {
        send(node, "reasoning.delta", { delta: "", replace: true });
      }
      reasoningEnd(node, message);
    } else if (event.type === "assistant/message" && Array.isArray(event.data.message?.content)) {
      const text = textOf(event.data.message.content);
      if (typeof text !== "string") {
        throw new Error("Invalid dsh settled text");
      }
      if (node.activation) {
        send(node, "subagent.message", {
          ...childIdentity(node),
          messageId: message.id,
          content: text,
          replace: true,
        });
      } else {
        if (text || message.started) {
          startMessage(node, message);
          send(node, "message.snapshot", { messageId: message.id, text });
          send(node, "message.completed", { messageId: message.id });
        }
        const reasoning = textOf(event.data.message.content, "reasoning");
        if (reasoning) {
          if (!message.reasoning) {
            send(node, "reasoning.started");
            message.reasoning = true;
          }
          send(node, "reasoning.delta", { delta: reasoning, replace: true });
        }
        reasoningEnd(node, message);
      }
      message.completed = true;
    } else {
      throw new Error("Invalid dsh assistant settlement type");
    }
    node.attempt = undefined;
  }
  listen("agent/assistant-stream", ({ agent, frame }) => {
    const node = nodes.get(agent.id);
    const resident = activations.get(agent.id);
    // Continuable children can outlive a gateway run while their inbox is
    // parked. Track the native revision without retaining the settled job.
    if (!node && resident?.agent === agent && Number.isSafeInteger(frame.revision)) {
      resident.revision = Math.max(resident.revision, frame.revision);
    }
    if (live(node) && node.agent === agent) {
      if (node.activation && frame.revision <= node.baselineRevision) {
        return;
      }
      observe(node, () => acceptFrame(node, frame));
      if (resident) {
        resident.revision = node.revision;
      }
    }
  });
  listen("subagent/start", (info) => {
    if (!info.local) {
      return;
    }
    const agent = ctx.agents.get(info.id);
    if (!agent) {
      return;
    }
    const parent = [...nodes.values()].find(
      (node) => live(node) && ctx.agents.isOwnedBy(agent.id, node.agent),
    );
    if (!parent) {
      return;
    }
    const existing = activations.get(info.id);
    if (existing?.info.runId === info.runId) {
      return;
    }
    const resident = { agent, parent: parent.agent, owner: parent.owner, info, revision: 0 };
    activations.set(agent.id, resident);
    const node = childNode(resident, parent.job);
    nodes.set(agent.id, node);
    observe(node, () => send(node, "subagent.started", childIdentity(node)));
  });
  listen("subagent/end", (info) => {
    const resident = activations.get(info.id);
    const node = nodes.get(info.id);
    if (
      !resident ||
      resident.info.runId !== info.runId ||
      resident.info.provider !== info.provider
    ) {
      return;
    }
    // The native manager disposes/releases the Agent before announcing end.
    // Ownership was checked at start; the paired activation remains evidence.
    // End also retires parked activations while no gateway run is observing.
    activations.delete(info.id);
    nodes.delete(info.id);
    observe(node, () => {
      const identity = childIdentity(node);
      if (info.lastAssistantMessage) {
        send(node, "subagent.result", { ...identity, content: textOf(info.lastAssistantMessage) });
      }
      const status =
        info.stopReason === "completed"
          ? "ok"
          : info.stopReason === "aborted"
            ? "cancelled"
            : "error";
      if (status === "error") {
        send(node, "subagent.failed", { ...identity, message: `dsh child ${info.stopReason}` });
      }
      send(node, "subagent.ended", { ...identity, status });
    });
  });
  function childNode(resident, job) {
    return {
      agent: resident.agent,
      owner: resident.owner,
      job,
      activation: resident.info,
      revision: resident.revision,
      baselineRevision: resident.revision,
      messages: new Map(),
    };
  }
  function ownedActivation(resident, record) {
    let current = resident;
    while (current?.owner === record) {
      if (
        ctx.agents.get(current.agent.id) !== current.agent ||
        !ctx.agents.isOwnedBy(current.agent.id, current.parent)
      ) {
        return false;
      }
      if (current.parent === record.handle.agent) {
        return true;
      }
      const parent = activations.get(current.parent.id);
      if (parent?.agent !== current.parent) {
        return false;
      }
      current = parent;
    }
    return false;
  }
  function bindResidents(record, job) {
    const bound = [];
    for (const [id, resident] of activations) {
      if (resident.owner !== record) {
        continue;
      }
      if (!ownedActivation(resident, record)) {
        activations.delete(id);
        continue;
      }
      const node = childNode(resident, job);
      nodes.set(id, node);
      bound.push(node);
    }
    return bound;
  }
  listen("session/event", (session, event) => {
    const node = nodes.get(session.id);
    if (!live(node) || node.agent.session !== session) {
      return;
    }
    observe(node, () => {
      const { type, data } = event;
      if (type === "assistant/message" || type === "assistant/attempt") {
        const attempt = node.attempt;
        if (attempt && data.turn === attempt.turn && data.step === attempt.step) {
          attempt.settlement = event;
        }
      } else if (type === "turn/end" && !node.activation) {
        node.job.terminal = data.reason;
        if (node.job.nativeFailure && event.seq >= node.job.nativeFailure.seq) {
          node.job.nativeFailure = undefined;
        }
      } else if (type === "tool/call") {
        let args;
        try {
          args = JSON.parse(data.arguments);
        } catch {
          args = { rawArguments: data.arguments };
        }
        if (!args || typeof args !== "object" || Array.isArray(args)) {
          args = { rawArguments: data.arguments };
        }
        node.tools ??= new Map();
        node.tools.set(data.callId, data.name);
        send(node, node.activation ? "subagent.tool.started" : "tool.started", {
          ...(node.activation ? childIdentity(node) : {}),
          toolCallId: data.callId,
          toolName: data.name,
          args,
        });
      } else if (type === "tool/result") {
        send(node, node.activation ? "subagent.tool.completed" : "tool.completed", {
          ...(node.activation ? childIdentity(node) : {}),
          toolCallId: data.message.toolCallId,
          toolName: node.tools?.get(data.message.toolCallId),
          content: textOf(data.message.content),
          isError: data.message.isError === true,
        });
      }
    });
  });
  listen("agent/error", ({ agent, error }) => {
    const node = nodes.get(agent.id);
    if (live(node) && node.agent === agent && !node.activation) {
      node.job.nativeFailure = { error, seq: agent.session.seq };
    }
  });
  for (const event of ["agent/inbox/inserted", "agent/inbox/claimed", "agent/inbox/discarded"]) {
    listen(event, ({ agent }) => {
      const node = nodes.get(agent.id);
      if (live(node) && node.agent === agent && !node.activation) {
        observe(node, () => queue(node.job));
      }
    });
  }
  async function disposeOwner(record) {
    record.disposing ??= (async () => {
      try {
        await ctx.subagents.drainDescendants([record.handle.agent]);
      } finally {
        try {
          await record.handle.dispose();
        } finally {
          for (const collection of [nodes, activations]) {
            for (const [id, value] of collection) {
              if (value.owner === record) {
                collection.delete(id);
              }
            }
          }
        }
      }
      sessions.delete(record.handle.agent.id);
    })();
    return record.disposing;
  }
  async function openSession(request) {
    const key = ownerKey(request);
    let record = sessions.get(request.nativeSessionId);
    if (record) {
      if (record.key !== key || record.disposing) {
        throw rejected("dsh native session belongs to another target or is closing");
      }
      return record.open;
    }
    record = { key };
    sessions.set(request.nativeSessionId, record);
    record.open = (async () => {
      const agentOptions = { provider, model };
      const options = request.resume
        ? { resumeSessionId: request.nativeSessionId, agentOptions }
        : { sessionId: request.nativeSessionId, meta: { cwd }, agentOptions };
      try {
        record.handle = await ctx.agents[request.resume ? "resume" : "create"]({
          ...options,
          signal: lifetime.signal,
        });
        return record;
      } catch (error) {
        sessions.delete(request.nativeSessionId);
        throw Object.assign(
          error instanceof Error ? error : new Error("dsh session initialization failed"),
          { rpcRejected: true },
        );
      }
    })();
    return record.open;
  }
  function active(request) {
    const record = sessions.get(request.nativeSessionId);
    const job = record?.job;
    if (!job?.active || job.request.runId !== request.runId || record.key !== ownerKey(request)) {
      throw rejected("Control does not target the active dsh run");
    }
    return job;
  }
  function discardControls(job) {
    const discardedSteering = [];
    const discardedFollowUp = [];
    const pending = [...job.agent.inbox.nextStep, ...job.agent.inbox.nextTurn];
    for (const message of pending) {
      const control = job.controls.get(message.id);
      if (!control || !job.agent.inbox.remove(message.id)) {
        continue;
      }
      (control.operation === "steer" ? discardedSteering : discardedFollowUp).push(control.content);
    }
    queue(job, {
      ...(discardedSteering.length ? { discardedSteering } : {}),
      ...(discardedFollowUp.length ? { discardedFollowUp } : {}),
      reason: job.aborted ? "cancelled" : "run_settled",
    });
  }
  return {
    capabilities,
    async run(request, { emit, onAccepted, signal }) {
      if (closing || !request.nativeSessionId || !request.runId || request.mentions?.length) {
        throw rejected(
          "dsh plugin requires an owned native session/run and does not support mentions",
        );
      }
      await ctx.loader.await();
      if (closing || signal?.aborted) {
        throw rejected("dsh plugin is closing");
      }
      const record = await openSession(request);
      const agent = record.handle.agent;
      if (
        closing ||
        record.job ||
        ctx.agents.get(agent.id) !== agent ||
        agent.id !== request.nativeSessionId ||
        agent.status !== "idle"
      ) {
        throw rejected("dsh session is not available for this run");
      }
      const job = { request, agent, emit, controls: new Map(), active: true, admitted: false };
      job.stop = () => {
        // Lost observation owns the whole native handle. Drain descendants now,
        // before quiescence waits, so teardown cannot hang on an active child.
        job.stopping ??= (async () => {
          try {
            await disposeOwner(record);
          } catch (error) {
            job.failure ??= error;
          }
        })();
        return job.stopping;
      };
      record.job = job;
      const root = { agent, owner: record, job, revision: 0, messages: new Map() };
      nodes.set(agent.id, root);
      const residents = bindResidents(record, job);
      const abort = () => {
        job.aborted = true;
        agent.cancel({ kind: "disposed" });
        void job.stop();
      };
      signal?.addEventListener("abort", abort, { once: true });
      try {
        if (signal?.aborted) {
          abort();
        } else {
          agent.followup(inputMessage(request.content));
          job.nativeAdmitted = true;
          onAccepted?.();
          job.admitted = true;
          for (const node of residents) {
            send(node, "subagent.started", childIdentity(node));
          }
          queue(job);
        }
        while (true) {
          await agent.whenIdle();
          const seq = agent.session.seq;
          const joined = await ctx.subagents.waitForChildren(agent);
          if (!joined && agent.status === "idle" && agent.session.seq === seq) {
            break;
          }
        }
        if (job.failure) {
          throw job.failure;
        }
        if (job.aborted) {
          throw new Error("dsh plugin observation was aborted");
        }
        if (job.nativeFailure) {
          throw job.nativeFailure.error;
        }
        if (job.terminal?.kind !== "completed") {
          throw new Error(
            job.terminal?.error?.message ??
              `dsh run ended without completion: ${job.terminal?.kind ?? "missing terminal"}`,
          );
        }
      } catch (error) {
        if (job.nativeAdmitted && (agent.status !== "idle" || job.poisoned || job.aborted)) {
          job.poisoned = true;
          await job.stop();
        }
        throw error;
      } finally {
        signal?.removeEventListener("abort", abort);
        try {
          discardControls(job);
        } finally {
          job.active = false;
          record.job = undefined;
          for (const [id, node] of nodes) {
            if (node.job === job) {
              nodes.delete(id);
            }
          }
          if (job.poisoned || job.aborted) {
            await disposeOwner(record);
          }
        }
      }
    },
    async control(request, { operation, content }) {
      const job = active(request);
      if (
        !["steer", "follow_up"].includes(operation) ||
        typeof content !== "string" ||
        !content.trim()
      ) {
        throw rejected("Invalid dsh input control");
      }
      const message = inputMessage(content);
      job.controls.set(message.id, { operation, content });
      try {
        job.agent[operation === "steer" ? "steer" : "followup"](message);
      } catch (error) {
        job.controls.delete(message.id);
        throw error;
      }
      queue(job);
      return { status: "queued" };
    },
    close() {
      if (closing) {
        return closing;
      }
      lifetime.abort();
      closing = (async () => {
        const records = [...sessions.values()];
        await Promise.allSettled(records.map((record) => record.open));
        for (const record of records) {
          if (record.job) {
            record.job.aborted = true;
            record.handle.agent.cancel({ kind: "disposed" });
          }
        }
        try {
          const results = await Promise.allSettled(
            records.filter((record) => record.handle).map(disposeOwner),
          );
          const failures = results
            .filter((result) => result.status === "rejected")
            .map((result) => result.reason);
          if (failures.length) {
            throw new AggregateError(failures, "dsh plugin cleanup failed");
          }
        } finally {
          for (const dispose of disposers.splice(0)) {
            dispose();
          }
        }
      })();
      return closing;
    },
  };
}

/** The Loader owns this effect; network readiness never gates host startup. */
export function apply(ctx, config) {
  if (
    !config ||
    ![config.url, config.runtimeId, config.agentId, config.tokenEnv].every(
      (value) => typeof value === "string" && value.trim(),
    )
  ) {
    throw new Error("dsh plugin requires url, runtimeId, agentId and tokenEnv");
  }
  const token = process.env[config.tokenEnv];
  if (!token) {
    throw new Error("dsh plugin token environment variable is missing");
  }
  const bridge = createDshBridge(ctx, config);
  ctx.effect(() => {
    const lifetime = new AbortController();
    let peer;
    const connecting = (async () => {
      try {
        peer = await client.connectPlugin({
          url: config.url,
          runtimeId: config.runtimeId,
          agentId: config.agentId,
          token,
          capabilities,
          run: (request, context) => bridge.run(request, context),
          control: (request, input) => bridge.control(request, input),
          signal: lifetime.signal,
        });
      } catch (error) {
        if (!lifetime.signal.aborted) {
          const message = error instanceof Error ? error.message : "Connection failed";
          ctx.logger.warn(`Push Channel is offline: ${message}`);
        }
      }
    })();
    ctx.provide("pushChannel", {
      notify(target, notification) {
        if (!peer || lifetime.signal.aborted) {
          return Promise.reject(new Error("Push Channel is offline"));
        }
        return peer.notify(target, notification);
      },
    });
    return async () => {
      lifetime.abort();
      try {
        await connecting;
        await peer?.close();
      } finally {
        await bridge.close();
      }
    };
  }, "push-channel.connection");
}
