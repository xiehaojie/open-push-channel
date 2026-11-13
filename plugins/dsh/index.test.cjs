const assert = require("node:assert/strict");
const test = require("node:test");
const { validateAdapterSignal } = require("../../protocol/index.cjs");

// A boundary fixture for the published Cordis/Agent interfaces, not a native
// loader or provider proof. Native installation is verified separately.
function host() {
  const listeners = new Map();
  const agents = new Map();
  const owners = new Map();
  const creates = [];
  const resumes = [];
  const inputs = [];
  const disposed = [];
  const emit = (name, ...args) => {
    for (const listener of listeners.get(name) ?? []) {
      listener(...args);
    }
  };
  function makeAgent(id, parent, configure, lineageParent = parent) {
    const events = new Map();
    let release;
    const agent = {
      id,
      status: "idle",
      session: {
        id,
        seq: 0,
        header: lineageParent ? { parentSession: lineageParent.id } : {},
        eventAt() {
          throw new Error("Deprecated synchronous history reader must not be used");
        },
      },
      inbox: {
        nextStep: [],
        nextTurn: [],
        remove(messageId) {
          for (const messages of [agent.inbox.nextStep, agent.inbox.nextTurn]) {
            const index = messages.findIndex((message) => message.id === messageId);
            if (index < 0) {
              continue;
            }
            const [message] = messages.splice(index, 1);
            emit("agent/inbox/discarded", { agent, message });
            return true;
          }
          return false;
        },
      },
      followup(message) {
        inputs.push({ agent, operation: "follow_up", message });
        agent.inbox.nextTurn.push(message);
        emit("agent/inbox/inserted", { agent, message });
        if (agent.status === "running") {
          return;
        }
        agent.activeInput = message;
        agent.status = "running";
        agent.pending = new Promise((resolve) => {
          release = resolve;
        });
      },
      steer(message) {
        inputs.push({ agent, operation: "steer", message });
        agent.inbox.nextStep.push(message);
        emit("agent/inbox/inserted", { agent, message });
      },
      whenIdle: () => agent.pending ?? Promise.resolve(),
      cancel(cause, options = {}) {
        agent.cancelCause = cause;
        if (agent.status === "idle") {
          return;
        }
        if (!options.keepInbox) {
          agent.inbox.nextStep.length = 0;
          agent.inbox.nextTurn.length = 0;
        }
        agent.finish({ kind: "aborted", reason: cause });
      },
      frame(frame) {
        agent.revision = (agent.revision ?? 0) + 1;
        if (frame.type === "start") {
          agent.turn = frame.turn;
        }
        emit("agent/assistant-stream", { agent, frame: { ...frame, revision: agent.revision } });
      },
      event(type, data) {
        const event = { type, data, seq: agent.session.seq++, time: 1 };
        events.set(event.seq, event);
        emit("session/event", agent.session, event);
        return event.seq;
      },
      finish(reason = { kind: "completed" }) {
        const index = agent.inbox.nextTurn.indexOf(agent.activeInput);
        if (index >= 0) {
          agent.inbox.nextTurn.splice(index, 1);
        }
        agent.event("turn/end", { turn: agent.turn ?? 1, reason });
        agent.status = "idle";
        release?.();
      },
    };
    agent.ctx = ctx;
    configure?.(ctx, agent);
    agents.set(id, agent);
    if (parent) {
      owners.set(id, parent);
    }
    emit("session/created", agent.session);
    emit("agent/created", { agent, source: "startup" });
    return agent;
  }
  const ctx = {
    loader: { await: async () => {} },
    get(name) {
      return ctx[name];
    },
    on(name, listener) {
      const set = listeners.get(name) ?? new Set();
      set.add(listener);
      listeners.set(name, set);
      return () => set.delete(listener);
    },
    agents: {
      async create(options) {
        creates.push(options);
        const agent = makeAgent(options.sessionId, undefined, options.setup);
        return {
          agent,
          async dispose() {
            if (!agents.has(agent.id)) {
              return;
            }
            agent.cancel({ kind: "disposed" });
            disposed.push(agent.id);
            agents.delete(agent.id);
          },
        };
      },
      async resume(options) {
        resumes.push(options);
        if (options.resumeSessionId === "missing") {
          throw new Error("Session does not exist");
        }
        return ctx.agents.create({ sessionId: options.resumeSessionId, setup: options.setup });
      },
      get: (id) => agents.get(id),
      list: () => [...agents.values()],
      isOwnedBy: (id, parent) => owners.get(id) === parent,
    },
    subagents: {
      waitForChildren: async () => false,
      drainDescendants: async () => {},
    },
  };
  return { ctx, emit, agents, owners, creates, resumes, inputs, disposed, makeAgent, listeners };
}

const request = (nativeSessionId = "native-main", extra = {}) => ({
  clientId: "owner",
  runtimeId: "dsh",
  agentId: "main",
  conversationId: "chat",
  nativeSessionId,
  runId: "run-main",
  content: "hello",
  ...extra,
});
const frame = (type, extra = {}) => ({
  type,
  attemptId: "attempt-1",
  revision: 1,
  ...(type === "start" ? { turn: 1, step: 1 } : {}),
  ...extra,
});

async function setup(t) {
  const { createDshBridge } = await import("./index.mjs");
  const native = host();
  const bridge = createDshBridge(native.ctx, {
    provider: "fixture",
    model: "fixture",
    cwd: "/tmp",
  });
  t.after(() => bridge.close());
  const events = [];
  let accepted = 0;
  const run = (input = request()) =>
    bridge.run(input, {
      emit: (event) => {
        assert.ok(accepted > 0, "dsh signals must follow confirmed native input admission");
        events.push(validateAdapterSignal(event));
      },
      onAccepted: () => {
        accepted++;
      },
    });
  const ready = async () => {
    for (let index = 0; index < 20 && native.inputs.length === 0; index++) {
      await new Promise((resolve) => {
        setImmediate(resolve);
      });
    }
    assert.equal(native.inputs.length, 1, "native followup must be reached");
    return native.inputs[0].agent;
  };
  return { ...native, bridge, events, run, ready, accepted: () => accepted };
}

function settle(agent, text, attemptId = "attempt-1", messageId = "durable-answer", turn = 1) {
  const seq = agent.event("assistant/message", {
    turn,
    step: 1,
    message: { id: messageId, role: "assistant", content: [{ type: "text", text }] },
  });
  agent.frame(
    frame("end", {
      attemptId,
      index: 1,
      outcome: { kind: "committed", eventType: "assistant/message", seq },
    }),
  );
}

void test("dsh plugin emits real live deltas before completion and settles one authoritative message", async (t) => {
  const f = await setup(t);
  const running = f.run();
  const agent = await f.ready();
  agent.frame(frame("start"));
  agent.frame(
    frame("chunk", {
      index: 0,
      time: 1,
      chunk: { type: "text-delta", index: 0, text: "你好\u2028🙂" },
    }),
  );
  const delta = f.events.find((event) => event.type === "message.delta");
  assert.equal(delta.data.delta, "你好\u2028🙂");
  assert.equal(agent.status, "running");
  settle(agent, "你好\u2028🙂，校准");
  agent.finish();
  await running;
  assert.equal(f.accepted(), 1);
  assert.equal(f.events.filter((event) => event.type === "message.started").length, 1);
  assert.equal(f.events.filter((event) => event.type === "message.completed").length, 1);
  const snapshot = f.events.find((event) => event.type === "message.snapshot");
  assert.equal(snapshot.data.messageId, delta.data.messageId);
  assert.equal(snapshot.data.text, "你好\u2028🙂，校准");
  assert.equal(f.creates[0].sessionId, "native-main");
});

void test("dsh plugin isolates live child ownership and preserves tool and child output", async (t) => {
  const f = await setup(t);
  const running = f.run();
  const agent = await f.ready();
  const foreign = f.makeAgent("foreign", undefined, undefined, agent);
  foreign.frame(frame("start"));
  foreign.frame(
    frame("chunk", { index: 0, time: 1, chunk: { type: "text-delta", index: 0, text: "PRIVATE" } }),
  );
  const child = f.makeAgent("child", agent);
  f.emit("subagent/start", { runId: "child-run", provider: "spawn", id: "child", local: true });
  child.frame(frame("start", { attemptId: "child-attempt" }));
  child.frame(
    frame("chunk", {
      attemptId: "child-attempt",
      index: 0,
      time: 1,
      chunk: { type: "text-delta", index: 0, text: "child text" },
    }),
  );
  child.event("tool/call", {
    callId: "child-read",
    name: "read",
    arguments: '{"path":"test.txt"}',
  });
  child.event("tool/result", {
    message: { toolCallId: "child-read", content: [{ type: "text", text: "file" }] },
  });
  settle(child, "child text", "child-attempt", "child-durable");
  // Native in-process teardown releases the live Agent before publishing end.
  f.agents.delete(child.id);
  f.owners.delete(child.id);
  f.emit("subagent/end", {
    runId: "child-run",
    provider: "spawn",
    id: "child",
    local: true,
    stopReason: "completed",
    lastAssistantMessage: [{ type: "text", text: "child text" }],
  });
  agent.event("tool/call", { callId: "root-read", name: "read", arguments: "{}" });
  agent.event("tool/result", {
    message: { toolCallId: "root-read", content: [{ type: "text", text: "root file" }] },
  });
  agent.finish();
  await running;
  assert.ok(
    f.events.some(
      (event) =>
        event.type === "subagent.delta" &&
        event.data.childSessionId === "child" &&
        event.data.delta === "child text",
    ),
  );
  assert.ok(
    f.events.some(
      (event) => event.type === "subagent.tool.completed" && event.data.content === "file",
    ),
  );
  assert.ok(
    f.events.some((event) => event.type === "tool.completed" && event.data.content === "root file"),
  );
  assert.equal(JSON.stringify(f.events).includes("PRIVATE"), false);
  assert.equal(f.events.filter((event) => event.type === "subagent.ended").length, 1);
});

void test("dsh waits for children and the parent summary after its first idle boundary", async (t) => {
  const f = await setup(t);
  let releaseChildren;
  const childrenFinished = new Promise((resolve) => {
    releaseChildren = resolve;
  });
  let childWaits = 0;
  f.ctx.subagents.waitForChildren = async (agent) => {
    childWaits++;
    if (childWaits > 1) {
      return false;
    }
    await childrenFinished;
    agent.followup({
      id: "summary-input",
      role: "user",
      source: { kind: "user" },
      content: [{ type: "text", text: "child finished" }],
    });
    setImmediate(() => {
      agent.frame(frame("start", { attemptId: "summary-attempt", turn: 2 }));
      agent.frame(
        frame("chunk", {
          attemptId: "summary-attempt",
          index: 0,
          time: 2,
          chunk: { type: "text-delta", index: 0, text: "parent summary" },
        }),
      );
      settle(agent, "parent summary", "summary-attempt", "summary-durable", 2);
      agent.finish();
    });
    return true;
  };
  const running = f.run();
  let finished = false;
  void running.then(
    () => {
      finished = true;
    },
    () => {
      finished = true;
    },
  );
  try {
    const agent = await f.ready();
    const child = f.makeAgent("child", agent);
    const childRun = { runId: "child-run", provider: "spawn", id: "child", local: true };
    f.emit("subagent/start", childRun);
    agent.finish();
    await new Promise((resolve) => {
      setImmediate(resolve);
    });
    assert.equal(childWaits, 1);
    assert.equal(
      finished,
      false,
      "root idle must not finish a run while descendants are progressing",
    );
    child.finish();
    f.agents.delete(child.id);
    f.owners.delete(child.id);
    f.emit("subagent/end", { ...childRun, stopReason: "completed" });
    releaseChildren();
    await running;
    assert.ok(childWaits >= 2);
    assert.ok(
      f.events.some(
        (event) => event.type === "message.delta" && event.data.delta === "parent summary",
      ),
    );
  } finally {
    releaseChildren();
    await running.catch(() => {});
  }
});

void test("dsh abandoned attempts fail visibly and are never reported as completed messages", async (t) => {
  const f = await setup(t);
  const running = f.run();
  const rejected = assert.rejects(running, /abandon|failed|stream/i);
  const agent = await f.ready();
  agent.frame(frame("start"));
  agent.frame(
    frame("chunk", { index: 0, time: 1, chunk: { type: "text-delta", index: 0, text: "partial" } }),
  );
  agent.frame(frame("end", { index: 1, outcome: { kind: "abandoned" } }));
  agent.finish();
  await rejected;
  assert.equal(f.events.filter((event) => event.type === "message.completed").length, 0);
});

void test("dsh retries replace failed attempt previews instead of appending a second answer", async (t) => {
  const f = await setup(t);
  const running = f.run();
  const agent = await f.ready();
  agent.frame(frame("start"));
  agent.frame(
    frame("chunk", {
      index: 0,
      time: 1,
      chunk: { type: "text-delta", index: 0, text: "failed preview" },
    }),
  );
  const messageId = f.events.find((event) => event.type === "message.delta").data.messageId;
  const seq = agent.event("assistant/attempt", { turn: 1, step: 1, stream: [] });
  agent.frame(
    frame("end", {
      index: 1,
      outcome: { kind: "committed", eventType: "assistant/attempt", seq },
    }),
  );
  agent.frame(frame("start", { attemptId: "attempt-2" }));
  agent.frame(
    frame("chunk", {
      attemptId: "attempt-2",
      index: 0,
      time: 2,
      chunk: { type: "text-delta", index: 0, text: "successful answer" },
    }),
  );
  settle(agent, "successful answer", "attempt-2");
  agent.finish();
  await running;
  assert.equal(f.events.filter((event) => event.type === "message.started").length, 1);
  assert.equal(f.events.filter((event) => event.type === "message.completed").length, 1);
  const snapshots = f.events.filter((event) => event.type === "message.snapshot");
  assert.ok(snapshots.some((event) => event.data.text === ""));
  assert.equal(snapshots.at(-1).data.messageId, messageId);
  assert.equal(snapshots.at(-1).data.text, "successful answer");
});

void test("dsh plugin rejects failed explicit restore without creating another native session", async (t) => {
  const f = await setup(t);
  await assert.rejects(f.run(request("missing", { resume: true })), /does not exist/);
  assert.equal(f.creates.length, 0);
  assert.equal(f.accepted(), 0);
});

void test("dsh translator failures reject the owning run even when Cordis contains listener errors", async (t) => {
  const f = await setup(t);
  const running = f.run();
  const rejected = assert.rejects(running, /invalid|stream|chunk|text/i);
  const agent = await f.ready();
  agent.frame(frame("start"));
  assert.doesNotThrow(() =>
    agent.frame(
      frame("chunk", {
        index: 0,
        time: 1,
        chunk: { type: "text-delta", index: 0, text: 123 },
      }),
    ),
  );
  agent.finish();
  await rejected;
  assert.equal(f.events.filter((event) => event.type === "message.completed").length, 0);
});

void test("dsh control targets one active run and reports the native pending queue", async (t) => {
  const f = await setup(t);
  const running = f.run();
  await assert.rejects(
    f.bridge.control(request(), { operation: "steer", content: "during initialization" }),
    (error) => error.rpcRejected === true,
  );
  const agent = await f.ready();
  const foreign = {
    id: "foreign-input",
    role: "user",
    source: { kind: "user" },
    content: [{ type: "text", text: "leave me" }],
  };
  agent.inbox.nextStep.push(foreign);
  await assert.rejects(
    f.bridge.control(request("native-main", { runId: "stale" }), {
      operation: "steer",
      content: "wrong",
    }),
    (error) => error.rpcRejected === true && /active|run|target/i.test(error.message),
  );
  assert.deepEqual(await f.bridge.control(request(), { operation: "steer", content: "change" }), {
    status: "queued",
  });
  assert.equal(agent.inbox.nextStep[1].content[0].text, "change");
  const queue = f.events.findLast((event) => event.type === "input.queue");
  assert.deepEqual(queue.data.steering, ["leave me", "change"]);
  agent.finish();
  await running;
  assert.deepEqual(agent.inbox.nextStep, [foreign]);
  const finalQueue = f.events.findLast((event) => event.type === "input.queue");
  assert.deepEqual(finalQueue.data.discardedSteering, ["change"]);
  assert.deepEqual(finalQueue.data.steering, ["leave me"]);
  assert.equal(finalQueue.data.reason, "run_settled");
});

void test("dsh plugin disposal stops owned work, removes observers, and never disposes foreign agents", async (t) => {
  const f = await setup(t);
  const running = f.run();
  const settled = assert.rejects(running, /aborted/);
  const agent = await f.ready();
  f.makeAgent("foreign");
  await f.bridge.close();
  await f.bridge.close();
  await settled;
  assert.deepEqual(agent.cancelCause, { kind: "disposed" });
  assert.deepEqual(f.disposed, ["native-main"]);
  assert.ok(f.agents.has("foreign"));
  assert.equal(
    [...f.listeners.values()].reduce((sum, set) => sum + set.size, 0),
    0,
  );
});

void test("dsh restore retains host configuration, reasoning, native failures and follow-up cleanup", async (t) => {
  const f = await setup(t);
  const running = f.run(request("restored", { resume: true }));
  const agent = await f.ready();
  assert.deepEqual(f.resumes[0].agentOptions, { provider: "fixture", model: "fixture" });
  await f.bridge.control(request("restored"), { operation: "follow_up", content: "next task" });
  agent.frame(frame("start"));
  agent.frame(
    frame("chunk", {
      index: 0,
      time: 1,
      chunk: { type: "reasoning-delta", index: 0, text: "thinking" },
    }),
  );
  const seq = agent.event("assistant/message", {
    turn: 1,
    step: 1,
    message: {
      id: "durable",
      role: "assistant",
      content: [
        { type: "reasoning", text: "final reasoning" },
        { type: "text", text: "answer" },
      ],
    },
  });
  agent.frame(
    frame("end", { index: 1, outcome: { kind: "committed", eventType: "assistant/message", seq } }),
  );
  agent.event("tool/call", { callId: "raw", name: "inspect", arguments: "invalid json" });
  agent.event("tool/result", {
    message: { toolCallId: "raw", content: [{ type: "text", text: "failed" }], isError: true },
  });
  f.emit("agent/error", { agent, error: new Error("transient handled failure") });
  agent.finish();
  await running;
  assert.equal(f.events.find((event) => event.type === "reasoning.delta").data.delta, "thinking");
  assert.equal(
    f.events.findLast((event) => event.type === "reasoning.delta").data.delta,
    "final reasoning",
  );
  assert.equal(f.events.filter((event) => event.type === "reasoning.completed").length, 1);
  assert.deepEqual(f.events.find((event) => event.type === "tool.started").data.args, {
    rawArguments: "invalid json",
  });
  assert.equal(f.events.find((event) => event.type === "tool.completed").data.isError, true);
  assert.deepEqual(
    f.events.findLast((event) => event.type === "input.queue").data.discardedFollowUp,
    ["next task"],
  );

  const next = f.run(request("restored", { resume: true, runId: "next-run" }));
  await new Promise(setImmediate);
  f.emit("agent/error", { agent, error: new Error("native provider failed") });
  agent.status = "idle";
  agent.finish({ kind: "error", error: { message: "native provider failed" } });
  await assert.rejects(next, /native provider failed/);
  assert.equal(f.resumes.length, 1, "owned live native session must be reused");
});

void test("dsh rejects invalid admission and controls without contaminating native state", async (t) => {
  const f = await setup(t);
  const { createDshBridge } = await import("./index.mjs");
  assert.throws(() => createDshBridge(f.ctx, { provider: "", model: "x" }), /provider/);
  assert.throws(() => createDshBridge(f.ctx, { provider: "x", model: "x" }), /cwd/);
  await assert.rejects(f.run(request("", {})), /requires/);
  await assert.rejects(f.run(request("x", { mentions: ["other"] })), /mentions/);
  const running = f.run();
  const agent = await f.ready();
  await assert.rejects(
    f.run(request("native-main", { conversationId: "other" })),
    /another target/,
  );
  await assert.rejects(f.run(request()), /not available/);
  await assert.rejects(
    f.bridge.control(request(), { operation: "steer", content: "" }),
    (error) => error.rpcRejected === true && /Invalid/.test(error.message),
  );
  agent.steer = () => {
    throw new Error("native control refused");
  };
  await assert.rejects(
    f.bridge.control(request(), { operation: "steer", content: "refused" }),
    /refused/,
  );
  agent.finish({ kind: "error", error: { message: "explicit terminal failure" } });
  await assert.rejects(running, /explicit terminal failure/);
  await f.bridge.close();
  await assert.rejects(f.run(), /closing|requires/);
});

void test("dsh disposes admitted native work when acceptance transport fails", async (t) => {
  const f = await setup(t);
  await assert.rejects(
    f.bridge.run(request(), {
      emit() {
        throw new Error("transport offline");
      },
      onAccepted() {
        throw new Error("transport offline");
      },
    }),
    /transport offline/,
  );
  assert.deepEqual(f.disposed, ["native-main"], "native execution cannot outlive lost observation");
  assert.equal(f.agents.has("native-main"), false);
});

void test("dsh lifecycle abort drains children before waiting for their completion", async (t) => {
  const f = await setup(t);
  const abort = new AbortController();
  let releaseChild;
  const child = new Promise((resolve) => {
    releaseChild = resolve;
  });
  let drained = 0;
  f.ctx.subagents.waitForChildren = async () => {
    await child;
    return false;
  };
  f.ctx.subagents.drainDescendants = async () => {
    drained++;
    releaseChild();
  };
  const running = f.bridge.run(request(), { emit() {}, onAccepted() {}, signal: abort.signal });
  const agent = await f.ready();
  agent.finish();
  await new Promise(setImmediate);
  abort.abort();
  let timer;
  try {
    await assert.rejects(
      Promise.race([
        running,
        new Promise((resolve, reject) => {
          timer = setTimeout(() => reject(new Error("blocked on children")), 100);
        }),
      ]),
      /aborted/,
    );
    assert.equal(drained, 1);
  } finally {
    clearTimeout(timer);
    releaseChild();
    await running.catch(() => {});
  }
});

void test("dsh plugin lifecycle publishes notify and aborts connections without gating host startup", async (t) => {
  const { apply } = await import("./index.mjs");
  const client = require("../client.cjs");
  const original = client.connectPlugin;
  const envName = "PUSH_CHANNEL_DSH_TEST_TOKEN";
  const oldToken = process.env[envName];
  process.env[envName] = "fixture-only";
  t.after(() => {
    client.connectPlugin = original;
    if (oldToken === undefined) {
      delete process.env[envName];
    } else {
      process.env[envName] = oldToken;
    }
  });
  const config = {
    url: "ws://127.0.0.1:1/plugins",
    runtimeId: "dsh",
    agentId: "main",
    tokenEnv: envName,
    provider: "fixture",
    model: "fixture",
    cwd: "/tmp",
  };
  assert.throws(() => apply({}, {}), /requires/);
  assert.throws(
    () => apply({}, { ...config, tokenEnv: "PUSH_CHANNEL_MISSING_TEST_TOKEN" }),
    /missing/,
  );
  for (const mode of ["connected", "offline", "pending"]) {
    const native = host();
    const warnings = [];
    const services = new Map();
    let cleanup;
    let connectionOptions;
    let closed = 0;
    let notifications = 0;
    native.ctx.logger = { warn: (message) => warnings.push(message) };
    native.ctx.provide = (key, value) => services.set(key, value);
    native.ctx.effect = (effect) => {
      cleanup = effect();
    };
    client.connectPlugin = (options) => {
      connectionOptions = options;
      if (mode === "offline") {
        return Promise.reject(new Error("connection refused"));
      }
      if (mode === "pending") {
        return new Promise((resolve, reject) => {
          options.signal.addEventListener("abort", () => reject(new Error("aborted")), {
            once: true,
          });
        });
      }
      return Promise.resolve({
        close: async () => {
          closed++;
        },
        notify: async (target, notification) => {
          notifications++;
          return { target, notification };
        },
      });
    };
    assert.equal(apply(native.ctx, config), undefined, "host startup must not wait for a gateway");
    const service = services.get("pushChannel");
    await assert.rejects(service.notify({}, {}), /offline/);
    await new Promise(setImmediate);
    if (mode === "connected") {
      const target = { clientId: "owner", conversationId: "chat" };
      assert.deepEqual(await service.notify(target, { eventId: "notice", content: "hello" }), {
        target,
        notification: { eventId: "notice", content: "hello" },
      });
      assert.equal(notifications, 1);
    } else if (mode === "offline") {
      assert.match(warnings[0], /offline.*refused/);
    }
    await cleanup();
    assert.equal(connectionOptions.signal.aborted, true);
    assert.equal(closed, mode === "connected" ? 1 : 0);
    await assert.rejects(service.notify({}, {}), /offline/);
    assert.equal(
      [...native.listeners.values()].every((listeners) => listeners.size === 0),
      true,
    );
    assert.equal(native.creates.length, 0);
  }
});

void test("dsh observes resident child activations across parent runs without adopting foreign lineage", async (t) => {
  const f = await setup(t);
  const first = f.run();
  const root = await f.ready();
  const child = f.makeAgent("resident", root);
  const activation = { runId: "resident-activation", provider: "spawn", id: child.id, local: true };
  f.emit("subagent/start", activation);
  const replaced = f.makeAgent("replaced-child", root);
  f.emit("subagent/start", { ...activation, id: replaced.id, runId: "replaced-activation" });
  const ended = f.makeAgent("ended-between-runs", root);
  const endedInfo = { ...activation, id: ended.id, runId: "ended-activation" };
  f.emit("subagent/start", endedInfo);
  child.frame(frame("start"));
  child.frame(
    frame("chunk", {
      index: 0,
      time: 1,
      chunk: { type: "text-delta", index: 0, text: "first child turn" },
    }),
  );
  settle(child, "first child turn");
  child.inbox.nextStep.push({
    id: "parked",
    role: "user",
    source: { kind: "user" },
    content: [{ type: "text", text: "parked during native interrupt" }],
  });
  root.finish();
  await first;
  assert.equal(child.status, "idle");
  assert.ok(f.agents.has(child.id), "idle native inbox can keep a child activation resident");
  const firstEventCount = f.events.length;
  f.agents.delete(ended.id);
  f.owners.delete(ended.id);
  f.emit("subagent/end", { ...endedInfo, stopReason: "completed" });
  // Same id and direct parent do not transfer an old activation to a new Agent.
  f.agents.delete(replaced.id);
  const replacement = f.makeAgent(replaced.id, root);
  const foreign = f.makeAgent("foreign-resident", undefined, undefined, root);
  f.emit("subagent/start", { ...activation, id: foreign.id, runId: "foreign-activation" });
  foreign.frame(frame("start"));
  foreign.frame(
    frame("chunk", { index: 0, time: 1, chunk: { type: "text-delta", index: 0, text: "PRIVATE" } }),
  );
  child.event("tool/call", { callId: "between-runs", name: "private-idle-tool", arguments: "{}" });
  assert.equal(
    f.events.length,
    firstEventCount,
    "no child signals may append to a settled parent run",
  );

  const secondEvents = [];
  let accepted = false;
  const second = f.bridge.run(request("native-main", { runId: "second-parent-run" }), {
    emit(event) {
      assert.equal(accepted, true);
      secondEvents.push(validateAdapterSignal(event));
    },
    onAccepted() {
      accepted = true;
    },
  });
  const secondSettled = second.catch(() => {});
  t.after(async () => {
    await f.bridge.close();
    await secondSettled;
  });
  await new Promise(setImmediate);
  assert.equal(f.inputs.length, 2);
  assert.equal(
    secondEvents.filter((event) => event.type === "subagent.started").length,
    1,
    "a retained native activation needs a fresh projection in the next parent run",
  );
  // No native subagent/start is re-emitted when sendMessage wakes a resident.
  // A duplicate from the old observation epoch must not become new-run output.
  f.emit("agent/assistant-stream", {
    agent: child,
    frame: {
      ...frame("chunk", {
        index: 0,
        time: 1,
        chunk: { type: "text-delta", index: 0, text: "OLD EPOCH" },
      }),
      revision: 2,
    },
  });
  replacement.frame(frame("start"));
  replacement.frame(
    frame("chunk", {
      index: 0,
      time: 1,
      chunk: { type: "text-delta", index: 0, text: "REPLACEMENT PRIVATE" },
    }),
  );
  child.inbox.nextStep.length = 0;
  child.frame(frame("start", { attemptId: "resident-second", turn: 2 }));
  child.frame(
    frame("chunk", {
      attemptId: "resident-second",
      index: 0,
      time: 2,
      chunk: { type: "text-delta", index: 0, text: "second child turn" },
    }),
  );
  child.event("tool/call", {
    callId: "resident-read",
    name: "read",
    arguments: '{"path":"test.txt"}',
  });
  child.event("tool/result", {
    message: { toolCallId: "resident-read", content: [{ type: "text", text: "resident file" }] },
  });
  settle(child, "second child final", "resident-second", "second-durable", 2);
  f.agents.delete(child.id);
  f.owners.delete(child.id);
  f.emit("subagent/end", {
    ...activation,
    stopReason: "completed",
    lastAssistantMessage: [{ type: "text", text: "second child final" }],
  });

  const parked = f.makeAgent("parked-at-unload", root);
  f.emit("subagent/start", { ...activation, id: parked.id, runId: "parked-activation" });
  parked.inbox.nextTurn.push({
    id: "parked-next",
    role: "user",
    source: { kind: "user" },
    content: [{ type: "text", text: "parked" }],
  });
  root.finish();
  await second;
  assert.ok(
    secondEvents.some(
      (event) => event.type === "subagent.delta" && event.data.delta === "second child turn",
    ),
  );
  assert.ok(
    secondEvents.some(
      (event) => event.type === "subagent.tool.completed" && event.data.content === "resident file",
    ),
  );
  assert.ok(
    secondEvents.some(
      (event) => event.type === "subagent.result" && event.data.content === "second child final",
    ),
  );
  assert.equal(
    secondEvents.filter(
      (event) => event.type === "subagent.ended" && event.data.childSessionId === child.id,
    ).length,
    1,
  );
  assert.equal(JSON.stringify(secondEvents).includes("PRIVATE"), false);
  assert.equal(JSON.stringify(secondEvents).includes("OLD EPOCH"), false);
  assert.equal(f.events.length, firstEventCount);
  f.ctx.subagents.drainDescendants = async (parents) => {
    assert.deepEqual(parents, [root]);
    f.agents.delete(parked.id);
    f.owners.delete(parked.id);
    f.emit("subagent/end", {
      ...activation,
      id: parked.id,
      runId: "parked-activation",
      stopReason: "aborted",
    });
  };
  await f.bridge.close();
  assert.deepEqual([...f.agents.keys()], [replacement.id, foreign.id]);
  assert.equal(
    [...f.listeners.values()].every((listeners) => listeners.size === 0),
    true,
  );
});
