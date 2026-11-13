const assert = require("node:assert/strict");
const test = require("node:test");
const { PushGateway } = require("./gateway");
const { validateEvent } = require("../../../protocol/index.cjs");

void test("accepted user input is durable and deduplicated with the execution", async () => {
  let calls = 0;
  const gateway = new PushGateway({
    adapters: [
      {
        id: "test",
        async run() {
          calls += 1;
        },
      },
    ],
    publish() {},
  });
  const request = {
    clientId: "client",
    runtimeId: "test",
    agentId: "native",
    conversationId: "chat",
    requestId: "one",
    content: "remember this",
  };
  await gateway.run(request);
  await gateway.run(request);
  assert.equal(calls, 1);
  const inputs = gateway.store
    .events("client", "chat")
    .filter((event) => event.type === "input.accepted");
  assert.equal(inputs.length, 1);
  assert.equal(inputs[0].data.content, request.content);
  await gateway.close();
});

void test("gateway drives an injected runtime through the versioned event contract", async () => {
  const events = [];
  const gateway = new PushGateway({
    adapters: [
      {
        id: "test-runtime",
        capabilities: { streaming: true },
        async run(request, { emit }) {
          assert.equal(request.conversationId, "chat-1");
          emit({ type: "message.started", data: { messageId: "answer" } });
          emit({ type: "message.delta", data: { messageId: "answer", delta: "hello" } });
          emit({ type: "message.completed", data: { messageId: "answer" } });
        },
      },
    ],
    publish: (event) => events.push(event),
  });
  await gateway.run({
    runtimeId: "test-runtime",
    agentId: "main",
    conversationId: "chat-1",
    content: "hi",
  });
  assert.deepEqual(
    events.map((event) => event.type),
    [
      "input.accepted",
      "run.started",
      "message.started",
      "message.delta",
      "message.completed",
      "run.completed",
    ],
  );
  assert.deepEqual(
    events.map((event) => event.seq),
    [1, 2, 3, 4, 5, 6],
  );
  assert.equal(new Set(events.map((event) => event.runId)).size, 1);
  assert.equal(new Set(events.map((event) => event.eventId)).size, events.length);
  for (const event of events) {
    validateEvent(event);
  }
});

void test("gateway serializes a conversation through completion, but isolates runtimes", async () => {
  let release;
  const firstFinished = new Promise((resolve) => {
    release = resolve;
  });
  const calls = [];
  const adapters = ["one", "two"].map((id) => ({
    id,
    async run(request) {
      calls.push(`${id}:${request.content}`);
      if (request.content === "first") {
        await firstFinished;
      }
    },
  }));
  const gateway = new PushGateway({ adapters, publish() {} });
  const request = { runtimeId: "one", agentId: "main", conversationId: "same", content: "first" };
  const first = gateway.run(request);
  const second = gateway.run({ ...request, content: "second" });
  await gateway.run({ ...request, runtimeId: "two", content: "independent" });
  assert.deepEqual(calls, ["one:first", "two:independent"]);
  release();
  await Promise.all([first, second]);
  assert.deepEqual(calls, ["one:first", "two:independent", "one:second"]);
});

void test("adapter failures produce one failed terminal event and release the queue", async () => {
  const events = [];
  const gateway = new PushGateway({
    adapters: [
      {
        id: "test",
        async run() {
          throw new Error("upstream closed");
        },
      },
    ],
    publish: (event) => events.push(event),
  });
  const request = { runtimeId: "test", agentId: "main", conversationId: "chat", content: "hi" };
  await gateway.run(request);
  await gateway.run(request);
  assert.deepEqual(
    events.map((event) => event.type),
    ["input.accepted", "run.started", "run.failed", "input.accepted", "run.started", "run.failed"],
  );
  assert.match(events[2].data.message, /upstream closed/);
  assert.equal(gateway.queues.size, 0);
});

void test("protocol rejects invalid envelopes and event payloads", () => {
  assert.throws(() => validateEvent({ version: 99, type: "run.completed" }), /protocol/i);
  const { createEventWriter } = require("../../../protocol/index.cjs");
  const emitInvalidPayload = createEventWriter(
    { runtimeId: "test", agentId: "main", conversationId: "chat", runId: "run" },
    () => {},
  );
  assert.throws(
    () => emitInvalidPayload({ type: "message.delta", data: { messageId: "m" } }),
    /delta/,
  );
  assert.throws(() => emitInvalidPayload({ type: "unknown", data: {} }), /event/i);
});

void test("protocol validates optional fields and keeps run lifecycle gateway-owned", async () => {
  const { validateSignal } = require("../../../protocol/index.cjs");
  for (const signal of [
    { type: "tool.completed", data: { toolCallId: "t", isError: "false" } },
    { type: "tool.completed", data: { toolCallId: "t", toolName: 42 } },
    { type: "subagent.delta", data: { agentId: "child", childSessionId: 7 } },
    { type: "subagent.delta", data: { agentId: "child", delta: [] } },
    { type: "reasoning.delta", data: { delta: "text", replace: "false" } },
  ]) {
    assert.throws(() => validateSignal(signal), /Invalid/);
  }
  const { createEventWriter } = require("../../../protocol/index.cjs");
  const emitInvalidRequest = createEventWriter(
    { runtimeId: "test", agentId: "main", conversationId: "chat", runId: "run", requestId: 123 },
    () => {},
  );
  assert.throws(() => emitInvalidRequest({ type: "run.started", data: {} }), /requestId/);
  const events = [];
  const gateway = new PushGateway({
    adapters: [
      {
        id: "test",
        async run(request, { emit }) {
          emit({ type: "run.completed", data: {} });
        },
      },
    ],
    publish: (event) => events.push(event),
  });
  await gateway.run({ runtimeId: "test", agentId: "main", conversationId: "chat", content: "hi" });
  assert.equal(events.at(-1).type, "run.failed");
});

void test("active native controls keep the original run and durable request identity", async (t) => {
  let finish;
  const calls = [];
  const events = [];
  const gateway = new PushGateway({
    adapters: [
      {
        id: "pi",
        capabilities: { steering: true, followUp: true },
        async run(request) {
          calls.push(["run", request.content]);
          await new Promise((resolve) => {
            finish = resolve;
          });
        },
        async control(request, control) {
          calls.push([control.operation, control.content, request.runId]);
          return { status: "queued" };
        },
      },
    ],
    publish: (event) => events.push(event),
  });
  t.after(() => {
    finish?.();
    return gateway.close();
  });
  const session = gateway.startSession({
    clientId: "owner",
    conversationId: "chat",
    runtimeId: "pi",
    agentId: "main",
  });
  const first = gateway.submit({
    ...session,
    content: "original",
    requestId: "root-query",
    replyId: "root-answer",
  });
  await new Promise(setImmediate);
  const control = {
    ...session,
    runId: first.receipt.runId,
    controlId: "control-one",
    operation: "steer",
    content: "adjust",
  };
  assert.equal((await gateway.control(control)).status, "queued");
  assert.equal((await gateway.control(control)).status, "queued");
  await assert.rejects(gateway.control({ ...control, content: "different" }), /conflict/i);
  await assert.rejects(
    gateway.control({ ...control, controlId: "stale", runId: "wrong-run" }),
    /active run/i,
  );
  await assert.rejects(
    gateway.control({ ...control, clientId: "other", controlId: "foreign" }),
    /session|owner/i,
  );
  assert.deepEqual(calls, [
    ["run", "original"],
    ["steer", "adjust", first.receipt.runId],
  ]);
  const receipts = events.filter((event) => event.type === "input.control");
  assert.deepEqual(
    receipts.map((event) => event.data.status),
    ["dispatching", "queued"],
  );
  assert.ok(
    receipts.every(
      (event) =>
        event.runId === first.receipt.runId &&
        event.requestId === "root-query" &&
        event.replyId === "root-answer",
    ),
  );
  finish();
  await first.completion;
  assert.equal((await gateway.control(control)).status, "queued");
  await assert.rejects(gateway.control({ ...control, controlId: "late" }), /active run/i);
  assert.equal(events.filter((event) => event.type === "input.accepted").length, 1);
  assert.equal(events.filter((event) => event.type === "run.completed").length, 1);
});

void test("native controls reject unsupported runtimes without admitting a run", async (t) => {
  const gateway = new PushGateway({ adapters: [{ id: "openclaw", async run() {} }], publish() {} });
  t.after(() => gateway.close());
  const session = gateway.startSession({
    clientId: "owner",
    conversationId: "chat",
    runtimeId: "openclaw",
    agentId: "main",
  });
  await assert.rejects(
    gateway.control({
      ...session,
      runId: "run",
      controlId: "control",
      operation: "follow_up",
      content: "later",
    }),
    /support/i,
  );
  assert.equal(gateway.capabilities("openclaw").steering, false);
  assert.equal(gateway.capabilities("openclaw").followUp, false);
  assert.deepEqual(gateway.store.events("owner", "chat"), []);
});

void test("native control rejection and lost receipts do not invent a parent failure or execution", async (t) => {
  let finish;
  const events = [];
  const gateway = new PushGateway({
    adapters: [
      {
        id: "pi",
        capabilities: { steering: true },
        run() {
          return new Promise((resolve) => {
            finish = resolve;
          });
        },
        async control(request, input) {
          if (input.content === "handled") return { status: "handled" };
          throw Object.assign(new Error("Control receipt unavailable"), {
            rpcRejected: input.content === "rejected",
          });
        },
      },
    ],
    publish: (event) => events.push(event),
  });
  t.after(() => {
    finish?.();
    return gateway.close();
  });
  const session = gateway.startSession({
    clientId: "owner",
    conversationId: "chat",
    runtimeId: "pi",
    agentId: "main",
  });
  const running = gateway.submit({ ...session, content: "original" });
  await new Promise(setImmediate);
  for (const [content, status] of [
    ["handled", "handled"],
    ["rejected", "rejected"],
    ["lost", "unknown"],
  ]) {
    const receipt = await gateway.control({
      ...session,
      runId: running.receipt.runId,
      controlId: content,
      operation: "steer",
      content,
    });
    assert.equal(receipt.status, status);
  }
  assert.equal(
    events.some((event) => event.type === "run.failed" || event.type === "run.completed"),
    false,
  );
  finish();
  await running.completion;
  assert.equal(events.at(-1).type, "run.completed");
  assert.equal(events.filter((event) => event.type === "input.accepted").length, 1);
});

void test("adapter signals cannot forge gateway control receipts and queue payloads are checked", () => {
  const { validateAdapterSignal, validateSignal } = require("../../../protocol/index.cjs");
  const receipt = {
    type: "input.control",
    data: { controlId: "one", operation: "steer", content: "adjust", status: "queued" },
  };
  assert.equal(validateSignal(receipt), receipt);
  assert.throws(() => validateAdapterSignal(receipt), /gateway/i);
  for (const data of [
    { steering: [1], followUp: [] },
    { steering: [], followUp: [], discardedSteering: [1] },
    { steering: [], followUp: [], reason: "completed" },
  ]) {
    assert.throws(() => validateAdapterSignal({ type: "input.queue", data }), /Invalid/);
  }
});
