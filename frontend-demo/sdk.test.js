import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

function createClient(options) {
  const sent = [];
  class Socket {
    static OPEN = 1;
    readyState = 1;
    listeners = new Map();
    addEventListener(type, callback) {
      this.listeners.set(type, callback);
    }
    send(value) {
      sent.push(JSON.parse(value));
    }
    close() {
      this.readyState = 3;
      this.listeners.get("close")?.();
    }
  }
  const source = readFileSync(new URL("./sdk.js", import.meta.url), "utf8");
  const SDK = vm.runInNewContext(
    `${source}\nPushChannelSDK`,
    {
      WebSocket: Socket,
      URL,
      console,
      setInterval: () => 1,
      clearInterval() {},
    },
    { filename: new URL("./sdk.js", import.meta.url).pathname },
  );
  const client = new SDK("http://localhost:3001", "main", options);
  client.connect();
  client.socket.listeners.get("open")();
  return { client, sent };
}

void test("SDK keeps legacy callbacks and registration by default", () => {
  const { client, sent } = createClient({ sessionId: "chat" });
  assert.deepEqual(sent[0], { type: "register", agentId: "main", sessionId: "chat" });
  let text;
  client.onStreamChunk((event) => {
    text = event.content;
  });
  client.socket.listeners.get("message")({
    data: JSON.stringify({ type: "stream", content: "hello" }),
  });
  assert.equal(text, "hello");
});

void test("SDK selects runtimes per session, never during connection registration", () => {
  const { client, sent } = createClient({
    sessionId: "chat",
    runtimeId: "custom",
    protocolVersion: 1,
  });
  assert.equal(sent[0].runtimeId, undefined);
  assert.equal(sent[0].protocolVersion, 1);
  let received;
  client.onEvent((event) => {
    received = event;
  });
  const event = { version: 1, type: "message.delta", data: { messageId: "m", delta: "hi" } };
  client.socket.listeners.get("message")({ data: JSON.stringify(event) });
  assert.deepEqual(structuredClone(received), event);
  client.startSession("first", { runtimeId: "openclaw", agentId: "main" });
  client.startSession("second", { runtimeId: "pi", agentId: "coding" });
  assert.deepEqual(sent.slice(1), [
    { type: "session.start", sessionId: "first", runtimeId: "openclaw", agentId: "main" },
    { type: "session.start", sessionId: "second", runtimeId: "pi", agentId: "coding" },
  ]);
});

void test("SDK acknowledges sessions, sends session-scoped messages, and cancels", () => {
  const { client, sent } = createClient();
  const started = [];
  const cancelled = [];
  client.onSessionStarted((data) => started.push(data));
  client.onRunCancelled((data) => cancelled.push(data));
  client.startSession("chat", { runtimeId: "pi", agentId: "main" });
  const ack = {
    type: "session.started",
    sessionId: "chat",
    runtimeId: "pi",
    agentId: "main",
    capabilities: { cancel: true },
  };
  client.socket.listeners.get("message")({ data: JSON.stringify(ack) });
  client.sendMessage("hello", "chat", { queryMessageId: "q" });
  client.cancel("chat");
  client.socket.listeners.get("message")({
    data: JSON.stringify({ type: "run_cancelled", sessionId: "chat" }),
  });
  assert.deepEqual(structuredClone(started), [ack]);
  assert.deepEqual(sent.at(-2), {
    type: "message",
    content: "hello",
    sessionId: "chat",
    queryMessageId: "q",
  });
  assert.deepEqual(sent.at(-1), { type: "cancel", sessionId: "chat" });
  assert.equal(cancelled.length, 1);
  assert.throws(
    () => client.startSession("chat", { runtimeId: "openclaw", agentId: "main" }),
    /immutable|different/i,
  );
});

void test("SDK restores acknowledged subscriptions on reconnect without moving runtime selection to register", () => {
  const { client, sent } = createClient();
  client.startSession("chat", { runtimeId: "pi", agentId: "main" });
  client.socket.listeners.get("message")({
    data: JSON.stringify({
      type: "session.started",
      sessionId: "chat",
      runtimeId: "pi",
      agentId: "main",
      capabilities: {},
    }),
  });
  client.disconnect();
  client.connect();
  client.socket.listeners.get("open")();
  assert.equal(sent.at(-1).type, "register");
  client.socket.listeners.get("message")({
    data: JSON.stringify({ type: "registered", agentId: "main", runtimes: [] }),
  });
  assert.deepEqual(sent.at(-1), {
    type: "session.start",
    sessionId: "chat",
    runtimeId: "pi",
    agentId: "main",
  });
});

void test("SDK canonical cancellation invokes both canonical and cancellation callbacks", () => {
  const { client } = createClient({ protocolVersion: 1 });
  let event;
  let cancelled;
  client.onEvent((value) => {
    event = value;
  });
  client.onRunCancelled((value) => {
    cancelled = value;
  });
  const value = { version: 1, type: "run.cancelled", conversationId: "chat", data: {} };
  client.socket.listeners.get("message")({ data: JSON.stringify(value) });
  assert.deepEqual(structuredClone(event), value);
  assert.deepEqual(structuredClone(cancelled), value);
});

void test("SDK restores first-message sessions only after their binding is acknowledged", () => {
  const { client, sent } = createClient();
  const emit = (data) => client.socket.listeners.get("message")({ data: JSON.stringify(data) });
  client.sendMessage("hello", "implicit", { runtimeId: "pi", agentId: "native" });
  emit({
    type: "session.started",
    sessionId: "implicit",
    runtimeId: "pi",
    agentId: "native",
    capabilities: {},
  });
  client.sendMessage("rejected", "denied", { runtimeId: "missing", agentId: "native" });
  emit({ type: "error", sessionId: "denied", message: "Unknown runtime" });
  client.disconnect();
  client.connect();
  client.socket.listeners.get("open")();
  const registerIndex = sent.length - 1;
  emit({ type: "registered", runtimes: [] });
  assert.deepEqual(sent.slice(registerIndex + 1), [
    { type: "session.start", sessionId: "implicit", runtimeId: "pi", agentId: "native" },
  ]);
});

void test("SDK exposes complete run lifecycle independently of stream-part boundaries", () => {
  const { client } = createClient();
  const lifecycle = [];
  client.onRunStarted((value) => lifecycle.push(value.type));
  client.onRunCompleted((value) => lifecycle.push(value.type));
  for (const type of ["run_started", "stream_end", "run_completed"]) {
    client.socket.listeners.get("message")({ data: JSON.stringify({ type, sessionId: "chat" }) });
  }
  assert.deepEqual(lifecycle, ["run_started", "run_completed"]);
});

void test("SDK ignores messages from a replaced connection", () => {
  const { client } = createClient();
  const previousSocket = client.socket;
  let messages = 0;
  client.onMessage(() => {
    messages += 1;
  });
  client.connect();
  previousSocket.listeners.get("message")({
    data: JSON.stringify({ type: "message", content: "old connection" }),
  });
  assert.equal(messages, 0);
});

void test("SDK deduplicates legacy replay while keeping multiple projections at one cursor", () => {
  const { client, sent } = createClient();
  const received = [];
  client.onMessage((data) => received.push(data.content));
  const emit = (data) => client.socket.listeners.get("message")({ data: JSON.stringify(data) });
  const first = {
    type: "message",
    sessionId: "chat",
    eventId: "e",
    cursor: 10,
    deliveryId: "e:0",
    content: "first",
  };
  emit(first);
  emit(first);
  emit({ ...first, deliveryId: "e:1", content: "second" });
  emit({ ...first, cursor: 9, deliveryId: "old:0", content: "stale" });
  emit({ ...first, sessionId: "other", content: "other session" });
  assert.deepEqual(received, ["first", "second", "other session"]);
  assert.equal(client.getCursor("chat"), 10);
  client.startSession("chat", { runtimeId: "pi", agentId: "main" });
  assert.equal(sent.at(-1).afterCursor, 10);
});

void test("SDK canonical replay isolates agent-wide cursors from conversation cursors", () => {
  const { client, sent } = createClient({ protocolVersion: 1 });
  const received = [];
  client.onEvent((data) => received.push(data.eventId));
  const emit = (data) => client.socket.listeners.get("message")({ data: JSON.stringify(data) });
  const event = {
    version: 1,
    type: "message.delta",
    eventId: "prompt",
    conversationId: "chat",
    audience: "conversation",
    cursor: 2,
    data: { messageId: "m", delta: "hi" },
  };
  emit(event);
  emit(event);
  emit({ ...event, eventId: "push", audience: "agent", cursor: 100 });
  emit({ ...event, eventId: "next", cursor: 3 });
  assert.deepEqual(received, ["prompt", "push", "next"]);
  assert.equal(client.getCursor("chat"), 3);
  assert.equal(client.getCursor(""), 100);
  client.startSession("chat", { runtimeId: "pi", agentId: "main" });
  emit({
    type: "session.started",
    sessionId: "chat",
    runtimeId: "pi",
    agentId: "main",
    capabilities: {},
  });
  client.disconnect();
  client.connect();
  client.socket.listeners.get("open")();
  emit({ type: "registered", runtimes: [] });
  assert.deepEqual(sent.at(-1), {
    type: "session.start",
    sessionId: "chat",
    runtimeId: "pi",
    agentId: "main",
    afterCursor: 3,
  });
});

void test("SDK sends an optional token only during registration", () => {
  const { client, sent } = createClient({ token: "test-credential" });
  assert.equal(sent[0].token, "test-credential");
  client.startSession("chat", { runtimeId: "pi", agentId: "main" });
  client.sendMessage("hello", "chat", { queryMessageId: "q" });
  client.cancel("chat");
  assert.equal(
    sent.slice(1).some((message) => Object.hasOwn(message, "token")),
    false,
  );
});

void test("SDK retries a rejected session ID with a corrected binding", () => {
  const { client, sent } = createClient();
  client.startSession("chat", { runtimeId: "unknown", agentId: "forbidden" });
  client.socket.listeners.get("message")({
    data: JSON.stringify({ type: "error", sessionId: "chat", message: "Unknown runtime" }),
  });
  assert.doesNotThrow(() => client.startSession("chat", { runtimeId: "pi", agentId: "main" }));
  assert.deepEqual(sent.at(-1), {
    type: "session.start",
    sessionId: "chat",
    runtimeId: "pi",
    agentId: "main",
  });
  client.socket.listeners.get("message")({
    data: JSON.stringify({
      type: "session.started",
      sessionId: "chat",
      runtimeId: "pi",
      agentId: "main",
      capabilities: {},
    }),
  });
  assert.throws(
    () => client.startSession("chat", { runtimeId: "openclaw", agentId: "main" }),
    /immutable/i,
  );
});

void test("SDK never resubscribes a session that was rejected or never acknowledged", () => {
  const { client, sent } = createClient();
  client.startSession("rejected", { runtimeId: "unknown", agentId: "forbidden" });
  client.socket.listeners.get("message")({
    data: JSON.stringify({ type: "error", sessionId: "rejected", message: "Unknown runtime" }),
  });
  client.startSession("pending", { runtimeId: "pi", agentId: "main" });
  client.disconnect();
  client.connect();
  client.socket.listeners.get("open")();
  client.socket.listeners.get("message")({
    data: JSON.stringify({ type: "registered", runtimes: [] }),
  });
  assert.equal(sent.at(-1).type, "register");
});

void test("SDK inclusive resume delivers a missing projection but not its already-rendered sibling", () => {
  const { client, sent } = createClient();
  const received = [];
  client.onMessage((data) => received.push(data.content));
  const emit = (data) => client.socket.listeners.get("message")({ data: JSON.stringify(data) });
  const event = {
    type: "message",
    sessionId: "chat",
    cursor: 4,
    eventId: "e",
    deliveryId: "e:0",
    content: "before disconnect",
  };
  emit(event);
  client.startSession("chat", { runtimeId: "pi", agentId: "main" });
  emit({
    type: "session.started",
    sessionId: "chat",
    runtimeId: "pi",
    agentId: "main",
    capabilities: {},
  });
  client.disconnect();
  client.connect();
  client.socket.listeners.get("open")();
  emit({ type: "registered", runtimes: [] });
  assert.equal(sent.at(-1).afterCursor, 4);
  emit(event);
  emit({ ...event, deliveryId: "e:1", content: "missing sibling" });
  assert.deepEqual(received, ["before disconnect", "missing sibling"]);
});

void test("SDK sends native controls to an existing run with separate control identities", () => {
  const { client, sent } = createClient();
  for (const [operation, method] of [
    ["steer", "steer"],
    ["follow_up", "followUp"],
  ]) {
    const controlId = client[method]("继续检查边界", {
      sessionId: "chat",
      runId: "run-1",
      controlId: `control-${operation}`,
    });
    assert.equal(controlId, `control-${operation}`);
    assert.deepEqual(sent.at(-1), {
      type: "input.control",
      sessionId: "chat",
      runId: "run-1",
      controlId,
      operation,
      content: "继续检查边界",
    });
    assert.equal(sent.at(-1).queryMessageId, undefined);
    assert.equal(sent.at(-1).answerMessageId, undefined);
  }
  const first = client.steer("one", { sessionId: "chat", runId: "run-1" });
  const second = client.followUp("two", { sessionId: "chat", runId: "run-1" });
  assert.equal(typeof first, "string");
  assert.notEqual(first, second);
});

void test("SDK validates native control targets and never retries controls on reconnect", () => {
  const { client, sent } = createClient();
  for (const options of [{ runId: "run" }, { sessionId: "chat" }]) {
    assert.throws(() => client.steer("text", options), /session|run/i);
  }
  assert.throws(() => client.followUp("  ", { sessionId: "chat", runId: "run" }), /content|text/i);
  client.steer("once", { sessionId: "chat", runId: "run", controlId: "one" });
  client.disconnect();
  const count = sent.length;
  assert.equal(client.followUp("offline", { sessionId: "chat", runId: "run" }), false);
  assert.equal(sent.length, count);
  client.connect();
  client.socket.listeners.get("open")();
  client.socket.listeners.get("message")({
    data: JSON.stringify({ type: "registered", runtimes: [] }),
  });
  assert.equal(sent.filter((frame) => frame.type === "input.control").length, 1);
});

for (const canonical of [false, true]) {
  void test(`SDK exposes ${canonical ? "canonical" : "legacy"} control receipts and authoritative queues`, () => {
    const { client } = createClient(canonical ? { protocolVersion: 1 } : {});
    const receipts = [];
    const queues = [];
    const errors = [];
    client.onControlReceipt((data) => receipts.push(structuredClone(data)));
    client.onInputQueue((data) => queues.push(structuredClone(data)));
    client.onError((data) => errors.push(data));
    const receiptData = { controlId: "c", operation: "steer", content: "hint", status: "queued" };
    const queueData = { steering: ["hint"], followUp: [] };
    const envelope = { runId: "run", eventId: "receipt", cursor: 1 };
    const receipt = canonical
      ? {
          ...envelope,
          version: 1,
          type: "input.control",
          conversationId: "chat",
          data: receiptData,
        }
      : {
          ...envelope,
          type: "input_control",
          sessionId: "chat",
          deliveryId: "receipt:0",
          ...receiptData,
        };
    const queue = canonical
      ? {
          ...envelope,
          version: 1,
          type: "input.queue",
          eventId: "queue",
          cursor: 2,
          conversationId: "chat",
          data: queueData,
        }
      : {
          ...envelope,
          type: "input_queue",
          sessionId: "chat",
          cursor: 2,
          deliveryId: "queue:0",
          ...queueData,
        };
    for (const frame of [receipt, receipt, queue, queue]) {
      client.socket.listeners.get("message")({ data: JSON.stringify(frame) });
    }
    const rejection = {
      type: "input_control",
      sessionId: "chat",
      runId: "old-run",
      controlId: "late",
      status: "rejected",
      message: "Run is no longer active",
    };
    client.socket.listeners.get("message")({ data: JSON.stringify(rejection) });
    assert.deepEqual(receipts, [receipt, rejection]);
    assert.deepEqual(queues, [queue]);
    assert.deepEqual(errors, []);
  });
}
