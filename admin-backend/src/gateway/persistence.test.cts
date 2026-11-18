const assert = require("node:assert/strict");
const { once } = require("node:events");
const { mkdtempSync, rmSync } = require("node:fs");
const { tmpdir } = require("node:os");
const path = require("node:path");
const test = require("node:test");
const WebSocket = require("ws");
const { createGatewayServer } = require("./server.ts");
const { GatewayStore } = require("./store.ts");
const { createOpenClawAdapter } = require("../adapters/openclaw.ts");

async function launch(databasePath, run, otherAdapters = []) {
  const push = createOpenClawAdapter({ url: "http://127.0.0.1:1" });
  const adapter = { ...push, id: "native", run };
  const server = createGatewayServer({ databasePath, adapters: [...otherAdapters, adapter] });
  await new Promise((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  const base = `http://127.0.0.1:${server.address().port}`;
  const sockets = [];
  return {
    base,
    async connect(clientId, sessionId, { legacy = false } = {}) {
      const socket = new WebSocket(base.replace("http:", "ws:"));
      sockets.push(socket);
      const events = [];
      socket.on("message", (raw) => {
        const data = Array.isArray(raw) ? Buffer.concat(raw) : Buffer.from(raw);
        events.push(JSON.parse(data.toString()));
      });
      await once(socket, "open");
      const send = (data) => socket.send(JSON.stringify(data));
      const wait = async (predicate) => {
        const deadline = Date.now() + 1500;
        while (!events.some(predicate)) {
          if (Date.now() > deadline) {
            throw new Error(`Missing ${predicate}`);
          }
          await new Promise((resolve) => {
            setTimeout(resolve, 5);
          });
        }
        return events.find(predicate);
      };
      send({
        type: "register",
        agentId: clientId ?? "owner",
        protocolVersion: legacy ? undefined : 1,
        sessionId,
      });
      await wait((event) => event.type === "registered");
      return { socket, send, wait, events };
    },
    async close() {
      sockets.forEach((socket) => socket.terminate());
      await new Promise((resolve) => {
        server.close(resolve);
      });
    },
  };
}

void test("agent-wide push resolves the native target to its connected client owner", async (t) => {
  const directory = mkdtempSync(path.join(tmpdir(), "push-agent-wide-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const server = await launch(path.join(directory, "gateway.sqlite"), async () => {});
  t.after(() => server.close());
  const browser = await server.connect("owner");
  const other = await server.connect("other");
  browser.send({
    type: "session.start",
    sessionId: "owner-chat",
    runtimeId: "native",
    agentId: "assistant",
  });
  other.send({
    type: "session.start",
    sessionId: "other-chat",
    runtimeId: "native",
    agentId: "private",
  });
  await browser.wait((event) => event.type === "session.started");
  await other.wait((event) => event.type === "session.started");
  const response = await fetch(`${server.base}/send`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ agentId: "assistant", content: "broadcast" }),
  });
  assert.equal(response.status, 200);
  await browser.wait((event) => event.type === "message.delta" && event.data.delta === "broadcast");
  assert.equal(
    other.events.some((event) => event.type === "message.delta"),
    false,
  );
});

void test("mismatched pushes cannot bypass persisted bindings through legacy reconnect routing", async (t) => {
  const directory = mkdtempSync(path.join(tmpdir(), "push-bound-routing-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const other = { ...createOpenClawAdapter({ url: "http://127.0.0.1:1" }), id: "other" };
  const server = await launch(path.join(directory, "gateway.sqlite"), async () => {}, [other]);
  t.after(() => server.close());
  const first = await server.connect();
  first.send({
    type: "session.start",
    sessionId: "chat",
    runtimeId: "native",
    agentId: "assistant",
  });
  await first.wait((event) => event.type === "session.started");
  first.socket.close();
  await once(first.socket, "close");
  const resumed = await server.connect("owner", "chat");
  const stranger = await server.connect("stranger", "chat");
  for (const target of [
    { agentId: "owner", runtimeId: "other" },
    { agentId: "stranger", clientId: "stranger" },
  ]) {
    const response = await fetch(`${server.base}/send`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sessionId: "chat", content: "must not misdeliver", ...target }),
    });
    assert.equal(response.status, 404);
  }
  assert.equal(
    resumed.events.some((event) => event.origin === "push"),
    false,
  );
  assert.equal(
    stranger.events.some((event) => event.origin === "push"),
    false,
  );
});

void test("SQLite restart retains native binding, input/output replay and duplicate-request protection", async (t) => {
  const directory = mkdtempSync(path.join(tmpdir(), "push-restart-"));
  const databasePath = path.join(directory, "gateway.sqlite");
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const calls = [];
  const run = async (request, { emit, onAccepted }) => {
    calls.push(request);
    onAccepted();
    emit({ type: "message.started", data: { messageId: "m" } });
    emit({ type: "message.delta", data: { messageId: "m", delta: "answer" } });
    emit({ type: "message.completed", data: { messageId: "m" } });
  };
  const first = await launch(databasePath, run);
  t.after(() => first.close());
  const browser = await first.connect();
  browser.send({
    type: "session.start",
    sessionId: "chat",
    runtimeId: "native",
    agentId: "assistant",
  });
  await browser.wait((event) => event.type === "session.started");
  const message = {
    type: "message",
    sessionId: "chat",
    content: "remember",
    queryMessageId: "request-1",
  };
  browser.send(message);
  const done = await browser.wait((event) => event.type === "run.completed");
  browser.send(message);
  await browser.wait((event) => event.type === "request_duplicate");
  assert.equal(calls.length, 1);
  assert.equal(calls[0].resume, false);
  browser.socket.close();
  await once(browser.socket, "close");
  const push = {
    agentId: "assistant",
    sessionId: "chat",
    content: "offline result",
    eventId: "notification-1",
  };
  for (let index = 0; index < 2; index += 1) {
    const response = await fetch(`${first.base}/send`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(push),
    });
    assert.equal(response.status, 200);
  }
  await first.close();
  const second = await launch(databasePath, run);
  t.after(() => second.close());
  const resumed = await second.connect();
  resumed.send({ type: "session.start", sessionId: "chat" });
  const input = await resumed.wait((event) => event.type === "input.accepted");
  assert.equal(input.data.content, "remember");
  await resumed.wait((event) => event.origin === "push" && event.type === "run.completed");
  assert.equal(
    resumed.events.filter(
      (event) => event.type === "message.delta" && event.data.delta === "offline result",
    ).length,
    1,
  );
  resumed.send(message);
  await resumed.wait((event) => event.type === "request_duplicate");
  assert.equal(calls.length, 1);
  resumed.send({ ...message, queryMessageId: "request-2", content: "continue" });
  await resumed.wait((event) => event.type === "run.completed" && event.requestId === "request-2");
  assert.equal(calls.length, 2);
  assert.equal(calls[1].resume, true);
  assert.equal(calls[1].nativeSessionId, calls[0].nativeSessionId);
  assert.equal(
    new Set(
      resumed.events.filter((event) => event.cursor <= done.cursor).map((event) => event.runId),
    ).size,
    1,
  );
  const stranger = await second.connect("stranger");
  stranger.send({ type: "session.start", sessionId: "chat" });
  await stranger.wait((event) => event.type === "session.started");
  assert.equal(
    stranger.events.some((event) => event.data?.delta),
    false,
  );
});

void test("clean preflight rejection does not turn a new session into a missing-native restore", async (t) => {
  const directory = mkdtempSync(path.join(tmpdir(), "push-preflight-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const requests = [];
  const server = await launch(path.join(directory, "gateway.sqlite"), async (request) => {
    requests.push(request);
    throw new Error("Runtime is not configured yet");
  });
  t.after(() => server.close());
  const browser = await server.connect();
  browser.send({
    type: "message",
    sessionId: "setup",
    runtimeId: "native",
    content: "first",
    queryMessageId: "first",
  });
  await browser.wait((event) => event.type === "run.failed" && event.requestId === "first");
  browser.send({
    type: "message",
    sessionId: "setup",
    content: "retry after setup",
    queryMessageId: "second",
  });
  await browser.wait((event) => event.type === "run.failed" && event.requestId === "second");
  assert.deepEqual(
    requests.map((request) => request.resume),
    [false, false],
  );
});

void test("equal conversation names isolate clients, native identities, replay and targeted push", async (t) => {
  const directory = mkdtempSync(path.join(tmpdir(), "push-owner-scope-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const requests = [];
  const server = await launch(path.join(directory, "gateway.sqlite"), async (request, { emit }) => {
    requests.push(request);
    emit({ type: "message.started", data: { messageId: "answer" } });
    emit({ type: "message.delta", data: { messageId: "answer", delta: request.clientId } });
    emit({ type: "message.completed", data: { messageId: "answer" } });
  });
  t.after(() => server.close());
  const first = await server.connect("first");
  const second = await server.connect("second");
  for (const browser of [first, second]) {
    browser.send({
      type: "message",
      sessionId: "chat",
      runtimeId: "native",
      agentId: "assistant",
      content: "private",
      queryMessageId: "same-request-id",
    });
    await browser.wait((event) => event.type === "run.completed");
  }
  assert.equal(requests.length, 2);
  assert.notEqual(requests[0].nativeSessionId, requests[1].nativeSessionId);
  assert.ok(
    first.events.filter((event) => event.version).every((event) => event.clientId === "first"),
  );
  assert.ok(
    second.events.filter((event) => event.version).every((event) => event.clientId === "second"),
  );
  const sendPush = (target) =>
    fetch(`${server.base}/send`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ agentId: "assistant", content: "private notification", ...target }),
    });
  assert.equal((await sendPush({ sessionId: "chat" })).status, 404);
  assert.equal((await sendPush({ sessionId: "chat", clientId: "first" })).status, 200);
  assert.equal((await sendPush({ sessionId: requests[1].nativeSessionId })).status, 200);
  for (const browser of [first, second]) {
    await browser.wait((event) => event.origin === "push" && event.type === "run.completed");
    assert.equal(
      browser.events.filter((event) => event.origin === "push" && event.type === "message.delta")
        .length,
      1,
    );
  }
});

void test("persisted native and conversation address collisions require a unique push target", async (t) => {
  const directory = mkdtempSync(path.join(tmpdir(), "push-address-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const databasePath = path.join(directory, "gateway.sqlite");
  const store = new GatewayStore({ path: databasePath });
  try {
    const binding = { runtimeId: "native", agentId: "assistant", nativeStarted: false };
    store.saveSession({
      ...binding,
      clientId: "first",
      conversationId: "chat",
      nativeSessionId: "shared",
    });
    store.saveSession({
      ...binding,
      clientId: "second",
      conversationId: "shared",
      nativeSessionId: "shared",
    });
  } finally {
    store.close();
  }

  const server = await launch(databasePath, async () => assert.fail("A push must not run an agent"));
  t.after(() => server.close());
  const first = await server.connect("first", "chat");
  const second = await server.connect("second", "shared");
  const sendPush = (target) =>
    fetch(`${server.base}/api/send`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        agentId: "assistant",
        sessionId: "shared",
        content: "direct notification",
        ...target,
      }),
    });
  assert.equal((await sendPush({})).status, 404);
  assert.equal((await sendPush({ clientId: "missing" })).status, 404);
  const targets = [
    ["first", first, "chat"],
    ["second", second, "shared"],
  ];
  for (const [clientId] of targets) {
    assert.equal((await sendPush({ clientId })).status, 200);
  }
  for (const [clientId, browser, conversationId] of targets) {
    browser.send({ type: "ping" });
    await browser.wait((event) => event.type === "pong");
    const deliveries = browser.events.filter(
      (event) => event.origin === "push" && event.type === "message.delta",
    );
    assert.deepEqual(deliveries.map((event) => [event.clientId, event.conversationId]), [
      [clientId, conversationId],
    ]);
  }
});

void test("cursor replay includes the boundary and preserves legacy stream state", async (t) => {
  const directory = mkdtempSync(path.join(tmpdir(), "push-replay-cursor-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const databasePath = path.join(directory, "gateway.sqlite");
  const target = { clientId: "owner", conversationId: "chat", runtimeId: "native", agentId: "assistant" };
  const store = new GatewayStore({ path: databasePath });
  let boundary;
  let terminal;
  try {
    store.saveSession({ ...target, nativeSessionId: "native-chat", nativeStarted: true });
    let seq = 0;
    const append = (type, data, extra = {}) =>
      store.appendEvent({
        ...target,
        version: 1,
        eventId: `event-${++seq}`,
        runId: "run",
        replyId: "answer",
        origin: "prompt",
        audience: "conversation",
        seq,
        timestamp: Date.now(),
        type,
        data,
        ...extra,
      });
    append("run.started", {});
    append("message.started", { messageId: "part" });
    boundary = append("message.delta", { messageId: "part", delta: "resumed" });
    append(
      "message.delta",
      { messageId: "feed", delta: "agent feed" },
      { audience: "agent", origin: "push", runId: "feed" },
    );
    terminal = append("run.completed", {});
  } finally {
    store.close();
  }

  const server = await launch(databasePath, async () => assert.fail("Replay must not run an agent"));
  t.after(() => server.close());
  const subscribe = async (legacy) => {
    const browser = await server.connect("owner", undefined, { legacy });
    browser.send({ type: "session.start", sessionId: "chat", afterCursor: boundary.cursor });
    browser.send({ type: "ping" });
    await browser.wait((event) => event.type === "pong");
    return browser.events.filter((event) => event.cursor !== undefined);
  };
  assert.deepEqual(await subscribe(false), [boundary, terminal]);
  const legacy = await subscribe(true);
  assert.deepEqual(
    legacy.map(({ type, cursor, answerMessageId, deliveryId }) => [
      type, cursor, answerMessageId, deliveryId,
    ]),
    [
      ["stream", boundary.cursor, "answer-1", `${boundary.eventId}:0`],
      ["stream_end", terminal.cursor, "answer-1", `${terminal.eventId}:0`],
      ["run_completed", terminal.cursor, "answer-1", `${terminal.eventId}:1`],
    ],
  );
});
