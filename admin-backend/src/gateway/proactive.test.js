const assert = require("node:assert/strict");
const { once } = require("node:events");
const { mkdtempSync, rmSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const test = require("node:test");
const WebSocket = require("ws");
const { createGatewayServer } = require("./server");

async function setup(t, run, databasePath) {
  const auth = {
    clients: [
      {
        id: "owner",
        token: "fixture-client",
        targets: ["dsh", "pi"].map((runtimeId) => ({ runtimeId, agentId: "main" })),
      },
    ],
    pushToken: "fixture-service",
  };
  const server = createGatewayServer({
    auth,
    databasePath,
    adapters: ["dsh", "pi"].map((id) => ({ id, run })),
  });
  await new Promise((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  const base = `http://127.0.0.1:${server.address().port}`;
  const sockets = [];
  t.after(async () => {
    sockets.forEach((socket) => socket.terminate());
    await server.shutdown();
  });
  const post = (route, body, token = auth.pushToken) =>
    fetch(`${base}${route}`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
      body: JSON.stringify(body),
    });
  async function connect() {
    const socket = new WebSocket(base.replace("http:", "ws:"));
    sockets.push(socket);
    const events = [];
    socket.on("message", (raw) => events.push(JSON.parse(Buffer.from(raw).toString())));
    await once(socket, "open");
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
    const send = (value) => socket.send(JSON.stringify(value));
    send({ type: "register", token: "fixture-client", protocolVersion: 1 });
    await wait((event) => event.type === "registered");
    return { socket, send, wait, events };
  }
  return { auth, post, connect, server };
}

for (const runtimeId of ["dsh", "pi"]) {
  void test(`${runtimeId}: gateway notifications work without a native push decoder, including offline replay`, async (t) => {
    let calls = 0;
    const { connect, post } = await setup(t, async () => {
      calls += 1;
    });
    const first = await connect();
    first.send({ type: "session.start", sessionId: "chat", runtimeId, agentId: "main" });
    const started = await first.wait((event) => event.type === "session.started");
    first.socket.close();
    await once(first.socket, "close");
    const body = {
      clientId: "owner",
      sessionId: "chat",
      agentId: "main",
      runtimeId,
      eventId: "notification",
      content: "offline notification",
    };
    for (let index = 0; index < 2; index += 1) {
      assert.equal((await post("/send", body)).status, 200);
    }
    assert.equal(started.capabilities.proactivePush, true);
    const resumed = await connect();
    resumed.send({ type: "session.start", sessionId: "chat" });
    await resumed.wait((event) => event.type === "run.completed" && event.origin === "push");
    const text = resumed.events.filter((event) => event.type === "message.delta");
    assert.equal(text.length, 1);
    assert.equal(text[0].data.delta, "offline notification");
    assert.equal(text[0].runtimeId, runtimeId);
    assert.equal(calls, 0);
    const signal = {
      type: "tool.completed",
      data: { toolCallId: "background-tool", content: "done" },
    };
    assert.equal(
      (await post("/api/send", { ...body, eventId: "progress", content: undefined, signal }))
        .status,
      200,
    );
    await resumed.wait((event) => event.type === "tool.completed");
    assert.equal((await post("/send", { ...body, eventId: "bad", content: 123 })).status, 400);
    assert.equal(
      (
        await post("/send", {
          ...body,
          eventId: "bad",
          content: undefined,
          signal: { type: "run.completed", data: {} },
        })
      ).status,
      400,
    );
    assert.equal((await post("/send", { ...body, content: "conflicting retry" })).status, 400);
    assert.equal((await post("/send", { ...body, clientId: "stranger" })).status, 404);
    assert.equal((await post("/send", body, "wrong")).status, 401);
  });
}

void test("HTTP tasks acknowledge durable admission, serialize with chat and never rerun duplicate events", async (t) => {
  const calls = [];
  const gate = Promise.withResolvers();
  t.after(() => gate.resolve());
  const { connect, post } = await setup(t, async (request, { emit, onAccepted }) => {
    calls.push(request);
    onAccepted();
    if (request.content === "first") {
      await gate.promise;
    }
    emit({ type: "message.started", data: { messageId: "answer" } });
    emit({ type: "message.snapshot", data: { messageId: "answer", text: request.content } });
    emit({ type: "message.completed", data: { messageId: "answer" } });
  });
  const browser = await connect();
  browser.send({ type: "session.start", sessionId: "chat", runtimeId: "pi", agentId: "main" });
  await browser.wait((event) => event.type === "session.started");
  const task = { clientId: "owner", sessionId: "chat", eventId: "job-1", content: "first" };
  const accepted = await post("/api/tasks", task);
  assert.equal(accepted.status, 202);
  const receipt = await accepted.json();
  assert.equal(typeof receipt.runId, "string");
  assert.equal(receipt.duplicate, false);
  assert.equal(receipt.status, "accepted");
  await browser.wait((event) => event.type === "run.started");
  const duplicate = await post("/api/tasks", task);
  assert.equal(duplicate.status, 202);
  assert.equal((await duplicate.json()).runId, receipt.runId);
  browser.send({ type: "message", sessionId: "chat", queryMessageId: "chat-2", content: "second" });
  await browser.wait((event) => event.type === "input.accepted" && event.requestId === "chat-2");
  assert.equal(calls.length, 1);
  gate.resolve();
  await browser.wait((event) => event.type === "run.completed" && event.requestId === "chat-2");
  assert.deepEqual(
    calls.map((request) => request.content),
    ["first", "second"],
  );
  assert.equal(calls[0].nativeSessionId, calls[1].nativeSessionId);
  assert.equal(calls[1].resume, true);
  assert.ok(
    browser.events
      .filter((event) => event.runId === receipt.runId)
      .every((event) => event.origin === "task"),
  );
  assert.equal((await post("/api/tasks", { ...task, content: "changed" })).status, 400);
});

void test("HTTP task execution requires service auth, an existing authorized binding and an idempotency key", async (t) => {
  let calls = 0;
  const { connect, post, auth } = await setup(t, async () => {
    calls += 1;
  });
  const browser = await connect();
  browser.send({ type: "session.start", sessionId: "chat", runtimeId: "pi", agentId: "main" });
  await browser.wait((event) => event.type === "session.started");
  const task = { clientId: "owner", sessionId: "chat", eventId: "job", content: "work" };
  assert.equal((await post("/api/tasks", task, "fixture-client")).status, 401);
  for (const body of [
    [],
    {},
    { ...task, eventId: undefined },
    { ...task, content: 1 },
    { ...task, command: "untrusted executable" },
  ]) {
    assert.equal((await post("/api/tasks", body)).status, 400);
  }
  for (const body of [
    { ...task, clientId: "stranger" },
    { ...task, sessionId: "missing" },
    { ...task, runtimeId: "dsh" },
    { ...task, agentId: "other" },
  ]) {
    assert.equal((await post("/api/tasks", body)).status, 404);
  }
  auth.clients[0].targets = [];
  assert.equal((await post("/api/tasks", task)).status, 403);
  assert.equal(calls, 0);
});

void test("offline external tasks survive restart as replayable results, not new executions", async (t) => {
  const directory = mkdtempSync(join(tmpdir(), "push-offline-task-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const databasePath = join(directory, "gateway.sqlite");
  let calls = 0;
  const finished = Promise.withResolvers();
  const run = async (request, { emit, onAccepted }) => {
    calls += 1;
    onAccepted();
    emit({ type: "message.started", data: { messageId: "result" } });
    emit({ type: "message.snapshot", data: { messageId: "result", text: "offline result" } });
    emit({ type: "message.completed", data: { messageId: "result" } });
    finished.resolve();
  };
  const first = await setup(t, run, databasePath);
  const browser = await first.connect();
  browser.send({ type: "session.start", sessionId: "chat", runtimeId: "dsh", agentId: "main" });
  await browser.wait((event) => event.type === "session.started");
  browser.socket.close();
  await once(browser.socket, "close");
  const task = { clientId: "owner", sessionId: "chat", eventId: "offline-job", content: "work" };
  const accepted = await first.post("/api/tasks", task);
  assert.equal(accepted.status, 202);
  const receipt = await accepted.json();
  await finished.promise;
  await first.server.shutdown();
  const second = await setup(t, run, databasePath);
  const duplicate = await second.post("/api/tasks", task);
  assert.equal(duplicate.status, 202);
  assert.deepEqual(await duplicate.json(), { ...receipt, duplicate: true, status: "completed" });
  const resumed = await second.connect();
  resumed.send({ type: "session.start", sessionId: "chat" });
  const completed = await resumed.wait((event) => event.type === "run.completed");
  assert.equal(completed.origin, "task");
  assert.equal(completed.runId, receipt.runId);
  assert.equal(
    resumed.events.find((event) => event.type === "message.snapshot").data.text,
    "offline result",
  );
  assert.equal(calls, 1);
});
