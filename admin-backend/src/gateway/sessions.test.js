const assert = require("node:assert/strict");
const { once } = require("node:events");
const test = require("node:test");
const WebSocket = require("ws");
const { createGatewayServer } = require("./server");

async function client(port, agentId = "client") {
  const socket = new WebSocket(`ws://127.0.0.1:${port}`);
  const events = [];
  socket.on("message", (raw) => {
    const data = Array.isArray(raw) ? Buffer.concat(raw) : Buffer.from(raw);
    events.push(JSON.parse(data.toString()));
  });
  await once(socket, "open");
  const send = (value) => socket.send(JSON.stringify(value));
  const wait = async (predicate) => {
    const deadline = Date.now() + 1500;
    while (!events.some(predicate)) {
      if (Date.now() > deadline) {
        throw new Error(`Missing event: ${predicate}`);
      }
      await new Promise((resolve) => {
        setTimeout(resolve, 5);
      });
    }
    return events.find(predicate);
  };
  send({ type: "register", agentId, protocolVersion: 1 });
  await wait((event) => event.type === "registered");
  return { socket, events, send, wait };
}

async function setup(t, adapters) {
  const server = createGatewayServer({ adapters });
  await new Promise((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  const clients = [];
  t.after(async () => {
    for (const socket of clients) {
      socket.terminate();
    }
    await new Promise((resolve) => {
      server.close(resolve);
    });
  });
  return async (id) => {
    const connected = await client(server.address().port, id);
    clients.push(connected.socket);
    return connected;
  };
}

void test("registration discovers runtimes; each new session selects its runtime without launching it", async (t) => {
  const calls = [];
  const adapters = ["first", "second"].map((id) => ({
    id,
    capabilities: { streaming: true },
    async run(request, { emit }) {
      calls.push(request);
      emit({ type: "message.started", data: { messageId: "m" } });
      emit({ type: "message.delta", data: { messageId: "m", delta: id } });
      emit({ type: "message.completed", data: { messageId: "m" } });
    },
  }));
  const connect = await setup(t, adapters);
  const connected = await connect();
  const registered = connected.events.find((event) => event.type === "registered");
  assert.deepEqual(
    registered.runtimes.map((runtime) => runtime.id),
    ["first", "second"],
  );
  assert.equal(registered.runtimeId, undefined);
  for (const [sessionId, runtimeId] of [
    ["a", "first"],
    ["b", "second"],
  ]) {
    connected.send({ type: "session.start", sessionId, runtimeId, agentId: "native-agent" });
    const started = await connected.wait(
      (event) => event.type === "session.started" && event.sessionId === sessionId,
    );
    assert.equal(started.runtimeId, runtimeId);
    assert.equal(started.agentId, "native-agent");
  }
  assert.equal(calls.length, 0);
  connected.send({ type: "message", sessionId: "b", content: "hello" });
  const done = await connected.wait((event) => event.type === "run.completed");
  assert.equal(done.runtimeId, "second");
  assert.equal(done.agentId, "native-agent");
  assert.equal(done.conversationId, "b");
  assert.equal(calls[0].agentId, "native-agent");
  assert.equal(calls[0].clientId, "client");
  connected.send({
    type: "session.start",
    sessionId: "b",
    runtimeId: "first",
    agentId: "native-agent",
  });
  await connected.wait((event) => event.type === "error" && /bound/.test(event.message));
  connected.send({ type: "message", sessionId: "b", runtimeId: "first", content: "must not run" });
  await connected.wait((event) => event.type === "error" && event.sessionId === "b");
  assert.equal(calls.length, 1);
});

void test("first message can bind a session; reconnect resumes binding, not another runtime", async (t) => {
  let calls = 0;
  const connect = await setup(t, [
    {
      id: "first",
      async run() {
        calls += 1;
      },
    },
    {
      id: "second",
      async run() {
        calls += 1;
      },
    },
  ]);
  const first = await connect();
  first.send({
    type: "message",
    sessionId: "chat",
    runtimeId: "second",
    agentId: "native",
    content: "hi",
  });
  await first.wait((event) => event.type === "run.completed");
  const admitted = first.events.find((event) => event.type === "session.started");
  assert.equal(admitted?.runtimeId, "second");
  assert.equal(admitted?.agentId, "native");
  assert.ok(
    first.events.indexOf(admitted) <
      first.events.findIndex((event) => event.type === "run.started"),
  );
  first.socket.close();
  const resumed = await connect();
  resumed.send({ type: "session.start", sessionId: "chat" });
  const started = await resumed.wait((event) => event.type === "session.started");
  assert.equal(started.runtimeId, "second");
  assert.equal(started.agentId, "native");
  assert.equal(calls, 1);
  resumed.send({
    type: "message",
    sessionId: "chat",
    content: "continue",
    queryMessageId: "continued",
  });
  const done = await resumed.wait(
    (event) => event.type === "run.completed" && event.requestId === "continued",
  );
  assert.equal(done.runtimeId, "second");
  assert.equal(calls, 2);
  assert.equal(resumed.events.filter((event) => event.type === "session.started").length, 1);
});

void test("native cancellation is capability-gated and settles only after the adapter finishes", async (t) => {
  let settle;
  let started = false;
  const connect = await setup(t, [
    {
      id: "cancellable",
      capabilities: { cancel: true },
      async run() {
        started = true;
        return new Promise((resolve) => {
          settle = () => resolve({ status: "cancelled" });
        });
      },
      async cancel() {
        settle();
      },
    },
    { id: "plain", async run() {} },
  ]);
  const connected = await connect();
  connected.send({
    type: "session.start",
    sessionId: "plain",
    runtimeId: "plain",
    agentId: "native",
  });
  await connected.wait((event) => event.type === "session.started");
  connected.send({ type: "cancel", sessionId: "plain" });
  await connected.wait((event) => event.type === "error" && /cancel/.test(event.message));
  connected.send({ type: "message", sessionId: "work", runtimeId: "cancellable", content: "hi" });
  await connected.wait((event) => event.type === "run.started");
  assert.equal(started, true);
  connected.send({ type: "cancel", sessionId: "work" });
  await connected.wait((event) => event.type === "run.cancelled");
  assert.equal(
    connected.events.some((event) => event.type === "run.completed"),
    false,
  );
});
