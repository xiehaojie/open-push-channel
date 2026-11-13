const assert = require("node:assert/strict");
const { once } = require("node:events");
const test = require("node:test");
const WebSocket = require("ws");
const { createGatewayServer } = require("./server");
const { createOpenClawAdapter } = require("../adapters/openclaw");

void test("configured authentication derives client identity, checks targets and protects push", async (t) => {
  let calls = 0;
  const mentions = [];
  const server = createGatewayServer({
    adapters: [
      {
        id: "allowed",
        agents: ["native", "private-agent"],
        async run(request) {
          calls += 1;
          mentions.push(request.mentions);
        },
      },
      {
        id: "other",
        async run() {
          calls += 1;
        },
      },
    ],
    auth: {
      clients: [
        {
          id: "owner",
          token: "test-fixture-token",
          targets: [
            { runtimeId: "allowed", agentId: "native" },
            { runtimeId: "other", agentId: "private-agent" },
          ],
        },
      ],
      pushToken: "test-fixture-push",
    },
  });
  await new Promise((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = `127.0.0.1:${server.address().port}`;
  const sockets = [];
  t.after(async () => {
    sockets.forEach((socket) => socket.terminate());
    await new Promise((resolve) => {
      server.close(resolve);
    });
  });
  const wrong = new WebSocket(`ws://${address}`);
  sockets.push(wrong);
  await once(wrong, "open");
  const rejected = once(wrong, "close");
  wrong.send(JSON.stringify({ type: "register", agentId: "owner", token: "wrong" }));
  const rejectTimeout = new Promise((_, reject) => {
    const timer = setTimeout(
      () => reject(new Error("Unauthenticated register was not rejected")),
      1000,
    );
    timer.unref();
  });
  assert.equal((await Promise.race([rejected, rejectTimeout]))[0], 4001);
  const origin = "https://app.example.test";
  const socket = new WebSocket(`ws://${address}`, { origin });
  sockets.push(socket);
  await once(socket, "open");
  const receive = async (send) => {
    const message = once(socket, "message");
    socket.send(JSON.stringify(send));
    return JSON.parse((await message)[0]);
  };
  const registered = await receive({
    type: "register",
    agentId: "spoofed",
    token: "test-fixture-token",
    protocolVersion: 1,
  });
  assert.equal(registered.agentId, "owner");
  assert.deepEqual(
    registered.runtimes.map((runtime) => runtime.id),
    ["allowed", "other"],
  );
  assert.deepEqual(registered.runtimes[0].agents, ["native"]);
  const denied = await receive({
    type: "session.start",
    sessionId: "forbidden",
    runtimeId: "other",
    agentId: "native",
  });
  assert.equal(denied.type, "error");
  assert.equal(calls, 0);
  const allowed = await receive({
    type: "session.start",
    sessionId: "owned",
    runtimeId: "allowed",
    agentId: "native",
  });
  assert.equal(allowed.type, "session.started");
  const rejectedMention = await receive({
    type: "message",
    sessionId: "owned",
    content: "fixture",
    mentions: [{ agentId: "private-agent" }],
  });
  assert.equal(rejectedMention.type, "error");
  assert.match(rejectedMention.message, /Mention target is not authorized/);
  assert.equal(calls, 0);
  const acceptedMention = await receive({
    type: "message",
    sessionId: "owned",
    content: "fixture",
    mentions: [{ agentId: "native" }],
  });
  assert.equal(acceptedMention.type, "input.accepted");
  if (!mentions.length) {
    await once(socket, "message");
  }
  assert.deepEqual(mentions, [[{ agentId: "native" }]]);
  const catalog = await fetch(`http://${address}/api/runtimes`, {
    headers: { Origin: origin, Authorization: "Bearer test-fixture-push" },
  });
  assert.equal(catalog.status, 200);
  assert.equal(catalog.headers.get("access-control-allow-origin"), "*");
  await catalog.text();
  const response = await fetch(`http://${address}/send`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ agentId: "native", sessionId: "owned", content: "unauthorized" }),
  });
  assert.equal(response.status, 401);
  for (const alias of ["/api/send", "/api/tasks"]) {
    const deniedPush = await fetch(`http://${address}${alias}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ agentId: "native", sessionId: "owned", content: "unauthorized" }),
    });
    assert.equal(deniedPush.status, 401, alias);
  }
  for (const alias of ["/send/", "/SEND", "/api/SEND/", "//SEND/"]) {
    const retiredAlias = await fetch(`http://${address}${alias}`, { method: "POST" });
    assert.equal(retiredAlias.status, 404, alias);
  }
});

void test("conversation replay excludes same-named agent broadcasts after target authorization changes", async (t) => {
  const auth = {
    clients: [
      {
        id: "owner",
        token: "fixture-owner-token",
        targets: [
          { runtimeId: "openclaw", agentId: "chat" },
          { runtimeId: "openclaw", agentId: "other" },
        ],
      },
    ],
    pushToken: "fixture-push-token",
  };
  const server = createGatewayServer({
    auth,
    adapters: [createOpenClawAdapter({ url: "http://127.0.0.1:1/webhook" })],
  });
  await new Promise((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = `127.0.0.1:${server.address().port}`;
  const sockets = [];
  t.after(() => {
    sockets.forEach((socket) => socket.terminate());
    return server.shutdown();
  });
  const connect = async () => {
    const socket = new WebSocket(`ws://${address}`);
    sockets.push(socket);
    const events = [];
    socket.on("message", (raw) => {
      const buffer = Buffer.concat(Array.isArray(raw) ? raw : [Buffer.from(raw)]);
      events.push(JSON.parse(buffer.toString("utf8")));
    });
    await once(socket, "open");
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
    const send = (data) => socket.send(JSON.stringify(data));
    send({ type: "register", token: "fixture-owner-token", protocolVersion: 1 });
    await wait((event) => event.type === "registered");
    return { socket, events, send, wait };
  };
  const first = await connect();
  for (const [sessionId, agentId] of [
    ["private", "chat"],
    ["chat", "other"],
  ]) {
    first.send({ type: "session.start", sessionId, agentId, runtimeId: "openclaw" });
    await first.wait((event) => event.type === "session.started" && event.sessionId === sessionId);
  }
  for (const payload of [
    { agentId: "chat", content: "revoked target broadcast", eventId: "hidden-broadcast" },
    {
      agentId: "other",
      sessionId: "chat",
      content: "allowed history",
      eventId: "visible-conversation",
    },
  ]) {
    const response = await fetch(`http://${address}/send`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: "Bearer fixture-push-token" },
      body: JSON.stringify(payload),
    });
    assert.equal(response.status, 200);
  }
  first.socket.close();
  await once(first.socket, "close");
  // A fresh registration reads the updated principal, as after restarting with
  // narrower auth config while retaining the existing journal and bindings.
  auth.clients[0].targets = [{ runtimeId: "openclaw", agentId: "other" }];
  const resumed = await connect();
  resumed.send({
    type: "session.start",
    sessionId: "chat",
    runtimeId: "openclaw",
    agentId: "other",
  });
  await resumed.wait(
    (event) => event.type === "run.completed" && event.requestId === "push:visible-conversation",
  );
  const replayed = resumed.events.filter((event) => event.version === 1);
  assert.ok(
    replayed.some(
      (event) => event.type === "message.delta" && event.data.delta === "allowed history",
    ),
  );
  assert.ok(
    replayed.every((event) => event.audience === "conversation" && event.agentId === "other"),
  );
});
