const assert = require("node:assert/strict");
const { once } = require("node:events");
const test = require("node:test");
const { WebSocketServer } = require("../admin-backend/node_modules/ws");
const { connectPlugin } = require("./client.cjs");
const wire = require("../protocol/plugin.cjs");

const capabilities = wire.capabilities({ streaming: true });
const request = {
  clientId: "owner",
  runtimeId: "dsh",
  agentId: "main",
  conversationId: "chat",
  nativeSessionId: "native-session",
  runId: "run-one",
  content: "hello",
  resume: false,
};

async function wait(predicate) {
  const deadline = Date.now() + 2000;
  while (!predicate()) {
    assert.ok(Date.now() < deadline, "Missing plugin protocol observation");
    await new Promise((resolve) => {
      setTimeout(resolve, 5);
    });
  }
  return predicate();
}

async function setup(t, respond = true) {
  const server = new WebSocketServer({ port: 0, host: "127.0.0.1" });
  const peers = [];
  const clients = [];
  const release = [];
  t.after(async () => {
    for (const finish of release) {
      finish();
    }
    for (const client of clients) {
      await client.close();
    }
    for (const peer of peers) {
      peer.socket.terminate();
    }
    await new Promise((resolve) => {
      server.close(resolve);
    });
  });
  server.on("connection", (socket) => {
    const events = [];
    const peer = { socket, events, send: (frame) => socket.send(JSON.stringify(frame)) };
    peers.push(peer);
    socket.on("message", (raw) => {
      const frame = JSON.parse(Buffer.from(raw).toString());
      events.push(frame);
      if (frame.type === "plugin.hello" && respond) {
        peer.send({ type: "plugin.ready" });
      }
    });
  });
  await once(server, "listening");
  const defaults = {
    url: `ws://127.0.0.1:${server.address().port}/plugins`,
    runtimeId: "dsh",
    agentId: "main",
    token: "fixture-native",
    capabilities,
    async run() {},
  };
  const connect = async (options = {}) => {
    const client = await connectPlugin({ ...defaults, ...options });
    clients.push(client);
    return client;
  };
  return { peers, release, defaults, connect };
}

void test("plugin client rejects secret-bearing URLs, invalid capabilities and missing advertised handlers", async () => {
  const defaults = {
    runtimeId: "dsh",
    agentId: "main",
    token: "fixture",
    capabilities,
    async run() {},
  };
  for (const url of [
    "not a valid URL",
    "https://localhost/plugins",
    "ws://secret:secret@localhost/plugins",
    "ws://localhost/plugins?token=secret",
  ]) {
    await assert.rejects(connectPlugin({ ...defaults, url }), (error) => {
      assert.equal(error.message.includes("secret"), false);
      return /URL|WS/.test(error.message);
    });
  }
  await assert.rejects(
    connectPlugin({ ...defaults, url: "ws://localhost", capabilities: { cancel: true } }),
    /handlers/i,
  );
  assert.throws(() => wire.capabilities({ streaming: "yes" }), /capabilities/i);
  assert.throws(() => wire.capabilities({ shell: true }), /fields/i);
  assert.throws(
    () =>
      wire.notification(
        { clientId: "owner", conversationId: "chat", runtimeId: "foreign" },
        { eventId: "one", content: "hello" },
      ),
    /fields/i,
  );
  assert.throws(
    () =>
      wire.notification(
        { clientId: "owner", conversationId: "chat" },
        { eventId: "one", content: "hello", signal: {} },
      ),
    /one/i,
  );
  const aborted = AbortSignal.abort();
  await assert.rejects(
    connectPlugin({ ...defaults, url: "ws://localhost", signal: aborted }),
    /aborted/,
  );
  assert.throws(() => wire.text(""), /text/);
  const selected = wire.request({
    ...request,
    mentions: [{ agentId: "reviewer", label: "Review" }],
    command: "forbidden",
  });
  assert.equal(Object.hasOwn(selected, "command"), false);
  for (const changed of [
    { content: 1 },
    { resume: "yes" },
    { origin: "foreign" },
    { mentions: "reviewer" },
    { mentions: [{ agentId: "reviewer", label: 1 }] },
  ]) {
    assert.throws(() => wire.request({ ...request, ...changed }));
  }
});

void test("plugin lifecycle abort releases an incomplete handshake before a client is returned", async (t) => {
  const f = await setup(t, false);
  const controller = new AbortController();
  const connecting = f.connect({ signal: controller.signal });
  const rejected = assert.rejects(connecting, /closed|aborted/i);
  await wait(() => f.peers[0]?.events.length);
  const closed = once(f.peers[0].socket, "close");
  controller.abort();
  await rejected;
  await closed;
});

void test("plugin close is idempotent and aborts observation without awaiting a native run", async (t) => {
  const f = await setup(t);
  const gate = Promise.withResolvers();
  f.release.push(() => gate.resolve());
  let nativeSignal;
  const client = await f.connect({
    async run(_request, { onAccepted, signal }) {
      nativeSignal = signal;
      onAccepted();
      await gate.promise;
    },
  });
  f.peers[0].send({ type: "plugin.run", runId: request.runId, request });
  await wait(() => f.peers[0].events.find((frame) => frame.type === "plugin.accepted"));
  await client.close();
  await client.close();
  await assert.rejects(
    client.notify(
      { clientId: "owner", conversationId: "chat" },
      { eventId: "late", content: "late" },
    ),
    /offline/,
  );
  assert.equal(nativeSignal.aborted, true);
  assert.equal(
    f.peers[0].events.some((frame) => frame.type === "plugin.result"),
    false,
  );
});

void test(
  "handshake timeout closes an initially silent peer and rejects the pending connection",
  { timeout: 6000 },
  async (t) => {
    const f = await setup(t, false);
    const connecting = f.connect();
    const rejected = assert.rejects(connecting, /timed out/);
    await wait(() => f.peers[0]?.events.length);
    const closed = once(f.peers[0].socket, "close");
    await rejected;
    await closed;
  },
);

void test("malformed initial handshake does not leave an inaccessible live client", async (t) => {
  const f = await setup(t, false);
  const connecting = f.connect();
  const rejected = assert.rejects(connecting, /handshake rejected/);
  await wait(() => f.peers[0]?.events.length);
  const closed = once(f.peers[0].socket, "close");
  f.peers[0].send({ type: "foreign.ready" });
  await rejected;
  await closed;
});

for (const status of ["cancelled", "deferred"]) {
  void test(`native plugin preserves ${status} rather than inventing completion`, async (t) => {
    const f = await setup(t);
    await f.connect({
      async run(_request, { onAccepted }) {
        onAccepted();
        onAccepted();
        return { status, message: "native outcome" };
      },
    });
    const peer = f.peers[0];
    peer.send({ type: "plugin.run", runId: request.runId, request });
    const result = await wait(() => peer.events.find((frame) => frame.type === "plugin.result"));
    assert.equal(result.status, status);
    assert.equal(result.message, "native outcome");
    assert.equal(peer.events.filter((frame) => frame.type === "plugin.accepted").length, 1);
  });
}

void test("output before acceptance and invalid host results become explicit run failures", async (t) => {
  const f = await setup(t);
  await f.connect({
    async run(input, { emit, onAccepted }) {
      if (input.content === "early") {
        emit({ type: "message.started", data: { messageId: "answer" } });
      }
      if (input.content === "unaccepted") {
        return undefined;
      }
      onAccepted();
      return { status: "invented" };
    },
  });
  const peer = f.peers[0];
  for (const content of ["early", "unaccepted", "invalid-result"]) {
    peer.send({
      type: "plugin.run",
      runId: content,
      request: { ...request, runId: content, content },
    });
    const result = await wait(() =>
      peer.events.find((frame) => frame.type === "plugin.result" && frame.runId === content),
    );
    assert.equal(result.status, "failed");
  }
});

void test("native cancel is acknowledged separately and finishes through the owned run", async (t) => {
  const f = await setup(t);
  const cancelled = Promise.withResolvers();
  f.release.push(() => cancelled.resolve());
  await f.connect({
    capabilities: { ...capabilities, cancel: true },
    async run(_request, { onAccepted }) {
      onAccepted();
      await cancelled.promise;
      return { status: "cancelled" };
    },
    async cancel(input) {
      assert.equal(input.runId, request.runId);
      cancelled.resolve();
    },
  });
  const peer = f.peers[0];
  peer.send({ type: "plugin.run", runId: request.runId, request });
  await wait(() => peer.events.find((frame) => frame.type === "plugin.accepted"));
  peer.send({ type: "plugin.cancel", id: "cancel", runId: request.runId });
  const receipt = await wait(() => peer.events.find((frame) => frame.type === "plugin.receipt"));
  assert.equal(receipt.status, "handled");
  const result = await wait(() => peer.events.find((frame) => frame.type === "plugin.result"));
  assert.equal(result.status, "cancelled");
});

void test("unsupported commands and controls for missing runs are rejected, not reported queued", async (t) => {
  const f = await setup(t);
  const gate = Promise.withResolvers();
  f.release.push(() => gate.resolve());
  await f.connect({
    async run(_request, { onAccepted }) {
      onAccepted();
      await gate.promise;
    },
  });
  const peer = f.peers[0];
  peer.send({ type: "plugin.run", runId: request.runId, request });
  await wait(() => peer.events.find((frame) => frame.type === "plugin.accepted"));
  const commands = [
    { type: "plugin.cancel", id: "cancel", runId: request.runId },
    {
      type: "plugin.control",
      id: "steer",
      runId: request.runId,
      operation: "steer",
      content: "hello",
    },
    { type: "plugin.cancel", id: "missing", runId: "missing" },
  ];
  for (const command of commands) {
    peer.send(command);
    const receipt = await wait(() =>
      peer.events.find((frame) => frame.type === "plugin.receipt" && frame.id === command.id),
    );
    assert.equal(receipt.status, "rejected");
  }
});

for (const rejected of [true, false]) {
  void test(`plugin run errors preserve explicit native rejection=${rejected}`, async (t) => {
    const f = await setup(t);
    await f.connect({
      async run() {
        throw Object.assign(new Error("native failure"), { rpcRejected: rejected });
      },
    });
    const peer = f.peers[0];
    peer.send({ type: "plugin.run", runId: request.runId, request });
    const result = await wait(() => peer.events.find((frame) => frame.type === "plugin.result"));
    assert.equal(result.status, "failed");
    assert.equal(result.rejected, rejected);
    assert.equal(
      peer.events.some((frame) => frame.type === "plugin.accepted"),
      false,
    );
  });
}

void test("plugin controls distinguish queued, explicit rejected and unknown native outcomes", async (t) => {
  const f = await setup(t);
  const gate = Promise.withResolvers();
  f.release.push(() => gate.resolve());
  await f.connect({
    capabilities: { ...capabilities, steering: true },
    async run(_request, { onAccepted }) {
      onAccepted();
      await gate.promise;
    },
    async control(target, input) {
      assert.equal(target.runId, request.runId);
      if (input.content === "queued") {
        return { status: "queued" };
      }
      throw Object.assign(new Error("control failed"), {
        rpcRejected: input.content === "rejected",
      });
    },
  });
  const peer = f.peers[0];
  peer.send({ type: "plugin.run", runId: request.runId, request });
  await wait(() => peer.events.find((frame) => frame.type === "plugin.accepted"));
  for (const status of ["queued", "rejected", "unknown"]) {
    peer.send({
      type: "plugin.control",
      id: status,
      runId: request.runId,
      operation: "steer",
      content: status,
    });
    const receipt = await wait(() =>
      peer.events.find((frame) => frame.type === "plugin.receipt" && frame.id === status),
    );
    assert.equal(receipt.status, status);
  }
  gate.resolve();
  await wait(() => peer.events.find((frame) => frame.type === "plugin.result"));
});

void test("malformed notification receipts reject their pending operation instead of silently losing it", async (t) => {
  const f = await setup(t);
  const client = await f.connect();
  const pending = client.notify(
    { clientId: "owner", conversationId: "chat" },
    { eventId: "notice", content: "hello" },
  );
  const rejected = assert.rejects(pending, /unknown|disconnect/i);
  const push = await wait(() => f.peers[0].events.find((frame) => frame.type === "plugin.push"));
  f.peers[0].send({
    type: "plugin.notified",
    id: push.id,
    receipt: { runId: "notice-run", duplicate: "not-a-boolean" },
  });
  await rejected;
});

void test("invalid run identity is rejected before calling the host handler", async (t) => {
  const f = await setup(t);
  let runs = 0;
  await f.connect({
    async run() {
      runs++;
    },
  });
  const peer = f.peers[0];
  const closed = once(peer.socket, "close");
  peer.send({
    type: "plugin.run",
    runId: request.runId,
    request: { ...request, agentId: "foreign" },
  });
  assert.equal((await closed)[0], 4002);
  assert.equal(runs, 0);
});

void test("transport reconnects metadata but never resubmits an interrupted native run", async (t) => {
  const f = await setup(t);
  const gate = Promise.withResolvers();
  f.release.push(() => gate.resolve());
  let runs = 0;
  let nativeSignal;
  await f.connect({
    async run(_request, { onAccepted, signal }) {
      runs++;
      nativeSignal = signal;
      onAccepted();
      await gate.promise;
    },
  });
  f.peers[0].send({ type: "plugin.run", runId: request.runId, request });
  await wait(() => f.peers[0].events.find((frame) => frame.type === "plugin.accepted"));
  f.peers[0].socket.terminate();
  await wait(() => f.peers[1]?.events.find((frame) => frame.type === "plugin.hello"));
  assert.equal(nativeSignal.aborted, true);
  assert.equal(runs, 1);
  assert.equal(
    f.peers[1].events.some((frame) => ["plugin.accepted", "plugin.result"].includes(frame.type)),
    false,
  );
});
