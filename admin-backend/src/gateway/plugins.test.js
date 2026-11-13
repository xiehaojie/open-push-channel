const assert = require("node:assert/strict");
const { randomBytes } = require("node:crypto");
const { once } = require("node:events");
const net = require("node:net");
const { mkdtempSync, rmSync } = require("node:fs");
const { tmpdir } = require("node:os");
const path = require("node:path");
const test = require("node:test");
const WebSocket = require("ws");
const { createRuntimeOptions } = require("./runtime-config");
const { createGatewayServer } = require("./server");

const capabilities = {
  streaming: true,
  tools: true,
  reasoning: true,
  subagents: true,
  cancel: false,
  mentions: false,
  sessionRestore: true,
  steering: false,
  followUp: false,
};

function configured(t, overrides = {}) {
  const directory = mkdtempSync(path.join(tmpdir(), "push-plugin-"));
  const resources = { release: [], plugins: [], sockets: [], server: null, options: null };
  const tokens = {
    PUSH_PLUGIN_TEST_NATIVE: "fixture-native",
    PUSH_PLUGIN_TEST_CLIENT: "fixture-client",
    PUSH_PLUGIN_TEST_PUSH: "fixture-push",
  };
  const previous = Object.fromEntries(Object.keys(tokens).map((key) => [key, process.env[key]]));
  Object.assign(process.env, tokens);
  t.after(async () => {
    const errors = [];
    const clean = async (action) => {
      try {
        await action();
        return true;
      } catch (error) {
        errors.push(error);
        return false;
      }
    };
    // Release controlled native work before shutting down clients/server; keep
    // SQLite and env alive until every owned resource has finished cleanup.
    for (const release of resources.release) {
      await clean(release);
    }
    for (const plugin of resources.plugins) {
      await clean(() => plugin.close());
    }
    for (const socket of resources.sockets) {
      await clean(() => socket.terminate());
    }
    let storageClosed = true;
    if (resources.server) {
      storageClosed = await clean(() => resources.server.shutdown());
    } else {
      for (const adapter of resources.options?.adapters ?? []) {
        storageClosed = (await clean(() => adapter.close())) && storageClosed;
      }
    }
    try {
      // A rejected shutdown does not prove SQLite closed. Keep failed fixture
      // data recoverable instead of unlinking a database still owned by a task.
      if (storageClosed) {
        rmSync(directory, { recursive: true, force: true });
      }
    } finally {
      for (const [key, value] of Object.entries(previous)) {
        if (value === undefined) {
          delete process.env[key];
        } else {
          process.env[key] = value;
        }
      }
    }
    if (errors.length) {
      const message = storageClosed
        ? "Plugin fixture cleanup failed"
        : `Plugin fixture cleanup failed; retained temporary directory ${directory}`;
      throw new AggregateError(errors, message);
    }
  });
  resources.options = createRuntimeOptions({
    databasePath: path.join(directory, "gateway.sqlite"),
    runtimes: [
      {
        id: "dsh",
        type: "plugin",
        agents: ["main"],
        tokenEnv: "PUSH_PLUGIN_TEST_NATIVE",
        capabilities,
        ...overrides,
      },
    ],
    auth: {
      clients: [
        {
          id: "owner",
          tokenEnv: "PUSH_PLUGIN_TEST_CLIENT",
          targets: [{ runtimeId: "dsh", agentId: "main" }],
        },
      ],
      pushTokenEnv: "PUSH_PLUGIN_TEST_PUSH",
    },
  });
  return { options: resources.options, resources };
}

async function setup(t, overrides) {
  const { options, resources } = configured(t, overrides);
  const server = createGatewayServer(options);
  resources.server = server;
  await new Promise((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  const url = `ws://127.0.0.1:${server.address().port}`;
  async function socketAt(suffix = "") {
    const socket = new WebSocket(url + suffix);
    resources.sockets.push(socket);
    const events = [];
    socket.on("message", (raw) => events.push(JSON.parse(Buffer.from(raw).toString())));
    await once(socket, "open");
    return {
      socket,
      events,
      send: (event) => socket.send(JSON.stringify(event)),
      async wait(predicate) {
        const deadline = Date.now() + 2000;
        while (!events.some(predicate)) {
          assert.ok(Date.now() < deadline, `Missing event matching ${predicate}`);
          await new Promise((resolve) => {
            setTimeout(resolve, 5);
          });
        }
        return events.find(predicate);
      },
    };
  }
  async function browser() {
    const client = await socketAt();
    client.send({ type: "register", token: "fixture-client", protocolVersion: 1 });
    await client.wait((event) => event.type === "registered");
    return client;
  }
  return { options, resources, server, url, socketAt, browser };
}

void test("plugin configuration declares remote agents without native commands or managed homes", async (t) => {
  const { options } = configured(t);
  assert.deepEqual(options.nativeHomes, []);
  assert.deepEqual(options.adapters[0].agents, ["main"]);
  assert.deepEqual(options.adapters[0].capabilities, capabilities);
  assert.throws(() => options.adapters[0].assertAgent("private"), /declared|configured|agent/i);
});

void test(
  "native plugin streams through the authenticated gateway before completing, then replays without rerunning",
  { timeout: 10000 },
  async (t) => {
    const { url, browser, resources } = await setup(t);
    const { connectPlugin } = require("../../../plugins/client.cjs");
    const finish = Promise.withResolvers();
    resources.release.push(() => finish.resolve());
    const calls = [];
    const plugin = await connectPlugin({
      url: `${url}/plugins`,
      runtimeId: "dsh",
      agentId: "main",
      token: "fixture-native",
      capabilities,
      async run(request, { emit, onAccepted }) {
        calls.push(request);
        assert.equal(typeof request.nativeSessionId, "string");
        onAccepted();
        emit({ type: "message.started", data: { messageId: "answer" } });
        emit({ type: "message.delta", data: { messageId: "answer", delta: "实时🌏" } });
        await finish.promise;
        emit({ type: "message.snapshot", data: { messageId: "answer", text: "实时🌏完整回复" } });
        emit({ type: "message.completed", data: { messageId: "answer" } });
      },
    });
    resources.plugins.push(plugin);
    const first = await browser();
    first.send({ type: "session.start", sessionId: "chat", runtimeId: "dsh", agentId: "main" });
    await first.wait((event) => event.type === "session.started");
    first.send({ type: "message", sessionId: "chat", queryMessageId: "request", content: "hello" });
    const delta = await first.wait((event) => event.type === "message.delta");
    assert.equal(delta.data.delta, "实时🌏");
    assert.equal(delta.clientId, "owner");
    assert.equal(delta.runtimeId, "dsh");
    assert.equal(
      first.events.some((event) => event.type === "run.completed"),
      false,
    );
    finish.resolve();
    await first.wait((event) => event.type === "run.completed");
    assert.equal(first.events.filter((event) => event.type === "message.snapshot").length, 1);
    first.socket.close();
    await once(first.socket, "close");
    const resumed = await browser();
    resumed.send({ type: "session.start", sessionId: "chat" });
    await resumed.wait((event) => event.type === "run.completed");
    assert.equal(calls.length, 1);
    assert.equal(resumed.events.filter((event) => event.type === "message.snapshot").length, 1);
  },
);

void test(
  "plugin authentication cannot reuse browser credentials, change targets, or misdeclare capabilities",
  { timeout: 10000 },
  async (t) => {
    const { socketAt } = await setup(t);
    for (const override of [
      { token: "fixture-client" },
      { agentId: "private" },
      { runtimeId: "another-runtime" },
      { capabilities: { ...capabilities, cancel: true } },
    ]) {
      const peer = await socketAt("/plugins");
      const closed = once(peer.socket, "close");
      peer.send({
        type: "plugin.hello",
        runtimeId: "dsh",
        agentId: "main",
        token: "fixture-native",
        capabilities,
        ...override,
      });
      const timer = setTimeout(() => peer.socket.terminate(), 1000);
      const [code] = await closed;
      clearTimeout(timer);
      assert.notEqual(code, 1006, "Plugin handshake must be explicitly rejected");
      assert.notEqual(code, 1000);
      assert.equal(
        peer.events.some((event) => ["registered", "plugin.ready"].includes(event.type)),
        false,
      );
    }
  },
);

void test(
  "gateway native controls project only public input fields and preserve their receipts",
  { timeout: 10000 },
  async (t) => {
    const nativeCapabilities = { ...capabilities, steering: true, followUp: true };
    const { url, browser, resources } = await setup(t, { capabilities: nativeCapabilities });
    const { connectPlugin } = require("../../../plugins/client.cjs");
    const finish = Promise.withResolvers();
    resources.release.push(() => finish.resolve());
    const calls = [];
    const plugin = await connectPlugin({
      url: `${url}/plugins`,
      runtimeId: "dsh",
      agentId: "main",
      token: "fixture-native",
      capabilities: nativeCapabilities,
      async run(request, { onAccepted, emit }) {
        onAccepted();
        emit({ type: "message.started", data: { messageId: "answer" } });
        await finish.promise;
      },
      async control(request, input) {
        assert.equal(request.clientId, "owner");
        assert.equal(request.conversationId, "chat");
        calls.push(input);
        return { status: input.operation === "steer" ? "handled" : "queued" };
      },
    });
    resources.plugins.push(plugin);
    const client = await browser();
    client.send({ type: "session.start", sessionId: "chat", runtimeId: "dsh", agentId: "main" });
    await client.wait((event) => event.type === "session.started");
    client.send({ type: "message", sessionId: "chat", content: "hello" });
    const started = await client.wait((event) => event.type === "message.started");
    for (const [operation, status] of [
      ["steer", "handled"],
      ["follow_up", "queued"],
    ]) {
      client.send({
        type: "input.control",
        sessionId: "chat",
        runId: started.runId,
        controlId: operation,
        operation,
        content: operation,
      });
      const receipt = await client.wait(
        (event) =>
          event.type === "input.control" &&
          event.data.controlId === operation &&
          event.data.status !== "dispatching",
      );
      assert.equal(receipt.data.status, status);
    }
    assert.deepEqual(calls, [
      { operation: "steer", content: "steer" },
      { operation: "follow_up", content: "follow_up" },
    ]);
    finish.resolve();
    await client.wait((event) => event.type === "run.completed");
  },
);

for (const disconnect of [false, true]) {
  void test(
    `native completion preserves an in-transit control before ${disconnect ? "disconnect" : "late rejection"}`,
    { timeout: 10000 },
    async (t) => {
      const nativeCapabilities = { ...capabilities, steering: true };
      const { url, browser, resources, options } = await setup(t, {
        capabilities: nativeCapabilities,
      });
      const { connectPlugin } = require("../../../plugins/client.cjs");
      const finish = Promise.withResolvers();
      const buffered = Promise.withResolvers();
      const nativeCompleted = Promise.withResolvers();
      resources.release.push(() => finish.resolve());
      let releaseControl;
      let nativeSocket;
      const adapter = options.adapters[0];
      const attach = adapter.attachPlugin;
      adapter.attachPlugin = (socket, ...args) => {
        nativeSocket = socket;
        const send = socket.send.bind(socket);
        // Delay only the independent gateway->native direction. Native completion
        // must remain deliverable while the already-written control is in transit.
        socket.send = (raw) => {
          if (JSON.parse(raw).type === "plugin.control") {
            releaseControl = () => send(raw);
            buffered.resolve();
          } else {
            send(raw);
          }
        };
        attach(socket, ...args);
        socket.on("message", (raw) => {
          if (JSON.parse(Buffer.from(raw).toString()).type === "plugin.result") {
            nativeCompleted.resolve();
          }
        });
      };
      const plugin = await connectPlugin({
        url: `${url}/plugins`,
        runtimeId: "dsh",
        agentId: "main",
        token: "fixture-native",
        capabilities: nativeCapabilities,
        async run(request, { onAccepted, emit }) {
          onAccepted();
          emit({ type: "message.started", data: { messageId: "answer" } });
          await finish.promise;
        },
        async control() {
          assert.fail("An input delivered after native completion must be rejected");
        },
      });
      resources.plugins.push(plugin);
      const client = await browser();
      client.send({ type: "session.start", sessionId: "chat", runtimeId: "dsh", agentId: "main" });
      await client.wait((event) => event.type === "session.started");
      client.send({ type: "message", sessionId: "chat", content: "hello" });
      const started = await client.wait((event) => event.type === "message.started");
      client.send({
        type: "input.control",
        sessionId: "chat",
        runId: started.runId,
        controlId: "late",
        operation: "steer",
        content: "late steering",
      });
      await buffered.promise;
      finish.resolve();
      await nativeCompleted.promise;
      if (disconnect) {
        nativeSocket.close(1000, "Controlled connection loss");
      } else if (nativeSocket.readyState === nativeSocket.OPEN) {
        releaseControl();
      }
      const receipt = await client.wait(
        (event) =>
          event.type === "input.control" &&
          event.data.controlId === "late" &&
          event.data.status !== "dispatching",
      );
      const terminal = await client.wait((event) =>
        ["run.completed", "run.failed"].includes(event.type),
      );
      assert.equal(receipt.data.status, disconnect ? "unknown" : "rejected");
      assert.equal(terminal.type, "run.completed");
      assert.ok(client.events.indexOf(receipt) < client.events.indexOf(terminal));
      if (!disconnect) {
        assert.equal(nativeSocket.readyState, nativeSocket.OPEN);
      }
    },
  );
}

void test(
  "an unavailable plugin fails visibly without creating native homes or executing a fallback",
  { timeout: 10000 },
  async (t) => {
    const { browser } = await setup(t);
    const client = await browser();
    client.send({ type: "session.start", sessionId: "chat", runtimeId: "dsh", agentId: "main" });
    await client.wait((event) => event.type === "session.started");
    client.send({
      type: "message",
      sessionId: "chat",
      queryMessageId: "offline",
      content: "hello",
    });
    const failed = await client.wait((event) => event.type === "run.failed");
    assert.match(failed.data.message, /offline|connected|unavailable/i);
    assert.equal(
      client.events.some((event) => event.type === "run.completed"),
      false,
    );
  },
);

void test(
  "native plugin notifications use their own scoped identity and replay once without executing an agent",
  { timeout: 10000 },
  async (t) => {
    const { url, browser, resources, options } = await setup(t);
    const { connectPlugin } = require("../../../plugins/client.cjs");
    let runs = 0;
    const plugin = await connectPlugin({
      url: `${url}/plugins`,
      runtimeId: "dsh",
      agentId: "main",
      token: "fixture-native",
      capabilities,
      async run() {
        runs += 1;
      },
    });
    resources.plugins.push(plugin);
    const first = await browser();
    first.send({ type: "session.start", sessionId: "chat", runtimeId: "dsh", agentId: "main" });
    await first.wait((event) => event.type === "session.started");
    first.socket.close();
    await once(first.socket, "close");
    const target = { clientId: "owner", conversationId: "chat" };
    const notice = { eventId: "native-background-event", content: "原生通知🌏" };
    const receipt = await plugin.notify(target, notice);
    assert.equal(receipt.duplicate, false);
    const duplicate = await plugin.notify(target, notice);
    assert.equal(duplicate.duplicate, true);
    assert.equal(duplicate.runId, receipt.runId);
    await assert.rejects(
      plugin.notify({ ...target, clientId: "stranger" }, notice),
      /target|session|authorized/i,
    );
    await assert.rejects(
      plugin.notify(target, {
        eventId: "lifecycle-injection",
        signal: { type: "run.completed", data: {} },
      }),
      /signal|event|adapter|lifecycle/i,
    );
    const resumed = await browser();
    resumed.send({ type: "session.start", sessionId: "chat" });
    await resumed.wait((event) => event.type === "run.completed" && event.origin === "push");
    const text = resumed.events.filter((event) => event.type === "message.delta");
    assert.equal(text.length, 1);
    assert.equal(text[0].data.delta, notice.content);
    assert.equal(text[0].runtimeId, "dsh");
    assert.equal(text[0].agentId, "main");
    assert.equal(runs, 0);
    options.auth.clients[0].targets = [];
    await assert.rejects(
      plugin.notify(target, { ...notice, eventId: "revoked-target" }),
      /target|authorized/i,
    );
  },
);

for (const tokenEnv of ["PUSH_PLUGIN_TEST_CLIENT", "PUSH_PLUGIN_TEST_PUSH"]) {
  void test(`plugin credentials cannot reuse ${tokenEnv}`, (t) => {
    assert.throws(() => configured(t, { tokenEnv }), /distinct|reuse|token/i);
  });
}

void test(
  "only one live native plugin can own a target and it cannot join browser subscriptions",
  { timeout: 10000 },
  async (t) => {
    const { socketAt } = await setup(t);
    const hello = {
      type: "plugin.hello",
      runtimeId: "dsh",
      agentId: "main",
      token: "fixture-native",
      capabilities,
    };
    const owner = await socketAt("/plugins");
    owner.send(hello);
    await owner.wait((event) => event.type === "plugin.ready");
    const duplicate = await socketAt("/plugins");
    const rejected = once(duplicate.socket, "close");
    duplicate.send(hello);
    const timer = setTimeout(() => duplicate.socket.terminate(), 1000);
    const [code] = await rejected;
    clearTimeout(timer);
    assert.notEqual(code, 1006);
    assert.equal(
      duplicate.events.some((event) => event.type === "plugin.ready"),
      false,
    );
    const closed = once(owner.socket, "close");
    owner.send({ type: "register", token: "fixture-client", agentId: "owner" });
    const ownerTimer = setTimeout(() => owner.socket.terminate(), 1000);
    const [ownerCode] = await closed;
    clearTimeout(ownerTimer);
    assert.notEqual(ownerCode, 1006);
    assert.equal(
      owner.events.some((event) => event.type === "registered"),
      false,
    );
  },
);

void test(
  "a replacement plugin connection cannot finish an old run or receive its prompt again",
  { timeout: 10000 },
  async (t) => {
    const { socketAt, browser } = await setup(t);
    const hello = {
      type: "plugin.hello",
      runtimeId: "dsh",
      agentId: "main",
      token: "fixture-native",
      capabilities,
    };
    const native = await socketAt("/plugins");
    native.send(hello);
    await native.wait((event) => event.type === "plugin.ready");
    const client = await browser();
    client.send({ type: "session.start", sessionId: "chat", runtimeId: "dsh", agentId: "main" });
    await client.wait((event) => event.type === "session.started");
    client.send({
      type: "message",
      sessionId: "chat",
      queryMessageId: "once",
      content: "execute once",
      token: "must-not-be-forwarded",
      command: "must-not-run",
      env: { SECRET: "must-not-be-forwarded" },
      cwd: "/must-not-select",
    });
    const run = await native.wait((event) => event.type === "plugin.run");
    for (const field of ["token", "command", "env", "cwd", "nativeStarted"]) {
      assert.equal(Object.hasOwn(run.request, field), false, field);
    }
    native.send({ type: "plugin.accepted", runId: run.runId });
    native.send({
      type: "plugin.signal",
      runId: run.runId,
      signal: { type: "message.started", data: { messageId: "answer" } },
    });
    await client.wait((event) => event.type === "message.started");
    native.socket.close();
    await once(native.socket, "close");
    await client.wait((event) => event.type === "run.failed");
    const replacement = await socketAt("/plugins");
    replacement.send(hello);
    await replacement.wait((event) => event.type === "plugin.ready");
    assert.equal(
      replacement.events.some((event) => event.type === "plugin.run"),
      false,
    );
    const closed = once(replacement.socket, "close");
    replacement.send({ type: "plugin.result", runId: run.runId, status: "completed" });
    const timer = setTimeout(() => replacement.socket.terminate(), 1000);
    const [code] = await closed;
    clearTimeout(timer);
    assert.notEqual(code, 1006);
    assert.equal(
      client.events.some((event) => event.type === "run.completed"),
      false,
    );
    assert.equal(native.events.filter((event) => event.type === "plugin.run").length, 1);
  },
);

void test(
  "malformed upgrade request targets cannot crash the gateway before authentication",
  { timeout: 10000 },
  async (t) => {
    const { server, resources } = await setup(t);
    const raw = net.connect(server.address().port, "127.0.0.1");
    resources.release.push(() => raw.destroy());
    raw.on("error", () => {});
    await once(raw, "connect");
    const closed = once(raw, "close");
    raw.write(
      `GET //[ HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: ${randomBytes(16).toString("base64")}\r\n\r\n`,
    );
    await closed;
    const health = await fetch(`http://127.0.0.1:${server.address().port}/healthz`);
    assert.equal(health.status, 200);
  },
);
