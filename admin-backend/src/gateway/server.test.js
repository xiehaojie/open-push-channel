const assert = require("node:assert/strict");
const { once } = require("node:events");
const test = require("node:test");
const WebSocket = require("ws");
const { createGatewayServer } = require("./server");

void test("standalone entry serves the demo, runtime discovery and health without launching agents", async (t) => {
  const server = createGatewayServer({
    adapters: [
      {
        id: "test",
        async run() {
          throw new Error("Must not launch");
        },
      },
    ],
  });
  await new Promise((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  t.after(
    () =>
      new Promise((resolve) => {
        server.close(resolve);
      }),
  );
  const base = `http://127.0.0.1:${server.address().port}`;
  const page = await fetch(`${base}/demo/`);
  assert.equal(page.status, 200);
  assert.match(await page.text(), /session-id-input/);
  const sdk = await fetch(`${base}/demo/sdk.js`);
  assert.match(await sdk.text(), /startSession/);
  const catalog = await fetch(`${base}/api/runtimes`);
  assert.deepEqual(
    (await catalog.json()).map((runtime) => runtime.id),
    ["test"],
  );
  assert.equal((await fetch(`${base}/healthz`)).status, 200);
  assert.equal((await fetch(`${base}/demo/README.md`)).status, 404);
});

function receive(socket, type) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      socket.off("message", onMessage);
      reject(new Error(`Missing ${type}`));
    }, 1500);
    const onMessage = (raw) => {
      const data = Array.isArray(raw) ? Buffer.concat(raw) : Buffer.from(raw);
      const event = JSON.parse(data.toString());
      if (event.type !== type) {
        return;
      }
      clearTimeout(timeout);
      socket.off("message", onMessage);
      resolve(event);
    };
    socket.on("message", onMessage);
  });
}

void test("unauthenticated HTTP and WebSocket requests require the gateway's loopback origin", async (t) => {
  const server = createGatewayServer();
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => server.shutdown());
  const { port } = server.address();
  const base = `http://127.0.0.1:${port}`;
  const alias = `gateway.example.test:${port}`;
  const cases = [
    { origin: "https://app.example.test", status: 403 },
    { origin: `http://${alias}`, host: alias, status: 403 },
    { origin: base, status: 200 },
    { status: 200 },
  ];
  for (const { origin, host, status } of cases) {
    const headers = {
      ...(origin ? { Origin: origin } : {}),
      ...(host ? { Host: host } : {}),
    };
    const response = await fetch(`${base}/api/runtimes`, { headers });
    assert.equal(response.status, status);
    assert.equal(response.headers.get("access-control-allow-origin"), null);
    await response.text();
    const socket = new WebSocket(base.replace("http:", "ws:"), { headers });
    if (status === 403) {
      await assert.rejects(once(socket, "open"), /Unexpected server response: 403/);
      continue;
    }
    await once(socket, "open");
    const closed = once(socket, "close");
    socket.close();
    await closed;
  }
});

void test(
  "standalone gateway serves chat and HTTP push without the admin/database application",
  { timeout: 4000 },
  async (t) => {
    const signals = (text) => [
      { type: "message.started", data: { messageId: "message" } },
      { type: "message.delta", data: { messageId: "message", delta: text } },
      { type: "message.completed", data: { messageId: "message" } },
    ];
    const server = createGatewayServer({
      adapters: [
        {
          id: "test",
          async run(request, { emit }) {
            for (const event of signals(request.content)) {
              emit(event);
            }
          },
        },
      ],
    });
    await new Promise((resolve) => {
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = `127.0.0.1:${server.address().port}`;
    const socket = new WebSocket(`ws://${address}`);
    t.after(async () => {
      socket.close();
      await new Promise((resolve) => {
        server.close(resolve);
      });
    });
    await once(socket, "open");
    const registered = receive(socket, "registered");
    socket.send(JSON.stringify({ type: "register", agentId: "main", sessionId: "chat" }));
    await registered;
    const chat = receive(socket, "stream");
    socket.send(JSON.stringify({ type: "message", sessionId: "chat", content: "hello" }));
    assert.equal((await chat).content, "hello");
    const pushed = receive(socket, "stream");
    const response = await fetch(`http://${address}/send`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ agentId: "main", sessionId: "chat", content: "background" }),
    });
    assert.equal(response.status, 200);
    assert.equal((await pushed).content, "background");
    const bad = await fetch(`http://${address}/api/send`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    assert.equal(bad.status, 400);
  },
);
