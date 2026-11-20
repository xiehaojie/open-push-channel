const assert = require("node:assert/strict");
const { execFile } = require("node:child_process");
const { randomUUID } = require("node:crypto");
const { once } = require("node:events");
const { mkdtemp, rm } = require("node:fs/promises");
const { createServer } = require("node:http");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { setTimeout: delay } = require("node:timers/promises");
const test = require("node:test");
const { promisify } = require("node:util");
const { createGatewayServer } = require("../../admin-backend/src/gateway/server.ts");

void test("Go backend delivers notifications and tasks without duplicate execution or redirects", { timeout: 120000 }, async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "push-go-"));
  let server;
  let socket;
  let redirect;
  t.after(async () => {
    try {
      if (socket && socket.readyState === WebSocket.OPEN) {
        const closed = once(socket, "close");
        socket.close();
        await closed;
      }
      if (redirect) await new Promise((resolve) => redirect.close(resolve));
      if (server) await server.shutdown();
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
  const execute = promisify(execFile);
  const binary = join(directory, process.platform === "win32" ? "backend.exe" : "backend");
  await execute("go", ["build", "-o", binary, join(__dirname, "main.go")], { timeout: 90000 });
  const token = randomUUID();
  const pushToken = randomUUID();
  let calls = 0;
  server = createGatewayServer({
    databasePath: join(directory, "gateway.sqlite"),
    auth: {
      clients: [{ id: "demo", token, targets: [{ runtimeId: "openclaw", agentId: "main" }] }],
      pushToken,
    },
    adapters: [{
      id: "openclaw",
      async run(request, { emit, onAccepted }) {
        calls += 1;
        onAccepted();
        emit({ type: "message.started", data: { messageId: "answer" } });
        emit({ type: "message.snapshot", data: { messageId: "answer", text: request.content } });
        emit({ type: "message.completed", data: { messageId: "answer" } });
      },
    }],
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const gateway = `http://127.0.0.1:${server.address().port}`;
  const events = [];
  socket = new WebSocket(gateway.replace("http:", "ws:"));
  socket.addEventListener("message", (event) => events.push(JSON.parse(event.data)));
  await once(socket, "open");
  const wait = async (predicate) => {
    const deadline = Date.now() + 3000;
    while (!events.some(predicate)) {
      assert.ok(Date.now() < deadline, `Missing gateway event: ${predicate}`);
      await delay(5);
    }
    return events.find(predicate);
  };
  socket.send(JSON.stringify({ type: "register", token, protocolVersion: 1 }));
  await wait((event) => event.type === "registered");
  socket.send(JSON.stringify({
    type: "session.start", sessionId: "quickstart", runtimeId: "openclaw", agentId: "main",
  }));
  await wait((event) => event.type === "session.started");
  const invoke = (mode, event, content, options = {}) => execute(binary, [
    "-gateway", options.gateway ?? gateway,
    "-mode", mode, "-client", "demo", "-session", options.session ?? "quickstart",
    "-runtime", "openclaw", "-agent", "main", "-event", event, "-content", content,
  ], {
    env: { ...process.env, PUSH_CHANNEL_PUSH_TOKEN: options.token ?? pushToken },
    timeout: 15000,
    killSignal: "SIGKILL",
  });

  assert.match((await invoke("send", "notice", "工单已处理 🌏")).stdout, /Notification accepted/);
  await wait((event) => event.type === "run.completed" && event.origin === "push");
  await invoke("send", "notice", "工单已处理 🌏");
  const text = events.filter((event) => event.type === "message.delta" && event.origin === "push");
  assert.equal(text.length, 1);
  assert.equal(text[0].data.delta, "工单已处理 🌏");
  assert.equal(calls, 0);

  const receipt = JSON.parse((await invoke("task", "summary", "总结处理结果 🌏")).stdout);
  assert.equal(typeof receipt.runId, "string");
  assert.ok(receipt.runId);
  assert.equal(receipt.duplicate, false);
  await wait((event) => event.type === "run.completed" && event.runId === receipt.runId);
  const duplicate = JSON.parse((await invoke("task", "summary", "总结处理结果 🌏")).stdout);
  assert.equal(duplicate.runId, receipt.runId);
  assert.equal(duplicate.duplicate, true);
  assert.equal(duplicate.status, "completed");
  const result = events.find((event) => event.type === "message.snapshot" && event.runId === receipt.runId);
  assert.equal(result.data.text, "总结处理结果 🌏");
  assert.equal(result.origin, "task");
  assert.equal(calls, 1);
  // Task completion is an ordered socket barrier: all earlier notification
  // frames have arrived before checking that a retry produced no second one.
  assert.equal(events.filter((event) => event.type === "message.delta" && event.origin === "push").length, 1);
  await assert.rejects(invoke("task", "summary", "changed"), /HTTP 400/);
  await assert.rejects(invoke("send", "unauthorized", "notice", { token: randomUUID() }), /HTTP 401/);
  await assert.rejects(invoke("send", "missing", "notice", { session: "missing" }), /HTTP 404/);
  assert.equal(calls, 1);

  let redirected = 0;
  redirect = createServer((request, response) => {
    if (request.url === "/api/send") {
      response.writeHead(307, { Location: "/capture" });
    } else {
      redirected += 1;
      response.writeHead(200);
    }
    response.end();
  });
  redirect.listen(0, "127.0.0.1");
  await once(redirect, "listening");
  await assert.rejects(invoke("send", "redirect", "notice", {
    gateway: `http://127.0.0.1:${redirect.address().port}`,
  }), /HTTP 307/);
  assert.equal(redirected, 0);
});
