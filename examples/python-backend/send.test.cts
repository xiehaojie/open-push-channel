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

void test("Python backend delivers notifications and tasks without duplicate execution or redirects", { timeout: 30000 }, async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "push-python-backend-"));
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
  const invoke = (mode, event, content, options = {}) => execute(process.env.PYTHON ?? "python3", [
    join(__dirname, "send.py"),
    "--gateway", options.gateway ?? gateway,
    "--mode", mode, "--client", "demo", "--session", options.session ?? "quickstart",
    "--runtime", "openclaw", "--agent", "main", "--event", event, "--content", content,
  ], {
    env: { ...process.env, PUSH_CHANNEL_PUSH_TOKEN: options.token ?? pushToken },
    timeout: 15000,
    killSignal: "SIGKILL",
  });

  const embedded = async (overrides = {}, serverToken = pushToken) => JSON.parse((await execute(
    process.env.PYTHON ?? "python3", ["-c", [
      "import json, os, sys",
      "from send import send_request",
      "try:",
      "    result = send_request(token=os.environ['PUSH_CHANNEL_PUSH_TOKEN'], **json.loads(sys.argv[1]))",
      "except (ValueError, RuntimeError) as error:",
      "    print(json.dumps({'error': type(error).__name__, 'message': str(error)}))",
      "else:",
      "    print(json.dumps(result))",
    ].join("\n"), JSON.stringify({
      gateway, mode: "send", client: "demo", session: "quickstart", agent: "main",
      event: "embedded-notice", content: "Embedded notification 🌏", ...overrides,
    })],
    { cwd: __dirname, env: { ...process.env, PUSH_CHANNEL_PUSH_TOKEN: serverToken, PYTHONDONTWRITEBYTECODE: "1" }, timeout: 15000, killSignal: "SIGKILL" },
  )).stdout);

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

  assert.equal(await embedded(), null);
  const task = await embedded({ mode: "task", event: "embedded-task", content: "Embedded task 🌏" });
  assert.equal(typeof task.runId, "string");
  assert.equal(typeof task.status, "string");
  assert.equal(task.duplicate, false);
  await wait((event) => event.type === "run.completed" && event.runId === task.runId);
  assert.equal(events.find((event) => event.type === "message.snapshot" && event.runId === task.runId).data.text, "Embedded task 🌏");
  assert.equal(events.filter((event) => event.type === "message.delta" && event.origin === "push").at(-1).data.delta, "Embedded notification 🌏");
  assert.equal(await embedded(), null);
  assert.deepEqual(await embedded({ mode: "task", event: "embedded-task", content: "Embedded task 🌏" }), { ...task, duplicate: true, status: "completed" });
  // Pong arrives after earlier notification frames, including any erroneous retry delivery.
  const pongs = events.filter((event) => event.type === "pong").length;
  socket.send(JSON.stringify({ type: "ping" }));
  await wait((event) => event.type === "pong" && events.filter((item) => item.type === "pong").length > pongs);
  assert.equal(calls, 2);
  assert.equal(events.filter((event) => event.type === "message.delta" && event.origin === "push").length, 2);
  for (const invalid of [{ content: " " }, { client: null }, { session: 1 }, { agent: [] }, { event: {} }, { runtime: false }, { gateway: null }, { mode: "invalid" }]) {
    assert.equal((await embedded(invalid)).error, "ValueError");
  }
  for (const [overrides, serverToken, status] of [[{}, "invalid-token", 401], [{ session: "missing" }, pushToken, 404]]) {
    const failure = await embedded(overrides, serverToken);
    assert.equal(failure.error, "RuntimeError");
    assert.match(failure.message, new RegExp(`HTTP ${status}`));
    assert.ok(!failure.message.includes(serverToken));
  }

  let redirected = 0;
  let redirectStatus = 301;
  let taskBody = "{}";
  redirect = createServer((request, response) => {
    if (request.url === "/api/send") {
      response.writeHead(redirectStatus, { Location: "/capture" });
    } else if (request.url === "/api/tasks") {
      response.writeHead(202, { "Content-Type": "application/json" });
      response.end(taskBody);
      return;
    } else {
      redirected += 1;
      response.writeHead(200);
    }
    response.end();
  });
  redirect.listen(0, "127.0.0.1");
  await once(redirect, "listening");
  const fixture = `http://127.0.0.1:${redirect.address().port}`;
  for (const status of [301, 302, 303, 307, 308]) {
    redirectStatus = status;
    await assert.rejects(invoke("send", "redirect", "notice", { gateway: fixture }), new RegExp(`HTTP ${status}`));
  }
  for (const body of ["invalid JSON", "{}", '{"runId":"run","status":"accepted","duplicate":"false"}']) {
    taskBody = body;
    await assert.rejects(invoke("task", "invalid-receipt", "notice", { gateway: fixture }), /invalid JSON|Invalid task receipt/);
  }
  assert.equal((await embedded({ gateway: fixture })).error, "RuntimeError");
  assert.equal((await embedded({ gateway: fixture, mode: "task" })).error, "RuntimeError");
  assert.equal(redirected, 0);
  await assert.rejects(invoke("send", "invalid", " "), /must be nonempty/);
  await assert.rejects(invoke("send", "invalid", "notice", { token: "" }), /valid gateway server token/);
  for (const invalid of ["file:///tmp/gateway", "http://user:secret@127.0.0.1", `${gateway}?secret=hidden`, `${gateway}#fragment`, `${gateway}\t`, `${gateway}\n`]) {
    await assert.rejects(invoke("send", "invalid", "notice", { gateway: invalid }), /HTTP or HTTPS base URL/);
  }
});
