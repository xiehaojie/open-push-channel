const assert = require("node:assert/strict");
const { fork } = require("node:child_process");
const { randomUUID } = require("node:crypto");
const { once } = require("node:events");
const { mkdtempSync, rmSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const test = require("node:test");
const WebSocket = require("ws");
const { createGatewayServer } = require("./server.ts");

function inbox(source, name, decode = (value) => value) {
  const events = [];
  source.on(name, (value) => events.push(decode(value)));
  return {
    events,
    async wait(predicate) {
      const signal = AbortSignal.timeout(5000);
      while (!events.some(predicate)) {
        await once(source, name, { signal });
      }
      return events.find(predicate);
    },
  };
}

void test("SIGKILL during an HTTP task preserves progress and fails recovery without reexecution", { timeout: 20000 }, async (t) => {
  const directory = mkdtempSync(join(tmpdir(), "push-task-crash-"));
  const databasePath = join(directory, "gateway.sqlite");
  const auth = {
    clients: [{
      id: "owner", token: randomUUID(), targets: [{ runtimeId: "pi", agentId: "main" }],
    }],
    pushToken: randomUUID(),
  };
  const child = fork(join(__dirname, "fixtures/task-crash-server.cts"), [], {
    execArgv: [], stdio: ["ignore", "inherit", "inherit", "ipc"],
  });
  const exited = once(child, "exit");
  void exited.catch(() => {});
  async function stopChild() {
    if (child.exitCode !== null || child.signalCode !== null || !child.pid) return exited;
    const stopped = once(child, "exit", { signal: AbortSignal.timeout(5000) });
    child.kill("SIGKILL");
    return stopped;
  }
  const ipc = inbox(child, "message");
  const sockets = [];
  let server;
  t.after(async () => {
    const cleanup = await Promise.allSettled([
      stopChild(),
      ...sockets.filter((socket) => socket.readyState !== WebSocket.CLOSED).map((socket) =>
        new Promise((resolve, reject) => {
          const timer = setTimeout(() => reject(new Error("Socket did not close")), 5000);
          socket.once("close", () => { clearTimeout(timer); resolve(); });
          // Terminating a CONNECTING ws emits error before close; still await close.
          socket.once("error", () => {});
          socket.terminate();
        }),
      ),
      server?.shutdown(),
    ]);
    const failures = cleanup.filter((result) => result.status === "rejected");
    if (failures.length) throw new AggregateError(failures.map((result) => result.reason), "Cleanup failed");
    // Remove the database only after its child and parent owners have closed.
    rmSync(directory, { recursive: true, force: true });
  });
  const beforeExit = (promise) => Promise.race([
    promise,
    exited.then(([code, signal]) => { throw new Error(`Fixture exited early: ${code ?? signal}`); }),
  ]);
  child.send({ auth, databasePath });
  const ready = await beforeExit(ipc.wait((event) => event.type === "ready"));
  let base = `http://127.0.0.1:${ready.port}`;
  async function connect(start) {
    const socket = new WebSocket(base.replace("http:", "ws:"));
    sockets.push(socket);
    const messages = inbox(socket, "message", (raw) => JSON.parse(Buffer.from(raw).toString()));
    await once(socket, "open", { signal: AbortSignal.timeout(5000) });
    const send = (data) => socket.send(JSON.stringify(data));
    send({ type: "register", token: auth.clients[0].token, protocolVersion: 1 });
    await messages.wait((event) => event.type === "registered");
    send({ type: "session.start", sessionId: "chat", ...start });
    const binding = await messages.wait((event) => event.type === "session.started");
    const barrier = async () => {
      const count = messages.events.filter((event) => event.type === "pong").length;
      send({ type: "ping" });
      await messages.wait(() => messages.events.filter((event) => event.type === "pong").length > count);
    };
    return { ...messages, binding, barrier };
  }
  const browser = await connect({ runtimeId: "pi", agentId: "main" });
  const task = { clientId: "owner", sessionId: "chat", eventId: "crash-job", content: "work" };
  const post = () => fetch(`${base}/api/tasks`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${auth.pushToken}` },
    body: JSON.stringify(task),
    signal: AbortSignal.timeout(5000),
  });
  const accepted = await post();
  assert.equal(accepted.status, 202);
  const receipt = await accepted.json();
  assert.equal(receipt.duplicate, false);
  assert.equal(receipt.status, "accepted");
  const inflight = await beforeExit(ipc.wait((event) => event.type === "inflight"));
  assert.equal(inflight.runId, receipt.runId);
  const partial = await browser.wait((event) => event.type === "message.delta");
  assert.equal(partial.runId, receipt.runId);
  assert.equal(partial.data.delta, "before crash");
  await browser.barrier();
  assert.equal(browser.events.some((event) => event.type === "run.completed" || event.type === "run.failed"), false);
  // A native effect may precede its completion record (Temporal's idempotence discussion).
  // This gateway records uncertainty instead of automatically retrying the task.
  // https://github.com/temporalio/documentation/blob/a069a5f1359e95539a51280896c19185894bfec6/docs/best-practices/error-handling.mdx#idempotence
  assert.deepEqual(await stopChild(), [null, "SIGKILL"]);

  let calls = 0;
  let failed;
  for (let restart = 0; restart < 2; restart += 1) {
    server = createGatewayServer({
      auth, databasePath, adapters: [{ id: "pi", async run() { calls += 1; } }],
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening", { signal: AbortSignal.timeout(5000) });
    base = `http://127.0.0.1:${server.address().port}`;
    const resumed = await connect();
    assert.deepEqual(resumed.binding, browser.binding);
    await resumed.barrier();
    const events = resumed.events.filter((event) => event.runId === receipt.runId);
    assert.deepEqual(events.map((event) => event.type), [
      "input.accepted", "run.started", "message.started", "message.delta", "run.failed",
    ]);
    assert.deepEqual(events.find((event) => event.type === "message.delta"), partial);
    assert.ok(events.every((event) => event.origin === "task" && event.requestId === "task:crash-job"));
    const terminal = events.at(-1);
    assert.match(terminal.data.message, /outcome is unknown/);
    assert.match(terminal.data.message, /not replayed/);
    if (failed) assert.deepEqual(terminal, failed);
    failed = terminal;
    const duplicate = await post();
    assert.equal(duplicate.status, 202);
    assert.deepEqual(await duplicate.json(), { ...receipt, duplicate: true, status: "failed" });
    await resumed.barrier();
    assert.equal(resumed.events.filter((event) => event.type === "run.failed").length, 1);
    assert.equal(calls, 0);
    await server.shutdown();
    server = undefined;
  }
});
