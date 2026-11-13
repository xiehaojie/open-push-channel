const assert = require("node:assert/strict");
const { EventEmitter } = require("node:events");
const test = require("node:test");
const { createPluginAdapter } = require("./plugin");
const wire = require("../../../protocol/plugin.cjs");

const capabilities = wire.capabilities({ streaming: true, steering: true, cancel: true });
const request = {
  clientId: "owner",
  runtimeId: "dsh",
  agentId: "main",
  conversationId: "chat",
  nativeSessionId: "native",
  runId: "run",
  content: "hello",
};

function setup(t) {
  const adapter = createPluginAdapter({
    id: "dsh",
    agents: ["main"],
    token: "fixture",
    capabilities,
  });
  t.after(() => adapter.close());
  const connect = (
    notify = () => {
      throw new Error("Unexpected notification");
    },
  ) => {
    const socket = new EventEmitter();
    socket.OPEN = 1;
    socket.readyState = 1;
    socket.sent = [];
    socket.send = (raw) => socket.sent.push(JSON.parse(raw));
    socket.close = (code) => {
      socket.code = code;
      socket.readyState = 3;
      socket.emit("close");
    };
    socket.receive = (value) => socket.emit("message", Buffer.from(JSON.stringify(value)));
    adapter.attachPlugin(
      socket,
      { type: "plugin.hello", runtimeId: "dsh", agentId: "main", token: "fixture", capabilities },
      notify,
    );
    return socket;
  };
  return { adapter, connect };
}

for (const rejected of [true, false]) {
  void test(`remote run failure preserves pre-admission rejection=${rejected}`, async (t) => {
    const { adapter, connect } = setup(t);
    const peer = connect();
    let accepted = 0;
    let dispatched = 0;
    const running = adapter.run(request, {
      emit() {},
      onAccepted() {
        accepted++;
      },
      onDispatch() {
        dispatched++;
      },
    });
    const failed = assert.rejects(running, /native failure/);
    peer.receive({
      type: "plugin.result",
      runId: request.runId,
      status: "failed",
      message: "native failure",
      rejected,
    });
    await failed;
    assert.equal(dispatched, 1);
    assert.equal(accepted, rejected ? 0 : 1);
  });
}

void test("detached plugin epochs cannot notify or finish runs on a replacement connection", async (t) => {
  const { adapter, connect } = setup(t);
  let notifications = 0;
  const first = connect(() => {
    notifications++;
  });
  first.emit("error", new Error("transport lost"));
  const replacement = connect();
  first.receive({
    type: "plugin.push",
    id: "stale",
    target: { clientId: "owner", conversationId: "chat" },
    payload: { eventId: "stale", content: "private" },
  });
  assert.equal(notifications, 0);
  assert.equal(first.code, 4002);
  let accepted = 0;
  const running = adapter.run(request, {
    emit() {},
    onAccepted() {
      accepted++;
    },
  });
  replacement.receive({ type: "plugin.accepted", runId: request.runId });
  replacement.receive({ type: "plugin.result", runId: request.runId, status: "completed" });
  await running;
  assert.equal(accepted, 1);
});

void test("gateway abort rejects a pending native control instead of blocking run cleanup", async (t) => {
  const { adapter, connect } = setup(t);
  const peer = connect();
  const controller = new AbortController();
  const running = adapter.run(request, { emit() {}, onAccepted() {}, signal: controller.signal });
  const failedRun = assert.rejects(running, /stopped observing/);
  peer.receive({ type: "plugin.accepted", runId: request.runId });
  const control = adapter.control(request, { operation: "steer", content: "continue" });
  const failedControl = assert.rejects(control, /stopped observing/);
  controller.abort();
  await Promise.all([failedRun, failedControl]);
});

for (const status of ["queued", "handled", "rejected", "unknown"]) {
  void test(`remote input receipts preserve ${status}`, async (t) => {
    const { adapter, connect } = setup(t);
    const peer = connect();
    const running = adapter.run(request, { emit() {}, onAccepted() {} });
    peer.receive({ type: "plugin.accepted", runId: request.runId });
    const pending = adapter.control(request, { operation: "steer", content: "continue" });
    const observed = ["rejected", "unknown"].includes(status)
      ? assert.rejects(pending, (error) => error.rpcRejected === (status === "rejected"))
      : pending.then((receipt) => assert.equal(receipt.status, status));
    const command = peer.sent.find((frame) => frame.type === "plugin.control");
    peer.receive({ type: "plugin.receipt", id: command.id, status, message: "native outcome" });
    await observed;
    peer.receive({ type: "plugin.result", runId: request.runId, status: "completed" });
    await running;
  });
}

void test("cancel targets the active run and only its terminal result completes it", async (t) => {
  const { adapter, connect } = setup(t);
  const peer = connect();
  const running = adapter.run(request, { emit() {}, onAccepted() {} });
  peer.receive({ type: "plugin.accepted", runId: request.runId });
  const cancellation = adapter.cancel(request);
  const command = peer.sent.find((frame) => frame.type === "plugin.cancel");
  peer.receive({ type: "plugin.receipt", id: command.id, status: "handled" });
  await cancellation;
  peer.receive({ type: "plugin.result", runId: request.runId, status: "cancelled" });
  assert.deepEqual(await running, { status: "cancelled" });
});

for (const frame of [
  { type: "plugin.result", runId: request.runId, status: "completed" },
  {
    type: "plugin.signal",
    runId: request.runId,
    signal: { type: "message.started", data: { messageId: "answer" } },
  },
]) {
  void test(`plugin ${frame.type} cannot precede native acceptance`, async (t) => {
    const { adapter, connect } = setup(t);
    const peer = connect();
    const running = adapter.run(request, { emit() {}, onAccepted() {} });
    const failed = assert.rejects(running, /unknown|connection/i);
    peer.receive(frame);
    await failed;
    assert.equal(peer.code, 4002);
  });
}
