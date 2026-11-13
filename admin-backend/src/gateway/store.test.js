const assert = require("node:assert/strict");
const { mkdtempSync, rmSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { test } = require("node:test");
const { GatewayStore } = require("./store");

const session = {
  clientId: "client-a",
  conversationId: "conversation-a",
  runtimeId: "pi",
  agentId: "assistant",
  nativeSessionId: "native-session-a",
  nativeStarted: false,
};
const request = { ...session, requestId: "request-a", content: "Hello", mentions: [] };
function event(runId, seq, type = "run.started", context = request) {
  return {
    version: 1,
    eventId: `${runId}-${seq}`,
    runtimeId: context.runtimeId,
    agentId: context.agentId,
    clientId: context.clientId,
    conversationId: context.conversationId,
    runId,
    requestId: context.requestId,
    seq,
    timestamp: Date.now(),
    origin: "prompt",
    audience: "conversation",
    type,
    data: {},
  };
}
function diskStore(t) {
  const directory = mkdtempSync(join(tmpdir(), "push-gateway-store-"));
  const path = join(directory, "nested", "gateway.sqlite");
  let store = new GatewayStore({ path });
  t.after(() => {
    store.close();
    rmSync(directory, { recursive: true, force: true });
  });
  return {
    get store() {
      return store;
    },
    reopen() {
      store.close();
      store = new GatewayStore({ path });
      return store;
    },
  };
}

void test("session runtime, owner and native identity survive reopen and cannot be rebound", (t) => {
  const disk = diskStore(t);
  disk.store.saveSession(session);
  disk.store.saveSession({ ...session, nativeStarted: true });
  disk.store.saveSession(session);
  assert.deepEqual(disk.reopen().sessions(), [{ ...session, nativeStarted: true }]);
  for (const field of ["runtimeId", "agentId", "nativeSessionId"]) {
    assert.throws(
      () => disk.store.saveSession({ ...session, [field]: "different" }),
      /bound|binding/i,
    );
  }
});

void test("request IDs deduplicate across restart and reject changed execution input", (t) => {
  const disk = diskStore(t);
  assert.deepEqual(disk.store.claimRun(request, "run-a"), {
    fresh: true,
    runId: "run-a",
    events: [],
  });
  assert.deepEqual(disk.reopen().claimRun({ ...request, replyId: "new-reply" }, "run-b"), {
    fresh: false,
    runId: "run-a",
    status: "accepted",
    events: [],
  });
  for (const changed of [
    { content: "Other" },
    { runtimeId: "deepseek" },
    { agentId: "other" },
    { mentions: ["other"] },
  ]) {
    assert.throws(
      () => disk.store.claimRun({ ...request, ...changed }, "run-c"),
      /requestId.*conflict/i,
    );
  }
  disk.store.appendEvent(event("run-a", 1, "run.completed"));
  assert.equal(disk.reopen().claimRun(request, "run-d").status, "completed");
});

void test("dedupe belongs to a client and conversation; missing request IDs accept each run", (t) => {
  const { store } = diskStore(t);
  assert.equal(store.claimRun(request, "run-a").fresh, true);
  assert.equal(store.claimRun({ ...request, clientId: "client-b" }, "run-b").fresh, true);
  assert.equal(
    store.claimRun({ ...request, conversationId: "conversation-b" }, "run-c").fresh,
    true,
  );
  assert.equal(store.claimRun({ ...request, requestId: undefined }, "run-d").fresh, true);
  assert.equal(store.claimRun({ ...request, requestId: undefined }, "run-e").fresh, true);
});

void test("persisted cursor replay is ordered, exclusive and scoped to the client", (t) => {
  const disk = diskStore(t);
  const first = disk.store.appendEvent(event("run-a", 1));
  const foreign = { ...request, clientId: "client-b" };
  const hidden = disk.store.appendEvent(event("run-b", 1, "run.started", foreign));
  const last = disk.store.appendEvent(event("run-a", 2, "run.completed"));
  assert.ok(first.cursor < hidden.cursor && hidden.cursor < last.cursor);
  assert.deepEqual(disk.reopen().events("client-a", "conversation-a"), [first, last]);
  assert.deepEqual(disk.store.events("client-a", "conversation-a", first.cursor), [last]);
  assert.deepEqual(disk.store.events("client-b", "conversation-a"), [hidden]);
  assert.deepEqual(disk.store.events("client-a", "other"), []);
});

void test("restart marks interrupted runs unknown exactly once without replaying execution", (t) => {
  const disk = diskStore(t);
  disk.store.claimRun(request, "run-a");
  disk.store.appendEvent(event("run-a", 1));
  disk.store.appendEvent({
    ...event("run-a", 3, "message.delta"),
    data: { messageId: "message-a", delta: "partial" },
  });
  const recovered = disk.reopen().recoverInterrupted();
  assert.equal(recovered.length, 1);
  assert.equal(recovered[0].type, "run.failed");
  assert.equal(recovered[0].runId, "run-a");
  assert.equal(recovered[0].seq, 4);
  assert.equal(recovered[0].clientId, "client-a");
  assert.equal(recovered[0].requestId, "request-a");
  assert.match(recovered[0].data.message, /outcome is unknown.*not replayed/);
  assert.deepEqual(disk.reopen().recoverInterrupted(), []);
  assert.equal(disk.store.claimRun(request, "run-b").status, "failed");
  assert.deepEqual(
    disk.store.events("client-a", "conversation-a").map((item) => item.seq),
    [1, 3, 4],
  );
});

void test("unstarted queued runs recover but completed, cancelled and deferred runs do not", (t) => {
  const { store } = diskStore(t);
  store.claimRun(request, "queued");
  for (const [index, type] of [
    "run.completed",
    "run.cancelled",
    "run.deferred",
    "run.failed",
  ].entries()) {
    const context = { ...request, requestId: `request-${index}` };
    store.claimRun(context, `terminal-${index}`);
    store.appendEvent({
      ...event(`terminal-${index}`, 1, type, context),
      data: type === "run.deferred" || type === "run.failed" ? { message: "Done" } : {},
    });
  }
  const recovered = store.recoverInterrupted();
  assert.equal(recovered.length, 1);
  assert.equal(recovered[0].runId, "queued");
  assert.equal(recovered[0].seq, 1);
});

void test("an admitted external task interrupted before dispatch reports failure, never silently retries", (t) => {
  const disk = diskStore(t);
  const task = { ...request, origin: "task", requestId: "task:interrupted" };
  disk.store.saveSession(session);
  disk.store.claimRun(task, "queued-task");
  const recovered = disk.reopen().recoverInterrupted();
  assert.equal(recovered[0].origin, "task");
  assert.equal(recovered[0].type, "run.failed");
  assert.match(recovered[0].data.message, /not replayed/);
  assert.equal(disk.store.claimRun(task, "retry-task").status, "failed");
  assert.equal(disk.store.sessions()[0].nativeStarted, false);
});

void test("proactive request dedupe rejects a changed structured notification", (t) => {
  const { store } = diskStore(t);
  const push = {
    ...request,
    origin: "push",
    event: { type: "subagent.result", data: { agentId: "child", text: "original" } },
  };
  store.claimRun(push, "push-a");
  assert.equal(store.claimRun(push, "push-b").fresh, false);
  assert.throws(
    () =>
      store.claimRun(
        { ...push, event: { ...push.event, data: { agentId: "child", text: "changed" } } },
        "push-c",
      ),
    /requestId.*conflict/i,
  );
});

void test("recovery does not mark queued or initializing sessions as natively started", (t) => {
  const disk = diskStore(t);
  const untouched = {
    ...session,
    conversationId: "untouched",
    nativeSessionId: "native-untouched",
  };
  const finished = { ...session, conversationId: "finished", nativeSessionId: "native-finished" };
  const notification = {
    ...session,
    conversationId: "notification",
    nativeSessionId: "native-notification",
  };
  disk.store.saveSession(session);
  disk.store.saveSession(untouched);
  disk.store.saveSession(finished);
  disk.store.saveSession(notification);
  disk.store.claimRun({ ...request, ...notification, origin: "push" }, "notification");
  disk.store.claimRun(request, "unknown");
  const finishedRequest = { ...request, ...finished };
  disk.store.claimRun(finishedRequest, "finished");
  disk.store.appendEvent(event("finished", 1, "run.completed", finishedRequest));
  disk.reopen().recoverInterrupted();
  const saved = new Map(disk.store.sessions().map((item) => [item.conversationId, item]));
  assert.equal(saved.get(session.conversationId).nativeStarted, false);
  assert.equal(saved.get(untouched.conversationId).nativeStarted, false);
  assert.equal(saved.get(finished.conversationId).nativeStarted, false);
  assert.equal(saved.get(notification.conversationId).nativeStarted, false);
  assert.deepEqual(disk.reopen().recoverInterrupted(), []);
  assert.equal(
    disk.store.sessions().find((item) => item.conversationId === session.conversationId)
      .nativeStarted,
    false,
  );
});

void test("dispatch intent survives a crash for chat and external tasks, never queued notifications", (t) => {
  const disk = diskStore(t);
  disk.store.saveSession(session);
  disk.store.claimRun(request, "dispatching");
  disk.store.markDispatched("dispatching");
  assert.equal(disk.store.claimRun(request, "duplicate").status, "dispatching");
  const notification = {
    ...session,
    conversationId: "notification",
    nativeSessionId: "notification-native",
  };
  const push = { ...request, ...notification, origin: "push" };
  disk.store.saveSession(notification);
  disk.store.claimRun(push, "notification");
  const task = { ...session, conversationId: "task", nativeSessionId: "task-native" };
  disk.store.saveSession(task);
  disk.store.claimRun({ ...request, ...task, origin: "task", requestId: "task:external" }, "task");
  disk.store.markDispatched("task");
  const recovered = disk.reopen().recoverInterrupted();
  assert.deepEqual(recovered.map((item) => item.runId).toSorted(), [
    "dispatching",
    "notification",
    "task",
  ]);
  const saved = new Map(disk.store.sessions().map((item) => [item.conversationId, item]));
  assert.equal(saved.get(session.conversationId).nativeStarted, true);
  assert.equal(saved.get(notification.conversationId).nativeStarted, false);
  assert.equal(saved.get(task.conversationId).nativeStarted, true);
  assert.equal(disk.store.claimRun(request, "duplicate").status, "failed");
  disk.store.markDispatched("dispatching");
  assert.equal(disk.store.claimRun(request, "duplicate").status, "failed");
  assert.deepEqual(disk.reopen().recoverInterrupted(), []);
  assert.equal(
    disk.store.sessions().find((item) => item.conversationId === session.conversationId)
      .nativeStarted,
    true,
  );
});

void test("two clients may persist independent sessions with the same conversation ID", (t) => {
  const disk = diskStore(t);
  const other = {
    ...session,
    clientId: "client-b",
    runtimeId: "deepseek",
    nativeSessionId: "native-b",
  };
  disk.store.saveSession(session);
  disk.store.saveSession(other);
  disk.store.saveSession({ ...other, nativeStarted: true });
  assert.deepEqual(disk.reopen().sessions(), [session, { ...other, nativeStarted: true }]);
  disk.store.claimRun(request, "client-a-run");
  disk.store.markDispatched("client-a-run");
  disk.store.recoverInterrupted();
  assert.deepEqual(disk.store.sessions(), [
    { ...session, nativeStarted: true },
    { ...other, nativeStarted: true },
  ]);
});

void test("run admission atomically persists initial input events and dedupe never appends them twice", (t) => {
  const disk = diskStore(t);
  const input = { ...event("atomic", 1, "input.accepted"), data: { content: request.content } };
  const claim = disk.store.claimRun(request, "atomic", [input]);
  assert.equal(claim.fresh, true);
  assert.equal(claim.events.length, 1);
  assert.equal(claim.events[0].eventId, input.eventId);
  assert.ok(claim.events[0].cursor > 0);
  assert.deepEqual(disk.reopen().events(request.clientId, request.conversationId), claim.events);
  assert.deepEqual(disk.store.claimRun(request, "duplicate", [event("duplicate", 1)]), {
    fresh: false,
    runId: "atomic",
    status: "accepted",
    events: [],
  });
  assert.deepEqual(disk.store.events(request.clientId, request.conversationId), claim.events);
});

void test("invalid or failed initial event inserts roll back the claim and every partial event", (t) => {
  const { store } = diskStore(t);
  for (const [index, failure] of ["invalid", "duplicate", "scope"].entries()) {
    const context = { ...request, conversationId: `atomic-${index}` };
    const runId = `run-${index}`;
    const first = event(runId, 1, "run.started", context);
    const invalid =
      failure === "invalid"
        ? { ...event(runId, 2, "run.failed", context), data: {} }
        : failure === "duplicate"
          ? { ...event(runId, 2, "run.completed", context), eventId: first.eventId }
          : { ...event(runId, 2, "run.completed", context), clientId: "another-client" };
    assert.throws(() => store.claimRun(context, runId, [first, invalid]));
    assert.deepEqual(store.events(context.clientId, context.conversationId), []);
    const retried = store.claimRun(context, runId, [first]);
    assert.equal(retried.fresh, true);
    assert.deepEqual(store.events(context.clientId, context.conversationId), retried.events);
  }
});

void test("proactive event batches commit their terminal status with the dedupe claim", (t) => {
  const disk = diskStore(t);
  const push = { ...request, origin: "push" };
  const initial = [event("push-atomic", 1), event("push-atomic", 2, "run.completed")].map((item) =>
    Object.assign(item, { origin: "push" }),
  );
  const claim = disk.store.claimRun(push, "push-atomic", initial);
  assert.equal(claim.events.length, 2);
  assert.equal(claim.events[0].cursor < claim.events[1].cursor, true);
  assert.deepEqual(disk.reopen().claimRun(push, "another-push", initial), {
    fresh: false,
    runId: "push-atomic",
    status: "completed",
    events: [],
  });
  assert.deepEqual(disk.store.recoverInterrupted(), []);
});

void test("control receipts survive reopen, select the latest status and stay client scoped", (t) => {
  const disk = diskStore(t);
  disk.store.claimRun(request, "active");
  const controlEvent = (seq, status, context = request) => ({
    ...event("active", seq, "input.control", context),
    data: { controlId: "control", operation: "steer", content: "adjust", status },
  });
  disk.store.appendEvent(controlEvent(1, "dispatching"));
  const queued = disk.store.appendEvent(controlEvent(2, "queued"));
  disk.store.appendEvent(controlEvent(3, "handled", { ...request, clientId: "foreign" }));
  assert.deepEqual(disk.reopen().controlReceipt("client-a", "conversation-a", "control"), queued);
  assert.equal(disk.store.controlReceipt("unknown", "conversation-a", "control"), undefined);
  assert.equal(disk.store.controlReceipt("client-a", "other", "control"), undefined);
});

void test("restart journals an unknown control receipt before closing its interrupted parent", (t) => {
  const disk = diskStore(t);
  disk.store.claimRun(request, "interrupted-control");
  disk.store.appendEvent({
    ...event("interrupted-control", 1, "input.control"),
    data: {
      controlId: "lost-receipt",
      operation: "follow_up",
      content: "later",
      status: "dispatching",
    },
  });
  disk.reopen().recoverInterrupted();
  const events = disk.store.events(request.clientId, request.conversationId);
  assert.deepEqual(
    events.map((item) => [item.type, item.data.status]),
    [
      ["input.control", "dispatching"],
      ["input.control", "unknown"],
      ["run.failed", undefined],
    ],
  );
  assert.equal(
    disk.store.controlReceipt(request.clientId, request.conversationId, "lost-receipt").data.status,
    "unknown",
  );
  assert.deepEqual(disk.reopen().recoverInterrupted(), []);
});
