const assert = require("node:assert/strict");
const test = require("node:test");
const pushController = require("./pushController");
const { connections } = require("../websocket/index");

function createOpenSocket(messages) {
  return {
    OPEN: 1,
    readyState: 1,
    agentId: "main",
    sessionIds: new Set(["session-1"]),
    send(message) {
      messages.push(JSON.parse(message));
    },
  };
}

function waitFor(predicate, label) {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const check = () => {
      if (predicate()) {
        resolve();
        return;
      }
      if (Date.now() - started > 1500) {
        reject(new Error(`Timed out waiting for ${label}`));
        return;
      }
      setTimeout(check, 10);
    };
    check();
  });
}

void test("send assigns a distinct answer id to each outbound stream", async (t) => {
  const messages = [];
  connections.set("demo-agent", new Set([createOpenSocket(messages)]));
  t.after(() => {
    connections.delete("demo-agent");
  });

  await pushController.send({
    request: { body: { agentId: "demo-agent", sessionId: "session-1", content: "first" } },
  });
  await pushController.send({
    request: { body: { agentId: "demo-agent", sessionId: "session-1", content: "second" } },
  });

  await waitFor(
    () => messages.filter((message) => message.type === "stream_end").length === 2,
    "two stream completions",
  );

  const starts = messages.filter((message) => message.type === "stream_start");
  assert.equal(starts.length, 2);
  assert.notEqual(starts[0].answerMessageId, starts[1].answerMessageId);

  const streamIds = new Set(
    messages
      .filter((message) => message.type === "stream")
      .map((message) => message.answerMessageId),
  );
  assert.deepEqual(streamIds, new Set(starts.map((message) => message.answerMessageId)));
});

void test("send falls back to the socket subscribed to the session id", async (t) => {
  const messages = [];
  connections.set("main", new Set([createOpenSocket(messages)]));
  t.after(() => {
    connections.delete("main");
  });

  const ctx = {
    request: {
      body: {
        agentId: "session-1",
        sessionId: "session-1",
        content: "async result",
      },
    },
  };

  await pushController.send(ctx);

  assert.equal(ctx.status, 200);
  assert.equal(ctx.body, "Sent");

  await waitFor(
    () => messages.some((message) => message.type === "stream_end"),
    "fallback stream completion",
  );

  const start = messages.find((message) => message.type === "stream_start");
  assert.equal(start.sessionId, "session-1");
  assert.ok(start.answerMessageId.startsWith("push-main:session-1-"));
});

void test("send broadcasts structured subagent events without text streaming", async (t) => {
  const messages = [];
  connections.set("main", new Set([createOpenSocket(messages)]));
  t.after(() => {
    connections.delete("main");
  });

  const ctx = {
    request: {
      body: {
        agentId: "main",
        sessionId: "session-1",
        event: {
          type: "subagent_stream",
          agentId: "coder",
          childSessionKey: "agent:coder:subagent:child",
          toolCallId: "tool-2",
          toolName: "shell",
          content: "Tool call: shell",
        },
      },
    },
  };

  await pushController.send(ctx);

  assert.equal(ctx.status, 200);
  assert.equal(ctx.body, "Sent");
  const child = messages.find((message) => message.type === "subagent_stream");
  assert.equal(typeof child.eventId, "string");
  assert.equal(typeof child.cursor, "number");
  assert.equal(child.deliveryId, `${child.eventId}:0`);
  assert.equal(typeof child.runId, "string");
  assert.ok(child.runId.length > 0);
  const presentation = { ...child };
  delete presentation.eventId;
  delete presentation.cursor;
  delete presentation.deliveryId;
  delete presentation.runId;
  assert.deepEqual(
    [presentation],
    [
      {
        type: "subagent_stream",
        agentId: "coder",
        childSessionKey: "agent:coder:subagent:child",
        toolCallId: "tool-2",
        toolName: "shell",
        content: "Tool call: shell",
        sessionId: "session-1",
        runtimeId: "openclaw",
        origin: "push",
      },
    ],
  );
});

void test("agent-wide proactive notifications preserve the optional sessionId contract", async (t) => {
  const first = [];
  const second = [];
  const firstSocket = createOpenSocket(first);
  const secondSocket = createOpenSocket(second);
  secondSocket.sessionIds = new Set(["session-2"]);
  connections.set("main", new Set([firstSocket, secondSocket]));
  t.after(() => connections.delete("main"));
  const ctx = { request: { body: { agentId: "main", content: "notification" } } };
  await pushController.send(ctx);
  assert.equal(ctx.status, 200);
  assert.equal(first.find((event) => event.type === "stream").content, "notification");
  assert.equal(second.find((event) => event.type === "stream").content, "notification");
  assert.equal(first[0].sessionId, "");
});

void test("push refuses unknown agents and ambiguous conversation aliases", async (t) => {
  const first = [];
  const second = [];
  connections.set("one", new Set([createOpenSocket(first)]));
  connections.set("two", new Set([createOpenSocket(second)]));
  t.after(() => {
    connections.delete("one");
    connections.delete("two");
  });
  for (const body of [
    { agentId: "unknown", content: "no recipient" },
    { agentId: "session-1", sessionId: "session-1", content: "ambiguous" },
    { agentId: "unknown", sessionId: "session-1", content: "wrong agent" },
  ]) {
    const ctx = { request: { body } };
    await pushController.send(ctx);
    assert.equal(ctx.status, 404);
  }
  assert.deepEqual(first, []);
  assert.deepEqual(second, []);
});
