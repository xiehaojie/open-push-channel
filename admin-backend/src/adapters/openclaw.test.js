const assert = require("node:assert/strict");
const http = require("node:http");
const test = require("node:test");
const { createOpenClawAdapter } = require("./openclaw");

void test("OpenClaw rejects invalid webhook credentials before sending a request", () => {
  assert.throws(
    () =>
      createOpenClawAdapter({
        url: "http://127.0.0.1:1/webhook",
        webhookToken: "invalid fixture",
      }),
    { message: "Invalid OpenClaw webhook Bearer token format" },
  );
});

async function withUpstream(t, handler) {
  const server = http.createServer(handler);
  await new Promise((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  t.after(
    () =>
      new Promise((resolve) => {
        server.close(resolve);
      }),
  );
  return `http://127.0.0.1:${server.address().port}/webhook`;
}

void test("OpenClaw adapter preserves UTF-8 across SSE chunks, tools, reasoning and child events", async (t) => {
  const url = await withUpstream(t, (req, res) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
    });
    req.on("end", () => {
      const payload = JSON.parse(body);
      assert.equal(payload.sessionId, "chat");
      assert.equal(payload.nativeSessionId, "opaque-native");
      assert.deepEqual(payload.mentions, [{ agentId: "coder" }]);
      res.writeHead(200, { "Content-Type": "text/event-stream" });
      const bytes = Buffer.from(
        `data: ${JSON.stringify({ type: "content", delta: "你好" })}\r\n\r\n`,
      );
      const split = bytes.indexOf(Buffer.from("你好")) + 1;
      res.write(bytes.subarray(0, split));
      setImmediate(() => {
        res.write(bytes.subarray(split));
        for (const event of [
          { type: "tool_call", toolCallId: "t1", toolName: "shell", args: { command: "pwd" } },
          { type: "tool_result", toolCallId: "t1", content: "ok", isError: false },
          { type: "reasoning_start" },
          { type: "reasoning_delta", delta: "thinking", replace: true },
          { type: "reasoning_end" },
          {
            type: "subagent_stream",
            agentId: "coder",
            childSessionKey: "child",
            content: "working",
          },
          { type: "done" },
        ]) {
          res.write(`data: ${JSON.stringify(event)}\n\n`);
        }
        res.end();
      });
    });
  });
  const events = [];
  await createOpenClawAdapter({ url }).run(
    {
      agentId: "main",
      conversationId: "chat",
      nativeSessionId: "opaque-native",
      content: "hi",
      mentions: [{ agentId: "coder" }],
    },
    { emit: (event) => events.push(event) },
  );
  assert.equal(events.find((event) => event.type === "message.delta").data.delta, "你好");
  assert.equal(events.find((event) => event.type === "tool.started").data.toolCallId, "t1");
  assert.equal(events.find((event) => event.type === "tool.completed").data.content, "ok");
  assert.equal(events.find((event) => event.type === "reasoning.delta").data.replace, true);
  const child = events.find((event) => event.type === "subagent.delta");
  assert.equal(child.data.childSessionId, "child");
  assert.equal(child.data.agentId, "coder");
});

void test("OpenClaw adapter rejects a truncated or malformed stream instead of reporting completion", async (t) => {
  for (const body of ['data: {"type":"content","delta":"partial"}\n\n', "data: invalid\n\n"]) {
    const url = await withUpstream(t, (req, res) => {
      req.resume();
      res.writeHead(200, { "Content-Type": "text/event-stream" });
      res.end(body);
    });
    await assert.rejects(
      createOpenClawAdapter({ url }).run(
        { agentId: "main", conversationId: "chat", content: "hi" },
        { emit() {} },
      ),
      /stream|JSON/i,
    );
  }
});

void test("OpenClaw timeout defers the run and proactive events use the same conversion", async (t) => {
  const url = await withUpstream(t, (req, res) => {
    req.resume();
    res.writeHead(200, { "Content-Type": "text/event-stream" });
    res.end('data: {"type":"timeout_deferred","message":"waiting"}\n\ndata: {"type":"done"}\n\n');
  });
  const adapter = createOpenClawAdapter({ url });
  const result = await adapter.run(
    { agentId: "main", conversationId: "chat", content: "hi" },
    { emit() {} },
  );
  assert.equal(result.status, "deferred");
  const signals = adapter.decodePushEvent({
    type: "subagent_end",
    agentId: "coder",
    childSessionKey: "child",
    status: "success",
  });
  assert.equal(signals[0].type, "subagent.ended");
  assert.equal(signals[0].data.childSessionId, "child");
});

void test("OpenClaw preserves native message boundaries and authoritative snapshots through client projection", async (t) => {
  const { createClientProjector } = require("../gateway/client-events");
  const url = await withUpstream(t, (req, res) => {
    req.resume();
    res.writeHead(200, { "Content-Type": "text/event-stream" });
    for (const event of [
      { type: "message_start" },
      { type: "content", delta: "Hello" },
      { type: "tool_call", toolCallId: "read-1", toolName: "read", args: { path: "README.md" } },
      { type: "tool_result", toolCallId: "read-1", content: "read", isError: false },
      { type: "content_snapshot", text: "Hallo" },
      { type: "content_snapshot", text: "Hi" },
      { type: "message_start" },
      { type: "content", delta: "ha" },
      { type: "content", delta: "ha" },
      { type: "done" },
    ]) {
      res.write(`data: ${JSON.stringify(event)}\n\n`);
    }
    res.end();
  });
  const events = [];
  await createOpenClawAdapter({ url }).run(
    { agentId: "main", conversationId: "chat", content: "test" },
    { emit: (event) => events.push(event) },
  );
  const texts = new Map();
  for (const event of events) {
    if (event.type === "message.delta") {
      texts.set(event.data.messageId, (texts.get(event.data.messageId) || "") + event.data.delta);
    }
    if (event.type === "message.snapshot") {
      texts.set(event.data.messageId, event.data.text);
    }
  }
  assert.deepEqual([...texts.values()], ["Hi", "haha"]);
  assert.equal(events.filter((event) => event.type === "message.completed").length, 2);
  const firstMessageId = events.find((event) => event.type === "message.started").data.messageId;
  assert.ok(
    events
      .filter((event) => event.type === "message.snapshot")
      .every((event) => event.data.messageId === firstMessageId),
  );
  const project = createClientProjector();
  const clientEvents = events.flatMap((event) =>
    project({ ...event, conversationId: "chat", runtimeId: "openclaw" }),
  );
  assert.deepEqual(
    clientEvents.filter((event) => event.type === "stream_snapshot").map((event) => event.snapshot),
    ["Hallo", "Hi"],
  );
  assert.equal(clientEvents.filter((event) => event.type === "stream_start").length, 2);
});
