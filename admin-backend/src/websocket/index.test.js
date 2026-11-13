const assert = require("node:assert/strict");
const http = require("node:http");
const test = require("node:test");
const WebSocket = require("ws");
const { createOpenClawDecoder } = require("../adapters/openclaw");
const { createClientProjector } = require("../gateway/client-events");
const { createEventWriter, validateEvent } = require("../../../protocol/index.cjs");

function createPresentationPipeline() {
  let decoder;
  let messages;
  return (agentId, line, sessionId, requestId, replyId) => {
    messages = [];
    if (!decoder) {
      const project = createClientProjector();
      const emit = createEventWriter(
        {
          runtimeId: "openclaw",
          agentId,
          conversationId: sessionId,
          requestId,
          replyId,
          runId: "test",
        },
        (event) => messages.push(...project(event)),
      );
      decoder = createOpenClawDecoder(emit);
    }
    decoder.accept(JSON.parse(line.slice(6)));
    return { messages };
  };
}

function waitForMessage(socket, predicate, label) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`Timed out waiting for ${label}`));
    }, 1500);

    const onMessage = (raw) => {
      const data = JSON.parse(raw.toString());
      if (predicate(data)) {
        cleanup();
        resolve(data);
      }
    };

    const onClose = (code, reason) => {
      cleanup();
      reject(new Error(`Socket closed before ${label}: ${code} ${reason.toString()}`));
    };

    const cleanup = () => {
      clearTimeout(timer);
      socket.off("message", onMessage);
      socket.off("close", onClose);
    };

    socket.on("message", onMessage);
    socket.on("close", onClose);
  });
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

function collectMessages(socket) {
  const messages = [];
  const waiters = [];

  socket.on("message", (raw) => {
    const data = JSON.parse(raw.toString());
    messages.push(data);
    for (const waiter of waiters.slice()) {
      if (!waiter.predicate(data)) {
        continue;
      }
      waiter.cleanup();
      waiter.resolve(data);
    }
  });

  return {
    messages,
    waitFor(predicate, label) {
      const existing = messages.find(predicate);
      if (existing) {
        return Promise.resolve(existing);
      }

      return new Promise((resolve, reject) => {
        const waiter = {
          predicate,
          resolve,
          cleanup: () => {
            clearTimeout(timer);
            const index = waiters.indexOf(waiter);
            if (index !== -1) {
              waiters.splice(index, 1);
            }
          },
        };
        const timer = setTimeout(() => {
          waiter.cleanup();
          reject(new Error(`Timed out waiting for ${label}`));
        }, 1500);
        waiters.push(waiter);
      });
    },
  };
}

function closeConnections(connections) {
  for (const sockets of connections.values()) {
    for (const socket of sockets) {
      socket.close();
    }
  }
}

function listen(server) {
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      resolve(server.address().port);
    });
  });
}

function closeServer(server) {
  return new Promise((resolve) => {
    server.close(resolve);
  });
}

void test("client projection preserves child session details on subagent events", () => {
  const processSSEEvent = createPresentationPipeline();

  const childSessionKey = "agent:researcher:subagent:child";
  const callResult = processSSEEvent(
    "main",
    `data: ${JSON.stringify({
      type: "subagent_tool_call",
      agentId: "researcher",
      label: "Researcher",
      childSessionKey,
      toolCallId: "tool-1",
      toolName: "sessions_yield",
      args: { reason: "need context" },
    })}`,
    "session-subagents",
    "query-subagents",
    "answer-subagents",
  );

  assert.equal(callResult.messages.length, 1);
  assert.equal(callResult.messages[0].type, "subagent_tool_call");
  assert.equal(callResult.messages[0].agentId, "researcher");
  assert.equal(callResult.messages[0].label, "Researcher");
  assert.equal(callResult.messages[0].childSessionKey, childSessionKey);
  assert.equal(callResult.messages[0].toolCallId, "tool-1");
  assert.equal(callResult.messages[0].toolName, "sessions_yield");
  assert.deepEqual(callResult.messages[0].args, { reason: "need context" });
  assert.equal(callResult.messages[0].sessionId, "session-subagents");
  assert.equal(callResult.messages[0].queryMessageId, "query-subagents");

  const result = processSSEEvent(
    "main",
    `data: ${JSON.stringify({
      type: "subagent_tool_result",
      agentId: "researcher",
      childSessionKey,
      toolCallId: "tool-1",
      toolName: "sessions_yield",
      content: [{ text: "yielded" }],
      isError: false,
    })}`,
    "session-subagents",
    "query-subagents",
    "answer-subagents",
  );

  assert.equal(result.messages.length, 1);
  assert.equal(result.messages[0].type, "subagent_tool_result");
  assert.deepEqual(result.messages[0].content, [{ text: "yielded" }]);
  assert.equal(result.messages[0].isError, false);

  const message = processSSEEvent(
    "main",
    `data: ${JSON.stringify({
      type: "subagent_message",
      agentId: "researcher",
      childSessionKey,
      messageId: "assistant-1",
      content: "我先查询天气。",
    })}`,
    "session-subagents",
    "query-subagents",
    "answer-subagents",
  );

  assert.equal(message.messages.length, 1);
  assert.equal(message.messages[0].type, "subagent_message");
  assert.equal(message.messages[0].messageId, "assistant-1");
  assert.equal(message.messages[0].content, "我先查询天气。");
});

void test("client projection forwards reasoning lifecycle events", () => {
  const processSSEEvent = createPresentationPipeline();
  const eventContext = ["main", "session-reasoning", "query-reasoning", "answer-reasoning"];

  const start = processSSEEvent(
    eventContext[0],
    `data: ${JSON.stringify({ type: "reasoning_start" })}`,
    eventContext[1],
    eventContext[2],
    eventContext[3],
  );
  const delta = processSSEEvent(
    eventContext[0],
    `data: ${JSON.stringify({ type: "reasoning_delta", delta: "Checking files" })}`,
    eventContext[1],
    eventContext[2],
    eventContext[3],
  );
  const replacement = processSSEEvent(
    eventContext[0],
    `data: ${JSON.stringify({ type: "reasoning_delta", delta: "Reading\n\nChecking files", replace: true })}`,
    eventContext[1],
    eventContext[2],
    eventContext[3],
  );
  const end = processSSEEvent(
    eventContext[0],
    `data: ${JSON.stringify({ type: "reasoning_end" })}`,
    eventContext[1],
    eventContext[2],
    eventContext[3],
  );

  assert.deepEqual(start.messages[0], {
    type: "reasoning_start",
    runtimeId: "openclaw",
    runId: "test",
    sessionId: "session-reasoning",
    queryMessageId: "query-reasoning",
  });
  assert.deepEqual(delta.messages[0], {
    type: "reasoning_delta",
    runtimeId: "openclaw",
    runId: "test",
    delta: "Checking files",
    sessionId: "session-reasoning",
    queryMessageId: "query-reasoning",
  });
  assert.equal(replacement.messages[0].replace, true);
  assert.equal(replacement.messages[0].delta, "Reading\n\nChecking files");
  assert.deepEqual(end.messages[0], {
    type: "reasoning_end",
    runtimeId: "openclaw",
    runId: "test",
    sessionId: "session-reasoning",
    queryMessageId: "query-reasoning",
  });
});

function createSocket(port) {
  const socket = new WebSocket(`ws://127.0.0.1:${port}`);
  return new Promise((resolve, reject) => {
    socket.once("open", () => resolve(socket));
    socket.once("error", reject);
  });
}

void test(
  "register accepts a demo agent when gateway authentication is unconfigured",
  { timeout: 3000 },
  async (t) => {
    const server = http.createServer();
    const { initWebSocket } = require("./index");
    const connections = initWebSocket(server);
    const port = await listen(server);
    t.after(async () => {
      closeConnections(connections);
      await closeServer(server);
    });

    const socket = await createSocket(port);
    t.after(() => socket.close());

    socket.send(JSON.stringify({ type: "register", agentId: "demo-agent" }));

    const registered = await waitForMessage(
      socket,
      (data) => data.type === "registered",
      "registered",
    );
    assert.equal(registered.agentId, "demo-agent");
    assert.deepEqual(registered.files, []);
    assert.equal(registered.path, "");
  },
);

void test(
  "message flow forwards ids and preserves long deltas without guessing snapshots",
  { timeout: 3000 },
  async (t) => {
    const longSnapshot = `${"A".repeat(220)} done`;
    const upstream = http.createServer((req, res) => {
      assert.equal(req.method, "POST");
      let body = "";
      req.on("data", (chunk) => {
        body += chunk.toString();
      });
      req.on("end", () => {
        const payload = JSON.parse(body);
        assert.equal(payload.agentId, "demo-agent");
        assert.equal(payload.sessionId, "session-1");
        assert.equal(payload.content, "hello");

        res.writeHead(200, { "Content-Type": "text/event-stream" });
        res.write(`data: ${JSON.stringify({ type: "content", delta: longSnapshot })}\n\n`);
        res.write(`data: ${JSON.stringify({ type: "done" })}\n\n`);
        res.end();
      });
    });
    const upstreamPort = await listen(upstream);
    t.after(async () => {
      await closeServer(upstream);
    });

    process.env.OPENCLAW_WEBHOOK_URL = `http://127.0.0.1:${upstreamPort}/webhook`;

    delete require.cache[require.resolve("./index")];
    const server = http.createServer();
    const { initWebSocket } = require("./index");
    const connections = initWebSocket(server);
    const port = await listen(server);
    t.after(async () => {
      closeConnections(connections);
      await closeServer(server);
    });

    const socket = await createSocket(port);
    const messages = collectMessages(socket);
    t.after(() => socket.close());
    socket.send(JSON.stringify({ type: "register", agentId: "demo-agent" }));
    await messages.waitFor((data) => data.type === "registered", "registered");

    socket.send(
      JSON.stringify({
        type: "message",
        content: "hello",
        sessionId: "session-1",
        queryMessageId: "query-1",
        answerMessageId: "answer-1",
      }),
    );

    const sent = await messages.waitFor((data) => data.type === "message_sent", "message_sent");
    assert.equal(sent.sessionId, "session-1");
    assert.equal(sent.queryMessageId, "query-1");
    assert.equal(sent.answerMessageId, "answer-1");

    const start = await messages.waitFor((data) => data.type === "stream_start", "stream_start");
    assert.equal(start.sessionId, "session-1");
    assert.equal(start.queryMessageId, "query-1");
    assert.equal(start.answerMessageId, "answer-1-1");

    const snapshot = await messages.waitFor((data) => data.type === "stream", "stream");
    assert.equal(snapshot.content, longSnapshot);
    assert.equal(snapshot.sessionId, "session-1");
    assert.equal(snapshot.queryMessageId, "query-1");
    assert.equal(snapshot.answerMessageId, "answer-1-1");

    const end = await messages.waitFor((data) => data.type === "stream_end", "stream_end");
    assert.equal(end.sessionId, "session-1");
    assert.equal(end.queryMessageId, "query-1");
    assert.equal(end.answerMessageId, "answer-1-1");
  },
);

void test("message flow forwards structured mentions to OpenClaw", { timeout: 3000 }, async (t) => {
  let receivedPayload;
  const upstream = http.createServer((req, res) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk.toString();
    });
    req.on("end", () => {
      receivedPayload = JSON.parse(body);
      res.writeHead(200, { "Content-Type": "text/event-stream" });
      res.write(`data: ${JSON.stringify({ type: "done" })}\n\n`);
      res.end();
    });
  });
  const upstreamPort = await listen(upstream);
  t.after(async () => {
    await closeServer(upstream);
  });

  process.env.OPENCLAW_WEBHOOK_URL = `http://127.0.0.1:${upstreamPort}/webhook`;

  delete require.cache[require.resolve("./index")];
  const server = http.createServer();
  const { initWebSocket } = require("./index");
  const connections = initWebSocket(server);
  const port = await listen(server);
  t.after(async () => {
    closeConnections(connections);
    await closeServer(server);
  });

  const socket = await createSocket(port);
  const messages = collectMessages(socket);
  t.after(() => socket.close());
  socket.send(JSON.stringify({ type: "register", agentId: "main", sessionId: "session-mentions" }));
  await messages.waitFor((data) => data.type === "registered", "registered");

  socket.send(
    JSON.stringify({
      type: "message",
      content: "Please ask @researcher and @coder",
      sessionId: "session-mentions",
      mentions: [
        { agentId: "researcher", label: "researcher" },
        { agentId: "coder", label: "coder" },
      ],
    }),
  );

  await waitFor(() => Boolean(receivedPayload), "upstream payload");
  assert.deepEqual(receivedPayload.mentions, [
    { agentId: "researcher", label: "researcher" },
    { agentId: "coder", label: "coder" },
  ]);
});

void test(
  "subagent SSE events are broadcast to the active session",
  { timeout: 3000 },
  async (t) => {
    const upstream = http.createServer((req, res) => {
      req.resume();
      req.on("end", () => {
        res.writeHead(200, { "Content-Type": "text/event-stream" });
        res.write(
          `data: ${JSON.stringify({
            type: "subagent_start",
            agentId: "researcher",
            label: "Researcher",
            childSessionKey: "agent:researcher:subagent:child",
          })}\n\n`,
        );
        res.write(
          `data: ${JSON.stringify({
            type: "subagent_tool_call",
            agentId: "researcher",
            childSessionKey: "agent:researcher:subagent:child",
            toolCallId: "tool-1",
            toolName: "sessions_yield",
            args: { reason: "reading docs" },
          })}\n\n`,
        );
        res.write(
          `data: ${JSON.stringify({ type: "subagent_result", agentId: "researcher", content: "found answer" })}\n\n`,
        );
        res.write(
          `data: ${JSON.stringify({ type: "subagent_error", agentId: "coder", message: "missing workspace" })}\n\n`,
        );
        res.write(
          `data: ${JSON.stringify({
            type: "subagent_end",
            agentId: "researcher",
            childSessionKey: "agent:researcher:subagent:child",
            status: "success",
          })}\n\n`,
        );
        res.write(`data: ${JSON.stringify({ type: "done" })}\n\n`);
        res.end();
      });
    });
    const upstreamPort = await listen(upstream);
    t.after(async () => {
      await closeServer(upstream);
    });

    process.env.OPENCLAW_WEBHOOK_URL = `http://127.0.0.1:${upstreamPort}/webhook`;

    delete require.cache[require.resolve("./index")];
    const server = http.createServer();
    const { initWebSocket } = require("./index");
    const connections = initWebSocket(server);
    const port = await listen(server);
    t.after(async () => {
      closeConnections(connections);
      await closeServer(server);
    });

    const socket = await createSocket(port);
    const messages = collectMessages(socket);
    t.after(() => socket.close());
    socket.send(
      JSON.stringify({ type: "register", agentId: "main", sessionId: "session-subagents" }),
    );
    await messages.waitFor((data) => data.type === "registered", "registered");

    socket.send(
      JSON.stringify({
        type: "message",
        content: "Please ask @researcher",
        sessionId: "session-subagents",
        queryMessageId: "query-subagents",
        answerMessageId: "answer-subagents",
      }),
    );

    const start = await messages.waitFor(
      (data) => data.type === "subagent_start",
      "subagent_start",
    );
    assert.equal(start.sessionId, "session-subagents");
    assert.equal(start.queryMessageId, "query-subagents");
    assert.equal(start.agentId, "researcher");
    assert.equal(start.label, "Researcher");
    assert.equal(start.childSessionKey, "agent:researcher:subagent:child");

    const stream = await messages.waitFor(
      (data) => data.type === "subagent_tool_call",
      "subagent_tool_call",
    );
    assert.equal(stream.agentId, "researcher");
    assert.deepEqual(stream.args, { reason: "reading docs" });
    assert.equal(stream.sessionId, "session-subagents");
    assert.equal(stream.childSessionKey, "agent:researcher:subagent:child");
    assert.equal(stream.toolCallId, "tool-1");
    assert.equal(stream.toolName, "sessions_yield");

    const result = await messages.waitFor(
      (data) => data.type === "subagent_result",
      "subagent_result",
    );
    assert.equal(result.agentId, "researcher");
    assert.equal(result.content, "found answer");

    const error = await messages.waitFor(
      (data) => data.type === "subagent_error",
      "subagent_error",
    );
    assert.equal(error.agentId, "coder");
    assert.equal(error.message, "missing workspace");

    const end = await messages.waitFor((data) => data.type === "subagent_end", "subagent_end");
    assert.equal(end.agentId, "researcher");
    assert.equal(end.status, "success");
    assert.equal(end.childSessionKey, "agent:researcher:subagent:child");
  },
);

void test(
  "messages stay scoped to the active session for the same agent",
  { timeout: 3000 },
  async (t) => {
    const upstream = http.createServer((req, res) => {
      req.resume();
      req.on("end", () => {
        res.writeHead(200, { "Content-Type": "text/event-stream" });
        res.write(`data: ${JSON.stringify({ type: "content", delta: "reply" })}\n\n`);
        res.write(`data: ${JSON.stringify({ type: "done" })}\n\n`);
        res.end();
      });
    });
    const upstreamPort = await listen(upstream);
    t.after(async () => {
      await closeServer(upstream);
    });

    process.env.OPENCLAW_WEBHOOK_URL = `http://127.0.0.1:${upstreamPort}/webhook`;

    delete require.cache[require.resolve("./index")];
    const server = http.createServer();
    const { initWebSocket } = require("./index");
    const connections = initWebSocket(server);
    const port = await listen(server);
    t.after(async () => {
      closeConnections(connections);
      await closeServer(server);
    });

    const first = await createSocket(port);
    const second = await createSocket(port);
    const firstMessages = collectMessages(first);
    const secondMessages = collectMessages(second);
    t.after(() => {
      first.close();
      second.close();
    });

    first.send(JSON.stringify({ type: "register", agentId: "demo-agent", sessionId: "session-a" }));
    second.send(
      JSON.stringify({ type: "register", agentId: "demo-agent", sessionId: "session-b" }),
    );
    await firstMessages.waitFor((data) => data.type === "registered", "first registered");
    await secondMessages.waitFor((data) => data.type === "registered", "second registered");

    first.send(JSON.stringify({ type: "message", content: "hello", sessionId: "session-a" }));

    const firstStream = await firstMessages.waitFor(
      (data) => data.type === "stream" && data.content === "reply",
      "first stream",
    );
    assert.equal(firstStream.sessionId, "session-a");

    await assert.rejects(
      secondMessages.waitFor(
        (data) => data.type === "stream" || data.type === "message_sent",
        "leaked event",
      ),
      /Timed out waiting for leaked event/,
    );
  },
);

void test(
  "same session messages are forwarded to OpenClaw serially",
  { timeout: 5000 },
  async (t) => {
    let activeRequests = 0;
    let maxActiveRequests = 0;
    let releaseFirstResponse;
    const receivedBodies = [];

    const upstream = http.createServer((req, res) => {
      activeRequests += 1;
      maxActiveRequests = Math.max(maxActiveRequests, activeRequests);

      let body = "";
      req.on("data", (chunk) => {
        body += chunk.toString();
      });
      req.on("end", () => {
        receivedBodies.push(JSON.parse(body));
        res.writeHead(200, { "Content-Type": "text/event-stream" });

        if (receivedBodies.length === 1) {
          releaseFirstResponse = () => {
            res.write(`data: ${JSON.stringify({ type: "content", delta: "first" })}\n\n`);
            res.write(`data: ${JSON.stringify({ type: "done" })}\n\n`);
            res.end();
            activeRequests -= 1;
          };
          return;
        }

        res.write(`data: ${JSON.stringify({ type: "content", delta: "second" })}\n\n`);
        res.write(`data: ${JSON.stringify({ type: "done" })}\n\n`);
        res.end();
        activeRequests -= 1;
      });
    });
    const upstreamPort = await listen(upstream);
    t.after(async () => {
      await closeServer(upstream);
    });

    process.env.OPENCLAW_WEBHOOK_URL = `http://127.0.0.1:${upstreamPort}/webhook`;

    delete require.cache[require.resolve("./index")];
    const server = http.createServer();
    const { initWebSocket } = require("./index");
    const connections = initWebSocket(server);
    const port = await listen(server);
    t.after(async () => {
      closeConnections(connections);
      await closeServer(server);
    });

    const socket = await createSocket(port);
    const messages = collectMessages(socket);
    t.after(() => socket.close());

    socket.send(
      JSON.stringify({ type: "register", agentId: "demo-agent", sessionId: "session-a" }),
    );
    await messages.waitFor((data) => data.type === "registered", "registered");

    socket.send(JSON.stringify({ type: "message", content: "first", sessionId: "session-a" }));
    socket.send(JSON.stringify({ type: "message", content: "second", sessionId: "session-a" }));

    await waitFor(
      () => receivedBodies.length === 1 && typeof releaseFirstResponse === "function",
      "first upstream request",
    );
    await new Promise((resolve) => {
      setTimeout(resolve, 100);
    });
    assert.equal(receivedBodies.length, 1);
    assert.equal(maxActiveRequests, 1);

    releaseFirstResponse();
    await waitFor(() => receivedBodies.length === 2, "second upstream request");
    assert.equal(maxActiveRequests, 1);
    assert.deepEqual(
      receivedBodies.map((body) => body.content),
      ["first", "second"],
    );
  },
);

void test("versioned clients use an injected adapter and never share events across runtimes", async (t) => {
  delete require.cache[require.resolve("./index")];
  const { initWebSocket } = require("./index");
  const server = http.createServer();
  const adapters = ["first", "second"].map((id) => ({
    id,
    capabilities: { streaming: true, tools: true },
    async run(request, { emit }) {
      emit({ type: "message.started", data: { messageId: "reply" } });
      emit({ type: "message.delta", data: { messageId: "reply", delta: id } });
      emit({ type: "message.completed", data: { messageId: "reply" } });
    },
  }));
  const connections = initWebSocket(server, { adapters });
  const port = await listen(server);
  t.after(async () => {
    closeConnections(connections);
    await closeServer(server);
  });
  const first = await createSocket(port);
  const second = await createSocket(port);
  const firstMessages = collectMessages(first);
  const secondMessages = collectMessages(second);
  t.after(() => {
    first.close();
    second.close();
  });
  for (const [socket, runtimeId] of [
    [first, "first"],
    [second, "second"],
  ]) {
    socket.send(
      JSON.stringify({
        type: "register",
        runtimeId,
        agentId: "main",
        sessionId: runtimeId,
        protocolVersion: 1,
      }),
    );
  }
  const registered = await firstMessages.waitFor(
    (data) => data.type === "registered",
    "registered",
  );
  assert.equal(registered.runtimes[0].capabilities.streaming, true);
  await secondMessages.waitFor((data) => data.type === "registered", "registered");
  first.send(
    JSON.stringify({
      type: "message",
      content: "hi",
      sessionId: "first",
      runtimeId: "first",
      queryMessageId: "request-1",
    }),
  );
  const firstResult = await firstMessages.waitFor(
    (data) => data.type === "message.delta",
    "canonical delta",
  );
  validateEvent(firstResult);
  assert.equal(firstResult.runtimeId, "first");
  assert.equal(firstResult.data.delta, "first");
  assert.equal(firstResult.requestId, "request-1");
  second.send(
    JSON.stringify({ type: "message", content: "hi", sessionId: "second", runtimeId: "second" }),
  );
  const secondResult = await secondMessages.waitFor(
    (data) => data.type === "message.delta",
    "second delta",
  );
  assert.equal(secondResult.runtimeId, "second");
  assert.equal(secondResult.data.delta, "second");
  await assert.rejects(
    secondMessages.waitFor((data) => data.runtimeId === "first", "runtime leak"),
    /Timed out/,
  );
});

void test("legacy and versioned clients receive the same proactive tool and subagent delivery", async (t) => {
  delete require.cache[require.resolve("./index")];
  const { initWebSocket, publishPush } = require("./index");
  const server = http.createServer();
  const connections = initWebSocket(server);
  const port = await listen(server);
  t.after(async () => {
    closeConnections(connections);
    await closeServer(server);
  });
  const legacy = await createSocket(port);
  const canonical = await createSocket(port);
  const oldMessages = collectMessages(legacy);
  const newMessages = collectMessages(canonical);
  t.after(() => {
    legacy.close();
    canonical.close();
  });
  legacy.send(JSON.stringify({ type: "register", agentId: "main", sessionId: "chat" }));
  canonical.send(
    JSON.stringify({ type: "register", agentId: "main", sessionId: "chat", protocolVersion: 1 }),
  );
  await oldMessages.waitFor((data) => data.type === "registered", "legacy registered");
  await newMessages.waitFor((data) => data.type === "registered", "canonical registered");
  publishPush({ agentId: "main", sessionId: "chat", content: "background result" });
  const oldText = await oldMessages.waitFor((data) => data.type === "stream", "legacy push");
  const newText = await newMessages.waitFor(
    (data) => data.type === "message.delta",
    "canonical push",
  );
  assert.equal(oldText.content, newText.data.delta);
  assert.equal(newText.origin, "push");
  assert.notEqual(newText.eventId, newText.runId);
  publishPush({
    agentId: "main",
    sessionId: "chat",
    event: {
      type: "subagent_tool_call",
      agentId: "coder",
      childSessionKey: "child",
      toolCallId: "tool",
      toolName: "shell",
      args: {},
    },
  });
  const oldTool = await oldMessages.waitFor(
    (data) => data.type === "subagent_tool_call",
    "legacy child tool",
  );
  const newTool = await newMessages.waitFor(
    (data) => data.type === "subagent.tool.started",
    "canonical child tool",
  );
  assert.equal(oldTool.childSessionKey, newTool.data.childSessionId);
  assert.equal(newTool.agentId, "main");
  assert.equal(newTool.data.agentId, "coder");
  assert.equal(newTool.conversationId, "chat");
});

void test("a truncated upstream stream sends a visible error instead of a successful terminal event", async (t) => {
  const upstream = http.createServer((req, res) => {
    req.resume();
    res.writeHead(200, { "Content-Type": "text/event-stream" });
    res.end('data: {"type":"content","delta":"partial"}\n\n');
  });
  const upstreamPort = await listen(upstream);
  t.after(() => closeServer(upstream));
  delete require.cache[require.resolve("./index")];
  const { initWebSocket } = require("./index");
  const { createOpenClawAdapter } = require("../adapters/openclaw");
  const server = http.createServer();
  const connections = initWebSocket(server, {
    adapters: [createOpenClawAdapter({ url: `http://127.0.0.1:${upstreamPort}` })],
  });
  const port = await listen(server);
  t.after(async () => {
    closeConnections(connections);
    await closeServer(server);
  });
  const socket = await createSocket(port);
  const messages = collectMessages(socket);
  t.after(() => socket.close());
  socket.send(JSON.stringify({ type: "register", agentId: "main", sessionId: "chat" }));
  await messages.waitFor((data) => data.type === "registered", "registered");
  socket.send(JSON.stringify({ type: "message", sessionId: "chat", content: "hi" }));
  await messages.waitFor((data) => data.type === "stream_end", "closed partial message");
  const error = await messages.waitFor((data) => data.type === "error", "upstream failure");
  assert.match(error.message, /before done/);
});

for (const protocolVersion of [undefined, 1]) {
  void test(`native controls cross the real WebSocket boundary for ${protocolVersion ? "canonical" : "legacy"} clients`, async (t) => {
    delete require.cache[require.resolve("./index")];
    const { initWebSocket } = require("./index");
    const server = http.createServer();
    const calls = [];
    let finish;
    const sockets = [];
    initWebSocket(server, {
      adapters: [
        {
          id: "pi",
          capabilities: { steering: true, followUp: true },
          async run(request, { emit }) {
            calls.push(["run", request.content]);
            emit({ type: "message.started", data: { messageId: "native-answer" } });
            emit({ type: "message.delta", data: { messageId: "native-answer", delta: "working" } });
            await new Promise((resolve) => {
              finish = resolve;
            });
            emit({ type: "message.completed", data: { messageId: "native-answer" } });
          },
          async control(request, input) {
            calls.push([input.operation, input.content, request.runId]);
            return { status: "queued" };
          },
        },
        { id: "openclaw", async run() {} },
      ],
      auth: {
        clients: [
          {
            id: "owner",
            token: "owner-fixture",
            targets: [
              { runtimeId: "pi", agentId: "main" },
              { runtimeId: "openclaw", agentId: "main" },
            ],
          },
          { id: "other", token: "other-fixture", targets: [{ runtimeId: "pi", agentId: "main" }] },
        ],
      },
    });
    const port = await listen(server);
    t.after(async () => {
      finish?.();
      for (const socket of sockets) {
        socket.terminate();
      }
      await server.shutdown();
    });
    const connect = async (token, version = protocolVersion) => {
      const socket = await createSocket(port);
      sockets.push(socket);
      const feed = collectMessages(socket);
      const send = (data) => socket.send(JSON.stringify(data));
      send({ type: "register", token, protocolVersion: version });
      await feed.waitFor((data) => data.type === "registered", "registration");
      return { send, ...feed };
    };
    const observer = await connect("owner-fixture", 1);
    const client = await connect("owner-fixture");
    for (const peer of [observer, client]) {
      peer.send({ type: "session.start", sessionId: "chat", runtimeId: "pi", agentId: "main" });
      await peer.waitFor((data) => data.type === "session.started", "session binding");
    }
    client.send({
      type: "message",
      sessionId: "chat",
      content: "original",
      queryMessageId: "root-query",
      answerMessageId: "root-answer",
    });
    const started = await observer.waitFor((data) => data.type === "run.started", "parent run");
    await observer.waitFor((data) => data.type === "message.delta", "parent text");
    const control = {
      type: "input.control",
      sessionId: "chat",
      runId: started.runId,
      controlId: "control-one",
      operation: "steer",
      content: "adjust",
    };
    client.send(control);
    const receipt = await client.waitFor(
      (data) =>
        protocolVersion
          ? data.type === "input.control" &&
            data.data.controlId === control.controlId &&
            data.data.status === "queued"
          : data.type === "input_control" &&
            data.controlId === control.controlId &&
            data.status === "queued",
      "correlated native control receipt",
    );
    assert.equal(receipt.runId, started.runId);
    assert.equal(protocolVersion ? receipt.requestId : receipt.queryMessageId, "root-query");
    assert.equal(
      protocolVersion ? receipt.replyId : receipt.answerMessageId,
      protocolVersion ? "root-answer" : "root-answer-1",
    );
    for (const { controlId, changed, reason } of [
      { controlId: "stale", changed: { runId: "old-run" }, reason: /active run/i },
      {
        controlId: "wrong-runtime",
        changed: { runtimeId: "openclaw" },
        reason: /target|bound|runtime/i,
      },
      {
        controlId: "wrong-agent",
        changed: { agentId: "private-agent" },
        reason: /target|bound|agent/i,
      },
    ]) {
      client.send({ ...control, ...changed, controlId });
      const rejected = await client.waitFor(
        (data) => data.type === "input_control" && data.controlId === controlId,
        "direct control rejection",
      );
      assert.equal(rejected.status, "rejected");
      assert.equal(rejected.runId, changed.runId ?? started.runId);
      assert.equal(rejected.sessionId, "chat");
      assert.match(rejected.message, reason);
    }
    const foreign = await connect("other-fixture");
    foreign.send({ ...control, controlId: "foreign" });
    const denied = await foreign.waitFor(
      (data) => data.type === "input_control" && data.controlId === "foreign",
      "owner rejection",
    );
    assert.equal(denied.status, "rejected");
    assert.equal(denied.runId, started.runId);
    assert.equal(denied.content, undefined);
    client.send({
      type: "session.start",
      sessionId: "unsupported",
      runtimeId: "openclaw",
      agentId: "main",
    });
    await client.waitFor(
      (data) => data.type === "session.started" && data.sessionId === "unsupported",
      "unsupported runtime binding",
    );
    client.send({ ...control, sessionId: "unsupported", controlId: "unsupported" });
    const unsupported = await client.waitFor(
      (data) => data.type === "input_control" && data.controlId === "unsupported",
      "unsupported control rejection",
    );
    assert.equal(unsupported.status, "rejected");
    assert.match(unsupported.message, /support/i);
    assert.equal(unsupported.runId, started.runId);
    const reconnected = await connect("owner-fixture", 1);
    reconnected.send({ type: "session.start", sessionId: "chat" });
    const replayed = await reconnected.waitFor(
      (data) =>
        data.type === "input.control" &&
        data.data.controlId === "control-one" &&
        data.data.status === "queued",
      "control receipt replay",
    );
    assert.equal(replayed.eventId, receipt.eventId);
    assert.deepEqual(calls, [
      ["run", "original"],
      ["steer", "adjust", started.runId],
    ]);
    assert.equal(
      observer.messages.some((data) => data.type === "run.failed" || data.type === "run.completed"),
      false,
    );
    finish();
    await observer.waitFor((data) => data.type === "run.completed", "parent completion");
    assert.equal(observer.messages.filter((data) => data.type === "run.started").length, 1);
  });
}
