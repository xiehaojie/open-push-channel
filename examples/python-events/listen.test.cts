const assert = require("node:assert/strict");
const { execFile } = require("node:child_process");
const { randomUUID } = require("node:crypto");
const { once } = require("node:events");
const { mkdtemp, rm } = require("node:fs/promises");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { setTimeout: delay } = require("node:timers/promises");
const test = require("node:test");
const { promisify } = require("node:util");
const { createGatewayServer } = require("../../admin-backend/src/gateway/server.ts");

void test("Python listener delivers live events and resumes offline history", { timeout: 30000 }, async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "push-python-"));
  const token = randomUUID();
  const pushToken = randomUUID();
  const server = createGatewayServer({
    databasePath: join(directory, "gateway.sqlite"),
    auth: {
      clients: [{ id: "demo", token, targets: [{ runtimeId: "openclaw", agentId: "main" }] }],
      pushToken,
    },
    adapters: [{
      id: "openclaw",
      async run() {
        assert.fail("An event subscriber must not execute an agent");
      },
    }],
  });
  t.after(async () => {
    await server.shutdown();
    await rm(directory, { recursive: true, force: true });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const host = `127.0.0.1:${server.address().port}`;
  const execute = promisify(execFile);
  const listener = (cursor, count, credential = token) =>
    execute(
      process.env.PYTHON ?? "python3",
      [
        join(__dirname, "listen.py"),
        "--gateway", `ws://${host}`,
        "--session", "quickstart",
        "--after-cursor", String(cursor),
        "--count", String(count),
      ],
      {
        env: { ...process.env, PUSH_GATEWAY_CLIENT_TOKEN: credential },
        timeout: 12000,
        killSignal: "SIGKILL",
      },
    );
  const post = (content) =>
    fetch(`http://${host}/api/send`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${pushToken}` },
      body: JSON.stringify({
        clientId: "demo",
        sessionId: "quickstart",
        runtimeId: "openclaw",
        agentId: "main",
        eventId: content,
        content,
      }),
      signal: AbortSignal.timeout(5000),
    });
  const sendWhenReady = async () => {
    // The listener creates its binding during startup. A notification accepted
    // just before subscription is recovered by session.start's journal replay.
    for (let attempt = 0; attempt < 100; attempt++) {
      const response = await post("在线通知");
      const status = response.status;
      await response.text();
      if (status === 200) return;
      assert.equal(status, 404);
      await delay(25);
    }
    assert.fail("Python listener did not create its conversation");
  };
  const decode = (stdout) => stdout.trim().split("\n").map((line) => JSON.parse(line));
  const [live] = await Promise.all([listener(0, 5), sendWhenReady()]);
  const initial = decode(live.stdout);
  assert.equal(initial.length, 5);
  assert.equal(initial.find((event) => event.type === "message.delta").data.delta, "在线通知");
  assert.equal(initial.at(-1).type, "run.completed");

  const response = await post("离线通知");
  assert.equal(response.status, 200);
  await response.text();
  const checkpoint = initial.at(-1).cursor;
  const resumed = decode((await listener(checkpoint, 5)).stdout);
  assert.equal(resumed.length, 5);
  assert.ok(resumed.every((event) => event.cursor > checkpoint));
  assert.equal(resumed.find((event) => event.type === "message.delta").data.delta, "离线通知");
  assert.equal(resumed.at(-1).type, "run.completed");
  const complete = decode((await listener(0, 10)).stdout);
  assert.deepEqual(complete, [...initial, ...resumed]);
  assert.ok(complete.every((event) =>
    event.clientId === "demo" && event.conversationId === "quickstart" && event.origin === "push",
  ));
  await assert.rejects(listener(0, 1, randomUUID()), /4001/);
  await assert.rejects(listener(-1, 1), /--after-cursor must be/);
});
