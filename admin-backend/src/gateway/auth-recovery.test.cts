const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
const { once } = require("node:events");
const { mkdtemp, rm } = require("node:fs/promises");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { setTimeout: delay } = require("node:timers/promises");
const test = require("node:test");
const WebSocket = require("ws");
const { createGatewayServer } = require("./server.ts");

void test("authenticated SQLite recovery isolates shared conversation names and rechecks revoked targets", { timeout: 15000 }, async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "push-auth-recovery-"));
  const principals = ["openclaw", "pi"].map((runtimeId, index) => ({
    id: `client-${index}`, token: randomUUID(), targets: [{ runtimeId, agentId: "main" }],
  }));
  const pushToken = randomUUID();
  const sockets = [];
  let server;
  const stop = async () => {
    sockets.splice(0).forEach((socket) => socket.terminate());
    if (server) {
      await server.shutdown();
      server = undefined;
    }
  };
  t.after(async () => {
    try {
      await stop();
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
  const start = async (revokeFirst = false) => {
    server = createGatewayServer({
      databasePath: join(directory, "gateway.sqlite"),
      auth: {
        pushToken,
        clients: principals.map((principal, index) => ({
          ...principal, targets: revokeFirst && index === 0 ? [] : principal.targets,
        })),
      },
      adapters: principals.map(({ targets: [{ runtimeId }] }) => ({
        id: runtimeId,
        async run() { assert.fail("Notification recovery must not run an agent"); },
      })),
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening", { signal: AbortSignal.timeout(3000) });
  };
  const connect = async (principal) => {
    const socket = new WebSocket(`ws://127.0.0.1:${server.address().port}`);
    sockets.push(socket);
    const events = [];
    socket.on("message", (raw) => events.push(JSON.parse(Buffer.from(raw).toString())));
    await once(socket, "open", { signal: AbortSignal.timeout(3000) });
    const send = (payload) => socket.send(JSON.stringify(payload));
    const wait = async (predicate) => {
      const deadline = Date.now() + 3000;
      while (!predicate()) {
        assert.ok(Date.now() < deadline, `Missing recovery event: ${predicate}`);
        await delay(5);
      }
    };
    const barrier = async () => {
      const before = events.filter((event) => event.type === "pong").length;
      send({ type: "ping" });
      await wait(() => events.filter((event) => event.type === "pong").length > before);
    };
    send({ type: "register", token: principal.token, agentId: "spoofed", protocolVersion: 1 });
    await wait(() => events.some((event) => event.type === "registered"));
    assert.equal(events.find((event) => event.type === "registered").agentId, principal.id);
    return { events, send, wait, barrier };
  };
  const canonical = (browser) => browser.events.filter((event) => event.version === 1);
  const subscribe = async (browser, afterCursor = 0) => {
    // Omitted runtime/agent fields must resolve the persisted owner's binding.
    browser.send({ type: "session.start", sessionId: "chat", afterCursor });
    await browser.barrier();
  };
  const notify = async (principal, browser, eventId) => {
    const response = await fetch(`http://127.0.0.1:${server.address().port}/api/send`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${pushToken}` },
      body: JSON.stringify({
        clientId: principal.id, sessionId: "chat", ...principal.targets[0],
        eventId, content: `${principal.id}-${eventId}`,
      }),
      signal: AbortSignal.timeout(3000),
    });
    assert.equal(response.status, 200, await response.text());
    await browser.wait(() => browser.events.some((event) =>
      event.type === "run.completed" && event.requestId === `push:${eventId}`,
    ));
  };

  await start();
  const initial = await Promise.all(principals.map(connect));
  for (const [index, browser] of initial.entries()) {
    browser.send({ type: "session.start", sessionId: "chat", ...principals[index].targets[0] });
    await browser.barrier();
    assert.ok(browser.events.some((event) => event.type === "session.started"));
  }
  for (const eventId of ["one", "two"]) {
    for (const [index, principal] of principals.entries()) {
      await notify(principal, initial[index], eventId);
    }
  }
  await Promise.all(initial.map((browser) => browser.barrier()));
  const originals = initial.map(canonical);
  for (const [index, events] of originals.entries()) {
    assert.equal(events.length, 10);
    assert.ok(events.every((event) =>
      event.clientId === principals[index].id && event.conversationId === "chat" &&
      event.runtimeId === principals[index].targets[0].runtimeId && event.agentId === "main",
    ));
  }
  const boundary = originals[1].find((event) =>
    event.type === "run.completed" && event.requestId === "push:one",
  ).cursor;
  assert.ok(originals[0][4].cursor < boundary && boundary < originals[0][5].cursor);
  await stop();

  await start();
  const resumed = await Promise.all(principals.map(connect));
  for (const [index, browser] of resumed.entries()) {
    await subscribe(browser, boundary);
    assert.ok(browser.events.some((event) => event.type === "session.started"));
    assert.deepEqual(canonical(browser), originals[index].filter((event) => event.cursor >= boundary));
  }
  assert.equal(canonical(resumed[0]).length, 5);
  assert.equal(canonical(resumed[1]).length, 6);
  await stop();

  // A cursor is not authorization; restored bindings must obey the fresh principal.
  // Reference: https://github.com/centrifugal/centrifugal.dev/blob/69da71466274f741bfe924bbf4a62a9124ec5776/docs/server/history_and_recovery.md#how-recovery-works
  await start(true);
  const revoked = await connect(principals[0]);
  await subscribe(revoked, boundary);
  assert.match(revoked.events.find((event) => event.type === "error").message, /not authorized/);
  assert.equal(revoked.events.some((event) => event.type === "session.started"), false);
  assert.deepEqual(canonical(revoked), []);
  const allowed = await connect(principals[1]);
  await subscribe(allowed);
  assert.ok(allowed.events.some((event) => event.type === "session.started"));
  assert.deepEqual(canonical(allowed), originals[1]);
});
