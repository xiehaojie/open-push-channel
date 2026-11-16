import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { createGatewayServer } from "../admin-backend/src/gateway/server.ts";
import { GatewayStore } from "../admin-backend/src/gateway/store.ts";
import type { PushEvent } from "../protocol/index.ts";

const timeoutMs = 10_000;
const runtimeId = "benchmark";
const agentId = "main";

function integerAtLeast(value: string, label: string, minimum = 1) {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < minimum) {
    throw new Error(`${label} must be an integer of at least ${minimum}`);
  }
  return number;
}

type BenchmarkFrames = {
  registered: { agentId: string };
  "session.started": { sessionId: string };
  "message.delta": Extract<PushEvent, { type: "message.delta" }>;
};

function waitFor(socket: WebSocket, type: "open"): Promise<void>;
function waitFor<K extends keyof BenchmarkFrames>(
  socket: WebSocket,
  type: K,
): Promise<BenchmarkFrames[K]>;
function waitFor(socket: WebSocket, type: "open" | keyof BenchmarkFrames): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const listeners = new AbortController();
    const { signal } = listeners;
    const finish = (error?: unknown, event?: unknown) => {
      clearTimeout(timer);
      listeners.abort();
      if (error) reject(error);
      else resolve(event);
    };
    const timer = setTimeout(() => finish(new Error(`Timed out waiting for ${type}`)), timeoutMs);
    socket.addEventListener("error", () => finish(new Error("WebSocket failed")), { signal });
    socket.addEventListener("close", () => finish(new Error("WebSocket closed")), { signal });
    socket.addEventListener(
      type === "open" ? "open" : "message",
      (message) => {
        if (type === "open") {
          finish();
          return;
        }
        try {
          const event = JSON.parse((message as MessageEvent<string>).data);
          if (event.type === "error") throw new Error(event.message);
          if (event.type === type) finish(null, event);
        } catch (error) {
          finish(error);
        }
      },
      { signal },
    );
  });
}

function seedHistory(databasePath: string, count: number) {
  const store = new GatewayStore({ path: databasePath });
  try {
    for (let index = 0; index < count; index++) {
      store.saveSession({
        clientId: "offline-client",
        conversationId: `historical-${index}`,
        nativeSessionId: `native-historical-${index}`,
        runtimeId,
        agentId,
        nativeStarted: false,
      });
    }
  } finally {
    store.close();
  }
}

async function benchmark(clientCount: number, rounds: number, historicalSessions: number) {
  const directory = await mkdtemp(join(tmpdir(), "open-push-channel-benchmark-"));
  const pushToken = randomUUID();
  const clients = Array.from({ length: clientCount }, (_, index) => ({
    id: `client-${index}`,
    token: randomUUID(),
    targets: [{ runtimeId, agentId }],
  }));
  let server;
  try {
    const databasePath = join(directory, "events.sqlite");
    if (historicalSessions) seedHistory(databasePath, historicalSessions);
    server = createGatewayServer({
      databasePath,
      auth: { clients, pushToken },
      adapters: [{
        id: runtimeId,
        async run() {
          throw new Error("Push must not run an agent");
        },
      }],
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const address = server.address();
    assert(address && typeof address === "object");
    const { port } = address;
    const sockets = await Promise.all(clients.map(async (client) => {
      const socket = new WebSocket(`ws://127.0.0.1:${port}`);
      await waitFor(socket, "open");
      const registered = waitFor(socket, "registered");
      socket.send(JSON.stringify({ type: "register", token: client.token, protocolVersion: 1 }));
      assert.equal((await registered).agentId, client.id);
      const started = waitFor(socket, "session.started");
      socket.send(JSON.stringify({ type: "session.start", sessionId: client.id, runtimeId, agentId }));
      assert.equal((await started).sessionId, client.id);
      return socket;
    }));

    async function notify(client: (typeof clients)[number], index: number, round: number | "warmup") {
      const start = performance.now();
      const content = `notification-${round}-${index}`;
      const delivered = waitFor(sockets[index], "message.delta").then((event) => {
        assert.equal(event.conversationId, client.id);
        assert.equal(event.origin, "push");
        assert.equal(event.data.delta, content);
        return performance.now() - start;
      });
      const accepted = fetch(`http://127.0.0.1:${port}/api/send`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${pushToken}` },
        body: JSON.stringify({
          clientId: client.id,
          sessionId: client.id,
          agentId,
          runtimeId,
          eventId: content,
          content,
        }),
        signal: AbortSignal.timeout(timeoutMs),
      }).then(async (response) => assert.equal(response.status, 200, await response.text()));
      const [milliseconds] = await Promise.all([delivered, accepted]);
      return milliseconds;
    }

    // Warm the connections and HTTP pool before measuring.
    await Promise.all(clients.map((client, index) => notify(client, index, "warmup")));
    const samples: number[] = [];
    const start = performance.now();
    for (let round = 0; round < rounds; round++) {
      const batch = await Promise.all(clients.map((client, index) => notify(client, index, round)));
      samples.push(...batch);
    }
    const seconds = (performance.now() - start) / 1000;
    samples.sort((a, b) => a - b);
    const percentile = (p: number) => Number(samples[Math.ceil(p * samples.length) - 1].toFixed(2));
    console.log(JSON.stringify({
      node: process.version,
      platform: process.platform,
      arch: process.arch,
      clients: clientCount,
      historicalSessions,
      rounds,
      deliveries: samples.length,
      deliveriesPerSecond: Number((samples.length / seconds).toFixed(2)),
      deliveryMs: { p50: percentile(0.50), p95: percentile(0.95), p99: percentile(0.99) },
    }, null, 2));
  } finally {
    if (server) await server.shutdown();
    await rm(directory, { recursive: true, force: true });
  }
}

Promise.resolve()
  .then(() => benchmark(
    integerAtLeast(process.argv[2] ?? "25", "clients"),
    integerAtLeast(process.argv[3] ?? "10", "rounds"),
    integerAtLeast(process.argv[4] ?? "0", "historical sessions", 0),
  ))
  .catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
