import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, expect, test, vi } from "vitest";
import { sendPushEvent, sendPushMessage } from "./send.js";

afterEach(() => vi.unstubAllEnvs());

test("push retries retain one event id and optionally authenticate with a header", async () => {
  vi.stubEnv("PUSH_CHANNEL_PUSH_TOKEN", "fixture-push-token");
  const requests: { authorization: string | undefined; body: Record<string, unknown> }[] = [];
  const server = createServer((request, response) => {
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => {
      chunks.push(chunk);
    });
    request.on("end", () => {
      const body = Buffer.concat(chunks).toString();
      requests.push({ authorization: request.headers.authorization, body: JSON.parse(body) });
      response.writeHead(requests.length === 1 ? 500 : 200).end();
    });
  });
  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  const middlewareUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  try {
    await sendPushMessage({
      middlewareUrl,
      agentId: "main",
      sessionId: "chat",
      content: "result",
      retries: 2,
    });
    await sendPushEvent({
      middlewareUrl,
      agentId: "main",
      sessionId: "chat",
      event: { type: "subagent_result", agentId: "child", content: "done" },
      retries: 1,
    });
    expect(requests).toHaveLength(3);
    expect(requests[0].authorization).toBe("Bearer fixture-push-token");
    expect(requests[0].body.eventId).toEqual(expect.any(String));
    expect(requests[0].body).toEqual(requests[1].body);
    expect(requests[2].body.eventId).not.toBe(requests[0].body.eventId);
    expect(requests[2].body).not.toHaveProperty("token");
  } finally {
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
  }
});
