import * as fs from "node:fs";
import * as http from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import type { PluginRuntime } from "openclaw/plugin-sdk/runtime-store";
import { afterEach, describe, expect, it, vi } from "vitest";
import { testing as monitorTesting } from "./monitor.js";
import { setPushChannelRuntime } from "./runtime.js";
import { testing as subagentOrchestratorTesting } from "./subagent-orchestrator.js";

async function allocatePort(): Promise<number> {
  const server = http.createServer();
  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
  if (!address || typeof address === "string") {
    throw new Error("failed to allocate port");
  }
  return address.port;
}

async function waitForServer(url: string): Promise<void> {
  const deadline = Date.now() + 2_000;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      await fetch(url);
      return;
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => {
        setTimeout(resolve, 25);
      });
    }
  }
  throw lastError;
}

function startMonitorPushChannel(
  opts: Parameters<typeof import("./monitor.js").monitorPushChannel>[0],
): Promise<void> {
  return import("./monitor.js").then(({ monitorPushChannel }) => monitorPushChannel(opts));
}

describe("push-channel monitor", () => {
  afterEach(() => {
    subagentOrchestratorTesting.setSpawnMentionedSubagentImplForTest();
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
  });

  it("rejects network listening without an inbound token", async () => {
    vi.stubEnv("PUSH_CHANNEL_WEBHOOK_TOKEN", "");
    await expect(
      startMonitorPushChannel({
        config: { channels: { "push-channel": { listenHost: "0.0.0.0" } } } as never,
        runtime: { log: vi.fn() } as never,
        accountId: "default",
        abortSignal: new AbortController().signal,
      }),
    ).rejects.toThrow(/requires PUSH_CHANNEL_WEBHOOK_TOKEN/);
  });

  it.each([
    ["   ", "", "Invalid OpenClaw webhook Bearer token format"],
    ["invalid fixture", "", "Invalid OpenClaw webhook Bearer token format"],
    ["fixture-shared", "fixture-shared", "OpenClaw webhook and push tokens must be distinct"],
  ])(
    "rejects invalid or shared webhook credentials before listening",
    async (webhook, push, message) => {
      vi.stubEnv("PUSH_CHANNEL_WEBHOOK_TOKEN", webhook);
      vi.stubEnv("PUSH_CHANNEL_PUSH_TOKEN", push);
      await expect(
        startMonitorPushChannel({
          config: {},
          runtime: { log: vi.fn() } as never,
          accountId: "default",
          abortSignal: new AbortController().signal,
        }),
      ).rejects.toThrow(message);
    },
  );

  it("restricts token-free webhook requests to the listener's loopback origin", async () => {
    vi.stubEnv("PUSH_CHANNEL_WEBHOOK_TOKEN", "");
    const port = await allocatePort();
    const controller = new AbortController();
    const monitor = startMonitorPushChannel({
      config: { channels: { "push-channel": { listenPort: port } } } as never,
      runtime: { log: vi.fn() } as never,
      accountId: "default",
      abortSignal: controller.signal,
    });
    const base = `http://127.0.0.1:${port}`;
    const alias = `webhook.example.test:${port}`;
    const url = `${base}/webhook`;
    try {
      await waitForServer(url);
      for (const [headers, status] of [
        [{ Origin: "https://app.example.test" }, 403],
        [{ Origin: `http://${alias}`, Host: alias }, 403],
        [{ Origin: base }, 400],
        [{}, 400],
      ] as const) {
        const response = await fetch(url, {
          method: "POST",
          headers: { "Content-Type": "application/json", ...headers },
          body: "{}",
        });
        await response.text();
        expect(response.status).toBe(status);
      }
    } finally {
      controller.abort();
      await monitor;
    }
  });

  it("checks the bearer token and body before dispatching", async () => {
    vi.stubEnv("PUSH_CHANNEL_WEBHOOK_TOKEN", "fixture-inbound");
    const port = await allocatePort();
    const controller = new AbortController();
    const monitor = startMonitorPushChannel({
      config: { channels: { "push-channel": { listenPort: port } } } as never,
      runtime: { log: vi.fn() } as never,
      accountId: "default",
      abortSignal: controller.signal,
    });
    const url = `http://127.0.0.1:${port}/webhook`;
    try {
      await waitForServer(url);
      const post = (body: string, type: string, token?: string) =>
        fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": type,
            ...(token ? { Authorization: `Bearer ${token}` } : {}),
          },
          body,
        });
      const cases = [
        [await post("{}", "application/json"), 401],
        [await post("{}", "application/json", "wrong"), 401],
        [await post("{}", "text/plain", "fixture-inbound"), 415],
        [await post("{}", "application/json", "fixture-inbound"), 400],
      ] as const;
      for (const [response, status] of cases) {
        await response.text();
        expect(response.status).toBe(status);
      }
      const oversizedStatus = await new Promise<number | undefined>((resolve, reject) => {
        const request = http.request(
          url,
          {
            method: "POST",
            headers: {
              Authorization: "Bearer fixture-inbound",
              "Content-Type": "application/json",
              "Content-Length": 1024 * 1024 + 1,
            },
          },
          (response) => {
            response.resume();
            resolve(response.statusCode);
          },
        );
        request.on("error", reject);
        request.end();
      });
      expect(oversizedStatus).toBe(413);
    } finally {
      controller.abort();
      await monitor;
    }
  });

  it.each(["error", "empty", "multiple"])(
    "settles HTTP streaming after dispatcher cleanup (%s)",
    async (outcome) => {
      const port = await allocatePort();
      const controller = new AbortController();
      setPushChannelRuntime({
        channel: {
          reply: {
            finalizeInboundContext: (ctx: unknown) => ctx,
            withReplyDispatcher: async ({
              run,
              dispatcher,
              onSettled,
            }: {
              run: () => Promise<unknown>;
              dispatcher: { markComplete: () => void; waitForIdle: () => Promise<void> };
              onSettled?: () => void | Promise<void>;
            }) => {
              try {
                return await run();
              } finally {
                // Match the injected core owner's finally semantics, including
                // failures: cleanup is not evidence that dispatch succeeded.
                dispatcher.markComplete();
                await dispatcher.waitForIdle();
                await onSettled?.();
              }
            },
            dispatchReplyFromConfig: async ({
              dispatcher,
            }: {
              dispatcher: { sendFinalReply: (payload: { text: string }) => boolean };
            }) => {
              if (outcome === "error") {
                throw new Error("dispatch failed");
              }
              if (outcome === "multiple") {
                expect(dispatcher.sendFinalReply({ text: "first" })).toBe(true);
                expect(dispatcher.sendFinalReply({ text: "second" })).toBe(true);
              }
            },
          },
        },
      } as unknown as PluginRuntime);
      const monitor = startMonitorPushChannel({
        config: {
          channels: { "push-channel": { enabled: true, listenPort: port, listenPath: "/webhook" } },
        } as never,
        runtime: { log: vi.fn() } as never,
        accountId: "default",
        abortSignal: controller.signal,
      });
      try {
        await waitForServer(`http://127.0.0.1:${port}/health`);
        const response = await fetch(`http://127.0.0.1:${port}/webhook`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ agentId: "main", sessionId: "error-test", content: "hello" }),
        });
        const events = (await response.text())
          .split("\n")
          .filter((line) => line.startsWith("data: "))
          .map((line) => JSON.parse(line.slice(6)));
        expect(events.filter((event) => event.type === "done")).toHaveLength(1);
        if (outcome === "error") {
          expect(events).toEqual([
            { type: "error", message: "OpenClaw dispatch failed" },
            { type: "done" },
          ]);
        } else {
          expect(events.some((event) => event.type === "error")).toBe(false);
          expect(
            events.filter((event) => event.type === "content").map((event) => event.delta),
          ).toEqual(outcome === "multiple" ? ["first", "second"] : []);
        }
      } finally {
        controller.abort();
        await monitor;
      }
    },
  );

  it.each([undefined, "gateway-native-session"])(
    "ensures isolated native transcript before dispatch (%s)",
    async (nativeSessionId) => {
      const port = await allocatePort();
      const abortController = new AbortController();
      const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "push-channel-test-"));
      const storePath = path.join(tmpDir, "sessions.json");
      const sessionId = "test-session-uuid";
      const recordSessionMetaFromInbound = vi.fn(async () => ({
        sessionId,
        updatedAt: Date.now(),
      }));
      const dispatchReplyFromConfig = vi.fn(async () => ({ text: "ok" }));
      const withReplyDispatcher = vi.fn(async (params: { run: () => Promise<unknown> }) => {
        return await params.run();
      });
      setPushChannelRuntime({
        channel: {
          session: {
            resolveStorePath: vi.fn(() => storePath),
            recordSessionMetaFromInbound,
            recordInboundSession: vi.fn(async () => {}),
          },
          reply: {
            finalizeInboundContext: vi.fn((ctx) => ctx),
            withReplyDispatcher,
            dispatchReplyFromConfig,
          },
        },
        agent: {
          session: {
            resolveStorePath: vi.fn(() => storePath),
            resolveSessionFilePath: vi.fn((sid: string) => path.join(tmpDir, `${sid}.jsonl`)),
          },
        },
      } as unknown as PluginRuntime);

      const monitor = startMonitorPushChannel({
        config: {
          channels: {
            "push-channel": {
              enabled: true,
              middlewareUrl: "http://127.0.0.1:1",
              listenPort: port,
              listenPath: "/webhook",
            },
          },
        } as never,
        runtime: { log: vi.fn() } as never,
        accountId: "default",
        abortSignal: abortController.signal,
      });
      try {
        await waitForServer(`http://127.0.0.1:${port}/health`);

        const response = await fetch(`http://127.0.0.1:${port}/webhook`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            agentId: "main",
            sessionId: "demo-session",
            ...(nativeSessionId ? { nativeSessionId } : {}),
            content: "hello",
          }),
        });
        await response.text();

        // Session metadata should be recorded before dispatch
        expect(recordSessionMetaFromInbound).toHaveBeenCalledTimes(1);
        expect(recordSessionMetaFromInbound).toHaveBeenCalledWith(
          expect.objectContaining({
            sessionKey: `agent:main:channel:push-channel:direct:${nativeSessionId ?? "demo-session"}`,
          }),
        );
        expect(dispatchReplyFromConfig).toHaveBeenCalledTimes(1);
        expect(dispatchReplyFromConfig).toHaveBeenCalledWith(
          expect.objectContaining({
            replyOptions: expect.objectContaining({
              onPartialReply: expect.any(Function),
              onAssistantMessageStart: expect.any(Function),
              onReasoningStream: expect.any(Function),
              onReasoningEnd: expect.any(Function),
            }),
          }),
        );

        // The transcript file should have been created (topic-suffixed)
        const transcriptPath = path.join(
          tmpDir,
          `${sessionId}-topic-${nativeSessionId ?? "demo-session"}.jsonl`,
        );
        expect(fs.existsSync(transcriptPath)).toBe(true);
        const content = fs.readFileSync(transcriptPath, "utf-8");
        const header = JSON.parse(content.trim());
        expect(header.type).toBe("session");
        expect(header.id).toBe(sessionId);
        expect(header.version).toBe(3);
      } finally {
        abortController.abort();
        await monitor;
        fs.rmSync(tmpDir, { recursive: true, force: true });
      }
    },
  );

  it("spawns mentioned agents directly and returns their results to the main agent", async () => {
    const port = await allocatePort();
    const abortController = new AbortController();
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "push-channel-mentions-test-"));
    const storePath = path.join(tmpDir, "sessions.json");
    let finalizedContext: Record<string, unknown> | null = null;
    const dispatchReplyFromConfig = vi.fn(async (_params: { ctx: Record<string, unknown> }) => ({
      text: "ok",
    }));
    const waitForRun = vi.fn(async () => ({ status: "ok" as const }));
    const getSessionMessages = vi.fn(async (params: { sessionKey: string }) => ({
      messages: [
        {
          id: `${params.sessionKey}:assistant`,
          message: {
            role: "assistant",
            content: [
              {
                type: "text",
                text: params.sessionKey.includes("researcher")
                  ? "Researcher found the background."
                  : "Coder produced the implementation notes.",
              },
            ],
          },
        },
      ],
    }));
    const spawnMentionedSubagent = vi
      .fn()
      .mockResolvedValueOnce({
        status: "accepted",
        runId: "run-researcher",
        childSessionKey: "agent:researcher:subagent:child-a",
      })
      .mockResolvedValueOnce({
        status: "accepted",
        runId: "run-coder",
        childSessionKey: "agent:coder:subagent:child-b",
      });
    subagentOrchestratorTesting.setSpawnMentionedSubagentImplForTest(spawnMentionedSubagent);
    setPushChannelRuntime({
      subagent: {
        run: vi.fn(),
        waitForRun,
        getSessionMessages,
        getSession: vi.fn(async () => ({ messages: [] })),
        deleteSession: vi.fn(async () => {}),
      },
      channel: {
        session: {
          resolveStorePath: vi.fn(() => storePath),
          recordSessionMetaFromInbound: vi.fn(async () => ({
            sessionId: "mention-session",
            updatedAt: Date.now(),
          })),
          recordInboundSession: vi.fn(async () => {}),
        },
        reply: {
          finalizeInboundContext: vi.fn((ctx) => {
            finalizedContext = ctx as Record<string, unknown>;
            return ctx;
          }),
          withReplyDispatcher: vi.fn(async (params: { run: () => Promise<unknown> }) => {
            return await params.run();
          }),
          dispatchReplyFromConfig,
        },
      },
      agent: {
        session: {
          resolveStorePath: vi.fn(() => storePath),
          resolveSessionFilePath: vi.fn((sid: string) => path.join(tmpDir, `${sid}.jsonl`)),
        },
      },
    } as unknown as PluginRuntime);

    const monitor = startMonitorPushChannel({
      config: {
        channels: {
          "push-channel": {
            enabled: true,
            middlewareUrl: "http://127.0.0.1:1",
            listenPort: port,
            listenPath: "/webhook",
          },
        },
      } as never,
      runtime: { log: vi.fn() } as never,
      accountId: "default",
      abortSignal: abortController.signal,
    });
    try {
      await waitForServer(`http://127.0.0.1:${port}/health`);

      const response = await fetch(`http://127.0.0.1:${port}/webhook`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          agentId: "main",
          sessionId: "demo-session",
          content: "please coordinate this",
          mentions: [
            { agentId: "researcher", label: "Researcher" },
            { agentId: "coder", label: "Coder" },
          ],
        }),
      });
      await response.text();

      expect(dispatchReplyFromConfig).toHaveBeenCalledTimes(1);
      expect(spawnMentionedSubagent).toHaveBeenCalledTimes(2);
      expect(spawnMentionedSubagent).toHaveBeenNthCalledWith(
        1,
        expect.objectContaining({
          task: "please coordinate this",
          agentId: "researcher",
          label: "Researcher",
          parentSessionKey: "agent:main:channel:push-channel:direct:demo-session",
          requesterAgentId: "main",
        }),
      );
      expect(spawnMentionedSubagent).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({
          task: "please coordinate this",
          agentId: "coder",
          label: "Coder",
          parentSessionKey: "agent:main:channel:push-channel:direct:demo-session",
          requesterAgentId: "main",
        }),
      );
      expect(waitForRun).toHaveBeenCalledTimes(2);
      expect(waitForRun).toHaveBeenCalledWith({
        runId: "run-researcher",
        timeoutMs: monitorTesting.MENTION_SUBAGENT_WAIT_TIMEOUT_MS,
      });
      expect(waitForRun).toHaveBeenCalledWith({
        runId: "run-coder",
        timeoutMs: monitorTesting.MENTION_SUBAGENT_WAIT_TIMEOUT_MS,
      });
      expect(getSessionMessages).toHaveBeenCalledWith({
        sessionKey: "agent:researcher:subagent:child-a",
      });
      expect(getSessionMessages).toHaveBeenCalledWith({
        sessionKey: "agent:coder:subagent:child-b",
      });
      expect(finalizedContext).not.toBeNull();
      const bodyForAgent = finalizedContext?.["BodyForAgent"] ?? "";
      expect(bodyForAgent).toBe("please coordinate this");
      expect(finalizedContext?.["OriginatingTo"]).toBe("demo-session");
      expect(finalizedContext?.["WasMentioned"]).toBe(true);
      const dispatchedCtx = dispatchReplyFromConfig.mock.calls[0]?.[0]?.ctx as
        | Record<string, unknown>
        | undefined;
      const dispatchedBodyForAgent = dispatchedCtx?.["BodyForAgent"] ?? "";
      expect(dispatchedBodyForAgent).toContain("用户原始任务：\nplease coordinate this");
      expect(dispatchedBodyForAgent).toContain("## Researcher (@researcher)");
      expect(dispatchedBodyForAgent).toContain("Researcher found the background.");
      expect(dispatchedBodyForAgent).toContain("## Coder (@coder)");
      expect(dispatchedBodyForAgent).toContain("Coder produced the implementation notes.");
      expect(dispatchedCtx?.["WasMentioned"]).toBe(false);
    } finally {
      abortController.abort();
      await monitor;
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("does not mark plain direct push messages as mentioned", async () => {
    const port = await allocatePort();
    const abortController = new AbortController();
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "push-channel-direct-test-"));
    const storePath = path.join(tmpDir, "sessions.json");
    let finalizedContext: Record<string, unknown> | null = null;
    setPushChannelRuntime({
      channel: {
        session: {
          resolveStorePath: vi.fn(() => storePath),
          recordSessionMetaFromInbound: vi.fn(async () => ({
            sessionId: "direct-session",
            updatedAt: Date.now(),
          })),
          recordInboundSession: vi.fn(async () => {}),
        },
        reply: {
          finalizeInboundContext: vi.fn((ctx) => {
            finalizedContext = ctx as Record<string, unknown>;
            return ctx;
          }),
          withReplyDispatcher: vi.fn(async (params: { run: () => Promise<unknown> }) => {
            return await params.run();
          }),
          dispatchReplyFromConfig: vi.fn(async () => ({ text: "ok" })),
        },
      },
      agent: {
        session: {
          resolveStorePath: vi.fn(() => storePath),
          resolveSessionFilePath: vi.fn((sid: string) => path.join(tmpDir, `${sid}.jsonl`)),
        },
      },
    } as unknown as PluginRuntime);

    const monitor = startMonitorPushChannel({
      config: {
        channels: {
          "push-channel": {
            enabled: true,
            middlewareUrl: "http://127.0.0.1:1",
            listenPort: port,
            listenPath: "/webhook",
          },
        },
      } as never,
      runtime: { log: vi.fn() } as never,
      accountId: "default",
      abortSignal: abortController.signal,
    });
    try {
      await waitForServer(`http://127.0.0.1:${port}/health`);

      const response = await fetch(`http://127.0.0.1:${port}/webhook`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          agentId: "main",
          sessionId: "demo-session",
          content: "hello without mentions",
        }),
      });
      await response.text();

      expect(finalizedContext?.["WasMentioned"]).toBe(false);
    } finally {
      abortController.abort();
      await monitor;
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
