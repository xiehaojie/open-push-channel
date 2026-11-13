import { describe, expect, it, vi } from "vitest";
import { registerStreamingToolHooks } from "./index.js";
import { sendPushEvent } from "./src/send.js";
import { testing as monitorTesting } from "./src/monitor.js";
import { setPushChannelRuntime } from "./src/runtime.js";
import { testing as subagentOrchestratorTesting } from "./src/subagent-orchestrator.js";
import { clearWriter, rememberPushSessionTarget, setWriter } from "./src/tool-store.js";

vi.mock("./src/send.js", () => ({
  sendPushEvent: vi.fn(async () => {}),
}));

describe("push-channel subagent hook streaming", () => {
  it("waits for mentioned subagents with the push-channel timeout budget", async () => {
    const waitForRun = vi.fn(async () => ({ status: "success" }));
    const getSessionMessages = vi.fn(async () => ({
      messages: [
        {
          type: "message",
          message: {
            role: "assistant",
            content: "完成了",
          },
        },
      ],
    }));
    subagentOrchestratorTesting.setSpawnMentionedSubagentImplForTest(async () => ({
      status: "accepted",
      runId: "run-1",
      childSessionKey: "agent:researcher:subagent:child",
    }));

    try {
      await monitorTesting.dispatchMentionedSubagents({
        core: {
          subagent: {
            waitForRun,
            getSessionMessages,
          },
        } as never,
        account: {
          accountId: "push-account",
          enabled: true,
          configured: true,
          config: {
            enabled: true,
            listenHost: "127.0.0.1",
            listenPort: 18080,
            listenPath: "/push-channel",
            middlewareUrl: "http://push-middleware.local",
          },
        },
        payload: {
          agentId: "main",
          sessionId: "session-1",
          content: "@researcher 调研一下",
          mentions: [{ agentId: "researcher", label: "Researcher" }],
        },
        parentSessionKey: "agent:main:channel:push-channel:direct:session-1",
        channelId: "push-channel",
        log: vi.fn(),
      });

      expect(waitForRun).toHaveBeenCalledWith({
        runId: "run-1",
        timeoutMs: monitorTesting.MENTION_SUBAGENT_WAIT_TIMEOUT_MS,
      });
      expect(sendPushEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          middlewareUrl: "http://push-middleware.local",
          agentId: "main",
          sessionId: "session-1",
          event: expect.objectContaining({
            type: "subagent_result",
            agentId: "researcher",
            label: "Researcher",
            childSessionKey: "agent:researcher:subagent:child",
            content: "完成了",
            status: "success",
          }),
        }),
      );
      expect(sendPushEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          middlewareUrl: "http://push-middleware.local",
          agentId: "main",
          sessionId: "session-1",
          event: expect.objectContaining({
            type: "subagent_end",
            agentId: "researcher",
            label: "Researcher",
            childSessionKey: "agent:researcher:subagent:child",
            status: "success",
          }),
        }),
      );
      expect(monitorTesting.MENTION_SUBAGENT_WAIT_TIMEOUT_MS).toBe(5 * 60 * 1000);
    } finally {
      subagentOrchestratorTesting.setSpawnMentionedSubagentImplForTest();
    }
  });

  it("emits subagent_start from the current subagent_spawned hook", async () => {
    const handlers = new Map<
      string,
      (event: Record<string, unknown>, ctx: Record<string, unknown>) => void
    >();
    const api = {
      on: vi.fn(
        (
          name: string,
          handler: (event: Record<string, unknown>, ctx: Record<string, unknown>) => void,
        ) => {
          handlers.set(name, handler);
        },
      ),
    };
    const events: Array<Record<string, unknown>> = [];
    const requesterSessionKey = "agent:main:channel:push-channel:direct:session-1";
    setWriter(requesterSessionKey, (event) => events.push(event));

    try {
      registerStreamingToolHooks(api as never);
      handlers.get("subagent_spawned")?.(
        {
          agentId: "researcher",
          label: "Researcher",
          childSessionKey: "agent:researcher:subagent:child",
        },
        {
          requesterSessionKey,
        },
      );
      handlers.get("subagent_spawned")?.(
        {
          agentId: "researcher",
          label: "Researcher",
          childSessionKey: "agent:researcher:subagent:child",
        },
        {
          requesterSessionKey,
        },
      );

      expect(events).toEqual([
        {
          type: "subagent_start",
          agentId: "researcher",
          label: "Researcher",
          childSessionKey: "agent:researcher:subagent:child",
        },
      ]);
    } finally {
      clearWriter(requesterSessionKey);
    }
  });

  it("emits subagent_end with the spawned agent label", async () => {
    const handlers = new Map<
      string,
      (event: Record<string, unknown>, ctx: Record<string, unknown>) => void
    >();
    const api = {
      on: vi.fn(
        (
          name: string,
          handler: (event: Record<string, unknown>, ctx: Record<string, unknown>) => void,
        ) => {
          handlers.set(name, handler);
        },
      ),
    };
    const events: Array<Record<string, unknown>> = [];
    const requesterSessionKey = "agent:main:channel:push-channel:direct:session-2";
    const childSessionKey = "agent:researcher:subagent:child";
    setWriter(requesterSessionKey, (event) => events.push(event));

    try {
      registerStreamingToolHooks(api as never);
      handlers.get("subagent_spawned")?.(
        {
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
        },
        {
          requesterSessionKey,
        },
      );
      await Promise.resolve(
        handlers.get("subagent_ended")?.(
          {
            targetSessionKey: childSessionKey,
            reason: "subagent-complete",
            outcome: "ok",
          },
          {
            requesterSessionKey,
          },
        ),
      );

      expect(events.at(-1)).toEqual({
        type: "subagent_end",
        agentId: "researcher",
        label: "Researcher",
        childSessionKey,
        status: "success",
      });
    } finally {
      clearWriter(requesterSessionKey);
    }
  });

  it("forwards child session tool activity as subagent progress to the requester stream", async () => {
    const handlers = new Map<
      string,
      (event: Record<string, unknown>, ctx: Record<string, unknown>) => void
    >();
    const api = {
      on: vi.fn(
        (
          name: string,
          handler: (event: Record<string, unknown>, ctx: Record<string, unknown>) => void,
        ) => {
          handlers.set(name, handler);
        },
      ),
    };
    const events: Array<Record<string, unknown>> = [];
    const requesterSessionKey = "agent:main:channel:push-channel:direct:session-3";
    const childSessionKey = "agent:researcher:subagent:child";
    setWriter(requesterSessionKey, (event) => events.push(event));

    try {
      registerStreamingToolHooks(api as never);
      handlers.get("subagent_spawned")?.(
        {
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
        },
        {
          requesterSessionKey,
        },
      );
      handlers.get("before_tool_call")?.(
        {
          toolCallId: "tool-1",
          toolName: "sessions_yield",
          params: { reason: "need more context" },
        },
        {
          sessionKey: childSessionKey,
        },
      );
      handlers.get("tool_result_persist")?.(
        {
          toolCallId: "tool-1",
          toolName: "sessions_yield",
          message: { content: [{ text: "yielded to parent" }] },
        },
        {
          sessionKey: childSessionKey,
        },
      );

      expect(events.slice(1)).toEqual([
        {
          type: "subagent_tool_call",
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
          toolCallId: "tool-1",
          toolName: "sessions_yield",
          args: { reason: "need more context" },
        },
        {
          type: "subagent_tool_result",
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
          toolCallId: "tool-1",
          toolName: "sessions_yield",
          content: [{ text: "yielded to parent" }],
        },
      ]);
    } finally {
      clearWriter(requesterSessionKey);
    }
  });

  it("pushes child session tool activity to the original channel session after the requester stream closes", async () => {
    const handlers = new Map<
      string,
      (event: Record<string, unknown>, ctx: Record<string, unknown>) => void
    >();
    const api = {
      on: vi.fn(
        (
          name: string,
          handler: (event: Record<string, unknown>, ctx: Record<string, unknown>) => void,
        ) => {
          handlers.set(name, handler);
        },
      ),
    };
    const requesterSessionKey = "agent:main:channel:push-channel:direct:session-4";
    const childSessionKey = "agent:coder:subagent:child";
    const streamedEvents: Array<Record<string, unknown>> = [];
    setWriter(requesterSessionKey, (event) => streamedEvents.push(event));
    rememberPushSessionTarget(requesterSessionKey, {
      middlewareUrl: "http://127.0.0.1:3001",
      agentId: "main",
      sessionId: "session-4",
    });

    registerStreamingToolHooks(api as never);
    handlers.get("subagent_spawned")?.(
      {
        agentId: "coder",
        label: "Coder",
        childSessionKey,
      },
      {
        requesterSessionKey,
      },
    );
    clearWriter(requesterSessionKey);
    handlers.get("before_tool_call")?.(
      {
        toolCallId: "tool-2",
        toolName: "shell",
        params: { command: "npm test" },
      },
      {
        sessionKey: childSessionKey,
      },
    );

    expect(sendPushEvent).toHaveBeenCalledWith({
      middlewareUrl: "http://127.0.0.1:3001",
      agentId: "main",
      sessionId: "session-4",
      event: {
        type: "subagent_tool_call",
        agentId: "coder",
        label: "Coder",
        childSessionKey,
        toolCallId: "tool-2",
        toolName: "shell",
        args: { command: "npm test" },
      },
    });
  });

  it("streams child session transcript updates into the requester stream", async () => {
    const handlers = new Map<
      string,
      (event: Record<string, unknown>, ctx: Record<string, unknown>) => void
    >();
    let transcriptListener:
      | ((update: { sessionKey?: string; message?: unknown; messageId?: string }) => void)
      | undefined;
    const api = {
      on: vi.fn(
        (
          name: string,
          handler: (event: Record<string, unknown>, ctx: Record<string, unknown>) => void,
        ) => {
          handlers.set(name, handler);
        },
      ),
      runtime: {
        events: {
          onSessionTranscriptUpdate: vi.fn((listener) => {
            transcriptListener = listener;
            return vi.fn();
          }),
        },
      },
    };
    const events: Array<Record<string, unknown>> = [];
    const requesterSessionKey = "agent:main:channel:push-channel:direct:session-live-transcript";
    const childSessionKey = "agent:researcher:subagent:child-live-transcript";
    setWriter(requesterSessionKey, (event) => events.push(event));

    try {
      registerStreamingToolHooks(api as never);
      handlers.get("subagent_spawned")?.(
        {
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
        },
        {
          requesterSessionKey,
        },
      );

      transcriptListener?.({
        sessionKey: childSessionKey,
        messageId: "assistant-live-1",
        message: {
          role: "assistant",
          content: [{ type: "text", text: "我开始" }],
        },
      });
      transcriptListener?.({
        sessionKey: childSessionKey,
        messageId: "assistant-live-1",
        message: {
          role: "assistant",
          content: [
            { type: "text", text: "我开始查资料。" },
            {
              type: "toolCall",
              id: "call-search",
              name: "web_search",
              arguments: { q: "OpenClaw push-channel" },
            },
          ],
        },
      });
      transcriptListener?.({
        sessionKey: childSessionKey,
        messageId: "tool-live-1",
        message: {
          role: "toolResult",
          toolCallId: "call-search",
          toolName: "web_search",
          content: [{ type: "text", text: "found docs" }],
          isError: false,
        },
      });

      expect(events.slice(1)).toEqual([
        {
          type: "subagent_stream",
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
          messageId: "assistant-live-1",
          content: "我",
          delta: "我",
        },
        {
          type: "subagent_stream",
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
          messageId: "assistant-live-1",
          content: "开",
          delta: "开",
        },
        {
          type: "subagent_stream",
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
          messageId: "assistant-live-1",
          content: "始",
          delta: "始",
        },
        {
          type: "subagent_stream",
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
          messageId: "assistant-live-1",
          content: "查",
          delta: "查",
        },
        {
          type: "subagent_stream",
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
          messageId: "assistant-live-1",
          content: "资",
          delta: "资",
        },
        {
          type: "subagent_stream",
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
          messageId: "assistant-live-1",
          content: "料",
          delta: "料",
        },
        {
          type: "subagent_stream",
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
          messageId: "assistant-live-1",
          content: "。",
          delta: "。",
        },
        {
          type: "subagent_tool_call",
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
          messageId: "assistant-live-1",
          toolCallId: "call-search",
          toolName: "web_search",
          args: { q: "OpenClaw push-channel" },
        },
        {
          type: "subagent_tool_result",
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
          messageId: "tool-live-1",
          toolCallId: "call-search",
          toolName: "web_search",
          content: [{ type: "text", text: "found docs" }],
          isError: false,
        },
      ]);
    } finally {
      clearWriter(requesterSessionKey);
    }
  });

  it("uses transcript events as primary source and registers cleanup for the listener", async () => {
    const handlers = new Map<
      string,
      (event: Record<string, unknown>, ctx: Record<string, unknown>) => void
    >();
    let transcriptListener:
      | ((update: { sessionKey?: string; message?: unknown; messageId?: string }) => void)
      | undefined;
    const unsubscribe = vi.fn();
    const registerRuntimeLifecycle = vi.fn();
    const api = {
      on: vi.fn(
        (
          name: string,
          handler: (event: Record<string, unknown>, ctx: Record<string, unknown>) => void,
        ) => {
          handlers.set(name, handler);
        },
      ),
      runtime: {
        events: {
          onSessionTranscriptUpdate: vi.fn((listener) => {
            transcriptListener = listener;
            return unsubscribe;
          }),
        },
      },
      lifecycle: {
        registerRuntimeLifecycle,
      },
    };
    const events: Array<Record<string, unknown>> = [];
    const requesterSessionKey = "agent:main:channel:push-channel:direct:session-live-dedupe";
    const childSessionKey = "agent:coder:subagent:child-live-dedupe";
    setWriter(requesterSessionKey, (event) => events.push(event));

    try {
      registerStreamingToolHooks(api as never);
      handlers.get("subagent_spawned")?.(
        {
          agentId: "coder",
          label: "Coder",
          childSessionKey,
        },
        {
          requesterSessionKey,
        },
      );

      transcriptListener?.({
        sessionKey: childSessionKey,
        messageId: "assistant-live-dedupe",
        message: {
          role: "assistant",
          content: [
            {
              type: "toolCall",
              id: "call-shell",
              name: "shell",
              arguments: { command: "npm test" },
            },
          ],
        },
      });
      handlers.get("before_tool_call")?.(
        {
          toolCallId: "call-shell",
          toolName: "shell",
          params: { command: "npm test" },
        },
        {
          sessionKey: childSessionKey,
        },
      );

      expect(events.filter((event) => event.type === "subagent_tool_call")).toEqual([
        {
          type: "subagent_tool_call",
          agentId: "coder",
          label: "Coder",
          childSessionKey,
          messageId: "assistant-live-dedupe",
          toolCallId: "call-shell",
          toolName: "shell",
          args: { command: "npm test" },
        },
      ]);
      expect(registerRuntimeLifecycle).toHaveBeenCalledWith({
        id: "push-channel-subagent-transcript-updates",
        description: "Unsubscribe push-channel child session transcript update streaming.",
        cleanup: expect.any(Function),
      });
      const lifecycle = registerRuntimeLifecycle.mock.calls[0]?.[0];
      lifecycle.cleanup({ reason: "restart" });
      expect(unsubscribe).toHaveBeenCalledTimes(1);
    } finally {
      clearWriter(requesterSessionKey);
    }
  });

  it("replays child session assistant text and tool records into the requester stream on subagent end", async () => {
    const handlers = new Map<
      string,
      (event: Record<string, unknown>, ctx: Record<string, unknown>) => unknown
    >();
    const api = {
      on: vi.fn(
        (
          name: string,
          handler: (event: Record<string, unknown>, ctx: Record<string, unknown>) => unknown,
        ) => {
          handlers.set(name, handler);
        },
      ),
    };
    const events: Array<Record<string, unknown>> = [];
    const requesterSessionKey = "agent:main:channel:push-channel:direct:session-5";
    const childSessionKey = "agent:researcher:subagent:child-session";
    setWriter(requesterSessionKey, (event) => events.push(event));
    setPushChannelRuntime({
      subagent: {
        getSessionMessages: vi.fn(async () => ({
          messages: [
            {
              type: "message",
              id: "assistant-1",
              message: {
                role: "assistant",
                content: [
                  { type: "text", text: "我先查询天气。" },
                  {
                    type: "toolCall",
                    id: "call-weather",
                    name: "web_fetch",
                    arguments: { url: "https://www.weather.com.cn/weather/101010800.shtml" },
                  },
                ],
              },
            },
            {
              type: "message",
              id: "tool-1",
              message: {
                role: "toolResult",
                toolCallId: "call-weather",
                toolName: "web_fetch",
                content: [{ type: "text", text: '{"status":200}' }],
                isError: false,
              },
            },
          ],
        })),
      },
    } as never);

    try {
      registerStreamingToolHooks(api as never);
      handlers.get("subagent_spawned")?.(
        {
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
        },
        {
          requesterSessionKey,
        },
      );

      await Promise.resolve(
        handlers.get("subagent_ended")?.(
          {
            targetSessionKey: childSessionKey,
            reason: "subagent-complete",
            outcome: "ok",
          },
          {
            requesterSessionKey,
          },
        ),
      );

      expect(events.slice(1)).toEqual([
        {
          type: "subagent_message",
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
          messageId: "assistant-1",
          content: "我先查询天气。",
        },
        {
          type: "subagent_tool_call",
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
          messageId: "assistant-1",
          toolCallId: "call-weather",
          toolName: "web_fetch",
          args: { url: "https://www.weather.com.cn/weather/101010800.shtml" },
        },
        {
          type: "subagent_tool_result",
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
          messageId: "tool-1",
          toolCallId: "call-weather",
          toolName: "web_fetch",
          content: [{ type: "text", text: '{"status":200}' }],
          isError: false,
        },
        {
          type: "subagent_end",
          agentId: "researcher",
          label: "Researcher",
          childSessionKey,
          status: "success",
        },
      ]);
    } finally {
      clearWriter(requesterSessionKey);
    }
  });
});
