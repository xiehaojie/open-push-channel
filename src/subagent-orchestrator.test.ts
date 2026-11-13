import { describe, expect, it, vi } from "vitest";
import type { PluginRuntime } from "openclaw/plugin-sdk/runtime-store";
import { spawnMentionedSubagent } from "./subagent-orchestrator.js";
import { setPushChannelRuntime } from "./runtime.js";

describe("push-channel mentioned subagent orchestration", () => {
  it("runs mentioned agents through the plugin runtime subagent API", async () => {
    const run = vi.fn(async () => ({ runId: "run-1" }));
    setPushChannelRuntime({
      subagent: {
        run,
      },
    } as unknown as PluginRuntime);

    const result = await spawnMentionedSubagent({
      task: "please research this",
      agentId: "researcher",
      label: "Researcher",
      parentSessionKey: "agent:main:channel:push-channel:direct:session-1",
      channelId: "push-channel",
      accountId: "default",
      sessionId: "session-1",
      requesterAgentId: "main",
    });

    expect(result).toEqual(
      expect.objectContaining({
        status: "accepted",
        runId: "run-1",
        childSessionKey: expect.stringMatching(/^agent:researcher:subagent:/),
      }),
    );
    expect(run).toHaveBeenCalledWith({
      sessionKey: result.childSessionKey,
      message: "please research this",
      deliver: false,
      idempotencyKey: expect.stringMatching(/^push-channel:session-1:researcher:/),
    });
  });
});
