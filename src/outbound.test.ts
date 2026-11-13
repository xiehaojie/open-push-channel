import { afterEach, describe, expect, it, vi } from "vitest";
import { pushChannelOutbound } from "./outbound.js";
import { rememberPushChannelSessionRoute, clearPushChannelSessionRouteForTest } from "./session-routes.js";
import { sendPushMessage } from "./send.js";

vi.mock("./send.js", () => ({
  sendPushMessage: vi.fn(async () => {}),
}));

describe("push-channel outbound", () => {
  afterEach(() => {
    vi.clearAllMocks();
    clearPushChannelSessionRouteForTest();
  });

  it("sends async follow-ups to the agent registered for the push session", async () => {
    rememberPushChannelSessionRoute({ sessionId: "main-followup-session", agentId: "main" });

    await pushChannelOutbound.sendText?.({
      cfg: {
        channels: {
          "push-channel": {
            middlewareUrl: "http://127.0.0.1:3001",
          },
        },
      } as never,
      to: "main-followup-session",
      text: "subagents are done",
    });

    expect(sendPushMessage).toHaveBeenCalledWith({
      middlewareUrl: "http://127.0.0.1:3001",
      agentId: "main",
      sessionId: "main-followup-session",
      content: "subagents are done",
    });
  });
});
