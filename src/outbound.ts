import type { ChannelOutboundAdapter } from "openclaw/plugin-sdk/channel-contract";
import { PUSH_CHANNEL_ID, resolvePushChannelConfig } from "./config.js";
import { sendPushMessage } from "./send.js";
import { resolvePushChannelAgentIdForSession } from "./session-routes.js";

export const pushChannelOutbound: ChannelOutboundAdapter = {
  deliveryMode: "direct",
  deliveryCapabilities: {
    durableFinal: {
      text: true,
    },
  },
  sendText: async ({ cfg, to, text, threadId }) => {
    const config = resolvePushChannelConfig(cfg);
    if (!config.middlewareUrl) {
      throw new Error("[PushChannel] middlewareUrl not configured");
    }
    const sessionId = threadId == null ? to : String(threadId);
    const agentId =
      resolvePushChannelAgentIdForSession(sessionId) ??
      resolvePushChannelAgentIdForSession(to) ??
      to;

    await sendPushMessage({
      middlewareUrl: config.middlewareUrl,
      agentId,
      sessionId,
      content: text,
    });

    return {
      channel: PUSH_CHANNEL_ID,
      messageId: `push-${Date.now()}`,
      timestamp: Date.now(),
      conversationId: sessionId,
    };
  },
};
