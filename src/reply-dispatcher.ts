import type { ServerResponse } from "node:http";
import { sendPushMessage } from "./send.js";
import { setWriter, clearWriter, rememberPushSessionTarget } from "./tool-store.js";

const REQUEST_TIMEOUT_HINT = "Request timed out before a response was generated";
const REASONING_EVENT_TYPES = new Set(["reasoning_start", "reasoning_delta", "reasoning_end"]);

type ReplyTextPayload = {
  text?: unknown;
  content?: unknown;
  isReasoningSnapshot?: unknown;
  delta?: unknown;
  replace?: unknown;
  isError?: unknown;
  isStatusNotice?: unknown;
  isFallbackNotice?: unknown;
  isCompactionNotice?: unknown;
};

function isTimeoutFinalReply(text: string): boolean {
  return text.includes(REQUEST_TIMEOUT_HINT);
}

function resolvePayloadText(payload: ReplyTextPayload): string {
  if (typeof payload.text === "string") {
    return payload.text;
  }
  return typeof payload.content === "string" ? payload.content : "";
}

function isNoticePayload(payload: ReplyTextPayload): boolean {
  return (
    payload.isError === true ||
    payload.isStatusNotice === true ||
    payload.isFallbackNotice === true ||
    payload.isCompactionNotice === true
  );
}

export function createStreamingReplyDispatcher(
  res: ServerResponse,
  sessionKey?: string,
  pushTarget?: {
    middlewareUrl?: string;
    agentId: string;
    sessionId?: string;
  },
) {
  // Send initial headers
  if (!res.headersSent) {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "Transfer-Encoding": "chunked",
      "X-Accel-Buffering": "no",
    });
    res.flushHeaders?.();
  }

  // Track whether any tokens were streamed via onPartialReply to avoid
  // duplicating content when sendBlockReply/sendFinalReply fire afterwards.
  let hasStreamedContent = false;
  let previousText = "";
  let standaloneReplySent = false;
  let timeoutDeferred = false;
  let previousReasoningText = "";
  let reasoningStarted = false;
  let toolExecutionStarted = false;
  let toolExecutionEnded = false;

  const emitEvent = (event: Record<string, unknown>) => {
    if (!REASONING_EVENT_TYPES.has(String(event.type))) {
      emitReasoningEnd();
    }
    if (!res.writableEnded) {
      res.write(`data: ${JSON.stringify(event)}\n\n`);
    }
  };

  const emitReasoningEnd = () => {
    if (!reasoningStarted) {
      return;
    }
    reasoningStarted = false;
    previousReasoningText = "";
    emitEvent({ type: "reasoning_end" });
  };

  const emitToolStart = () => {
    if (res.writableEnded || toolExecutionStarted) {
      return;
    }
    toolExecutionStarted = true;
    emitEvent({ type: "tool_start" });
  };

  const emitToolEnd = () => {
    if (res.writableEnded || !toolExecutionStarted || toolExecutionEnded) {
      return;
    }
    toolExecutionEnded = true;
    emitEvent({ type: "tool_end" });
  };

  // Register a writer so before_tool_call / tool_result_persist hooks can emit
  // tool_call / tool_result events directly into this SSE stream.
  if (sessionKey) {
    setWriter(sessionKey, (event) => {
      emitEvent(event);
    });
    if (pushTarget?.middlewareUrl) {
      rememberPushSessionTarget(sessionKey, {
        middlewareUrl: pushTarget.middlewareUrl,
        agentId: pushTarget.agentId,
        sessionId: pushTarget.sessionId,
      });
    }
  }

  const onPartialReply = (payload: ReplyTextPayload) => {
    const fullText = typeof payload.text === "string" ? payload.text : "";
    if (res.writableEnded || fullText === previousText) {
      return;
    }
    const replace = payload.replace === true || !fullText.startsWith(previousText);
    const delta = fullText.slice(previousText.length);
    previousText = fullText;
    hasStreamedContent = true;
    emitEvent(replace ? { type: "content_snapshot", text: fullText } : { type: "content", delta });
  };

  const onAssistantMessageStart = () => {
    previousText = "";
    hasStreamedContent = false;
    standaloneReplySent = false;
    emitEvent({ type: "message_start" });
  };

  const onReasoningStream = (payload: ReplyTextPayload) => {
    const text = typeof payload.text === "string" ? payload.text : "";
    if (res.writableEnded || !text) {
      return;
    }
    if (!reasoningStarted) {
      reasoningStarted = true;
      emitEvent({ type: "reasoning_start" });
    }
    if (text === previousReasoningText) {
      return;
    }
    const replace = payload.isReasoningSnapshot === true && !text.startsWith(previousReasoningText);
    const delta = replace
      ? text
      : text.startsWith(previousReasoningText)
        ? text.slice(previousReasoningText.length)
        : text;
    previousReasoningText = text;
    if (delta) {
      emitEvent({
        type: "reasoning_delta",
        delta,
        content: delta,
        snapshot: replace,
        ...(replace ? { replace } : {}),
      });
    }
  };

  const onReasoningEnd = () => {
    emitReasoningEnd();
  };

  const dispatcher = {
    sendToolResult: () => {
      emitToolStart();
      return true;
    },
    sendBlockReply: (payload: ReplyTextPayload) => {
      if (res.writableEnded) {
        return false;
      }
      const notice = isNoticePayload(payload);
      // Core compaction notices run between native messages, independently of
      // model block acknowledgments. Keep their boundary out of cumulative state.
      if (!hasStreamedContent || notice) {
        const text = resolvePayloadText(payload);
        if (text) {
          if (notice) {
            emitEvent({ type: "message_start" });
            standaloneReplySent = true;
          }
          emitEvent({ type: "content", delta: text });
        }
      }

      return true;
    },
    sendFinalReply: (payload: ReplyTextPayload) => {
      if (res.writableEnded) {
        return false;
      }
      const text = resolvePayloadText(payload);
      const timeoutFinal = typeof text === "string" && isTimeoutFinalReply(text);
      if (timeoutFinal) {
        // Timeout final replies usually mean tool work may still be running.
        // Emit an explicit marker so middleware/frontend can defer timeout UX
        // and keep waiting for follow-up push notifications.
        emitToolStart();
        timeoutDeferred = true;
        emitEvent({
          type: "timeout_deferred",
          message: "Tools are still running. Waiting for a follow-up notification.",
        });
      } else {
        emitToolEnd();
      }
      const isNotice = isNoticePayload(payload);
      // Normal final model text already arrived through authoritative partials.
      // Notices are separate payloads; never discard them with the model body.
      if (!timeoutFinal && text && (!hasStreamedContent || isNotice)) {
        if (isNotice || standaloneReplySent) {
          emitEvent({ type: "message_start" });
        }
        emitEvent({ type: "content", delta: text });
        standaloneReplySent = true;
      }
      return true;
    },
    waitForIdle: async () => {},
    getQueuedCounts: () => ({ tool: 0, block: 0, final: 0 }),
    getFailedCounts: () => ({ tool: 0, block: 0, final: 0 }),
    markComplete: () => {
      if (res.writableEnded) {
        return;
      }
      // Core settles this dispatcher in finally, even when dispatch failed.
      // The HTTP owner alone knows whether to end with success or error.
      emitReasoningEnd();
      if (!timeoutDeferred) {
        emitToolEnd();
      }
      if (sessionKey) {
        clearWriter(sessionKey);
      }
    },
  };

  return { dispatcher, onPartialReply, onAssistantMessageStart, onReasoningStream, onReasoningEnd };
}

export function createPushChannelReplyDispatcher(params: {
  middlewareUrl: string;
  agentId: string;
  sessionId?: string;
}) {
  const { middlewareUrl, agentId, sessionId } = params;
  return {
    sendToolResult: () => true,
    sendBlockReply: () => true,
    sendFinalReply: (payload: ReplyTextPayload) => {
      const text = resolvePayloadText(payload);
      if (text) {
        sendPushMessage({ middlewareUrl, agentId, sessionId, content: text }).catch(
          (err: unknown) => {
            console.warn(`Failed to send reply to ${agentId}: ${String(err)}`);
          },
        );
      }
      return true;
    },
    waitForIdle: async () => {},
    getQueuedCounts: () => ({ tool: 0, block: 0, final: 0 }),
    getFailedCounts: () => ({ tool: 0, block: 0, final: 0 }),
    markComplete: () => {},
  };
}
