const { randomUUID } = require("node:crypto");
const { StringDecoder } = require("node:string_decoder");
const fetch = require("node-fetch");
const { validateSignal } = require("../../../protocol/index.cjs");
const { registerAdapterCredential } = require("../gateway/auth");

const childTypes = {
  subagent_start: "subagent.started",
  subagent_message: "subagent.message",
  subagent_stream: "subagent.delta",
  subagent_tool_call: "subagent.tool.started",
  subagent_tool_result: "subagent.tool.completed",
  subagent_result: "subagent.result",
  subagent_error: "subagent.failed",
  subagent_end: "subagent.ended",
};

function createOpenClawDecoder(emit) {
  let messageId;
  let deferred;
  const send = (type, data = {}) => {
    emit(validateSignal({ type, data }));
  };
  const closeMessage = () => {
    if (!messageId) {
      return;
    }
    send("message.completed", { messageId });
    messageId = undefined;
  };
  return {
    get deferred() {
      return deferred;
    },
    accept(event) {
      if (!event || typeof event !== "object" || typeof event.type !== "string") {
        throw new Error("Invalid OpenClaw stream event");
      }
      if (event.type === "message_start") {
        closeMessage();
        return;
      }
      if (event.type === "content" || event.type === "content_snapshot") {
        const snapshot = event.type === "content_snapshot";
        const text = snapshot ? event.text : event.delta;
        if (typeof text !== "string") {
          throw new Error("Invalid OpenClaw content text");
        }
        if (!messageId) {
          messageId = randomUUID();
          send("message.started", { messageId });
        }
        send(snapshot ? "message.snapshot" : "message.delta", {
          messageId,
          ...(snapshot ? { text } : { delta: text }),
        });
        return;
      }
      if (event.type === "reasoning_start") {
        send("reasoning.started");
        return;
      }
      if (event.type === "reasoning_delta") {
        send("reasoning.delta", {
          delta: event.delta,
          ...(event.replace === true ? { replace: true } : {}),
        });
        return;
      }
      if (event.type === "reasoning_end") {
        send("reasoning.completed");
        return;
      }
      if (event.type === "error") {
        throw new Error(event.message || "OpenClaw dispatch failed");
      }
      if (event.type === "tool_start") {
        send("activity.started");
        return;
      }
      if (event.type === "tool_end") {
        send("activity.completed");
        return;
      }
      if (event.type === "tool_call" || event.type === "tool_result") {
        const { type, ...data } = event;
        if (type === "tool_call") {
          data.args ??= {};
        }
        send(type === "tool_call" ? "tool.started" : "tool.completed", data);
        return;
      }
      if (Object.hasOwn(childTypes, event.type)) {
        const data = {};
        for (const key of [
          "agentId",
          "label",
          "messageId",
          "toolCallId",
          "toolName",
          "args",
          "content",
          "delta",
          "isError",
          "message",
          "status",
        ]) {
          if (event[key] !== undefined) {
            data[key] = event[key];
          }
        }
        if (event.childSessionKey) {
          data.childSessionId = event.childSessionKey;
        }
        if (event.type === "subagent_tool_call") {
          data.args ??= {};
        }
        send(childTypes[event.type], data);
        return;
      }
      if (event.type === "timeout_deferred") {
        deferred = {
          status: "deferred",
          message: event.message || "Waiting for a follow-up notification.",
        };
        return;
      }
      if (event.type !== "done") {
        throw new Error(`Unknown OpenClaw stream event: ${event.type}`);
      }
      // Tool/child presentation is independent of a native assistant message.
      // Revisions retain that identity until the next native boundary or done.
      closeMessage();
    },
  };
}

function createOpenClawAdapter({ id = "openclaw", url, webhookToken: rawToken }) {
  const webhookToken = typeof rawToken === "string" ? rawToken.trim() : undefined;
  // node-fetch can include the complete invalid header value in an exception.
  if (
    rawToken !== undefined &&
    rawToken !== "" &&
    (!webhookToken || !/^[a-zA-Z0-9._~+/-]+=*$/.test(webhookToken))
  ) {
    throw new Error("Invalid OpenClaw webhook Bearer token format");
  }
  const adapter = {
    id,
    capabilities: {
      streaming: true,
      tools: true,
      reasoning: true,
      subagents: true,
      cancel: false,
      mentions: true,
      sessionRestore: true,
    },
    async run(request, { emit, signal, onAccepted, onDispatch }) {
      onDispatch?.();
      const response = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(webhookToken ? { Authorization: `Bearer ${webhookToken}` } : {}),
        },
        // Never forward the webhook credential to a redirect target.
        ...(webhookToken ? { redirect: "error" } : {}),
        body: JSON.stringify({
          agentId: request.agentId,
          sessionId: request.conversationId,
          ...(request.nativeSessionId ? { nativeSessionId: request.nativeSessionId } : {}),
          content: request.content,
          ...(request.mentions ? { mentions: request.mentions } : {}),
        }),
        signal,
      });
      if (!response.ok) {
        response.body?.destroy();
        throw new Error(`OpenClaw error: ${response.status} ${response.statusText}`);
      }
      if (!response.body || !response.headers.get("content-type")?.includes("text/event-stream")) {
        response.body?.destroy();
        throw new Error("OpenClaw response is not an SSE stream");
      }
      onAccepted?.();
      const decoder = createOpenClawDecoder(emit);
      const utf8 = new StringDecoder("utf8");
      let buffer = "";
      let data = [];
      let done = false;
      const line = (rawLine) => {
        const value = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
        if (value === "") {
          if (data.length === 0) {
            return;
          }
          const event = JSON.parse(data.join("\n"));
          data = [];
          decoder.accept(event);
          done = event.type === "done";
        } else if (value.startsWith("data:")) {
          const field = value.slice(5);
          data.push(field.startsWith(" ") ? field.slice(1) : field);
        }
      };
      // Iterate the entire response, rather than returning after installing
      // listeners: the gateway lock must cover the full agent activity.
      for await (const chunk of response.body) {
        buffer += utf8.write(chunk);
        let newline;
        while ((newline = buffer.indexOf("\n")) !== -1) {
          line(buffer.slice(0, newline));
          buffer = buffer.slice(newline + 1);
          if (done) {
            return decoder.deferred;
          }
        }
      }
      throw new Error("OpenClaw stream ended before done");
    },
    decodePushEvent(event) {
      const signals = [];
      const decoder = createOpenClawDecoder((signal) => signals.push(signal));
      decoder.accept(event);
      return signals;
    },
  };
  registerAdapterCredential(adapter.run, "webhook", webhookToken);
  return adapter;
}

module.exports = { createOpenClawAdapter, createOpenClawDecoder };
