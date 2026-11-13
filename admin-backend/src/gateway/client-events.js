const childTypes = {
  "subagent.started": "subagent_start",
  "subagent.message": "subagent_message",
  "subagent.delta": "subagent_stream",
  "subagent.tool.started": "subagent_tool_call",
  "subagent.tool.completed": "subagent_tool_result",
  "subagent.result": "subagent_result",
  "subagent.failed": "subagent_error",
  "subagent.ended": "subagent_end",
};

// frontend-demo/sdk.js is the existing public client contract. Keep its
// presentation names here so neither the gateway nor runtime adapters depend on them.
function createClientProjector() {
  let answerMessageId;
  let part = 0;
  let streamOpen = false;
  return (event) => {
    const { type, data } = event;
    const messages = [];
    const send = (payload) =>
      messages.push({
        ...payload,
        sessionId: event.audience === "agent" ? "" : event.conversationId,
        runtimeId: event.runtimeId,
        runId: event.runId,
        ...(event.origin ? { origin: event.origin } : {}),
        ...(event.requestId ? { queryMessageId: event.requestId } : {}),
        ...(answerMessageId ? { answerMessageId } : {}),
      });
    // Only canonical message/terminal boundaries close the parent stream;
    // interleaved tools or children must not detach a later snapshot correction.
    const close = () => {
      if (streamOpen) {
        send({ type: "stream_end" });
      }
      streamOpen = false;
    };
    if (type === "input.accepted") {
      send({
        type: "message_sent",
        content: data.content,
        role: "user",
        ...(event.replyId ? { answerMessageId: event.replyId } : {}),
      });
    } else if (type === "run.started") {
      send({ type: "run_started" });
    } else if (type === "input.control" || type === "input.queue") {
      send({ ...data, type: type === "input.control" ? "input_control" : "input_queue" });
    } else if (type === "message.started") {
      close();
      answerMessageId = event.replyId ? `${event.replyId}-${++part}` : data.messageId;
      streamOpen = true;
      send({ type: "stream_start", from: "Assistant" });
    } else if (type === "message.delta") {
      send({ type: "stream", content: data.delta, role: "assistant" });
    } else if (type === "message.snapshot") {
      send({ type: "stream_snapshot", snapshot: data.text, role: "assistant" });
    } else if (type === "message.completed") {
      close();
    } else if (type === "reasoning.started") {
      send({ type: "reasoning_start" });
    } else if (type === "reasoning.delta") {
      send({
        type: "reasoning_delta",
        delta: data.delta,
        ...(data.replace ? { replace: true } : {}),
      });
    } else if (type === "reasoning.completed") {
      send({ type: "reasoning_end" });
    } else if (type === "activity.started" || type === "activity.completed") {
      send({ type: type === "activity.started" ? "tool_start" : "tool_end" });
    } else if (type === "tool.started" || type === "tool.completed") {
      send({ ...data, type: type === "tool.started" ? "tool_call" : "tool_result" });
    } else if (Object.hasOwn(childTypes, type)) {
      const { childSessionId, ...payload } = data;
      send({
        ...payload,
        type: childTypes[type],
        ...(childSessionId ? { childSessionKey: childSessionId } : {}),
      });
    } else if (type === "run.failed") {
      close();
      send({ type: "error", message: data.message });
    } else if (type === "run.deferred") {
      close();
      send({ type: "timeout_deferred", message: data.message });
    } else if (type === "run.cancelled") {
      close();
      send({ type: "run_cancelled" });
    } else if (type === "run.completed") {
      close();
      send({ type: "run_completed" });
    }
    return messages;
  };
}

module.exports = { createClientProjector };
