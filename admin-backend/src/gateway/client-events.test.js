const assert = require("node:assert/strict");
const test = require("node:test");
const { createClientProjector } = require("./client-events");

void test("control and queue projections preserve the active run without replacing answer identity", () => {
  const project = createClientProjector();
  const base = { conversationId: "chat", runtimeId: "pi", runId: "active", requestId: "root", replyId: "answer" };
  assert.equal(project({ ...base, type: "run.started", data: {} })[0].runId, "active");
  project({ ...base, type: "message.started", data: { messageId: "native" } });
  const receipt = project({ ...base, type: "input.control", data: { controlId: "control", operation: "steer", content: "adjust", status: "queued" } })[0];
  assert.equal(receipt.type, "input_control");
  assert.equal(receipt.controlId, "control");
  assert.equal(receipt.runId, "active");
  assert.equal(receipt.queryMessageId, "root");
  assert.equal(receipt.answerMessageId, "answer-1");
  const queue = project({ ...base, type: "input.queue", data: { steering: ["adjust"], followUp: [] } })[0];
  assert.equal(queue.type, "input_queue");
  assert.deepEqual(queue.steering, ["adjust"]);
  assert.equal(queue.runId, "active");
});

for (const { name, events, projectedTypes } of [
  {
    name: "tool execution",
    events: [
      {
        type: "tool.started",
        data: { toolCallId: "read-1", toolName: "read", args: { path: "marker.txt" } },
      },
      { type: "tool.completed", data: { toolCallId: "read-1", content: "marker" } },
    ],
    projectedTypes: ["tool_call", "tool_result"],
  },
  {
    name: "activity indicators",
    events: [
      { type: "activity.started", data: {} },
      { type: "activity.completed", data: {} },
    ],
    projectedTypes: ["tool_start", "tool_end"],
  },
  {
    name: "child execution",
    events: [
      { type: "subagent.started", data: { agentId: "child", childSessionId: "child-session" } },
      { type: "subagent.delta", data: { agentId: "child", delta: "child progress" } },
      { type: "subagent.result", data: { agentId: "child", content: "child result" } },
      { type: "subagent.ended", data: { agentId: "child", status: "ok" } },
    ],
    projectedTypes: ["subagent_start", "subagent_stream", "subagent_result", "subagent_end"],
  },
]) {
  void test(`client projection preserves the native message across ${name} until message.completed`, () => {
    const project = createClientProjector();
    const signals = [
      { type: "message.started", data: { messageId: "native-message" } },
      { type: "message.delta", data: { messageId: "native-message", delta: "draft" } },
      ...events,
      { type: "message.snapshot", data: { messageId: "native-message", text: "corrected" } },
      { type: "message.completed", data: { messageId: "native-message" } },
    ];
    const projected = signals.flatMap((signal) =>
      project({
        ...signal,
        conversationId: "parent-session",
        runtimeId: "runtime",
        replyId: "parent-answer",
      }),
    );
    assert.deepEqual(
      projected.map((event) => event.type),
      ["stream_start", "stream", ...projectedTypes, "stream_snapshot", "stream_end"],
    );
    assert.ok(projected.every((event) => event.answerMessageId === "parent-answer-1"));
    assert.ok(projected.every((event) => event.sessionId === "parent-session"));
    assert.equal(projected.at(-2).snapshot, "corrected");
    if (name === "tool execution") {
      assert.deepEqual(projected.find((event) => event.type === "tool_call").args, {
        path: "marker.txt",
      });
      assert.equal(projected.find((event) => event.type === "tool_result").content, "marker");
    }
    if (name === "child execution") {
      assert.equal(
        projected.find((event) => event.type === "subagent_start").childSessionKey,
        "child-session",
      );
      assert.equal(
        projected.find((event) => event.type === "subagent_result").content,
        "child result",
      );
    }
  });
}
