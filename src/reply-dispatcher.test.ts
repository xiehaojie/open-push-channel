import { describe, expect, it } from "vitest";
import { createStreamingReplyDispatcher } from "./reply-dispatcher.js";

function streamingFixture() {
  const writes: string[] = [];
  const response = {
    headersSent: true,
    writableEnded: false,
    write: (chunk: string) => writes.push(chunk),
    end() {
      this.writableEnded = true;
    },
  };
  const stream = createStreamingReplyDispatcher(response as never);
  const events = () => writes.map((write) => JSON.parse(write.slice(6)) as Record<string, unknown>);
  const text = () =>
    events().reduce(
      (current, event) =>
        event.type === "content_snapshot"
          ? String(event.text)
          : event.type === "content"
            ? current + String(event.delta)
            : current,
      "",
    );
  return { stream, response, events, text };
}

describe("createStreamingReplyDispatcher", () => {
  it("replaces same-length and shorter revisions without deleting legitimate repeated text", () => {
    const { stream, text, events } = streamingFixture();
    stream.onPartialReply({ text: "Hello", delta: "Hello" });
    stream.onPartialReply({ text: "Hallo", replace: true });
    stream.onPartialReply({ text: "ha", replace: true });
    stream.onPartialReply({ text: "haha", delta: "ha" });
    expect(text()).toBe("haha");
    expect(
      events()
        .filter((event) => event.type === "content_snapshot")
        .map((event) => event.text),
    ).toEqual(["Hallo", "ha"]);
  });

  it("does not treat block delivery as a new cumulative partial message", () => {
    const { stream, text } = streamingFixture();
    stream.onPartialReply({ text: "AB" });
    stream.dispatcher.sendBlockReply({ text: "AB" });
    stream.onPartialReply({ text: "ABCD" });
    expect(text()).toBe("ABCD");
  });

  it("rotates only at explicit native assistant message boundaries", () => {
    const { stream, events } = streamingFixture();
    stream.onAssistantMessageStart();
    stream.onPartialReply({ text: "long first answer" });
    stream.onAssistantMessageStart();
    stream.onPartialReply({ text: "ha" });
    stream.onPartialReply({ text: "haha" });
    expect(events().filter((event) => event.type === "message_start")).toHaveLength(2);
    expect(
      events()
        .filter((event) => event.type === "content")
        .map((event) => event.delta),
    ).toEqual(["long first answer", "ha", "ha"]);
  });

  it("keeps multiple command or nonstream final payloads until dispatcher settlement", () => {
    const { stream, response, events } = streamingFixture();
    expect(stream.dispatcher.sendFinalReply({ text: "first" })).toBe(true);
    expect(stream.dispatcher.sendFinalReply({ text: "second" })).toBe(true);
    expect(response.writableEnded).toBe(false);
    expect(events().some((event) => event.type === "done")).toBe(false);
    stream.dispatcher.markComplete();
    expect(response.writableEnded).toBe(false);
    expect(events().filter((event) => event.type === "done")).toHaveLength(0);
    expect(
      events()
        .filter((event) => event.type === "content")
        .map((event) => event.delta),
    ).toEqual(["first", "second"]);
  });

  it("settles a deferred timeout without reporting unfinished tools as completed", () => {
    const { stream, events, response } = streamingFixture();
    stream.dispatcher.sendFinalReply({ text: "Request timed out before a response was generated" });
    stream.dispatcher.markComplete();
    expect(response.writableEnded).toBe(false);
    expect(events().map((event) => event.type)).toEqual(["tool_start", "timeout_deferred"]);
  });

  it.each(["isError", "isStatusNotice", "isFallbackNotice", "isCompactionNotice"] as const)(
    "does not swallow a %s final after a partial preview",
    (noticeFlag) => {
      const { stream, events } = streamingFixture();
      stream.onPartialReply({ text: "preview" });
      stream.dispatcher.sendFinalReply({ text: "delivery warning", [noticeFlag]: true });
      stream.dispatcher.markComplete();
      expect(
        events()
          .filter((event) => event.type === "content")
          .map((event) => event.delta),
      ).toEqual(["preview", "delivery warning"]);
    },
  );

  it("does not replay a normal streamed final and can settle an empty no-reply turn", () => {
    const { stream, events, response } = streamingFixture();
    stream.onPartialReply({ text: "answer" });
    stream.dispatcher.sendFinalReply({ text: "answer" });
    stream.dispatcher.markComplete();
    expect(events().filter((event) => event.type === "content")).toHaveLength(1);
    expect(response.writableEnded).toBe(false);
    const empty = streamingFixture();
    empty.stream.dispatcher.markComplete();
    empty.stream.dispatcher.markComplete();
    expect(empty.events()).toEqual([]);
    expect(empty.response.writableEnded).toBe(false);
  });

  it("does not let a final notice make the already-streamed normal final repeat", () => {
    const { stream, events } = streamingFixture();
    stream.onAssistantMessageStart();
    stream.onPartialReply({ text: "answer" });
    stream.dispatcher.sendFinalReply({ text: "notice", isStatusNotice: true });
    stream.dispatcher.sendFinalReply({ text: "answer" });
    stream.dispatcher.markComplete();
    expect(
      events()
        .filter((event) => event.type === "content")
        .map((event) => event.delta),
    ).toEqual(["answer", "notice"]);
  });

  it("does not swallow core compaction block notices after streamed model text", () => {
    const { stream, events } = streamingFixture();
    stream.onPartialReply({ text: "answer" });
    stream.dispatcher.sendBlockReply({ text: "Compacting context", isCompactionNotice: true });
    stream.dispatcher.sendFinalReply({ text: "answer" });
    expect(
      events()
        .filter((event) => event.type === "content")
        .map((event) => event.delta),
    ).toEqual(["answer", "Compacting context"]);
  });

  it("emits a reasoning lifecycle with incremental deltas", () => {
    const writes: string[] = [];
    const response = {
      headersSent: true,
      writableEnded: false,
      write: (chunk: string) => writes.push(chunk),
    };
    const { onReasoningEnd, onReasoningStream } = createStreamingReplyDispatcher(response as never);

    onReasoningStream({ text: "Reviewing" });
    onReasoningStream({ text: "Reviewing the request" });
    onReasoningEnd();

    expect(writes).toEqual([
      'data: {"type":"reasoning_start"}\n\n',
      'data: {"type":"reasoning_delta","delta":"Reviewing","content":"Reviewing","snapshot":false}\n\n',
      'data: {"type":"reasoning_delta","delta":" the request","content":" the request","snapshot":false}\n\n',
      'data: {"type":"reasoning_end"}\n\n',
    ]);
  });

  it("cleans up reasoning-only turns without ending the HTTP response", () => {
    const { stream, events, response } = streamingFixture();
    stream.onReasoningStream({ text: "thinking" });
    stream.dispatcher.markComplete();
    expect(events().map((event) => event.type)).toEqual([
      "reasoning_start",
      "reasoning_delta",
      "reasoning_end",
    ]);
    expect(response.writableEnded).toBe(false);
  });

  it("marks reordered reasoning snapshots as replacements", () => {
    const writes: string[] = [];
    const response = {
      headersSent: true,
      writableEnded: false,
      write: (chunk: string) => writes.push(chunk),
    };
    const { onReasoningStream } = createStreamingReplyDispatcher(response as never);

    onReasoningStream({ text: "Checking", isReasoningSnapshot: true });
    onReasoningStream({ text: "Reading\n\nChecking", isReasoningSnapshot: true });

    expect(writes.at(-1)).toBe(
      'data: {"type":"reasoning_delta","delta":"Reading\\n\\nChecking","content":"Reading\\n\\nChecking","snapshot":true,"replace":true}\n\n',
    );
  });

  it("ends reasoning at non-reasoning events and starts a fresh later segment", () => {
    const writes: string[] = [];
    const response = {
      headersSent: true,
      writableEnded: false,
      write: (chunk: string) => writes.push(chunk),
      end: () => {},
    };
    const { dispatcher, onAssistantMessageStart, onPartialReply, onReasoningStream } =
      createStreamingReplyDispatcher(response as never);

    onReasoningStream({ text: "First thought" });
    onPartialReply({ text: "Visible answer" });
    onReasoningStream({ text: "Second thought" });
    dispatcher.sendToolResult();
    onReasoningStream({ text: "Third thought" });
    onAssistantMessageStart();
    dispatcher.sendFinalReply({ text: "Final answer" });
    dispatcher.markComplete();

    const events = writes.map((write) => JSON.parse(write.slice(6)) as { type: string });
    expect(events.map((event) => event.type)).toEqual([
      "reasoning_start",
      "reasoning_delta",
      "reasoning_end",
      "content",
      "reasoning_start",
      "reasoning_delta",
      "reasoning_end",
      "tool_start",
      "reasoning_start",
      "reasoning_delta",
      "reasoning_end",
      "message_start",
      "tool_end",
      "content",
    ]);
  });
});
