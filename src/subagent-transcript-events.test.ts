import { describe, expect, it } from "vitest";
import {
  createSubagentEventsFromMessages,
  extractSubagentAssistantText,
} from "./subagent-transcript-events.js";

describe("subagent transcript event projection", () => {
  it("converts child assistant text, tool calls, and tool results into subagent events", () => {
    const events = createSubagentEventsFromMessages({
      messages: [
        {
          type: "message",
          id: "assistant-1",
          message: {
            role: "assistant",
            content: [
              { type: "thinking", thinking: "internal" },
              { type: "text", text: "我先查询天气。" },
              {
                type: "toolCall",
                id: "call_weather",
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
            toolCallId: "call_weather",
            toolName: "web_fetch",
            content: [{ type: "text", text: "{\"status\":200}" }],
            isError: false,
          },
        },
      ],
      agentId: "researcher",
      label: "researcher",
      childSessionKey: "agent:researcher:subagent:child",
    });

    expect(events).toEqual([
      {
        type: "subagent_message",
        agentId: "researcher",
        label: "researcher",
        childSessionKey: "agent:researcher:subagent:child",
        messageId: "assistant-1",
        content: "我先查询天气。",
      },
      {
        type: "subagent_tool_call",
        agentId: "researcher",
        label: "researcher",
        childSessionKey: "agent:researcher:subagent:child",
        messageId: "assistant-1",
        toolCallId: "call_weather",
        toolName: "web_fetch",
        args: { url: "https://www.weather.com.cn/weather/101010800.shtml" },
      },
      {
        type: "subagent_tool_result",
        agentId: "researcher",
        label: "researcher",
        childSessionKey: "agent:researcher:subagent:child",
        messageId: "tool-1",
        toolCallId: "call_weather",
        toolName: "web_fetch",
        content: [{ type: "text", text: "{\"status\":200}" }],
        isError: false,
      },
    ]);
  });

  it("keeps multiple assistant text blocks together and preserves raw partial args", () => {
    const events = createSubagentEventsFromMessages({
      messages: [
        {
          type: "message",
          id: "assistant-2",
          message: {
            role: "assistant",
            content: [
              { type: "text", text: "第一段" },
              { type: "text", text: "第二段" },
              {
                type: "toolCall",
                id: "call_partial",
                name: "sessions_yield",
                partialArgs: "{\"reason\":",
              },
            ],
          },
        },
      ],
      agentId: "coder",
      label: "Coder",
      childSessionKey: "agent:coder:subagent:child",
    });

    expect(events).toEqual([
      {
        type: "subagent_message",
        agentId: "coder",
        label: "Coder",
        childSessionKey: "agent:coder:subagent:child",
        messageId: "assistant-2",
        content: "第一段\n\n第二段",
      },
      {
        type: "subagent_tool_call",
        agentId: "coder",
        label: "Coder",
        childSessionKey: "agent:coder:subagent:child",
        messageId: "assistant-2",
        toolCallId: "call_partial",
        toolName: "sessions_yield",
        args: { partialArgs: "{\"reason\":" },
      },
    ]);
  });

  it("extracts assistant text for main-agent result synthesis", () => {
    expect(
      extractSubagentAssistantText([
        {
          message: {
            role: "assistant",
            content: [
              { type: "thinking", thinking: "internal" },
              { type: "text", text: "第一段结果" },
            ],
          },
        },
        {
          message: {
            role: "toolResult",
            content: "tool payload should not be included",
          },
        },
        {
          message: {
            role: "assistant",
            content: "第二段结果",
          },
        },
      ]),
    ).toBe("第一段结果\n\n第二段结果");
  });
});
