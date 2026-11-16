import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import path from "node:path";
import { StringDecoder } from "node:string_decoder";

import type { PiRpcEvent } from "./pi.ts";

type PiToolContext = {
  cwd: string;
  model?: { provider: string; id: string };
  thinkingLevel?: string;
  modelRegistry: {
    getProviderAuthStatus(provider: string): { source: string };
    getApiKeyForProvider(provider: string): Promise<string | undefined>;
  };
};
type PiToolResult = { content: { type: "text"; text: string }[]; isError?: true };
type PiToolUpdate = {
  content: [];
  details: {
    pushChannelSubagent: {
      version: 1;
      childSessionId: string;
      event: PiRpcEvent & { status?: string };
    };
  };
};
type PiExtensionApi = {
  on(name: "session_shutdown", handler: () => Promise<void>): void;
  getActiveTools(): string[];
  getAllTools(): { name: string; sourceInfo: { source: string; path: string } }[];
  registerTool(tool: {
    name: string;
    label: string;
    description: string;
    parameters: Record<string, unknown>;
    execute(
      toolCallId: string,
      args: { task: string },
      signal: AbortSignal | undefined,
      onUpdate: ((result: PiToolUpdate) => void) | undefined,
      context: PiToolContext,
    ): Promise<PiToolResult>;
  }): void;
};

// Pi's extension API carries partialResult.details unchanged over native RPC.
// Children deliberately load no extensions: this tool cannot recursively spawn itself.
export default function registerPiSubagent(pi: PiExtensionApi) {
  const children = new Map<
    ChildProcessWithoutNullStreams,
    { cancel(): void; closed: Promise<number | null> }
  >();
  pi.on("session_shutdown", async () => {
    // Native shutdown disposes without waiting for active tools. Hold this hook
    // until child stdio closes, or the parent could exit before its child cleans up.
    const active = [...children.values()];
    for (const child of active) child.cancel();
    await Promise.all(active.map((child) => child.closed));
  });
  pi.registerTool({
    name: "subagent",
    label: "Subagent",
    description:
      "Delegate one task to a separate Pi session in the current workspace. Waits for its final result. The child cannot delegate further.",
    parameters: {
      type: "object",
      properties: { task: { type: "string", minLength: 1, description: "The task to delegate" } },
      required: ["task"],
      additionalProperties: false,
    },
    async execute(_toolCallId, { task }, signal, onUpdate, ctx) {
      const errorResult = (message: string): PiToolResult => ({
        content: [{ type: "text", text: message }],
        isError: true,
      });
      if (signal?.aborted) return errorResult("Pi subagent cancelled");
      if (!ctx.model) return errorResult("Pi subagent requires an active model");
      const childSessionId = randomUUID();
      const args = [
        "--mode",
        "rpc",
        "--no-extensions",
        "--session-id",
        childSessionId,
        "--model",
        `${ctx.model.provider}/${ctx.model.id}`,
      ];
      if (ctx.thinkingLevel) args.push("--thinking", ctx.thinkingLevel);
      try {
        // CLI --api-key is process-local, unlike stored/environment auth. Copy
        // only the selected provider's runtime key using Pi's native CLI contract.
        if (ctx.modelRegistry.getProviderAuthStatus(ctx.model.provider).source === "runtime") {
          const apiKey = await ctx.modelRegistry.getApiKeyForProvider(ctx.model.provider);
          if (typeof apiKey !== "string" || !apiKey) {
            return errorResult("Pi subagent runtime credential is unavailable");
          }
          args.push("--api-key", apiKey);
        }
      } catch {
        return errorResult("Pi subagent runtime credential is unavailable");
      }
      if (signal?.aborted) return errorResult("Pi subagent cancelled");
      // Registry source metadata distinguishes native tools from same-name
      // custom overrides. Never restore a native tool hidden by a parent override.
      const active = new Set(pi.getActiveTools());
      const tools = pi
        .getAllTools()
        .filter(
          (tool) =>
            active.has(tool.name) &&
            tool.sourceInfo.source === "builtin" &&
            tool.sourceInfo.path === `builtin:${tool.name}`,
        )
        .map((tool) => tool.name)
        .sort();
      if (tools.length) args.push("--tools", tools.join(","));
      else args.push("--no-tools");
      const script = process.argv[1];
      let invocation;
      if (script && !script.startsWith("/$bunfs/root/") && existsSync(script)) {
        invocation = { command: process.execPath, args: [script, ...args] };
      } else if (!/^(node|bun)(\.exe)?$/i.test(path.basename(process.execPath))) {
        invocation = { command: process.execPath, args };
      } else {
        return errorResult("Pi subagent CLI entry is unavailable");
      }
      const child = spawn(invocation.command, invocation.args, {
        cwd: ctx.cwd,
        env: process.env,
        stdio: ["pipe", "pipe", "pipe"],
        shell: false,
      });
      const stateId = randomUUID();
      const promptId = randomUUID();
      let started = false;
      let accepted = false;
      let settled = false;
      let cancelled = false;
      let failed = false;
      let providerFailed = false;
      let output = "";
      const update = (event: PiToolUpdate["details"]["pushChannelSubagent"]["event"]) =>
        onUpdate?.({
          content: [],
          details: { pushChannelSubagent: { version: 1, childSessionId, event } },
        });
      const endInput = () => {
        if (!child.stdin.destroyed && !child.stdin.writableEnded) child.stdin.end();
      };
      const cancel = () => {
        cancelled = true;
        endInput();
      };
      const fail = () => {
        failed = true;
        endInput();
      };
      const finishIfSettled = () => {
        // A fast run can settle before its prompt receipt. Do not dispose it
        // until both native facts are known, and still await close below.
        if (accepted && settled) endInput();
      };
      const write = (record: Record<string, unknown>) =>
        child.stdin.write(`${JSON.stringify(record)}\n`);
      const accept = (event: PiRpcEvent) => {
        if (!event || typeof event.type !== "string") throw new Error("Invalid Pi child event");
        if (event.type === "response") {
          if (event.id === stateId) {
            if (event.success !== true || event.data?.sessionId !== childSessionId) {
              fail();
              return;
            }
            started = true;
            update({ type: "started" });
            if (!cancelled) write({ type: "prompt", id: promptId, message: task });
          } else if (event.id === promptId) {
            if (event.success !== true || event.data?.disposition !== "started") {
              fail();
              return;
            }
            accepted = true;
            finishIfSettled();
          }
          return;
        }
        if (event.type === "agent_settled") {
          settled = true;
          cancelled ||= event.aborted === true;
          finishIfSettled();
        } else if (event.type === "message_start" && event.message?.role === "assistant") {
          providerFailed = false;
        } else if (event.type === "message_end" && event.message?.role === "assistant") {
          providerFailed = event.message.stopReason === "error";
          cancelled ||= event.message.stopReason === "aborted";
          if (!providerFailed && !cancelled) {
            output = textContent(event.message.content);
            update({
              type: "message_end",
              message: { role: "assistant", content: event.message.content },
            });
          }
        } else if (
          event.type === "message_update" &&
          event.assistantMessageEvent?.type === "text_delta"
        ) {
          if (typeof event.assistantMessageEvent.delta !== "string")
            throw new Error("Invalid Pi child delta");
          update(event);
        } else if (event.type === "tool_execution_start" || event.type === "tool_execution_end") {
          update(event);
        } else if (event.type === "auto_retry_end" && event.success === false) {
          providerFailed = true;
        } else if (event.type === "extension_error" || event.type === "extension_ui_request") {
          fail();
        }
      };
      const decoder = new StringDecoder("utf8");
      let buffer = "";
      child.stdout.on("data", (chunk) => {
        buffer += decoder.write(chunk);
        let newline;
        try {
          while ((newline = buffer.indexOf("\n")) !== -1) {
            const line = buffer.slice(0, newline).replace(/\r$/, "");
            buffer = buffer.slice(newline + 1);
            accept(JSON.parse(line));
          }
        } catch {
          fail();
        }
      });
      child.stderr.resume();
      child.on("error", fail);
      child.stdin.on("error", fail);
      child.once("spawn", () => write({ type: "get_state", id: stateId }));
      const closed = new Promise<number | null>((resolve) =>
        child.once("close", (code) => resolve(code)),
      );
      children.set(child, { cancel, closed });
      signal?.addEventListener("abort", cancel, { once: true });
      if (signal?.aborted) cancel();
      try {
        const code = await closed;
        if (buffer || decoder.end()) failed = true;
        const status = cancelled
          ? "cancelled"
          : failed || providerFailed || !accepted || !settled || code !== 0
            ? "error"
            : "ok";
        if (started) update({ type: "finished", status });
        if (status !== "ok")
          return errorResult(`Pi subagent ${status === "cancelled" ? "cancelled" : "failed"}`);
        return { content: [{ type: "text", text: output }] };
      } finally {
        signal?.removeEventListener("abort", cancel);
        children.delete(child);
      }
    },
  });
}

function textContent(content: unknown) {
  return Array.isArray(content)
    ? content
        .filter((part) => part.type === "text" && typeof part.text === "string")
        .map((part) => part.text)
        .join("\n")
    : "";
}
