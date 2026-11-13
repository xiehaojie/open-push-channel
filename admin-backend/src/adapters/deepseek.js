const { spawn } = require("node:child_process");
const { createHash, randomUUID } = require("node:crypto");
const { StringDecoder } = require("node:string_decoder");
const { validateAdapterSignal } = require("../../../protocol/index.cjs");

// Native SDK contract (server identity 0.0.1), audited at upstream 687f282:
// https://github.com/deepseek-ai/deepseek-harness/tree/main/packages/sdk
// session/prompt acknowledges enqueue only; session/wait joins descendants and
// their root summaries. assistant/message is committed output, not live tokens.
function createDeepSeekAdapter({
  id = "deepseek-harness",
  command = "dsh",
  args = [],
  cwd = process.cwd(),
  env,
  excludedEnvKeys = [],
  provider = "deepseek-official",
  model = "deepseek-v4-flash",
} = {}) {
  const jobs = new Map();
  const namespace = randomUUID();
  const excludedEnv = new Set(
    ["PUSH_CHANNEL_PUSH_TOKEN", ...excludedEnvKeys].map((key) => key.toUpperCase()),
  );
  let runtime;
  let replacement;
  let closed = false;
  let closeTask;
  const nativeId = (request) =>
    "push-" +
    createHash("sha256")
      .update(
        JSON.stringify([namespace, id, request.clientId, request.agentId, request.conversationId]),
      )
      .digest("hex");

  function ownerOf(sessionId, peer) {
    const visited = new Set();
    let current = sessionId;
    while (typeof current === "string" && !visited.has(current)) {
      if (jobs.get(current)?.peer === peer) {
        return jobs.get(current);
      }
      visited.add(current);
      current = peer.parents.get(current);
    }
    return undefined;
  }

  function connect() {
    // The gateway owns these auth keys; model tools must not inherit them,
    // even if a configured native environment tries to re-add a case variant.
    const childEnv = { ...process.env, ...env };
    for (const key of Object.keys(childEnv)) {
      if (excludedEnv.has(key.toUpperCase())) {
        delete childEnv[key];
      }
    }
    const child = spawn(command, [...args, "--profile", "sdk"], {
      cwd,
      env: childEnv,
      stdio: ["pipe", "pipe", "pipe"],
      shell: false,
    });
    const pending = new Map();
    const peer = { child, failed: false, parents: new Map(), ownedSessions: new Set() };
    const fail = (error) => {
      peer.failed = true;
      peer.error = error;
      for (const operation of pending.values()) {
        operation.reject(error);
      }
      pending.clear();
    };
    peer.exited = new Promise((resolve) => {
      child.once("close", (code, signal) => {
        fail(new Error(`DeepSeek SDK process exited (code=${code}, signal=${signal})`));
        resolve();
      });
    });
    peer.stop = (error) => {
      fail(error);
      child.stdin.destroy();
      child.kill();
    };
    peer.request = (method, params, onWritten, beforeWrite) => {
      if (peer.failed) {
        return Promise.reject(new Error(peer.error.message));
      }
      const requestId = randomUUID();
      return new Promise((resolve, reject) => {
        // Persist dispatch intent before writing prompt bytes. A journal failure
        // rejects here without creating a pending operation or sending input.
        beforeWrite?.();
        pending.set(requestId, { resolve, reject });
        child.stdin.write(
          JSON.stringify({ jsonrpc: "2.0", id: requestId, method, params }) + "\n",
          (error) => {
            if (!error) {
              onWritten?.();
              return;
            }
            pending.delete(requestId);
            reject(new Error("DeepSeek SDK input write failed"));
          },
        );
      });
    };
    child.on("error", () => fail(new Error("Failed to start DeepSeek SDK process")));
    child.stdin.on("error", () => fail(new Error("DeepSeek SDK input closed unexpectedly")));
    // Consume diagnostics, but never publish potentially sensitive stderr to clients.
    child.stderr.resume();
    const accept = (record) => {
      if (!isRecord(record) || record.jsonrpc !== "2.0") {
        throw new Error("Invalid DeepSeek SDK protocol record");
      }
      if (Object.hasOwn(record, "id")) {
        const operation = pending.get(record.id);
        if (!operation) {
          return;
        }
        pending.delete(record.id);
        if (isRecord(record.error)) {
          operation.reject(
            Object.assign(new Error(record.error.message || "DeepSeek SDK request failed"), {
              rpcRejected: true,
            }),
          );
        } else if (Object.hasOwn(record, "result")) {
          operation.resolve(record.result);
        } else {
          operation.reject(new Error("Invalid DeepSeek SDK response"));
        }
        return;
      }
      const params = record.params;
      if (!isRecord(params)) {
        throw new Error("Invalid DeepSeek SDK notification");
      }
      if (record.method === "subagent.started") {
        if (
          typeof params.parentSessionId !== "string" ||
          typeof params.childSessionId !== "string"
        ) {
          throw new Error("Invalid DeepSeek SDK subagent lineage");
        }
        peer.parents.set(params.childSessionId, params.parentSessionId);
        ownerOf(params.parentSessionId, peer)?.accept(record.method, params);
      } else if (record.method === "subagent.finished") {
        ownerOf(params.parentSessionId, peer)?.accept(record.method, params);
      } else if (record.method === "session.event") {
        ownerOf(params.sessionId, peer)?.accept(record.method, params);
      }
    };
    const decoder = new StringDecoder("utf8");
    let buffer = "";
    child.stdout.on("data", (chunk) => {
      if (peer.failed) {
        return;
      }
      buffer += decoder.write(chunk);
      let newline;
      try {
        while ((newline = buffer.indexOf("\n")) !== -1) {
          const line = buffer.slice(0, newline).trim();
          buffer = buffer.slice(newline + 1);
          if (line) {
            accept(JSON.parse(line));
          }
        }
      } catch (error) {
        peer.stop(
          error instanceof SyntaxError ? new Error("Invalid DeepSeek SDK JSON protocol") : error,
        );
      }
    });
    peer.ready = peer
      .request("initialize", { cwd, provider, model })
      .then((result) => {
        if (result?.serverInfo?.name !== "deepseek-harness-sdk-runtime") {
          throw new Error("Unexpected DeepSeek SDK server identity");
        }
      })
      .catch((error) => {
        peer.stop(error);
        throw error;
      });
    return peer;
  }

  async function acquireRuntime() {
    if (replacement) {
      return replacement;
    }
    runtime ||= connect();
    if (!runtime.failed) {
      return runtime;
    }
    const previous = runtime;
    // Recovery belongs to a NEW explicit request. Reap the failed native peer
    // first; neither an accepted prompt nor its stale notifications may replay.
    replacement = (async () => {
      previous.stop(previous.error);
      await previous.exited;
      if (closed) {
        throw new Error("DeepSeek adapter is closed");
      }
      runtime = connect();
      return runtime;
    })();
    try {
      return await replacement;
    } finally {
      replacement = undefined;
    }
  }

  return {
    id,
    capabilities: {
      streaming: false,
      tools: true,
      reasoning: true,
      subagents: true,
      cancel: false,
      sessionRestore: true,
    },
    async run(request, { emit, signal, onAccepted, onDispatch }) {
      if (closed) {
        throw new Error("DeepSeek adapter is closed");
      }
      if (request.mentions?.length) {
        throw new Error("DeepSeek SDK does not support explicit mentions");
      }
      if (signal?.aborted) {
        throw new Error("DeepSeek gateway shutdown aborted the run");
      }
      const peer = await acquireRuntime();
      const rootId = request.nativeSessionId ?? nativeId(request);
      if (jobs.has(rootId)) {
        throw new Error("DeepSeek conversation already has an active run");
      }
      let terminal;
      let promptWritten = false;
      let nativeAccepted = false;
      const markAccepted = () => {
        if (!nativeAccepted) {
          nativeAccepted = true;
          onAccepted?.();
        }
      };
      const toolNames = new Map();
      const send = (type, data = {}) => emit(validateAdapterSignal({ type, data }));
      const job = {
        peer,
        accept(method, params) {
          if (method === "subagent.started") {
            send("subagent.started", {
              agentId: params.childSessionId,
              childSessionId: params.childSessionId,
            });
            return;
          }
          if (method === "subagent.finished") {
            const child = { agentId: params.childSessionId, childSessionId: params.childSessionId };
            if (params.lastAssistantMessage) {
              send("subagent.result", {
                ...child,
                content: contentText(params.lastAssistantMessage),
              });
            }
            if (params.status !== "ok") {
              send("subagent.failed", { ...child, message: String(params.stopReason) });
            }
            send("subagent.ended", { ...child, status: params.status });
            return;
          }
          const event = params.event;
          if (!isRecord(event) || typeof event.type !== "string" || !isRecord(event.data)) {
            throw new Error("Invalid DeepSeek session event");
          }
          const data = event.data;
          const isRoot = params.sessionId === rootId;
          const child = { agentId: params.sessionId, childSessionId: params.sessionId };
          if (event.type === "assistant/message") {
            const message = data.message;
            if (!isRecord(message) || !Array.isArray(message.content)) {
              throw new Error("Invalid DeepSeek assistant content");
            }
            if (!isRoot) {
              send("subagent.message", {
                ...child,
                messageId: message.id,
                content: contentText(message.content),
              });
              return;
            }
            const reasoning = contentText(message.content, "reasoning");
            if (reasoning) {
              send("reasoning.started");
              send("reasoning.delta", { delta: reasoning });
              send("reasoning.completed");
            }
            send("message.started", { messageId: message.id });
            send("message.snapshot", { messageId: message.id, text: contentText(message.content) });
            send("message.completed", { messageId: message.id });
          } else if (event.type === "tool/call") {
            toolNames.set(JSON.stringify([params.sessionId, data.callId]), data.name);
            send(isRoot ? "tool.started" : "subagent.tool.started", {
              ...(isRoot ? {} : child),
              toolCallId: data.callId,
              toolName: data.name,
              args: toolArguments(data.arguments),
            });
          } else if (event.type === "tool/result") {
            const message = data.message;
            if (!isRecord(message)) {
              throw new Error("Invalid DeepSeek tool result");
            }
            send(isRoot ? "tool.completed" : "subagent.tool.completed", {
              ...(isRoot ? {} : child),
              toolCallId: message.toolCallId,
              toolName: toolNames.get(JSON.stringify([params.sessionId, message.toolCallId])),
              content: contentText(message.content),
              isError: message.isError === true,
            });
          } else if (event.type === "turn/end" && isRoot) {
            if (!isRecord(data.reason) || typeof data.reason.kind !== "string") {
              throw new Error("Invalid DeepSeek turn outcome");
            }
            terminal = data.reason;
          }
        },
      };
      jobs.set(rootId, job);
      const abort = () => peer.stop(new Error("DeepSeek gateway shutdown aborted the run"));
      signal?.addEventListener("abort", abort, { once: true });
      try {
        if (closed || signal?.aborted) {
          abort();
        }
        await peer.ready;
        // Gateway SQLite owns the stable identity; the native runtime alone owns
        // its transcript. Missing/occupied restores must reject, never create anew.
        if (!peer.ownedSessions.has(rootId) && request.resume === true) {
          await peer.request("session/resume", { sessionId: rootId });
          peer.ownedSessions.add(rootId);
        }
        const receipt = await peer.request(
          "session/prompt",
          {
            sessionId: rootId,
            contentBlocks: [{ type: "text", text: request.content }],
          },
          () => {
            promptWritten = true;
          },
          onDispatch,
        );
        if (typeof receipt?.messageId !== "string" || !receipt.messageId) {
          throw new Error("Invalid DeepSeek prompt receipt");
        }
        markAccepted();
        peer.ownedSessions.add(rootId);
        await peer.request("session/wait", { sessionId: rootId });
        if (!terminal) {
          throw new Error("DeepSeek completed without a root turn outcome");
        }
        if (terminal.kind === "aborted") {
          return { status: "cancelled" };
        }
        if (terminal.kind !== "completed") {
          throw new Error(terminal.error?.message || `DeepSeek turn ended: ${terminal.kind}`);
        }
        return undefined;
      } catch (error) {
        // Lost receipt after a successful write is ambiguous: never recreate or
        // resend this prompt after restart. Clean RPC rejection remains retryable.
        if (promptWritten && !nativeAccepted && !error.rpcRejected) {
          markAccepted();
        }
        throw error;
      } finally {
        signal?.removeEventListener("abort", abort);
        if (jobs.get(rootId) === job) {
          jobs.delete(rootId);
        }
      }
    },
    close() {
      closed = true;
      closeTask ||= (async () => {
        if (!runtime) {
          return;
        }
        try {
          if (!runtime.failed) {
            await runtime.request("shutdown");
          }
        } finally {
          runtime.child.stdin.end();
          await runtime.exited;
        }
      })();
      return closeTask;
    },
  };
}

function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function contentText(content, type = "text") {
  if (!Array.isArray(content)) {
    throw new Error("Invalid DeepSeek content blocks");
  }
  return content
    .filter((block) => isRecord(block) && block.type === type && typeof block.text === "string")
    .map((block) => block.text)
    .join("\n");
}

function toolArguments(raw) {
  if (typeof raw !== "string") {
    throw new Error("Invalid DeepSeek tool arguments");
  }
  try {
    const parsed = JSON.parse(raw);
    if (isRecord(parsed)) {
      return parsed;
    }
  } catch {
    // Model-produced invalid arguments are a real tool failure state. Preserve
    // their source for display instead of terminating before tool/result arrives.
  }
  return { rawArguments: raw };
}

module.exports = { createDeepSeekAdapter };
