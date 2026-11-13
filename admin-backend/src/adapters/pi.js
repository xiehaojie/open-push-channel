const { spawn } = require("node:child_process");
const { randomUUID } = require("node:crypto");
const { StringDecoder } = require("node:string_decoder");
const path = require("node:path");
const { validateAdapterSignal } = require("../../../protocol/index.cjs");

// Pi 1.3.0 native RPC: https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/rpc.md
// agent_end is NOT terminal: automatic retries/compaction can follow it. Only
// agent_settled (or a handled prompt) releases the gateway's conversation lock.
function createPiAdapter({
  id = "pi",
  command = "pi",
  args = [],
  cwd = process.cwd(),
  env,
  excludedEnvKeys = [],
} = {}) {
  const sessions = new Map();
  const excludedEnv = new Set(
    ["PUSH_CHANNEL_PUSH_TOKEN", ...excludedEnvKeys].map((key) => key.toUpperCase()),
  );
  let closed = false;
  const keyFor = (request) =>
    JSON.stringify([request.clientId, request.agentId, request.conversationId]);

  function connect(key, request) {
    // --session-id creates when missing; --session refuses a missing restore.
    // Verify get_state below because --session also admits prefix/fork lookup.
    const sessionArgs = request.nativeSessionId
      ? [request.resume === true ? "--session" : "--session-id", request.nativeSessionId]
      : ["--no-session"];
    // Gateway auth is not an agent capability. Filter after merging overrides,
    // including case variants, so model tools cannot recover transport credentials.
    const childEnv = { ...process.env, ...env };
    for (const envName of Object.keys(childEnv)) {
      if (excludedEnv.has(envName.toUpperCase())) {
        delete childEnv[envName];
      }
    }
    const extension = path.join(__dirname, "pi-subagent.js");
    const child = spawn(
      command,
      [...args, "--extension", extension, "--mode", "rpc", ...sessionArgs],
      {
        cwd,
        env: childEnv,
        stdio: ["pipe", "pipe", "pipe"],
        shell: false,
      },
    );
    const pending = new Map();
    const session = { child, active: undefined, exited: false };
    session.closed = new Promise((resolve) => child.once("close", resolve));
    sessions.set(key, session);
    const fail = (error) => {
      session.error = error;
      for (const operation of pending.values()) {
        operation.reject(error);
      }
      pending.clear();
      session.active?.reject(error);
      session.active = undefined;
      if (sessions.get(key) === session) {
        sessions.delete(key);
      }
    };
    session.stop = (error) => {
      fail(error);
      // Native RPC EOF awaits session_shutdown hooks. The owned extension holds
      // that hook until children close; killing the parent early bypasses it.
      if (!child.stdin.destroyed && !child.stdin.writableEnded) child.stdin.end();
    };
    session.send = (type, fields, onWritten, beforeWrite) => {
      if (session.error) {
        return Promise.reject(new Error(session.error.message));
      }
      const requestId = randomUUID();
      return new Promise((resolve, reject) => {
        // Dispatch intent must be durable before any prompt bytes cross stdin.
        // Throwing here leaves no pending request and sends no native command.
        beforeWrite?.();
        pending.set(requestId, { resolve, reject });
        child.stdin.write(JSON.stringify({ ...fields, id: requestId, type }) + "\n", (error) => {
          if (!error) {
            onWritten?.();
            return;
          }
          pending.delete(requestId);
          reject(new Error("Pi RPC input write failed"));
        });
      });
    };
    child.on("error", () => fail(new Error("Failed to start Pi RPC process")));
    child.stdin.on("error", () => session.stop(new Error("Pi RPC input closed unexpectedly")));
    // Drain diagnostics without forwarding credentials or project paths to clients.
    child.stderr.resume();
    child.on("close", (code, signal) => {
      session.exited = true;
      fail(new Error(`Pi RPC process exited (code=${code}, signal=${signal})`));
    });

    const accept = (record) => {
      if (!record || typeof record !== "object" || typeof record.type !== "string") {
        throw new Error("Invalid Pi RPC protocol record");
      }
      if (record.type === "response") {
        const operation = pending.get(record.id);
        if (!operation) {
          return;
        }
        pending.delete(record.id);
        if (record.success === true) {
          operation.resolve(record);
        } else {
          operation.reject(
            Object.assign(new Error(record.error || "Pi RPC command failed"), {
              rpcRejected: true,
            }),
          );
        }
        return;
      }
      const active = session.active;
      if (!active) {
        return;
      }
      if (record.type === "extension_error") {
        throw new Error("Pi extension failed");
      }
      if (record.type === "extension_ui_request") {
        if (["select", "confirm", "input", "editor"].includes(record.method)) {
          throw new Error("Pi requested an unsupported interactive UI interaction");
        }
        return;
      }
      active.accept(record);
    };
    const decoder = new StringDecoder("utf8");
    let buffer = "";
    child.stdout.on("data", (chunk) => {
      buffer += decoder.write(chunk);
      let newline;
      try {
        // RPC framing is LF-only. readline would split valid U+2028/U+2029 strings.
        while ((newline = buffer.indexOf("\n")) !== -1) {
          const line = buffer.slice(0, newline).replace(/\r$/, "");
          buffer = buffer.slice(newline + 1);
          accept(JSON.parse(line));
        }
      } catch (error) {
        session.stop(
          error instanceof SyntaxError ? new Error("Invalid Pi RPC JSON protocol") : error,
        );
      }
    });
    const spawned = new Promise((resolve, reject) => {
      child.once("spawn", resolve);
      child.once("error", () => reject(new Error("Failed to start Pi RPC process")));
    });
    session.ready = spawned.then(async () => {
      if (request.nativeSessionId) {
        const response = await session.send("get_state");
        if (response.data?.sessionId !== request.nativeSessionId) {
          const error = new Error("Pi native session identity does not match the gateway binding");
          session.stop(error);
          throw error;
        }
      }
    });
    return session;
  }

  return {
    id,
    capabilities: {
      streaming: true,
      tools: true,
      reasoning: true,
      subagents: true,
      cancel: true,
      sessionRestore: true,
      steering: true,
      followUp: true,
    },
    async run(request, { emit, signal, onAccepted, onDispatch }) {
      if (closed) {
        throw new Error("Pi adapter is closed");
      }
      if (request.mentions?.length) {
        throw new Error("Pi does not support subagent mentions");
      }
      if (signal?.aborted) {
        throw new Error("Pi gateway shutdown aborted the run");
      }
      const key = keyFor(request);
      const session = sessions.get(key) || connect(key, request);
      if (session.active) {
        throw new Error("Pi conversation already has an active run");
      }
      let messageId;
      let failure;
      let settled;
      let accepted = false;
      let handled = false;
      let activitySeen = false;
      let nativeAccepted = false;
      let promptWritten = false;
      let disposition;
      const children = new Map();
      const markAccepted = () => {
        if (!nativeAccepted) {
          nativeAccepted = true;
          onAccepted?.();
        }
      };
      let resolveRun;
      let rejectRun;
      const result = new Promise((resolve, reject) => {
        resolveRun = resolve;
        rejectRun = reject;
      });
      const send = (type, data = {}) => emit(validateAdapterSignal({ type, data }));
      const startMessage = () => {
        messageId = randomUUID();
        send("message.started", { messageId });
      };
      const finish = () => {
        if (!accepted || (!handled && !settled) || active.pendingControls || active.draining) {
          return;
        }
        if (active.queue.steering.length || active.queue.followUp.length) {
          // RPC input hooks may queue after agent_settled. Drain their receipts
          // and clear residual input before another gateway run can reuse Pi.
          active.draining = true;
          void session
            .send("clear_queue")
            .then((response) => {
              const discarded = response.data;
              if (!Array.isArray(discarded?.steering) || !Array.isArray(discarded?.followUp)) {
                throw new Error("Invalid Pi queue-clear receipt");
              }
              send("input.queue", {
                steering: [],
                followUp: [],
                discardedSteering: discarded.steering,
                discardedFollowUp: discarded.followUp,
                reason: settled?.aborted ? "cancelled" : "run_settled",
              });
              active.queue = { steering: [], followUp: [] };
              active.draining = false;
              finish();
            })
            .catch((error) => session.stop(error));
          return;
        }
        if (settled?.aborted) {
          resolveRun({ status: "cancelled" });
        } else if (failure) {
          rejectRun(new Error(failure));
        } else {
          resolveRun();
        }
      };
      const checkHandledState = async () => {
        if (disposition !== "handled" || session.active !== active) {
          return;
        }
        const state = await session.send("get_state");
        if (session.active !== active) {
          return;
        }
        const running = state.data?.isStreaming || state.data?.isCompacting;
        if (running) {
          markAccepted();
        }
        handled = !activitySeen && !running;
        finish();
      };
      const active = {
        runId: request.runId,
        accepting: true,
        pendingControls: 0,
        controlTail: Promise.resolve(),
        queue: { steering: [], followUp: [] },
        draining: false,
        finish,
        reject: rejectRun,
        accept(event) {
          if (
            event.type === "agent_start" ||
            event.type === "message_start" ||
            event.type === "tool_execution_start"
          ) {
            activitySeen = true;
            markAccepted();
          }
          const update = event.assistantMessageEvent;
          if (event.type === "queue_update") {
            const data = { steering: event.steering, followUp: event.followUp };
            send("input.queue", data);
            active.queue = data;
          } else if (event.type === "message_start" && event.message?.role === "assistant") {
            failure = undefined;
            startMessage();
          } else if (event.type === "message_update") {
            if (!update || typeof update.type !== "string") {
              throw new Error("Invalid Pi message update");
            }
            if (update.type === "text_delta") {
              if (!messageId) {
                startMessage();
              }
              send("message.delta", { messageId, delta: update.delta });
            } else if (update.type === "thinking_start") {
              send("reasoning.started");
            } else if (update.type === "thinking_delta") {
              send("reasoning.delta", { delta: update.delta });
            } else if (update.type === "thinking_end") {
              send("reasoning.delta", { delta: update.content, replace: true });
              send("reasoning.completed");
            }
          } else if (event.type === "message_end" && event.message?.role === "assistant") {
            const message = event.message;
            if (message.stopReason === "error") {
              failure = message.errorMessage || "Pi provider failed";
            }
            if (!messageId) {
              startMessage();
            }
            send("message.snapshot", { messageId, text: textContent(message.content) });
            send("message.completed", { messageId });
            messageId = undefined;
          } else if (event.type === "tool_execution_start") {
            if (event.toolName === "subagent") children.set(event.toolCallId, {});
            send("tool.started", {
              toolCallId: event.toolCallId,
              toolName: event.toolName,
              args: event.args,
            });
          } else if (event.type === "tool_execution_update" && event.toolName === "subagent") {
            acceptChildUpdate(event, children, send);
          } else if (event.type === "tool_execution_end") {
            children.delete(event.toolCallId);
            send("tool.completed", {
              toolCallId: event.toolCallId,
              toolName: event.toolName,
              content: textContent(event.result?.content),
              isError: event.isError,
            });
          } else if (event.type === "auto_retry_end" && event.success === false) {
            failure = event.finalError || "Pi retry failed";
          } else if (event.type === "compaction_end" && disposition === "handled") {
            // Manual compaction has no agent_settled. Re-query on this event,
            // including when an older in-flight state response still says busy.
            void checkHandledState().catch(rejectRun);
          } else if (event.type === "agent_settled") {
            active.accepting = false;
            settled = event;
            finish();
          }
        },
      };
      session.active = active;
      const abort = () => session.stop(new Error("Pi gateway shutdown aborted the run"));
      signal?.addEventListener("abort", abort, { once: true });
      // Subscribe above before writing prompt: a fast run can settle before its
      // acceptance response reaches us. Both facts are required before success.
      active.promptResponse = session.ready.then(() =>
        session.send(
          "prompt",
          { message: request.content },
          () => {
            promptWritten = true;
          },
          onDispatch,
        ),
      );
      active.promptResponse
        .then(async (response) => {
          disposition = response.data?.disposition;
          if (!["started", "handled", "queued"].includes(disposition)) {
            session.stop(new Error("Unsupported Pi RPC version: prompt disposition is missing"));
            return;
          }
          if (disposition === "handled") {
            // Extension commands can handle input and still start independent work.
            // Only a truly idle handled prompt can finish without agent_settled.
            accepted = true;
            await checkHandledState();
            return;
          }
          markAccepted();
          accepted = true;
          finish();
        })
        .catch(rejectRun);
      try {
        return await result;
      } catch (error) {
        if (session.error) await session.closed;
        // A failed parent cannot send native child terminal frames. Close its
        // started cards here, while retaining any terminal already delivered.
        for (const child of children.values()) {
          if (!child.id || child.ended) continue;
          const identity = { agentId: child.id, childSessionId: child.id };
          send("subagent.failed", { ...identity, message: "Pi parent run failed" });
          send("subagent.ended", { ...identity, status: "error" });
          child.ended = true;
        }
        // A successfully written prompt whose receipt was lost may have run.
        // Mark it conservatively; explicit rejection/handled input is not admission.
        if (promptWritten && !nativeAccepted && !error.rpcRejected && disposition !== "handled") {
          markAccepted();
        }
        throw error;
      } finally {
        signal?.removeEventListener("abort", abort);
        if (session.active === active) {
          session.active = undefined;
        }
      }
    },
    async control(request, input) {
      const session = sessions.get(keyFor(request));
      const active = session?.active;
      const assertActive = () => {
        if (
          !active ||
          session.active !== active ||
          active.runId !== request.runId ||
          !active.accepting
        ) {
          throw Object.assign(new Error("Control must target the active run"), {
            rpcRejected: true,
          });
        }
      };
      assertActive();
      if (
        !["steer", "follow_up"].includes(input.operation) ||
        typeof input.content !== "string" ||
        !input.content.trim()
      ) {
        throw Object.assign(new Error("Invalid Pi input control"), { rpcRejected: true });
      }
      active.pendingControls++;
      const task = active.controlTail
        .catch(() => {})
        .then(async () => {
          await active.promptResponse;
          assertActive();
          const response = await session.send(
            input.operation,
            { message: input.content },
            undefined,
            assertActive,
          );
          const status = response.data?.disposition;
          if (!["queued", "handled"].includes(status)) {
            const error = new Error("Unsupported Pi RPC control disposition");
            session.stop(error);
            throw error;
          }
          return { status };
        });
      active.controlTail = task;
      try {
        return await task;
      } finally {
        active.pendingControls--;
        active.finish();
      }
    },
    async cancel(request) {
      const session = sessions.get(keyFor(request));
      const active = session?.active;
      if (!active) {
        return false;
      }
      // Abort must reach Pi while an async input hook is still pending. The run
      // observer retains pending controls and clears late queues before release.
      active.accepting = false;
      try {
        await active.promptResponse;
      } catch {
        return false;
      }
      if (session.active !== active) {
        return false;
      }
      await session.send("abort");
      await active.controlTail.catch(() => {});
      return true;
    },
    async close() {
      closed = true;
      await Promise.all(
        [...sessions.values()].map(
          (session) =>
            new Promise((resolve) => {
              if (session.exited) {
                resolve();
                return;
              }
              session.child.once("close", resolve);
              session.child.stdin.end();
            }),
        ),
      );
    },
  };
}

function textContent(content) {
  if (!Array.isArray(content)) {
    return "";
  }
  return content
    .filter((part) => part.type === "text" && typeof part.text === "string")
    .map((part) => part.text)
    .join("\n");
}

module.exports = { createPiAdapter };

function acceptChildUpdate(event, children, send) {
  const child = children.get(event.toolCallId);
  if (!child || child.ended) return;
  const record = event.partialResult?.details?.pushChannelSubagent;
  if (!record) return;
  const invalid = () => {
    throw new Error("Invalid Pi subagent protocol");
  };
  if (
    record.version !== 1 ||
    typeof record.childSessionId !== "string" ||
    !record.childSessionId.trim() ||
    !record.event ||
    typeof record.event.type !== "string"
  )
    invalid();
  const native = record.event;
  if (native.type === "started") {
    if (child.id) invalid();
    child.id = record.childSessionId;
    send("subagent.started", { agentId: child.id, childSessionId: child.id, label: "Pi" });
    return;
  }
  if (child.id !== record.childSessionId) invalid();
  const identity = { agentId: child.id, childSessionId: child.id };
  if (native.type === "message_update") {
    if (
      native.assistantMessageEvent?.type !== "text_delta" ||
      typeof native.assistantMessageEvent.delta !== "string"
    )
      invalid();
    send("subagent.delta", { ...identity, delta: native.assistantMessageEvent.delta });
  } else if (native.type === "message_end") {
    if (native.message?.role !== "assistant" || !Array.isArray(native.message.content)) invalid();
    child.output = textContent(native.message.content);
    send("subagent.message", { ...identity, content: child.output });
  } else if (native.type === "tool_execution_start") {
    try {
      send("subagent.tool.started", {
        ...identity,
        toolCallId: native.toolCallId,
        toolName: native.toolName,
        args: native.args,
      });
    } catch {
      invalid();
    }
  } else if (native.type === "tool_execution_end") {
    try {
      send("subagent.tool.completed", {
        ...identity,
        toolCallId: native.toolCallId,
        toolName: native.toolName,
        content: textContent(native.result?.content),
        isError: native.isError,
      });
    } catch {
      invalid();
    }
  } else if (native.type === "finished") {
    if (!["ok", "error", "cancelled"].includes(native.status)) invalid();
    if (native.status === "ok")
      send("subagent.result", { ...identity, content: child.output || "" });
    if (native.status === "error")
      send("subagent.failed", { ...identity, message: "Pi subagent failed" });
    send("subagent.ended", { ...identity, status: native.status });
    child.ended = true;
  } else invalid();
}
