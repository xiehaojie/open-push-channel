const { randomUUID } = require("node:crypto");
const { isDeepStrictEqual } = require("node:util");
const wire = require("../../../protocol/plugin.cjs");
const { registerAdapterCredential, sameToken } = require("../gateway/auth");

function createPluginAdapter({ id, agents, token, capabilities }) {
  const peers = new Map();
  let closed = false;
  const assertAgent = (agentId) => {
    if (!agents.includes(agentId)) {
      throw new Error("Agent is not declared for this runtime");
    }
  };
  const send = (peer, value) => {
    if (peer.socket.readyState !== peer.socket.OPEN) {
      throw new Error("Plugin is offline");
    }
    peer.socket.send(JSON.stringify(value));
  };
  const failRun = (job, error, rejected = false) => {
    // A written command can execute even when its native receipt is lost.
    // Only an explicit pre-admission rejection is safe to leave unaccepted.
    if (job.dispatched && !job.accepted && !rejected) {
      job.accepted = true;
      job.onAccepted?.();
    }
    job.reject(error);
  };
  function attachPlugin(socket, hello, notify) {
    wire.fields(hello, ["type", "runtimeId", "agentId", "token", "capabilities"]);
    assertAgent(hello.agentId);
    if (
      closed ||
      hello.type !== "plugin.hello" ||
      hello.runtimeId !== id ||
      !sameToken(hello.token, token) ||
      !isDeepStrictEqual(wire.capabilities(hello.capabilities), capabilities)
    ) {
      throw new Error("Plugin authentication or capabilities rejected");
    }
    if (peers.has(hello.agentId)) {
      throw new Error("Plugin target already has a live owner");
    }
    const peer = { socket, runs: new Map(), commands: new Map(), active: true };
    peers.set(hello.agentId, peer);
    const detach = () => {
      if (!peer.active) {
        return;
      }
      peer.active = false;
      if (peers.get(hello.agentId) === peer) {
        peers.delete(hello.agentId);
      }
      const error = new Error("Plugin connection lost; native execution outcome is unknown");
      for (const job of peer.runs.values()) {
        failRun(job, error);
      }
      peer.runs.clear();
      for (const pending of peer.commands.values()) {
        pending.reject(error);
      }
      peer.commands.clear();
    };
    peer.detach = detach;
    socket.once("close", detach);
    socket.once("error", detach);
    socket.on("message", (raw) => {
      if (!peer.active || peers.get(hello.agentId) !== peer) {
        socket.close(4002, "Plugin connection is no longer active");
        return;
      }
      try {
        const frame = JSON.parse(Buffer.from(raw).toString());
        if (frame.type === "plugin.push") {
          wire.fields(frame, ["type", "id", "target", "payload"]);
          wire.text(frame.id);
          let response;
          try {
            wire.notification(frame.target, frame.payload);
            response = { receipt: notify(hello.agentId, frame.target, frame.payload) };
          } catch (error) {
            response = { error: error.message };
          }
          send(peer, { type: "plugin.notified", id: frame.id, ...response });
          return;
        }
        if (frame.type === "plugin.receipt") {
          wire.fields(frame, ["type", "id", "status", "message"]);
          const pending = peer.commands.get(frame.id);
          if (!pending || !["queued", "handled", "rejected", "unknown"].includes(frame.status)) {
            throw new Error("Invalid plugin control receipt");
          }
          peer.commands.delete(frame.id);
          if (["rejected", "unknown"].includes(frame.status)) {
            pending.reject(
              Object.assign(new Error(frame.message || "Native control rejected"), {
                rpcRejected: frame.status === "rejected",
              }),
            );
          } else {
            pending.resolve({ status: frame.status });
          }
          return;
        }
        const job = peer.runs.get(frame.runId);
        if (!job) {
          throw new Error("Unknown plugin run");
        }
        if (frame.type === "plugin.accepted") {
          wire.fields(frame, ["type", "runId"]);
          if (job.accepted) {
            throw new Error("Duplicate plugin acceptance");
          }
          job.accepted = true;
          job.onAccepted?.();
        } else if (frame.type === "plugin.signal") {
          wire.fields(frame, ["type", "runId", "signal"]);
          if (!job.accepted) {
            throw new Error("Plugin signal preceded native acceptance");
          }
          job.emit(wire.signal(frame.signal));
        } else if (frame.type === "plugin.result") {
          wire.fields(frame, ["type", "runId", "status", "message", "rejected"]);
          if (!["completed", "cancelled", "deferred", "failed"].includes(frame.status)) {
            throw new Error("Invalid plugin run result");
          }
          if (frame.rejected !== undefined && typeof frame.rejected !== "boolean") {
            throw new Error("Invalid plugin rejection");
          }
          if (["failed", "deferred"].includes(frame.status)) {
            wire.text(frame.message);
          }
          // Full-duplex delivery can finish a native run before its in-transit
          // control arrives. Commands settle independently; the gateway journals
          // their receipts before publishing the canonical terminal event.
          if (frame.status !== "failed" && !job.accepted) {
            throw new Error("Plugin completed without native acceptance");
          }
          peer.runs.delete(frame.runId);
          if (frame.status === "failed") {
            const rejected = frame.rejected === true && !job.accepted;
            failRun(
              job,
              Object.assign(new Error(frame.message || "Native plugin run failed"), {
                rpcRejected: rejected,
              }),
              rejected,
            );
          } else if (frame.status === "completed") {
            job.resolve();
          } else {
            job.resolve({
              status: frame.status,
              ...(frame.message ? { message: frame.message } : {}),
            });
          }
        } else {
          throw new Error("Invalid plugin protocol frame");
        }
      } catch {
        detach();
        socket.close(4002, "Invalid plugin protocol frame");
      }
    });
    send(peer, { type: "plugin.ready" });
  }
  function peerFor(request) {
    assertAgent(request.agentId);
    const peer = peers.get(request.agentId);
    if (closed || !peer || peer.socket.readyState !== peer.socket.OPEN) {
      throw Object.assign(new Error("Plugin is offline; no native runtime fallback"), {
        rpcRejected: true,
      });
    }
    return peer;
  }
  function command(request, type, input = {}) {
    const peer = peerFor(request);
    if (!peer.runs.has(request.runId)) {
      return Promise.reject(
        Object.assign(new Error("No matching active plugin run"), { rpcRejected: true }),
      );
    }
    const commandId = randomUUID();
    return new Promise((resolve, reject) => {
      peer.commands.set(commandId, { resolve, reject, runId: request.runId });
      try {
        send(peer, { type, id: commandId, runId: request.runId, ...input });
      } catch (error) {
        peer.commands.delete(commandId);
        reject(error instanceof Error ? error : new Error("Plugin control dispatch failed"));
      }
    });
  }
  const adapter = {
    id,
    agents,
    capabilities,
    assertAgent,
    attachPlugin,
    async run(request, { emit, onDispatch, onAccepted, signal }) {
      const peer = peerFor(request);
      const safe = wire.request(request);
      if (signal?.aborted) {
        throw new Error("Gateway is shutting down");
      }
      if (peer.runs.has(safe.runId)) {
        throw new Error("Duplicate active plugin run");
      }
      const pending = Promise.withResolvers();
      const job = { ...pending, emit, onAccepted, accepted: false, dispatched: false };
      peer.runs.set(safe.runId, job);
      const abort = () => {
        peer.runs.delete(safe.runId);
        const error = new Error("Gateway stopped observing the plugin run");
        failRun(job, error);
        for (const [commandId, pendingControl] of peer.commands) {
          if (pendingControl.runId === safe.runId) {
            peer.commands.delete(commandId);
            pendingControl.reject(error);
          }
        }
      };
      signal?.addEventListener("abort", abort, { once: true });
      try {
        onDispatch?.();
        job.dispatched = true;
        send(peer, { type: "plugin.run", runId: safe.runId, request: safe });
      } catch (error) {
        peer.runs.delete(safe.runId);
        failRun(job, error);
      }
      try {
        return await pending.promise;
      } finally {
        signal?.removeEventListener("abort", abort);
      }
    },
    async control(request, input) {
      const supported =
        input.operation === "steer"
          ? capabilities.steering
          : input.operation === "follow_up" && capabilities.followUp;
      if (!supported) {
        throw Object.assign(new Error("Unsupported plugin input control"), { rpcRejected: true });
      }
      return command(request, "plugin.control", {
        operation: input.operation,
        content: input.content,
      });
    },
    async cancel(request) {
      if (!capabilities.cancel) {
        throw new Error("Plugin does not support cancellation");
      }
      await command(request, "plugin.cancel");
    },
    async close() {
      closed = true;
      for (const peer of peers.values()) {
        peer.detach();
        peer.socket.close(1001, "Gateway shutting down");
      }
    },
  };
  registerAdapterCredential(adapter.attachPlugin, "plugin", token);
  return adapter;
}

module.exports = { createPluginAdapter };
