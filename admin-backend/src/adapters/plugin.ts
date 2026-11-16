import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import type WebSocket from "ws";
import type { Adapter, RunRequest } from "../../../protocol/index.ts";
import * as wire from "../../../protocol/plugin.ts";
import { registerAdapterCredential, sameToken } from "../gateway/auth.ts";

type ActiveRequest = RunRequest & { runId: string };
type RunResult = Awaited<ReturnType<Adapter["run"]>>;
type ControlResult = Awaited<ReturnType<NonNullable<Adapter["control"]>>>;
type Notify = (
  agentId: string,
  target: wire.NotificationTarget,
  payload: wire.NotificationPayload,
) => wire.NotificationReceipt;
type RunJob = PromiseWithResolvers<RunResult> &
  Pick<Parameters<Adapter["run"]>[1], "emit" | "onAccepted"> & {
    accepted: boolean;
    dispatched: boolean;
  };
type Command = Pick<PromiseWithResolvers<ControlResult>, "resolve" | "reject"> & { runId: string };
type Peer = {
  socket: WebSocket;
  runs: Map<string, RunJob>;
  commands: Map<string, Command>;
  active: boolean;
  detach(): void;
};

export type PluginAdapter = Adapter & {
  agents: string[];
  assertAgent(agentId: unknown): void;
  attachPlugin(socket: WebSocket, hello: unknown, notify: Notify): void;
};

export function createPluginAdapter({
  id,
  agents,
  token,
  capabilities,
}: {
  id: string;
  agents: string[];
  token: string;
  capabilities: wire.PluginCapabilities;
}): PluginAdapter {
  const peers = new Map<string, Peer>();
  let closed = false;
  function assertAgent(agentId: unknown): asserts agentId is string {
    if (typeof agentId !== "string" || !agents.includes(agentId)) {
      throw new Error("Agent is not declared for this runtime");
    }
  }
  const send = (peer: Pick<Peer, "socket">, value: unknown) => {
    if (peer.socket.readyState !== peer.socket.OPEN) {
      throw new Error("Plugin is offline");
    }
    peer.socket.send(JSON.stringify(value));
  };
  const failRun = (job: RunJob, error: unknown, rejected = false) => {
    // A written command can execute even when its native receipt is lost.
    // Only an explicit pre-admission rejection is safe to leave unaccepted.
    if (job.dispatched && !job.accepted && !rejected) {
      job.accepted = true;
      job.onAccepted?.();
    }
    job.reject(error);
  };
  function attachPlugin(socket: WebSocket, value: unknown, notify: Notify) {
    const hello = wire.fields(value, ["type", "runtimeId", "agentId", "token", "capabilities"]);
    const { agentId } = hello;
    assertAgent(agentId);
    if (
      closed ||
      hello.type !== "plugin.hello" ||
      hello.runtimeId !== id ||
      !sameToken(hello.token, token) ||
      !isDeepStrictEqual(wire.capabilities(hello.capabilities), capabilities)
    ) {
      throw new Error("Plugin authentication or capabilities rejected");
    }
    if (peers.has(agentId)) {
      throw new Error("Plugin target already has a live owner");
    }
    const detach = () => {
      if (!peer.active) {
        return;
      }
      peer.active = false;
      if (peers.get(agentId) === peer) {
        peers.delete(agentId);
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
    const peer: Peer = { socket, runs: new Map(), commands: new Map(), active: true, detach };
    peers.set(agentId, peer);
    socket.once("close", detach);
    socket.once("error", detach);
    socket.on("message", (raw) => {
      if (!peer.active || peers.get(agentId) !== peer) {
        socket.close(4002, "Plugin connection is no longer active");
        return;
      }
      try {
        const frame = JSON.parse(Buffer.from(raw as Buffer).toString()) as Record<string, unknown>;
        if (frame.type === "plugin.push") {
          wire.fields(frame, ["type", "id", "target", "payload"]);
          wire.text(frame.id);
          let response;
          try {
            const { target, payload } = wire.notification(frame.target, frame.payload);
            response = { receipt: notify(agentId, target, payload) };
          } catch (error) {
            response = { error: (error as Error).message };
          }
          send(peer, { type: "plugin.notified", id: frame.id, ...response });
          return;
        }
        if (frame.type === "plugin.receipt") {
          wire.fields(frame, ["type", "id", "status", "message"]);
          const { id: commandId, status } = frame;
          if (typeof commandId !== "string") {
            throw new Error("Invalid plugin control receipt");
          }
          const pending = peer.commands.get(commandId);
          if (
            !pending ||
            (status !== "queued" &&
              status !== "handled" &&
              status !== "rejected" &&
              status !== "unknown")
          ) {
            throw new Error("Invalid plugin control receipt");
          }
          peer.commands.delete(commandId);
          if (status === "rejected" || status === "unknown") {
            pending.reject(
              Object.assign(new Error(String(frame.message || "Native control rejected")), {
                rpcRejected: status === "rejected",
              }),
            );
          } else {
            pending.resolve({ status });
          }
          return;
        }
        const { runId } = frame;
        if (typeof runId !== "string") {
          throw new Error("Unknown plugin run");
        }
        const job = peer.runs.get(runId);
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
          const { status } = frame;
          if (
            status !== "completed" &&
            status !== "cancelled" &&
            status !== "deferred" &&
            status !== "failed"
          ) {
            throw new Error("Invalid plugin run result");
          }
          if (frame.rejected !== undefined && typeof frame.rejected !== "boolean") {
            throw new Error("Invalid plugin rejection");
          }
          const result =
            status === "cancelled"
              ? ({ status, message: frame.message as string | undefined } as const)
              : status === "completed"
                ? ({ status } as const)
                : ({ status, message: wire.text(frame.message) } as const);
          // Full-duplex delivery can finish a native run before its in-transit
          // control arrives. Commands settle independently; the gateway journals
          // their receipts before publishing the canonical terminal event.
          if (status !== "failed" && !job.accepted) {
            throw new Error("Plugin completed without native acceptance");
          }
          peer.runs.delete(runId);
          if (result.status === "failed") {
            const rejected = frame.rejected === true && !job.accepted;
            failRun(
              job,
              Object.assign(new Error(result.message), { rpcRejected: rejected }),
              rejected,
            );
          } else if (result.status === "completed") {
            job.resolve();
          } else if (result.status === "deferred") {
            job.resolve({ status: result.status, message: result.message });
          } else {
            job.resolve({
              status: result.status,
              ...(result.message ? { message: result.message } : {}),
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
  function peerFor(request: RunRequest) {
    assertAgent(request.agentId);
    const peer = peers.get(request.agentId);
    if (closed || !peer || peer.socket.readyState !== peer.socket.OPEN) {
      throw Object.assign(new Error("Plugin is offline; no native runtime fallback"), {
        rpcRejected: true,
      });
    }
    return peer;
  }
  function command(
    request: ActiveRequest,
    type: "plugin.cancel" | "plugin.control",
    input: Partial<Parameters<NonNullable<Adapter["control"]>>[1]> = {},
  ) {
    const peer = peerFor(request);
    if (!peer.runs.has(request.runId)) {
      return Promise.reject(
        Object.assign(new Error("No matching active plugin run"), { rpcRejected: true }),
      );
    }
    const commandId = randomUUID();
    return new Promise<ControlResult>((resolve, reject) => {
      peer.commands.set(commandId, { resolve, reject, runId: request.runId });
      try {
        send(peer, { type, id: commandId, runId: request.runId, ...input });
      } catch (error) {
        peer.commands.delete(commandId);
        reject(error instanceof Error ? error : new Error("Plugin control dispatch failed"));
      }
    });
  }
  const adapter: PluginAdapter = {
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
      const pending = Promise.withResolvers<RunResult>();
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
