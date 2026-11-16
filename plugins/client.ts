import { randomUUID } from "node:crypto";
import * as wire from "../protocol/plugin.ts";
import type { Adapter, AdapterSignal } from "../protocol/index.ts";
import type {
  NotificationPayload,
  NotificationReceipt,
  NotificationTarget,
  PluginCapabilities,
  PluginRequest,
} from "../protocol/plugin.ts";

type PluginClient = {
  close(): Promise<void>;
  notify(target: NotificationTarget, payload: NotificationPayload): Promise<NotificationReceipt>;
};

type Job = {
  request: PluginRequest;
  abort: AbortController;
  controls: Set<Promise<void>>;
  accepted: boolean;
};

export async function connectPlugin(options: {
  url: string;
  runtimeId: string;
  agentId: string;
  token: string;
  capabilities: Partial<PluginCapabilities>;
  signal?: AbortSignal;
  run(request: PluginRequest, options: Parameters<Adapter["run"]>[1]): ReturnType<Adapter["run"]>;
  control?(
    request: PluginRequest,
    input: Parameters<NonNullable<Adapter["control"]>>[1],
  ): ReturnType<NonNullable<Adapter["control"]>>;
  cancel?(request: PluginRequest): Promise<void>;
}): Promise<PluginClient> {
  const { runtimeId, agentId, token, run, control, cancel, signal } = options;
  wire.text(runtimeId);
  wire.text(agentId);
  wire.text(token);
  let url: URL;
  try {
    url = new URL(options.url);
  } catch {
    throw new Error("Invalid plugin gateway URL");
  }
  if (
    !["ws:", "wss:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  ) {
    throw new Error("Plugin URL must use WS(S) without credentials, query or fragment");
  }
  const capabilities = wire.capabilities(options.capabilities);
  if (
    typeof run !== "function" ||
    (capabilities.cancel && typeof cancel !== "function") ||
    ((capabilities.steering || capabilities.followUp) && typeof control !== "function")
  ) {
    throw new TypeError("Plugin handlers must implement advertised capabilities");
  }
  if (signal?.aborted) {
    throw new Error("Plugin lifecycle aborted");
  }
  const ready = Promise.withResolvers<PluginClient>();
  const jobs = new Map<string, Job>();
  const notifications = new Map<string, Pick<PromiseWithResolvers<NotificationReceipt>, "resolve" | "reject">>();
  let socket: WebSocket | undefined;
  let closed = false;
  let connected = false;
  let established = false;
  let reconnect: ReturnType<typeof setTimeout> | undefined;
  let handshake: ReturnType<typeof setTimeout> | undefined;
  let retryDelay = 100;
  const send = (value: object, peer = socket) => {
    if (closed || !peer || peer !== socket || peer.readyState !== WebSocket.OPEN) {
      throw new Error("Plugin connection is not ready");
    }
    peer.send(JSON.stringify(value));
  };
  const abandon = () => {
    connected = false;
    for (const job of jobs.values()) {
      job.abort.abort();
    }
    jobs.clear();
    for (const pending of notifications.values()) {
      pending.reject(new Error("Plugin notification outcome is unknown after disconnect"));
    }
    notifications.clear();
  };
  const client: PluginClient = {
    async notify(target, payload) {
      wire.notification(target, payload);
      if (!connected) {
        throw new Error("Plugin is offline");
      }
      const id = randomUUID();
      return new Promise((resolve, reject) => {
        notifications.set(id, { resolve, reject });
        try {
          send({ type: "plugin.push", id, target, payload });
        } catch (error) {
          notifications.delete(id);
          reject(error instanceof Error ? error : new Error("Plugin notification dispatch failed"));
        }
      });
    },
    async close() {
      if (closed) {
        return;
      }
      closed = true;
      clearTimeout(reconnect);
      clearTimeout(handshake);
      signal?.removeEventListener("abort", abortLifecycle);
      abandon();
      ready.reject(new Error("Plugin lifecycle closed"));
      // Native jobs belong to the host plugin. Waiting for their completion
      // here would deadlock a host disposer that drains them after client.close().
      socket?.close(1000, "Plugin lifecycle closed");
    },
  };
  const abortLifecycle = () => {
    void client.close();
  };
  signal?.addEventListener("abort", abortLifecycle, { once: true });
  const failInitial = (error: Error) => {
    ready.reject(error);
    void client.close();
  };

  async function execute(frame: Record<string, unknown>, peer: WebSocket) {
    wire.fields(frame, ["type", "runId", "request"]);
    wire.fields(frame.request, wire.requestFields);
    const request = wire.request(frame.request);
    if (
      frame.runId !== request.runId ||
      request.runtimeId !== runtimeId ||
      request.agentId !== agentId ||
      jobs.has(request.runId)
    ) {
      throw new Error("Invalid plugin run target");
    }
    const job: Job = { request, abort: new AbortController(), controls: new Set(), accepted: false };
    jobs.set(request.runId, job);
    const active = () =>
      jobs.get(request.runId) === job && !job.abort.signal.aborted && socket === peer;
    const emit = (event: AdapterSignal) => {
      if (!active()) {
        throw new Error("Plugin run observation has ended");
      }
      if (!job.accepted) {
        throw new Error("Native acceptance must precede plugin output");
      }
      send({ type: "plugin.signal", runId: request.runId, signal: wire.signal(event) }, peer);
    };
    let result;
    try {
      const value = await run(request, {
        signal: job.abort.signal,
        emit,
        onAccepted() {
          if (!active()) {
            throw new Error("Plugin run observation has ended");
          }
          if (job.accepted) {
            return;
          }
          send({ type: "plugin.accepted", runId: request.runId }, peer);
          job.accepted = true;
        },
      });
      if (!job.accepted) {
        throw new Error("Native plugin completed without accepting input");
      }
      if (value !== undefined && !["cancelled", "deferred"].includes(value?.status)) {
        throw new Error("Invalid native plugin result");
      }
      result = {
        status: value?.status ?? "completed",
        ...(value?.message ? { message: value.message } : {}),
      };
    } catch (error) {
      result = {
        status: "failed",
        message: error instanceof Error ? error.message : "Native plugin run failed",
        rejected: (error as { rpcRejected?: boolean } | null)?.rpcRejected === true && !job.accepted,
      };
    }
    await Promise.allSettled(job.controls);
    if (active()) {
      send({ type: "plugin.result", runId: request.runId, ...result }, peer);
      jobs.delete(request.runId);
    }
  }

  async function input(frame: Record<string, unknown>, peer: WebSocket) {
    wire.fields(frame, [
      "type",
      "id",
      "runId",
      ...(frame.type === "plugin.control" ? ["operation", "content"] : []),
    ]);
    wire.text(frame.id);
    const runId = wire.text(frame.runId);
    const job = jobs.get(runId);
    const task = (async () => {
      let receipt;
      try {
        if (!job) {
          throw Object.assign(new Error("No matching active native run"), { rpcRejected: true });
        }
        if (frame.type === "plugin.cancel") {
          if (!capabilities.cancel || !cancel) {
            throw Object.assign(new Error("Unsupported cancellation"), { rpcRejected: true });
          }
          await cancel(job.request);
          receipt = { status: "handled" };
        } else {
          const content = wire.text(frame.content);
          const supported =
            frame.operation === "steer"
              ? capabilities.steering
              : frame.operation === "follow_up" && capabilities.followUp;
          if (!supported || !control || (frame.operation !== "steer" && frame.operation !== "follow_up")) {
            throw Object.assign(new Error("Unsupported native input control"), {
              rpcRejected: true,
            });
          }
          receipt = await control(job.request, {
            operation: frame.operation,
            content,
          });
          if (!["queued", "handled"].includes(receipt?.status)) {
            throw new Error("Invalid native control receipt");
          }
        }
      } catch (error) {
        receipt = {
          status: (error as { rpcRejected?: boolean } | null)?.rpcRejected === true ? "rejected" : "unknown",
          message: error instanceof Error ? error.message : "Native input rejected",
        };
      }
      if (socket === peer && connected && !closed && !job?.abort.signal.aborted) {
        send({ type: "plugin.receipt", id: frame.id, ...receipt }, peer);
      }
    })();
    job?.controls.add(task);
    try {
      await task;
    } finally {
      job?.controls.delete(task);
    }
  }

  function dial() {
    if (closed) {
      return;
    }
    const peer = new WebSocket(url);
    socket = peer;
    handshake = setTimeout(() => {
      if (peer !== socket || connected) {
        return;
      }
      if (!established) {
        failInitial(new Error("Plugin handshake timed out"));
      } else {
        peer.close(1000, "Plugin handshake timed out");
      }
    }, 3000);
    peer.addEventListener("open", () => {
      if (peer !== socket || closed) {
        return;
      }
      send({ type: "plugin.hello", runtimeId, agentId, token, capabilities }, peer);
    });
    peer.addEventListener("message", (event) => {
      if (peer !== socket || closed) {
        return;
      }
      let task;
      try {
        if (typeof event.data !== "string" || event.data.length > 4 * 1024 * 1024) {
          throw new Error("Invalid plugin frame size");
        }
        const frame = JSON.parse(event.data) as Record<string, unknown>;
        if (!connected) {
          wire.fields(frame, ["type"]);
          if (frame.type !== "plugin.ready") {
            throw new Error("Plugin handshake rejected");
          }
          clearTimeout(handshake);
          connected = true;
          established = true;
          retryDelay = 100;
          ready.resolve(client);
          return;
        }
        if (frame.type === "plugin.run") {
          task = execute(frame, peer);
        } else if (frame.type === "plugin.control" || frame.type === "plugin.cancel") {
          task = input(frame, peer);
        } else if (frame.type === "plugin.notified") {
          wire.fields(frame, ["type", "id", "receipt", "error"]);
          const id = frame.id as string;
          const pending = notifications.get(id);
          if (!pending) {
            throw new Error("Unknown plugin notification receipt");
          }
          if (frame.error) {
            pending.reject(new Error(wire.text(frame.error)));
          } else {
            const receipt = wire.fields(frame.receipt, ["runId", "duplicate"]);
            wire.text(receipt.runId);
            if (typeof receipt.duplicate !== "boolean") {
              throw new Error("Invalid plugin notification receipt");
            }
            pending.resolve(receipt as NotificationReceipt);
          }
          notifications.delete(id);
        } else {
          throw new Error("Unexpected plugin frame");
        }
        void task?.catch(() => peer.close(4002, "Invalid plugin protocol"));
      } catch {
        if (!established) {
          failInitial(new Error("Plugin handshake rejected"));
        } else {
          peer.close(4002, "Invalid plugin protocol");
        }
      }
    });
    peer.addEventListener("error", () => {
      if (peer !== socket || closed) {
        return;
      }
      if (!established) {
        failInitial(new Error("Cannot connect plugin gateway"));
      }
    });
    peer.addEventListener("close", () => {
      if (peer !== socket) {
        return;
      }
      clearTimeout(handshake);
      abandon();
      if (!established) {
        signal?.removeEventListener("abort", abortLifecycle);
        ready.reject(new Error("Plugin gateway handshake rejected"));
      } else if (!closed) {
        // Reconnect metadata only. Lost native commands/results are never replayed.
        reconnect = setTimeout(dial, retryDelay);
        retryDelay = Math.min(retryDelay * 2, 5000);
      }
    });
  }
  dial();
  return ready.promise;
}

export default { connectPlugin };
