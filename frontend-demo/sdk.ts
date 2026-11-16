import type { InputControl, Mention, PushEvent } from "../protocol/index.ts";
import type { CanonicalFrame, GatewayMessage, LegacyFrame, SessionTarget } from "./types.ts";

type SDKOptions = { sessionId?: string; protocolVersion?: number; token?: string };
type MessageMetadata = Partial<SessionTarget> & {
  queryMessageId?: string;
  answerMessageId?: string;
  mentions?: Mention[];
};
type PublicControlOptions = { sessionId?: string; runId?: string; controlId?: string };
type SocketError = Event & { sessionId?: never; runId?: never; message?: string };
type SDKEventMap = {
  message: LegacyFrame<"message">;
  messageSent: LegacyFrame<"message_sent">;
  registered: LegacyFrame<"registered">;
  sessionStarted: LegacyFrame<"session.started">;
  runStarted: LegacyFrame<"run_started"> | CanonicalFrame<"run.started">;
  runCompleted: LegacyFrame<"run_completed"> | CanonicalFrame<"run.completed">;
  runCancelled: LegacyFrame<"run_cancelled"> | CanonicalFrame<"run.cancelled">;
  controlReceipt: LegacyFrame<"input_control"> | CanonicalFrame<"input.control">;
  inputQueue: LegacyFrame<"input_queue"> | CanonicalFrame<"input.queue">;
  streamStart: LegacyFrame<"stream_start">;
  streamChunk: LegacyFrame<"stream">;
  streamSnapshot: LegacyFrame<"stream_snapshot">;
  streamEnd: LegacyFrame<"stream_end"> | LegacyFrame<"done">;
  toolStart: LegacyFrame<"tool_start">;
  toolEnd: LegacyFrame<"tool_end">;
  toolCall: LegacyFrame<"tool_call">;
  toolResult: LegacyFrame<"tool_result">;
  subagentStart: LegacyFrame<"subagent_start">;
  subagentMessage: LegacyFrame<"subagent_message">;
  subagentStream: LegacyFrame<"subagent_stream">;
  subagentToolCall: LegacyFrame<"subagent_tool_call">;
  subagentToolResult: LegacyFrame<"subagent_tool_result">;
  subagentResult: LegacyFrame<"subagent_result">;
  subagentError: LegacyFrame<"subagent_error">;
  subagentEnd: LegacyFrame<"subagent_end">;
  timeoutDeferred: LegacyFrame<"timeout_deferred">;
  error: LegacyFrame<"error"> | SocketError;
  event: PushEvent;
};

type Callbacks = { [T in keyof SDKEventMap]: ((data: SDKEventMap[T]) => void) | null } & {
  close: (() => void) | null;
};
type Cursor = { cursor: number; deliveryIds: Set<string> };

class PushChannelSDK {
  declare url: string;
  declare agentId: string;
  declare sessionId: string;
  declare protocolVersion: number | undefined;
  declare token: string | undefined;
  declare sessions: Map<string, SessionTarget>;
  declare cursors: Map<string, Cursor>;
  declare controlSequence: number;
  declare socket: WebSocket | null;
  declare callbacks: Callbacks;
  declare pingInterval: ReturnType<typeof setInterval> | null;

  constructor(url: string, agentId: string, options: SDKOptions = {}) {
    this.url = url;
    this.agentId = agentId;
    this.sessionId = options.sessionId || "";
    this.protocolVersion = options.protocolVersion;
    this.token = options.token;
    this.sessions = new Map();
    this.cursors = new Map();
    this.controlSequence = 0;
    this.socket = null;
    this.callbacks = {
      message: null,
      messageSent: null,
      registered: null,
      sessionStarted: null,
      runStarted: null,
      runCompleted: null,
      runCancelled: null,
      controlReceipt: null,
      inputQueue: null,
      streamStart: null,
      streamChunk: null,
      streamSnapshot: null,
      streamEnd: null,
      toolStart: null,
      toolEnd: null,
      toolCall: null,
      toolResult: null,
      subagentStart: null,
      subagentMessage: null,
      subagentStream: null,
      subagentToolCall: null,
      subagentToolResult: null,
      subagentResult: null,
      subagentError: null,
      subagentEnd: null,
      timeoutDeferred: null,
      error: null,
      close: null,
      event: null,
    };
    this.pingInterval = null;
  }

  connect() {
    const wsUrl = this.toWebSocketUrl(this.url);
    if (this.socket) {
      this.disconnect();
    }
    const socket = new WebSocket(wsUrl);
    this.socket = socket;

    this.socket.addEventListener("open", () => {
      if (this.socket !== socket) {
        return;
      }
      console.log("Connected to server");
      socket.send(
        JSON.stringify({
          type: "register",
          agentId: this.agentId,
          sessionId: this.sessionId,
          ...(this.protocolVersion !== undefined ? { protocolVersion: this.protocolVersion } : {}),
          ...(this.token ? { token: this.token } : {}),
        }),
      );

      this.pingInterval = setInterval(() => {
        if (this.socket?.readyState === WebSocket.OPEN) {
          this.socket.send(JSON.stringify({ type: "ping" }));
        }
      }, 30000);
    });

    this.socket.addEventListener("message", (event) => {
      if (this.socket !== socket) {
        return;
      }
      try {
        const data = JSON.parse(event.data) as GatewayMessage;

        if (data.type === "pong") {
          return;
        }
        if (!this.acceptDelivery(data)) {
          return;
        }

        if (data.version === 1 && this.protocolVersion === 1) {
          this.callbacks.event?.(data);
          if (data.type === "run.started") {
            this.callbacks.runStarted?.(data);
          }
          if (data.type === "run.completed") {
            this.callbacks.runCompleted?.(data);
          }
          if (data.type === "run.cancelled") {
            this.callbacks.runCancelled?.(data);
          }
          if (data.type === "input.control") {
            this.callbacks.controlReceipt?.(data);
          }
          if (data.type === "input.queue") {
            this.callbacks.inputQueue?.(data);
          }
          return;
        }

        if (data.type === "registered") {
          // Re-subscribe only after registration: reconnect must retain conversation ownership.
          for (const [sessionId, descriptor] of this.sessions) {
            this.startSession(sessionId, descriptor);
          }
          this.callbacks.registered?.(data);
        } else if (data.type === "session.started") {
          // Only an accepted binding is immutable and resumable; failed starts must remain retryable.
          this.sessions.set(data.sessionId, { runtimeId: data.runtimeId, agentId: data.agentId });
          this.callbacks.sessionStarted?.(data);
        } else if (data.type === "run_cancelled") {
          this.callbacks.runCancelled?.(data);
        } else if (data.type === "run_started") {
          this.callbacks.runStarted?.(data);
        } else if (data.type === "run_completed") {
          this.callbacks.runCompleted?.(data);
        } else if (data.type === "input_control") {
          this.callbacks.controlReceipt?.(data);
        } else if (data.type === "input_queue") {
          this.callbacks.inputQueue?.(data);
        } else if (data.type === "message_sent" && this.callbacks.messageSent) {
          this.callbacks.messageSent(data);
        } else if (data.type === "stream_start" && this.callbacks.streamStart) {
          this.callbacks.streamStart(data);
        } else if (data.type === "stream" && this.callbacks.streamChunk) {
          this.callbacks.streamChunk(data);
        } else if (data.type === "stream_snapshot" && this.callbacks.streamSnapshot) {
          this.callbacks.streamSnapshot(data);
        } else if (data.type === "stream_end" && this.callbacks.streamEnd) {
          this.callbacks.streamEnd(data);
        } else if (data.type === "tool_call" && this.callbacks.toolCall) {
          this.callbacks.toolCall(data);
        } else if (data.type === "tool_result" && this.callbacks.toolResult) {
          this.callbacks.toolResult(data);
        } else if (data.type === "tool_start" && this.callbacks.toolStart) {
          this.callbacks.toolStart(data);
        } else if (data.type === "tool_end" && this.callbacks.toolEnd) {
          this.callbacks.toolEnd(data);
        } else if (data.type === "subagent_start" && this.callbacks.subagentStart) {
          this.callbacks.subagentStart(data);
        } else if (data.type === "subagent_message" && this.callbacks.subagentMessage) {
          this.callbacks.subagentMessage(data);
        } else if (data.type === "subagent_stream" && this.callbacks.subagentStream) {
          this.callbacks.subagentStream(data);
        } else if (data.type === "subagent_tool_call" && this.callbacks.subagentToolCall) {
          this.callbacks.subagentToolCall(data);
        } else if (data.type === "subagent_tool_result" && this.callbacks.subagentToolResult) {
          this.callbacks.subagentToolResult(data);
        } else if (data.type === "subagent_result" && this.callbacks.subagentResult) {
          this.callbacks.subagentResult(data);
        } else if (data.type === "subagent_error" && this.callbacks.subagentError) {
          this.callbacks.subagentError(data);
        } else if (data.type === "subagent_end" && this.callbacks.subagentEnd) {
          this.callbacks.subagentEnd(data);
        } else if (data.type === "timeout_deferred" && this.callbacks.timeoutDeferred) {
          this.callbacks.timeoutDeferred(data);
        } else if (data.type === "message" && this.callbacks.message) {
          this.callbacks.message(data);
        } else if (data.type === "error" && this.callbacks.error) {
          this.callbacks.error(data);
        } else if (data.type === "done" && this.callbacks.streamEnd) {
          this.callbacks.streamEnd(data);
        }
      } catch (err) {
        console.error("Error parsing message", err);
      }
    });

    this.socket.addEventListener("error", (error) => {
      if (this.socket !== socket) {
        return;
      }
      console.error("WebSocket error:", error);
      if (this.callbacks.error) {
        this.callbacks.error(error);
      }
    });

    this.socket.addEventListener("close", () => {
      if (this.socket !== socket) {
        return;
      }
      console.log("Disconnected from server");
      if (this.pingInterval) {
        clearInterval(this.pingInterval);
      }
      if (this.callbacks.close) {
        this.callbacks.close();
      }
    });
  }

  sendMessage(content: string, sessionId?: string, metadata: MessageMetadata = {}) {
    if (this.socket && this.socket.readyState === WebSocket.OPEN) {
      const payload: MessageMetadata & { type: "message"; content: string; sessionId?: string } = {
        ...metadata,
        type: "message",
        content,
      };
      if (sessionId) {
        payload.sessionId = sessionId;
      }
      this.socket.send(JSON.stringify(payload));
      return true;
    }
    console.error("Socket is not open");
    return false;
  }

  startSession(sessionId: string, { runtimeId, agentId }: SessionTarget) {
    if (!sessionId || !runtimeId || !agentId) {
      throw new Error("Session, runtime, and target agent are required");
    }
    const existing = this.sessions.get(sessionId);
    if (existing && (existing.runtimeId !== runtimeId || existing.agentId !== agentId)) {
      throw new Error("Session runtime and target agent are immutable; start a different session");
    }
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      return false;
    }
    // Inclusive checkpoint: replay the last event again so missing sibling projections can arrive.
    const afterCursor = this.getCursor(sessionId);
    this.socket.send(
      JSON.stringify({
        type: "session.start",
        sessionId,
        runtimeId,
        agentId,
        ...(afterCursor !== undefined ? { afterCursor } : {}),
      }),
    );
    return true;
  }

  getCursor(sessionId: string) {
    return this.cursors.get(sessionId)?.cursor;
  }

  acceptDelivery(data: GatewayMessage) {
    const deliveryId = data.version === 1 ? data.eventId : data.deliveryId;
    if (
      data.cursor === undefined ||
      !Number.isSafeInteger(data.cursor) ||
      data.cursor < 0 ||
      typeof deliveryId !== "string"
    ) {
      return true;
    }
    const sessionId =
      data.audience === "agent" ? "" : (data.sessionId ?? data.conversationId ?? "");
    const previous = this.cursors.get(sessionId);
    if (previous && data.cursor < previous.cursor) {
      return false;
    }
    if (previous && data.cursor === previous.cursor) {
      if (previous.deliveryIds.has(deliveryId)) {
        return false;
      }
      previous.deliveryIds.add(deliveryId);
      return true;
    }
    // One canonical event can project to several legacy messages; retain their IDs until the cursor advances.
    this.cursors.set(sessionId, { cursor: data.cursor, deliveryIds: new Set([deliveryId]) });
    return true;
  }

  cancel(sessionId?: string) {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      return false;
    }
    this.socket.send(JSON.stringify({ type: "cancel", sessionId }));
    return true;
  }

  steer(content: string, options?: PublicControlOptions) {
    return this.sendControl("steer", content, options);
  }

  followUp(content: string, options?: PublicControlOptions) {
    return this.sendControl("follow_up", content, options);
  }

  sendControl(
    operation: InputControl["operation"],
    content: string,
    { sessionId, runId, controlId }: PublicControlOptions = {},
  ) {
    if (
      typeof sessionId !== "string" ||
      !sessionId.trim() ||
      typeof runId !== "string" ||
      !runId.trim()
    ) {
      throw new Error("Session and active run IDs are required");
    }
    if (typeof content !== "string" || !content.trim()) {
      throw new Error("Control content must be non-empty text");
    }
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      return false;
    }
    const resolvedControlId =
      controlId ??
      (globalThis.crypto?.randomUUID?.() || `control-${Date.now()}-${++this.controlSequence}`);
    // Controls target the existing run; request/reply identities remain owned by its original input.
    this.socket.send(
      JSON.stringify({
        type: "input.control",
        sessionId,
        runId,
        controlId: resolvedControlId,
        operation,
        content,
      }),
    );
    return resolvedControlId;
  }

  disconnect() {
    if (this.socket) {
      this.socket.close();
    }
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
    }
  }

  onMessage(callback: Callbacks["message"]) {
    this.callbacks.message = callback;
  }

  onEvent(callback: Callbacks["event"]) {
    this.callbacks.event = callback;
  }

  onMessageSent(callback: Callbacks["messageSent"]) {
    this.callbacks.messageSent = callback;
  }

  onRegistered(callback: Callbacks["registered"]) {
    this.callbacks.registered = callback;
  }

  onSessionStarted(callback: Callbacks["sessionStarted"]) {
    this.callbacks.sessionStarted = callback;
  }

  onRunCancelled(callback: Callbacks["runCancelled"]) {
    this.callbacks.runCancelled = callback;
  }

  onRunStarted(callback: Callbacks["runStarted"]) {
    this.callbacks.runStarted = callback;
  }

  onRunCompleted(callback: Callbacks["runCompleted"]) {
    this.callbacks.runCompleted = callback;
  }

  onControlReceipt(callback: Callbacks["controlReceipt"]) {
    this.callbacks.controlReceipt = callback;
  }

  onInputQueue(callback: Callbacks["inputQueue"]) {
    this.callbacks.inputQueue = callback;
  }

  onStreamStart(callback: Callbacks["streamStart"]) {
    this.callbacks.streamStart = callback;
  }

  onStreamChunk(callback: Callbacks["streamChunk"]) {
    this.callbacks.streamChunk = callback;
  }

  onStreamSnapshot(callback: Callbacks["streamSnapshot"]) {
    this.callbacks.streamSnapshot = callback;
  }

  onStreamEnd(callback: Callbacks["streamEnd"]) {
    this.callbacks.streamEnd = callback;
  }

  onToolStart(callback: Callbacks["toolStart"]) {
    this.callbacks.toolStart = callback;
  }

  onToolEnd(callback: Callbacks["toolEnd"]) {
    this.callbacks.toolEnd = callback;
  }

  onToolCall(callback: Callbacks["toolCall"]) {
    this.callbacks.toolCall = callback;
  }

  onToolResult(callback: Callbacks["toolResult"]) {
    this.callbacks.toolResult = callback;
  }

  onSubagentStart(callback: Callbacks["subagentStart"]) {
    this.callbacks.subagentStart = callback;
  }

  onSubagentMessage(callback: Callbacks["subagentMessage"]) {
    this.callbacks.subagentMessage = callback;
  }

  onSubagentStream(callback: Callbacks["subagentStream"]) {
    this.callbacks.subagentStream = callback;
  }

  onSubagentToolCall(callback: Callbacks["subagentToolCall"]) {
    this.callbacks.subagentToolCall = callback;
  }

  onSubagentToolResult(callback: Callbacks["subagentToolResult"]) {
    this.callbacks.subagentToolResult = callback;
  }

  onSubagentResult(callback: Callbacks["subagentResult"]) {
    this.callbacks.subagentResult = callback;
  }

  onSubagentError(callback: Callbacks["subagentError"]) {
    this.callbacks.subagentError = callback;
  }

  onSubagentEnd(callback: Callbacks["subagentEnd"]) {
    this.callbacks.subagentEnd = callback;
  }

  onTimeoutDeferred(callback: Callbacks["timeoutDeferred"]) {
    this.callbacks.timeoutDeferred = callback;
  }

  onError(callback: Callbacks["error"]) {
    this.callbacks.error = callback;
  }

  onClose(callback: Callbacks["close"]) {
    this.callbacks.close = callback;
  }

  toWebSocketUrl(url: string) {
    try {
      const parsed = new URL(url);
      if (parsed.protocol === "http:") {
        parsed.protocol = "ws:";
      } else if (parsed.protocol === "https:") {
        parsed.protocol = "wss:";
      }
      return parsed.toString();
    } catch {
      return url;
    }
  }
}

type SDKConstructor = typeof PushChannelSDK;
declare global {
  var PushChannelSDK: SDKConstructor;
}
globalThis.PushChannelSDK = PushChannelSDK;
