class PushChannelSDK {
  constructor(url, agentId, options = {}) {
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
        if (this.socket.readyState === WebSocket.OPEN) {
          this.socket.send(JSON.stringify({ type: "ping" }));
        }
      }, 30000);
    });

    this.socket.addEventListener("message", (event) => {
      if (this.socket !== socket) {
        return;
      }
      try {
        const data = JSON.parse(event.data);

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

  sendMessage(content, sessionId, metadata = {}) {
    if (this.socket && this.socket.readyState === WebSocket.OPEN) {
      const payload = { ...metadata, type: "message", content };
      if (sessionId) {
        payload.sessionId = sessionId;
      }
      this.socket.send(JSON.stringify(payload));
      return true;
    }
    console.error("Socket is not open");
    return false;
  }

  startSession(sessionId, { runtimeId, agentId }) {
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

  getCursor(sessionId) {
    return this.cursors.get(sessionId)?.cursor;
  }

  acceptDelivery(data) {
    const deliveryId = data.version === 1 ? data.eventId : data.deliveryId;
    if (!Number.isSafeInteger(data.cursor) || data.cursor < 0 || typeof deliveryId !== "string") {
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

  cancel(sessionId) {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      return false;
    }
    this.socket.send(JSON.stringify({ type: "cancel", sessionId }));
    return true;
  }

  steer(content, options) {
    return this.sendControl("steer", content, options);
  }

  followUp(content, options) {
    return this.sendControl("follow_up", content, options);
  }

  sendControl(operation, content, { sessionId, runId, controlId } = {}) {
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

  onMessage(callback) {
    this.callbacks.message = callback;
  }

  onEvent(callback) {
    this.callbacks.event = callback;
  }

  onMessageSent(callback) {
    this.callbacks.messageSent = callback;
  }

  onRegistered(callback) {
    this.callbacks.registered = callback;
  }

  onSessionStarted(callback) {
    this.callbacks.sessionStarted = callback;
  }

  onRunCancelled(callback) {
    this.callbacks.runCancelled = callback;
  }

  onRunStarted(callback) {
    this.callbacks.runStarted = callback;
  }

  onRunCompleted(callback) {
    this.callbacks.runCompleted = callback;
  }

  onControlReceipt(callback) {
    this.callbacks.controlReceipt = callback;
  }

  onInputQueue(callback) {
    this.callbacks.inputQueue = callback;
  }

  onStreamStart(callback) {
    this.callbacks.streamStart = callback;
  }

  onStreamChunk(callback) {
    this.callbacks.streamChunk = callback;
  }

  onStreamSnapshot(callback) {
    this.callbacks.streamSnapshot = callback;
  }

  onStreamEnd(callback) {
    this.callbacks.streamEnd = callback;
  }

  onToolStart(callback) {
    this.callbacks.toolStart = callback;
  }

  onToolEnd(callback) {
    this.callbacks.toolEnd = callback;
  }

  onToolCall(callback) {
    this.callbacks.toolCall = callback;
  }

  onToolResult(callback) {
    this.callbacks.toolResult = callback;
  }

  onSubagentStart(callback) {
    this.callbacks.subagentStart = callback;
  }

  onSubagentMessage(callback) {
    this.callbacks.subagentMessage = callback;
  }

  onSubagentStream(callback) {
    this.callbacks.subagentStream = callback;
  }

  onSubagentToolCall(callback) {
    this.callbacks.subagentToolCall = callback;
  }

  onSubagentToolResult(callback) {
    this.callbacks.subagentToolResult = callback;
  }

  onSubagentResult(callback) {
    this.callbacks.subagentResult = callback;
  }

  onSubagentError(callback) {
    this.callbacks.subagentError = callback;
  }

  onSubagentEnd(callback) {
    this.callbacks.subagentEnd = callback;
  }

  onTimeoutDeferred(callback) {
    this.callbacks.timeoutDeferred = callback;
  }

  onError(callback) {
    this.callbacks.error = callback;
  }

  onClose(callback) {
    this.callbacks.close = callback;
  }

  toWebSocketUrl(url) {
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

globalThis.PushChannelSDK = PushChannelSDK;
