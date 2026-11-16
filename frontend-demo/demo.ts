import type { InputQueue } from "../protocol/index.ts";
import type {
  Capabilities,
  ChildPresentation,
  Presentation,
  ReceiptDetails,
  SessionTarget,
  ToolPresentation,
} from "./types.ts";

type ControlUpdate = Omit<ReceiptDetails, "status"> & {
  status?: ReceiptDetails["status"] | "pending";
};
type ControlState = ControlUpdate & { node: HTMLDivElement };
type DemoSession = SessionTarget & {
  capabilities: Capabilities;
  controls: Map<string, ControlState>;
  discardedQueues: Map<string, HTMLDivElement>;
  ready: boolean;
  running?: boolean;
  toolRunning?: boolean;
  runId?: string;
  inputQueue?: Pick<InputQueue, "steering" | "followUp">;
};

let sdk: InstanceType<typeof PushChannelSDK> | null = null;
let connected = false;
let activeSessionId = "";
let generatedId = 0;
const sessions = new Map<string, DemoSession>();
const localQueryIds = new Set<string>();
const subagentTextQueues = new WeakMap<HTMLElement, Promise<void>>();
const runtimeNames = new Map([
  ["openclaw", "OpenClaw"],
  ["deepseek-harness", "DeepSeek Harness（dsh）"],
  ["pi", "Pi"],
]);
const runtimeLabel = (id: string) => runtimeNames.get(id) || id;

const connectForm = document.querySelector<HTMLFormElement>("#connect-form")!;
const disconnectBtn = document.querySelector<HTMLButtonElement>("#disconnect-btn")!;
const serverUrlInput = document.querySelector<HTMLInputElement>("#server-url-input")!;
const agentIdInput = document.querySelector<HTMLInputElement>("#agent-id-input")!;
const gatewayTokenInput = document.querySelector<HTMLInputElement>("#gateway-token-input")!;
const sessionIdInput = document.querySelector<HTMLInputElement>("#session-id-input")!;
const sessionForm = document.querySelector<HTMLFormElement>("#session-form")!;
const startSessionBtn = document.querySelector<HTMLButtonElement>("#start-session-btn")!;
const runtimeIdInput = document.querySelector<HTMLSelectElement>("#runtime-id-input")!;
const targetAgentInput = document.querySelector<HTMLInputElement>("#openclaw-agent-id-input")!;
const sessionSelect = document.querySelector<HTMLSelectElement>("#session-select")!;
const capabilityStatus = document.querySelector<HTMLParagraphElement>("#capability-status")!;
const cancelBtn = document.querySelector<HTMLButtonElement>("#cancel-btn")!;
const steerBtn = document.querySelector<HTMLButtonElement>("#steer-btn")!;
const followUpBtn = document.querySelector<HTMLButtonElement>("#follow-up-btn")!;
const inputQueueStatus = document.querySelector<HTMLParagraphElement>("#input-queue-status")!;
const connectionStatus = document.querySelector<HTMLParagraphElement>("#connection-status")!;
const agentDisplay = document.querySelector<HTMLSpanElement>("#agent-display")!;
const messagesDiv = document.querySelector<HTMLDivElement>("#messages")!;
const messageForm = document.querySelector<HTMLFormElement>("#message-form")!;
const messageInput = document.querySelector<HTMLInputElement>("#message-input")!;
const sendBtn = document.querySelector<HTMLButtonElement>("#send-btn")!;
const toolStatus = document.querySelector<HTMLSpanElement>("#tool-status")!;

function createSessionId(agentId: string) {
  if (window.crypto && typeof window.crypto.randomUUID === "function") {
    return `${agentId}-${window.crypto.randomUUID()}`;
  }
  return `${agentId}-${Date.now()}-${++generatedId}`;
}

function setConnected(value: boolean) {
  connected = value;
  disconnectBtn.disabled = !value;
  connectForm
    .querySelectorAll<HTMLInputElement | HTMLButtonElement>("input, button")
    .forEach((element) => {
      element.disabled = value;
    });
  sessionForm
    .querySelectorAll<HTMLInputElement | HTMLSelectElement | HTMLButtonElement>(
      "input, select, button",
    )
    .forEach((element) => {
      element.disabled = !value;
    });
  sessionSelect.disabled = sessions.size === 0;
  updateSessionControls();
}

function updateToolStatus() {
  toolStatus.hidden = !sessions.get(activeSessionId)?.toolRunning;
}

function isCurrentSession(data: Presentation) {
  // Keep subscribed background sessions updated; hide their nodes instead of dropping events.
  return !data.sessionId || sessions.has(data.sessionId);
}

function updateSessionControls() {
  const session = sessions.get(activeSessionId);
  const ready = connected && session !== undefined && session.ready;
  messageInput.disabled = !ready;
  sendBtn.disabled = !ready;
  cancelBtn.disabled = !ready || !session.capabilities.cancel || !session.running;
  const controllable = ready && session.running && session.runId;
  steerBtn.disabled = !controllable || !session.capabilities.steering;
  followUpBtn.disabled = !controllable || !session.capabilities.followUp;
  agentDisplay.textContent = session
    ? `${runtimeLabel(session.runtimeId)} · ${session.agentId}`
    : "尚未开始会话";
  capabilityStatus.textContent = session
    ? [
        session.capabilities.mentions ? "支持 @提及智能体分派任务" : "不支持 @提及，消息按原文发送",
        session.capabilities.subagents ? "支持子智能体" : "不支持子智能体",
        session.capabilities.cancel ? "支持停止" : "当前底座不支持停止",
        session.capabilities.steering ? "支持即时引导" : "不支持即时引导",
        session.capabilities.followUp ? "支持后续跟进" : "不支持后续跟进",
      ].join(" · ")
    : "请先开始会话，再发送消息。";
  messageInput.placeholder = session?.capabilities.mentions
    ? "输入消息，可用 @智能体 分派任务…"
    : "输入消息…";
  updateToolStatus();
  inputQueueStatus.hidden =
    !session || !(session.capabilities.steering || session.capabilities.followUp);
  inputQueueStatus.textContent = session?.inputQueue
    ? `底座待处理队列：即时引导 ${session.inputQueue.steering.length}，后续跟进 ${session.inputQueue.followUp.length}。${[...session.inputQueue.steering, ...session.inputQueue.followUp].join("；")}`
    : "队列状态未知，等待底座快照。";
}

function selectSession(sessionId: string) {
  activeSessionId = sessionId;
  sessionSelect.value = sessionId;
  for (const node of messagesDiv.children as HTMLCollectionOf<HTMLElement>) {
    node.hidden = Boolean(node.dataset.sessionId && node.dataset.sessionId !== sessionId);
  }
  updateSessionControls();
}

function setRunState({ sessionId, runId }: Presentation, running: boolean) {
  const session = sessionId ? sessions.get(sessionId) : undefined;
  if (sessionId && session) {
    // Late terminal events from an older run must not disable controls for its successor.
    if (!running && runId && session.runId && runId !== session.runId) return;
    if (running && runId && runId !== session.runId) {
      session.runId = runId;
      session.inputQueue = undefined;
    }
    session.running = running;
    if (!running) {
      session.toolRunning = false;
      markUnconfirmedControls(sessionId, session, runId || session.runId);
    }
  }
  updateSessionControls();
}

function extractToolResultText(content: unknown) {
  if (content == null) return null;
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    const parts = content
      .map((block: unknown) => {
        if (block == null) return "";
        if (typeof block === "string") return block;
        if (typeof block === "object" && "text" in block && typeof block.text === "string") {
          return block.text;
        }
        return JSON.stringify(block);
      })
      .filter(Boolean);
    return parts.length > 0 ? parts.join("\n") : null;
  }
  try {
    return JSON.stringify(content, null, 2);
  } catch {
    return String(content);
  }
}

function appendText(parent: HTMLElement, text: string) {
  parent.appendChild(document.createTextNode(text));
}

function appendMessage(sender: string, text: string, className: string, meta: Presentation = {}) {
  const div = document.createElement("div");
  div.className = `message ${className}`;
  if (meta.answerMessageId) div.dataset.answerMessageId = meta.answerMessageId;
  if (meta.queryMessageId) div.dataset.queryMessageId = meta.queryMessageId;
  div.dataset.sessionId = meta.sessionId || activeSessionId;
  div.hidden = Boolean(div.dataset.sessionId && div.dataset.sessionId !== activeSessionId);

  const label = document.createElement("strong");
  label.textContent = sender;
  const content = document.createElement("span");
  content.className = "content";
  appendText(content, text);

  div.append(label, content);
  messagesDiv.appendChild(div);
  messagesDiv.scrollTop = messagesDiv.scrollHeight;
  return div;
}

function appendSystemMessage(text: string, meta: Presentation = {}) {
  appendMessage("系统", text, "system", meta);
}

function updateControlReceipt(data: ControlUpdate) {
  const session = data.sessionId ? sessions.get(data.sessionId) : undefined;
  if (!session || !data.controlId) return;
  let control = session.controls.get(data.controlId);
  if (!control) {
    const node = appendMessage("输入控制", "", "system input-control", data);
    node.dataset.controlId = data.controlId;
    control = { node };
    session.controls.set(data.controlId, control);
  }
  Object.assign(control, data);
  const label =
    control.operation === "steer"
      ? "即时引导"
      : control.operation === "follow_up"
        ? "后续跟进"
        : "输入控制";
  const statuses = {
    pending: "正在发送，等待回执",
    dispatching: "正在交给底座，等待回执",
    queued: "已接收，等待处理",
    handled: "已交给底座，由输入处理器接管",
    rejected: "已拒绝",
    unknown: "状态未知，请核对底座后再决定是否重新发送",
  };
  control.node.querySelector("strong")!.textContent = label;
  control.node.querySelector(".content")!.textContent =
    `${control.content || ""}\n${(control.status && statuses[control.status]) || "状态未知"}${control.message ? `：${control.message}` : ""}`;
}

function markUnconfirmedControls(sessionId: string, session: DemoSession, runId?: string) {
  // Missing native acknowledgements remain unknown on disconnect or terminal replay, never retried.
  for (const [controlId, control] of session.controls) {
    if (
      (!runId || control.runId === runId) &&
      (control.status === "pending" || control.status === "dispatching")
    ) {
      updateControlReceipt({ sessionId, controlId, status: "unknown" });
    }
  }
}

function subagentSelector(data: ChildPresentation) {
  const agentId = data.childSessionKey || data.agentId || data.label || "unknown";
  const sessionId = data.sessionId || activeSessionId;
  return `.subagent-card[data-subagent-id="${CSS.escape(agentId)}"][data-session-id="${CSS.escape(sessionId)}"]`;
}

function ensureSubagentCard(data: ChildPresentation) {
  let card = messagesDiv.querySelector<HTMLDivElement>(subagentSelector(data));
  if (card) return card;

  const agentId = data.childSessionKey || data.agentId || data.label || "unknown";
  card = document.createElement("div");
  card.className = "message system subagent-card";
  card.dataset.subagentId = agentId;
  card.dataset.sessionId = data.sessionId || activeSessionId;
  card.hidden = card.dataset.sessionId !== activeSessionId;

  const header = document.createElement("div");
  header.className = "subagent-header";

  const name = document.createElement("strong");
  name.textContent = `@${data.label || agentId}`;

  const status = document.createElement("span");
  status.className = "subagent-status";
  status.textContent = "等待中";

  header.append(name, status);

  const progress = document.createElement("pre");
  progress.className = "subagent-progress";

  const tools = document.createElement("div");
  tools.className = "subagent-tools";

  const result = document.createElement("pre");
  result.className = "subagent-result";
  result.hidden = true;

  card.append(header, progress, tools, result);
  messagesDiv.appendChild(card);
  messagesDiv.scrollTop = messagesDiv.scrollHeight;
  return card;
}

function updateSubagentStatus(card: HTMLElement, statusText: string, stateClass?: string) {
  card.classList.remove("subagent-running", "subagent-done", "subagent-error");
  if (stateClass) card.classList.add(stateClass);
  const status = card.querySelector(".subagent-status");
  if (status) status.textContent = statusText;
}

function queueSubagentTypedText(
  element: HTMLElement,
  text: string,
  options: { newline?: boolean } = {},
) {
  if (!element || !text) return;
  const chars = Array.from(String(text));
  if (chars.length === 0) return;

  const previous = subagentTextQueues.get(element) || Promise.resolve();
  const next = previous
    .catch(() => {})
    .then(
      () =>
        new Promise<void>((resolve) => {
          let index = 0;
          if (options.newline && element.textContent) {
            element.textContent += "\n";
          }

          const tick = () => {
            element.textContent += chars[index] || "";
            index += 1;
            messagesDiv.scrollTop = messagesDiv.scrollHeight;
            if (index < chars.length) {
              setTimeout(tick, 12);
            } else {
              resolve();
            }
          };

          tick();
        }),
    );

  subagentTextQueues.set(element, next);
}

function appendSubagentProgress(data: ChildPresentation, text: string) {
  const card = ensureSubagentCard(data);
  const progress = card.querySelector<HTMLPreElement>(".subagent-progress");
  if (progress && text) {
    queueSubagentTypedText(progress, text, { newline: true });
  }
  messagesDiv.scrollTop = messagesDiv.scrollHeight;
  return card;
}

function ensureSubagentMessage(progress: HTMLElement, messageId: string) {
  let message = progress.querySelector<HTMLSpanElement>(
    `[data-message-id="${CSS.escape(messageId)}"]`,
  );
  if (!message) {
    message = document.createElement("span");
    message.dataset.messageId = messageId;
    if (progress.childNodes.length > 0) progress.appendChild(document.createTextNode("\n"));
    progress.appendChild(message);
  }
  return message;
}

function appendSubagentStream(data: ChildPresentation, text: string) {
  const card = ensureSubagentCard(data);
  const progress = card.querySelector<HTMLPreElement>(".subagent-progress");
  if (progress && text) {
    progress.dataset.hasLiveStream = "true";
    // Native deltas already stream. Keep each identified message synchronous so
    // an authoritative snapshot cannot be overwritten by queued preview letters.
    if (data.messageId) {
      ensureSubagentMessage(progress, data.messageId).textContent += text;
    } else {
      queueSubagentTypedText(progress, text);
    }
  }
  messagesDiv.scrollTop = messagesDiv.scrollHeight;
  return card;
}

function subagentToolSelector(data: ToolPresentation) {
  return `.subagent-tool-call[data-tool-call-id="${CSS.escape(data.toolCallId || "")}"]`;
}

function ensureSubagentToolCall(data: ChildPresentation & ToolPresentation) {
  const card = ensureSubagentCard(data);
  const tools = card.querySelector(".subagent-tools");
  const id = data.toolCallId || `${data.toolName || "tool"}-${Date.now()}`;
  let tool = tools
    ? tools.querySelector<HTMLDivElement>(subagentToolSelector({ ...data, toolCallId: id }))
    : null;
  if (tool) return tool;

  tool = document.createElement("div");
  tool.className = "subagent-tool-call";
  tool.dataset.toolCallId = id;

  const title = document.createElement("div");
  title.className = "subagent-tool-title";
  title.textContent = data.toolName || "工具调用";

  const args = document.createElement("pre");
  args.className = "subagent-tool-args";
  args.textContent =
    data.args && Object.keys(data.args).length > 0
      ? JSON.stringify(data.args, null, 2)
      : "（无参数）";

  tool.append(title, args);
  if (tools) tools.appendChild(tool);
  messagesDiv.scrollTop = messagesDiv.scrollHeight;
  return tool;
}

function appendSubagentToolResult(data: ChildPresentation & ToolPresentation) {
  const tool = ensureSubagentToolCall(data);
  tool.classList.add("tool-done");
  if (data.isError) tool.classList.add("tool-error");
  const existing = tool.querySelector(".subagent-tool-result");
  if (existing) existing.remove();
  const resultText = extractToolResultText(data.content);
  if (resultText !== null) {
    const result = document.createElement("pre");
    result.className = "subagent-tool-result";
    result.textContent = resultText;
    tool.appendChild(result);
  }
  messagesDiv.scrollTop = messagesDiv.scrollHeight;
  return tool;
}

function streamSelector(data: Presentation) {
  if (data.answerMessageId) {
    return `.stream-active[data-session-id="${CSS.escape(data.sessionId || activeSessionId)}"][data-answer-message-id="${CSS.escape(data.answerMessageId)}"] .content`;
  }
  if (data.sessionId) {
    return `.stream-active[data-session-id="${CSS.escape(data.sessionId)}"] .content`;
  }
  return `.stream-active[data-session-id="${CSS.escape(activeSessionId)}"] .content`;
}

function ensureStream(data: Presentation & { from?: string }) {
  const content = messagesDiv.querySelector(streamSelector(data));
  if (content) return content;

  const sender = !data.from || data.from === "Assistant" ? "助手" : data.from;
  const div = appendMessage(sender, "", "bot stream-active", data);
  return div.querySelector(".content")!;
}

function finalizeStream(data: Presentation) {
  const sessionSelector = `[data-session-id="${CSS.escape(data.sessionId || activeSessionId)}"]`;
  const answerSelector = data.answerMessageId
    ? `[data-answer-message-id="${CSS.escape(data.answerMessageId)}"]`
    : "";
  for (const active of messagesDiv.querySelectorAll(
    `.stream-active${sessionSelector}${answerSelector}`,
  )) {
    active.classList.remove("stream-active");
  }
}

function attachSdkHandlers(sdk: InstanceType<typeof PushChannelSDK>) {
  sdk.onRegistered((data) => {
    connectionStatus.textContent = "已连接";
    runtimeIdInput.replaceChildren();
    for (const runtime of data.runtimes || []) {
      const option = document.createElement("option");
      option.value = runtime.id;
      option.textContent = runtimeLabel(runtime.id);
      runtimeIdInput.appendChild(option);
    }
    setConnected(true);
    startSessionBtn.disabled = runtimeIdInput.options.length === 0;
  });

  sdk.onSessionStarted((data) => {
    const previous = sessions.get(data.sessionId);
    sessions.set(data.sessionId, {
      ...previous,
      runtimeId: data.runtimeId,
      agentId: data.agentId,
      capabilities: data.capabilities,
      controls: previous?.controls || new Map(),
      discardedQueues: previous?.discardedQueues || new Map(),
      ready: true,
    });
    if (!previous) {
      const option = document.createElement("option");
      option.value = data.sessionId;
      option.textContent = `${runtimeLabel(data.runtimeId)} · ${data.sessionId}`;
      sessionSelect.appendChild(option);
      sessionSelect.querySelector('option[value=""]')?.remove();
      sessionSelect.disabled = false;
      selectSession(data.sessionId);
      sessionIdInput.value = createSessionId(agentIdInput.value.trim());
    }
    updateSessionControls();
  });

  sdk.onMessageSent((data) => {
    if (!isCurrentSession(data)) return;
    if (data.queryMessageId && localQueryIds.has(data.queryMessageId)) return;
    appendMessage("用户", data.content || "", "you", data);
  });

  sdk.onMessage((data) => {
    if (!isCurrentSession(data)) return;
    appendMessage("助手", data.content || "", "bot", data);
  });

  sdk.onStreamStart((data) => {
    if (!isCurrentSession(data)) return;
    ensureStream(data);
  });

  sdk.onStreamChunk((data) => {
    if (!isCurrentSession(data)) return;
    const activeStream = ensureStream(data);
    activeStream.textContent += data.content || "";
    messagesDiv.scrollTop = messagesDiv.scrollHeight;
  });

  sdk.onStreamSnapshot((data) => {
    if (!isCurrentSession(data)) return;
    const activeStream = ensureStream(data);
    activeStream.textContent = data.snapshot || "";
    messagesDiv.scrollTop = messagesDiv.scrollHeight;
  });

  sdk.onStreamEnd((data) => {
    if (!isCurrentSession(data)) return;
    finalizeStream(data);
  });

  sdk.onToolCall((data) => {
    if (!isCurrentSession(data)) return;
    const div = appendMessage("工具", data.toolName || "工具调用", "system tool-call", data);
    div.dataset.toolCallId = data.toolCallId || "";
    const args = document.createElement("pre");
    args.className = "tool-args";
    args.textContent =
      data.args && Object.keys(data.args).length > 0
        ? JSON.stringify(data.args, null, 2)
        : "（无参数）";
    div.appendChild(args);
  });

  sdk.onToolResult((data) => {
    if (!isCurrentSession(data)) return;
    const id = data.toolCallId || "";
    const card = messagesDiv.querySelector(
      `.tool-call[data-session-id="${CSS.escape(data.sessionId || activeSessionId)}"][data-tool-call-id="${CSS.escape(id)}"]`,
    );
    if (card) {
      card.classList.add("tool-done");
      if (data.isError) card.classList.add("tool-error");
      const resultText = extractToolResultText(data.content);
      if (resultText !== null) {
        const pre = document.createElement("pre");
        pre.className = "tool-result";
        pre.textContent = resultText;
        card.appendChild(pre);
      }
    }
  });

  sdk.onToolStart((data) => {
    const session = sessions.get(data.sessionId || activeSessionId);
    if (session) session.toolRunning = true;
    updateToolStatus();
    appendSystemMessage("工具正在执行，后续回复可能稍后到达。", data);
  });

  sdk.onToolEnd((data) => {
    const session = sessions.get(data.sessionId || activeSessionId);
    if (session) session.toolRunning = false;
    updateToolStatus();
    appendSystemMessage("工具执行结束。", data);
  });

  sdk.onSubagentStart((data) => {
    if (!isCurrentSession(data)) return;
    const card = ensureSubagentCard(data);
    updateSubagentStatus(card, "运行中", "subagent-running");
  });

  sdk.onSubagentStream((data) => {
    if (!isCurrentSession(data)) return;
    const card = appendSubagentStream(data, String(data.content || data.delta || ""));
    updateSubagentStatus(card, "运行中", "subagent-running");
  });

  sdk.onSubagentMessage((data) => {
    if (!isCurrentSession(data)) return;
    const card = ensureSubagentCard(data);
    const progress = card.querySelector<HTMLPreElement>(".subagent-progress");
    if (progress && data.replace === true && data.messageId) {
      ensureSubagentMessage(progress, data.messageId).textContent = String(data.content || "");
      updateSubagentStatus(card, "运行中", "subagent-running");
      messagesDiv.scrollTop = messagesDiv.scrollHeight;
      return;
    }
    if (progress && progress.dataset.hasLiveStream === "true") {
      updateSubagentStatus(card, "运行中", "subagent-running");
      return;
    }
    const nextCard = appendSubagentProgress(data, String(data.content || ""));
    updateSubagentStatus(nextCard, "运行中", "subagent-running");
  });

  sdk.onSubagentToolCall((data) => {
    if (!isCurrentSession(data)) return;
    const card = ensureSubagentCard(data);
    ensureSubagentToolCall(data);
    updateSubagentStatus(card, "运行中", "subagent-running");
  });

  sdk.onSubagentToolResult((data) => {
    if (!isCurrentSession(data)) return;
    const card = ensureSubagentCard(data);
    appendSubagentToolResult(data);
    updateSubagentStatus(
      card,
      data.isError ? "失败" : "运行中",
      data.isError ? "subagent-error" : "subagent-running",
    );
  });

  sdk.onSubagentResult((data) => {
    if (!isCurrentSession(data)) return;
    const card = ensureSubagentCard(data);
    const result = card.querySelector<HTMLPreElement>(".subagent-result");
    if (result) {
      result.hidden = false;
      result.textContent = String(data.content || "");
    }
    updateSubagentStatus(card, "已完成", "subagent-done");
    messagesDiv.scrollTop = messagesDiv.scrollHeight;
  });

  sdk.onSubagentError((data) => {
    if (!isCurrentSession(data)) return;
    const card = appendSubagentProgress(data, data.message || "子智能体执行失败");
    updateSubagentStatus(card, "失败", "subagent-error");
  });

  sdk.onSubagentEnd((data) => {
    if (!isCurrentSession(data)) return;
    const card = ensureSubagentCard(data);
    if (data.status === "cancelled") {
      updateSubagentStatus(card, "已停止");
      return;
    }
    const failed = data.status === "error" || data.status === "failed";
    updateSubagentStatus(
      card,
      failed ? "失败" : "已完成",
      failed ? "subagent-error" : "subagent-done",
    );
  });

  sdk.onTimeoutDeferred((data) => {
    setRunState(data, false);
    appendSystemMessage(data.message || "工具仍在执行，正在等待后续通知。", data);
  });

  sdk.onControlReceipt((data) => {
    if (data.version !== 1) updateControlReceipt(data);
  });

  sdk.onInputQueue((data) => {
    if (data.version === 1) return;
    const session = data.sessionId ? sessions.get(data.sessionId) : undefined;
    if (!session || (session.runId && data.runId !== session.runId)) return;
    session.inputQueue = { steering: data.steering, followUp: data.followUp };
    const discarded = [...(data.discardedSteering || []), ...(data.discardedFollowUp || [])];
    if (discarded.length > 0) {
      // Native queue snapshots have text, not control IDs; do not infer which receipt was consumed.
      let node = session.discardedQueues.get(data.runId);
      if (!node) {
        node = appendMessage("未处理的队列内容", "", "system", data);
        session.discardedQueues.set(data.runId, node);
      }
      node.querySelector(".content")!.textContent =
        `${data.reason === "cancelled" ? "执行已停止" : "执行已结束"}，以下内容未处理：${discarded.join("；")}`;
    }
    updateSessionControls();
  });

  sdk.onError((data) => {
    const session = data.sessionId ? sessions.get(data.sessionId) : undefined;
    if (session && (!data.runId || !session.runId || data.runId === session.runId)) {
      // A failed native process cannot confirm whether its last queue snapshot still exists.
      session.inputQueue = undefined;
    }
    setRunState(data, false);
    appendSystemMessage(data.message || "连接发生错误，请检查网关地址和令牌后重连。", data);
  });

  sdk.onRunCancelled((data) => {
    if (data.version === 1) return;
    setRunState(data, false);
    finalizeStream(data);
    appendSystemMessage("本次执行已停止。", data);
  });

  sdk.onRunStarted((data) => {
    if (data.version !== 1 && data.origin !== "push") setRunState(data, true);
  });

  sdk.onRunCompleted((data) => {
    if (data.version !== 1 && data.origin !== "push") setRunState(data, false);
  });

  sdk.onClose(() => {
    for (const [sessionId, session] of sessions) {
      session.ready = false;
      session.inputQueue = undefined;
      markUnconfirmedControls(sessionId, session);
    }
    setConnected(false);
    connectionStatus.textContent = "未连接";
  });
}

connectForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const url = serverUrlInput.value.trim();
  const agentId = agentIdInput.value.trim();
  const token = gatewayTokenInput.value;
  if (!sessionIdInput.value.trim()) {
    sessionIdInput.value = createSessionId(agentId);
  }

  connectionStatus.textContent = "正在连接…";
  if (!sdk || sdk.url !== url || sdk.agentId !== agentId || sdk.token !== token) {
    if (sdk) sdk.disconnect();
    sdk = new PushChannelSDK(url, agentId, { token });
    attachSdkHandlers(sdk);
  }
  sdk.connect();
});

sessionForm.addEventListener("submit", (event) => {
  event.preventDefault();
  if (!sdk || !connected) return;
  try {
    sdk.startSession(sessionIdInput.value.trim(), {
      runtimeId: runtimeIdInput.value,
      agentId: targetAgentInput.value.trim(),
    });
  } catch (error) {
    appendSystemMessage(error instanceof Error ? error.message : String(error));
  }
});

sessionSelect.addEventListener("change", () => selectSession(sessionSelect.value));

messageForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const text = messageInput.value.trim();
  const sessionId = activeSessionId;
  const session = sessions.get(sessionId);
  if (!text || !sdk || !session?.ready || !connected) return;

  const queryMessageId = createSessionId("query");
  const answerMessageId = createSessionId("answer");
  localQueryIds.add(queryMessageId);
  appendMessage("我", text, "you", { queryMessageId, sessionId });
  const mentions = session.capabilities.mentions ? extractMentions(text) : [];
  sdk.sendMessage(text, sessionId, {
    queryMessageId,
    answerMessageId,
    ...(mentions.length > 0 ? { mentions } : {}),
  });
  // A fresh input has no active target until run_started; queued normal inputs keep the live run.
  if (!session.running) {
    session.runId = undefined;
    session.inputQueue = undefined;
  }
  setRunState({ sessionId }, true);
  messageInput.value = "";
});

cancelBtn.addEventListener("click", () => {
  if (sdk && !cancelBtn.disabled) {
    sdk.cancel(activeSessionId);
    cancelBtn.disabled = true;
  }
});

for (const [button, method, operation] of [
  [steerBtn, "steer", "steer"],
  [followUpBtn, "followUp", "follow_up"],
] as const) {
  button.addEventListener("click", () => {
    const content = messageInput.value.trim();
    if (!sdk || button.disabled || !content) return;
    const sessionId = activeSessionId;
    const runId = sessions.get(sessionId)?.runId;
    const controlId = sdk[method](content, { sessionId, runId });
    if (!controlId) return;
    updateControlReceipt({
      sessionId,
      runId,
      controlId,
      operation,
      content,
      status: "pending",
    });
    messageInput.value = "";
    messageInput.focus();
  });
}

disconnectBtn.addEventListener("click", () => {
  if (sdk) {
    sdk.disconnect();
  }
});

sessionIdInput.value = createSessionId(agentIdInput.value.trim());
if (window.location.protocol === "http:" || window.location.protocol === "https:") {
  serverUrlInput.value = window.location.origin;
}
agentIdInput.addEventListener("change", () => {
  sessionIdInput.value = createSessionId(agentIdInput.value.trim());
});
