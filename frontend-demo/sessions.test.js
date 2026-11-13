import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import test from "node:test";

const requireBackend = createRequire(new URL("../admin-backend/package.json", import.meta.url));
const { JSDOM } = requireBackend("jsdom");
const { createOpenClawDecoder } = requireBackend("./src/adapters/openclaw");
const { createClientProjector } = requireBackend("./src/gateway/client-events");

function createDemo(t, { token } = {}) {
  const html = readFileSync(new URL("./index.html", import.meta.url), "utf8");
  const dom = new JSDOM(html, { url: "http://localhost/", runScripts: "outside-only" });
  t.after(() => dom.window.close());
  const { window } = dom;
  window.CSS = { escape: (value) => String(value).replaceAll('"', '\\"') };
  const sockets = [];
  class Socket extends window.EventTarget {
    static OPEN = 1;
    readyState = 1;
    sent = [];
    constructor() {
      super();
      sockets.push(this);
    }
    send(value) {
      this.sent.push(JSON.parse(value));
    }
    close() {
      this.readyState = 3;
      this.dispatchEvent(new window.Event("close"));
    }
    emit(data) {
      this.dispatchEvent(new window.MessageEvent("message", { data: JSON.stringify(data) }));
    }
  }
  window.WebSocket = Socket;
  for (const file of ["sdk.js", "mentions.js"]) {
    const url = new URL(`./${file}`, import.meta.url);
    window.eval(`${readFileSync(url, "utf8")}\n//# sourceURL=${url.href}`);
  }
  window.eval(window.document.querySelector("script:not([src])").textContent);
  const get = (id) => window.document.getElementById(id);
  const submit = (id) => get(id).dispatchEvent(new window.Event("submit", { cancelable: true }));
  if (token) {
    get("gateway-token-input").value = token;
  }
  submit("connect-form");
  const socket = sockets[0];
  socket.dispatchEvent(new window.Event("open"));
  socket.emit({
    type: "registered",
    agentId: "demo-agent",
    runtimes: [
      { id: "openclaw", capabilities: { mentions: true, cancel: false } },
      { id: "pi", capabilities: { mentions: false, cancel: true } },
      { id: "deepseek", capabilities: { mentions: false, cancel: true } },
    ],
  });
  function start(id, runtimeId, capabilities = {}) {
    get("session-id-input").value = id;
    get("runtime-id-input").value = runtimeId;
    submit("session-form");
    socket.emit({
      type: "session.started",
      sessionId: id,
      runtimeId,
      agentId: "main",
      capabilities: {
        mentions: runtimeId === "openclaw",
        cancel: runtimeId !== "openclaw",
        ...capabilities,
      },
    });
  }
  return { get, submit, socket, sockets, start, window };
}

void test("demo presents Chinese controls while preserving runtime and session protocol values", (t) => {
  const { get, socket, start, window } = createDemo(t);
  assert.equal(window.document.documentElement.lang, "zh-CN");
  assert.equal(window.document.title, "Push Channel · 多底座演示");
  for (const [id, text] of [
    ["connect-btn", "连接"],
    ["disconnect-btn", "断开连接"],
    ["start-session-btn", "开始新会话"],
    ["send-btn", "发送"],
    ["cancel-btn", "停止"],
    ["connection-status", "已连接"],
  ]) {
    assert.equal(get(id).textContent, text);
  }
  assert.equal(get("message-input").placeholder, "输入消息…");
  assert.equal(get("runtime-id-input").querySelector('option[value="pi"]').textContent, "Pi");
  start("中文会话", "pi");
  assert.deepEqual(socket.sent.at(-1), {
    type: "session.start",
    sessionId: "中文会话",
    runtimeId: "pi",
    agentId: "main",
  });
  assert.match(get("capability-status").textContent, /不支持 @提及.*支持停止/);
  get("disconnect-btn").click();
  assert.equal(get("connection-status").textContent, "未连接");
});

void test("demo translates message roles, tool hints and child status without translating agent output", (t) => {
  const { get, socket, start } = createDemo(t);
  start("chat", "pi");
  socket.emit({ type: "stream_start", sessionId: "chat", from: "Assistant", answerMessageId: "a" });
  socket.emit({
    type: "stream",
    sessionId: "chat",
    answerMessageId: "a",
    content: "Original model output",
  });
  assert.equal(get("messages").querySelector("strong").textContent, "助手");
  assert.equal(get("messages").querySelector(".content").textContent, "Original model output");
  socket.emit({
    type: "tool_call",
    sessionId: "chat",
    toolCallId: "t",
    toolName: "read",
    args: {},
  });
  assert.equal(get("messages").querySelector(".tool-call strong").textContent, "工具");
  assert.equal(get("messages").querySelector(".tool-args").textContent, "（无参数）");
  socket.emit({ type: "subagent_start", sessionId: "chat", agentId: "worker" });
  assert.equal(get("messages").querySelector(".subagent-status").textContent, "运行中");
  socket.emit({
    type: "subagent_result",
    sessionId: "chat",
    agentId: "worker",
    content: "Original child output",
  });
  assert.equal(get("messages").querySelector(".subagent-status").textContent, "已完成");
  assert.equal(
    get("messages").querySelector(".subagent-result").textContent,
    "Original child output",
  );
  socket.emit({ type: "subagent_error", sessionId: "chat", agentId: "worker" });
  assert.equal(get("messages").querySelector(".subagent-status").textContent, "失败");
});

void test("disabled connection fields and buttons keep readable text contrast", (t) => {
  const { get, window } = createDemo(t);
  const style = window.document.createElement("style");
  style.textContent = readFileSync(new URL("./style.css", import.meta.url), "utf8");
  window.document.head.appendChild(style);
  const luminance = (color) => {
    const channels = color
      .match(/\d+/g)
      .slice(0, 3)
      .map((channel) => {
        const value = Number(channel) / 255;
        return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
      });
    return channels[0] * 0.2126 + channels[1] * 0.7152 + channels[2] * 0.0722;
  };
  for (const id of ["server-url-input", "connect-btn", "disconnect-btn", "cancel-btn"]) {
    get(id).disabled = true;
    const computed = window.getComputedStyle(get(id));
    const levels = [luminance(computed.color), luminance(computed.backgroundColor)].toSorted(
      (a, b) => a - b,
    );
    assert.ok((levels[1] + 0.05) / (levels[0] + 0.05) >= 4.5, id);
  }
});

void test("demo chooses runtime when starting sessions and retains background events across switches", (t) => {
  const { get, socket, sockets, start, window } = createDemo(t);
  assert.equal(socket.sent[0].runtimeId, undefined);
  assert.equal(get("message-input").disabled, true);
  start("oc-chat", "openclaw");
  start("pi-chat", "pi");
  assert.equal(sockets.length, 1);
  socket.emit({
    type: "stream",
    sessionId: "oc-chat",
    answerMessageId: "reply",
    content: "OpenClaw background reply",
  });
  socket.emit({
    type: "stream",
    sessionId: "pi-chat",
    answerMessageId: "reply",
    content: "Pi foreground reply",
  });
  const visible = () =>
    [...get("messages").querySelectorAll(".message")]
      .filter((element) => !element.hidden)
      .map((element) => element.textContent)
      .join(" ");
  assert.match(visible(), /Pi foreground reply/);
  assert.doesNotMatch(visible(), /OpenClaw background reply/);
  get("session-select").value = "oc-chat";
  get("session-select").dispatchEvent(new window.Event("change"));
  assert.match(visible(), /OpenClaw background reply/);
  assert.doesNotMatch(visible(), /Pi foreground reply/);
  assert.equal(get("cancel-btn").disabled, true);
  assert.match(get("capability-status").textContent, /@提及/);
});

void test("demo preserves tool and child cards and sends capability-aware cancellation and mentions", (t) => {
  const { get, socket, start, submit } = createDemo(t);
  start("pi-chat", "pi");
  get("message-input").value = "hello @worker";
  submit("message-form");
  assert.equal(socket.sent.at(-1).mentions, undefined);
  assert.equal(get("cancel-btn").disabled, false);
  get("cancel-btn").click();
  assert.deepEqual(socket.sent.at(-1), { type: "cancel", sessionId: "pi-chat" });
  socket.emit({
    type: "tool_call",
    sessionId: "pi-chat",
    toolCallId: "t",
    toolName: "read",
    args: { path: "<script>" },
  });
  socket.emit({
    type: "tool_result",
    sessionId: "pi-chat",
    toolCallId: "t",
    content: "<img src=x onerror=alert(1)>",
  });
  socket.emit({
    type: "subagent_start",
    sessionId: "pi-chat",
    childSessionKey: "child",
    label: "worker",
  });
  socket.emit({
    type: "subagent_result",
    sessionId: "pi-chat",
    childSessionKey: "child",
    content: "child result",
  });
  assert.match(get("messages").textContent, /child result/);
  assert.equal(get("messages").querySelectorAll("img,script").length, 0);
  assert.equal(get("messages").querySelector(".tool-call").classList.contains("tool-done"), true);
  socket.emit({ type: "run_cancelled", sessionId: "pi-chat" });
  assert.equal(get("cancel-btn").disabled, true);
  assert.match(get("messages").textContent, /已停止/);
});

void test("demo uses same-origin gateway and keeps stop active across tool stream parts but not completed runs", (t) => {
  const { get, socket, start, submit } = createDemo(t);
  assert.equal(get("server-url-input").value, "http://localhost");
  start("chat", "pi");
  get("message-input").value = "work";
  submit("message-form");
  socket.emit({ type: "stream_end", sessionId: "chat" });
  assert.equal(get("cancel-btn").disabled, false);
  socket.emit({ type: "run_completed", sessionId: "chat", origin: "push" });
  assert.equal(get("cancel-btn").disabled, false);
  socket.emit({ type: "run_completed", sessionId: "chat", origin: "prompt" });
  assert.equal(get("cancel-btn").disabled, true);
});

void test("externally triggered tasks enable Pi Stop without treating notifications as active runs", (t) => {
  const { get, socket, start } = createDemo(t);
  start("chat", "pi");
  socket.emit({ type: "run_started", sessionId: "chat", origin: "push" });
  assert.equal(get("cancel-btn").disabled, true);
  socket.emit({ type: "run_started", sessionId: "chat", origin: "task" });
  assert.equal(get("cancel-btn").disabled, false);
  socket.emit({ type: "run_completed", sessionId: "chat", origin: "push" });
  assert.equal(get("cancel-btn").disabled, false);
  get("cancel-btn").click();
  assert.deepEqual(socket.sent.at(-1), { type: "cancel", sessionId: "chat" });
  socket.emit({ type: "run_cancelled", sessionId: "chat", origin: "task" });
  assert.equal(get("cancel-btn").disabled, true);
});

void test("demo reconnect reuses its cursor and existing history without duplicate replay text", (t) => {
  const { get, socket, sockets, start, submit, window } = createDemo(t);
  start("chat", "pi");
  const chunk = {
    type: "stream",
    sessionId: "chat",
    answerMessageId: "reply",
    content: "once",
    cursor: 7,
    eventId: "e",
    deliveryId: "e:0",
  };
  socket.emit(chunk);
  get("disconnect-btn").click();
  submit("connect-form");
  const resumed = sockets[1];
  resumed.dispatchEvent(new window.Event("open"));
  resumed.emit({
    type: "registered",
    agentId: "demo-agent",
    runtimes: [{ id: "pi", capabilities: { cancel: true } }],
  });
  assert.equal(resumed.sent.at(-1).afterCursor, 7);
  resumed.emit({
    type: "session.started",
    sessionId: "chat",
    runtimeId: "pi",
    agentId: "main",
    capabilities: { cancel: true },
  });
  resumed.emit(chunk);
  assert.equal(get("messages").querySelector(".content").textContent, "once");
  assert.equal(get("message-input").disabled, false);
});

void test("demo assigns unique outbound request identities even within one millisecond", (t) => {
  const { get, socket, start, submit, window } = createDemo(t);
  start("chat", "pi");
  window.Date.now = () => 123;
  for (const text of ["first", "second"]) {
    get("message-input").value = text;
    submit("message-form");
  }
  const messages = socket.sent.filter((message) => message.type === "message");
  assert.notEqual(messages[0].queryMessageId, messages[1].queryMessageId);
  assert.notEqual(messages[0].answerMessageId, messages[1].answerMessageId);
});

void test("demo keeps optional tokens in the password field and reuses or replaces the SDK as credentials change", (t) => {
  const { get, socket, sockets, submit, window } = createDemo(t, { token: "test-credential" });
  assert.equal(get("gateway-token-input").type, "password");
  assert.equal(get("gateway-token-input").required, false);
  assert.equal(socket.sent[0].token, "test-credential");
  assert.equal(window.localStorage.length, 0);
  get("disconnect-btn").click();
  get("gateway-token-input").value = "replacement";
  submit("connect-form");
  sockets[1].dispatchEvent(new window.Event("open"));
  assert.equal(sockets[1].sent[0].token, "replacement");
});

void test("Pi child capability is independent of mentions and follows the selected session", (t) => {
  const { get, start, submit, socket, window } = createDemo(t);
  start("pi-with-children", "pi", { subagents: true });
  assert.match(get("capability-status").textContent, /(?:^| · )支持子智能体(?: · |$)/);
  assert.match(get("capability-status").textContent, /不支持 @提及/);
  get("message-input").value = "delegate @reviewer";
  submit("message-form");
  assert.equal(socket.sent.at(-1).mentions, undefined);
  start("pi-without-children", "pi", { subagents: false });
  assert.match(get("capability-status").textContent, /(?:^| · )不支持子智能体(?: · |$)/);
  get("session-select").value = "pi-with-children";
  get("session-select").dispatchEvent(new window.Event("change"));
  assert.match(get("capability-status").textContent, /(?:^| · )支持子智能体(?: · |$)/);
});

void test("Pi child streams and tools retain independent identities in background parent sessions", async (t) => {
  const { get, socket, start, window } = createDemo(t);
  start("pi-parent", "pi", { subagents: true });
  start("pi-other-parent", "pi", { subagents: true });
  for (const [sessionId, childSessionKey, content] of [
    ["pi-parent", "child-a", "A"],
    ["pi-parent", "child-b", "B"],
    ["pi-other-parent", "child-a", "C"],
  ]) {
    const identity = { sessionId, childSessionKey, agentId: "main", label: "reader" };
    socket.emit({ type: "subagent_start", ...identity });
    socket.emit({ type: "subagent_stream", ...identity, content });
    socket.emit({
      type: "subagent_tool_call",
      ...identity,
      toolCallId: "read-1",
      toolName: "read",
      args: { path: `${content}.txt` },
    });
    socket.emit({
      type: "subagent_tool_result",
      ...identity,
      toolCallId: "read-1",
      content: [{ type: "text", text: `file-${content}` }],
    });
    socket.emit({ type: "subagent_result", ...identity, content: `result-${content}` });
    socket.emit({ type: "subagent_end", ...identity, status: "ok" });
  }
  await new Promise((resolve) => setImmediate(resolve));
  const cards = [...get("messages").querySelectorAll(".subagent-card")];
  assert.equal(cards.length, 3);
  for (const [index, content] of ["A", "B", "C"].entries()) {
    const card = cards[index];
    assert.equal(card.querySelector("strong").textContent, "@reader");
    assert.equal(card.querySelector(".subagent-progress").textContent, content);
    assert.match(
      card.querySelector(".subagent-tool-args").textContent,
      new RegExp(`${content}\\.txt`),
    );
    assert.equal(card.querySelector(".subagent-tool-result").textContent, `file-${content}`);
    assert.equal(card.querySelector(".subagent-result").textContent, `result-${content}`);
    assert.equal(card.querySelector(".subagent-status").textContent, "已完成");
    assert.equal(card.hidden, index !== 2);
  }
  get("session-select").value = "pi-parent";
  get("session-select").dispatchEvent(new window.Event("change"));
  assert.deepEqual(
    cards.map((card) => card.hidden),
    [false, false, true],
  );
});

void test("Pi child failures and cancellation are not displayed as completed", (t) => {
  const { get, socket, start } = createDemo(t);
  start("pi-parent", "pi", { subagents: true });
  for (const status of ["error", "cancelled"]) {
    const identity = {
      sessionId: "pi-parent",
      childSessionKey: `child-${status}`,
      agentId: "main",
    };
    socket.emit({ type: "subagent_start", ...identity });
    if (status === "error") {
      socket.emit({
        type: "subagent_tool_call",
        ...identity,
        toolCallId: "read-1",
        toolName: "read",
        args: { path: "missing.txt" },
      });
      socket.emit({
        type: "subagent_tool_result",
        ...identity,
        toolCallId: "read-1",
        content: "File not found",
        isError: true,
      });
      socket.emit({ type: "subagent_error", ...identity, message: "Child execution failed" });
    }
    socket.emit({ type: "subagent_end", ...identity, status });
    const card = get("messages").querySelector(`[data-subagent-id="child-${status}"]`);
    assert.equal(
      card.querySelector(".subagent-status").textContent,
      status === "error" ? "失败" : "已停止",
    );
    assert.equal(card.classList.contains("subagent-done"), false);
    assert.equal(card.classList.contains("subagent-running"), false);
    if (status === "error") {
      assert.equal(
        card.querySelector(".subagent-tool-call").classList.contains("tool-error"),
        true,
      );
      assert.equal(card.querySelector(".subagent-tool-result").textContent, "File not found");
    }
  }
});

void test("dsh child replacement snapshots clear failed preview and prevent delayed characters returning", async (t) => {
  const { get, socket, start } = createDemo(t);
  start("dsh-parent", "deepseek", { subagents: true });
  const project = createClientProjector();
  const identity = { agentId: "child", childSessionId: "native-child", messageId: "child-message" };
  const emit = (type, data = {}) => {
    for (const payload of project({
      type,
      data: { ...identity, ...data },
      conversationId: "dsh-parent",
      runtimeId: "deepseek",
      runId: "parent-run",
    })) {
      socket.emit(payload);
    }
  };
  emit("subagent.started");
  emit("subagent.delta", { delta: "FAILED preview must disappear" });
  await new Promise((resolve) => {
    setTimeout(resolve, 20);
  });
  emit("subagent.message", { content: "", replace: true });
  const progress = get("messages").querySelector(".subagent-progress");
  assert.equal(progress.textContent, "");
  emit("subagent.delta", { delta: "Retry preview must be corrected" });
  emit("subagent.message", { content: "最终修订🌏", replace: true });
  await new Promise((resolve) => {
    setTimeout(resolve, 450);
  });
  assert.equal(progress.textContent, "最终修订🌏");
});

void test("dsh child authoritative snapshots replace only their message across multiple child messages", async (t) => {
  const { get, socket, start } = createDemo(t);
  start("dsh-parent", "deepseek", { subagents: true });
  const project = createClientProjector();
  const emit = (type, messageId, data = {}) => {
    for (const payload of project({
      type,
      data: { agentId: "child", childSessionId: "native-child", messageId, ...data },
      conversationId: "dsh-parent",
      runtimeId: "deepseek",
      runId: "parent-run",
    })) {
      socket.emit(payload);
    }
  };
  emit("subagent.started", "first");
  emit("subagent.delta", "first", { delta: "first draft" });
  emit("subagent.message", "first", { content: "第一条🌏", replace: true });
  emit("subagent.delta", "second", { delta: "second draft" });
  emit("subagent.message", "second", { content: "第二条✅", replace: true });
  await new Promise((resolve) => {
    setTimeout(resolve, 350);
  });
  const progress = get("messages").querySelector(".subagent-progress");
  assert.equal(progress.textContent, "第一条🌏\n第二条✅");
});

void test("Pi input controls require advertised capabilities and a current run, keeping normal send intact", (t) => {
  const { get, socket, start, submit } = createDemo(t);
  start("pi-controls", "pi", { steering: true, followUp: true });
  assert.equal(get("steer-btn")?.textContent.trim(), "即时引导");
  assert.equal(get("follow-up-btn")?.textContent.trim(), "后续跟进");
  assert.equal(get("steer-btn").disabled, true);
  get("message-input").value = "original";
  submit("message-form");
  const original = socket.sent.at(-1);
  assert.equal(original.type, "message");
  socket.emit({ type: "run_started", sessionId: "pi-controls", runId: "run-1", origin: "prompt" });
  assert.equal(get("message-input").disabled, false);
  assert.equal(get("steer-btn").disabled, false);
  assert.equal(get("follow-up-btn").disabled, false);
  for (const [button, operation] of [
    ["steer-btn", "steer"],
    ["follow-up-btn", "follow_up"],
  ]) {
    get("message-input").value = `<img src=x onerror=alert(1)> ${operation}`;
    get(button).click();
    const control = socket.sent.at(-1);
    assert.equal(control.type, "input.control");
    assert.equal(control.operation, operation);
    assert.equal(control.sessionId, "pi-controls");
    assert.equal(control.runId, "run-1");
    assert.ok(control.controlId);
    assert.notEqual(control.controlId, original.queryMessageId);
    assert.equal(control.queryMessageId, undefined);
    assert.equal(control.answerMessageId, undefined);
    assert.equal(get("message-input").value, "");
  }
  assert.equal(socket.sent.filter((frame) => frame.type === "message").length, 1);
  assert.equal(get("messages").querySelectorAll("img,script").length, 0);
  assert.equal(get("messages").querySelectorAll(".input-control").length, 2);
  socket.emit({
    type: "run_completed",
    sessionId: "pi-controls",
    runId: "run-1",
    origin: "prompt",
  });
  assert.equal(get("steer-btn").disabled, true);
  assert.equal(get("follow-up-btn").disabled, true);
  assert.equal(get("send-btn").disabled, false);
  start("unsupported", "openclaw");
  socket.emit({ type: "run_started", sessionId: "unsupported", runId: "oc", origin: "prompt" });
  assert.equal(get("steer-btn").disabled, true);
  assert.equal(get("follow-up-btn").disabled, true);
});

void test("input receipts update one safe card and queue snapshots remain authoritative across session switches", (t) => {
  const { get, socket, start, window } = createDemo(t);
  start("chat", "pi", { steering: true, followUp: true });
  socket.emit({ type: "run_started", sessionId: "chat", runId: "run", origin: "prompt" });
  get("message-input").value = "nudge";
  get("steer-btn").click();
  const { controlId } = socket.sent.at(-1);
  const receipt = {
    type: "input_control",
    sessionId: "chat",
    runId: "run",
    controlId,
    operation: "steer",
    content: "nudge",
    status: "queued",
  };
  socket.emit(receipt);
  socket.emit(receipt);
  assert.equal(get("messages").querySelectorAll(".input-control").length, 1);
  assert.match(get("messages").querySelector(".input-control").textContent, /已接收.*等待处理/);
  assert.doesNotMatch(
    get("messages").querySelector(".input-control").textContent,
    /已执行|已完成|已消费/,
  );
  socket.emit({
    type: "input_queue",
    sessionId: "chat",
    runId: "run",
    steering: ["<img src=x>"],
    followUp: ["later"],
  });
  assert.match(get("input-queue-status").textContent, /即时引导.*1.*后续跟进.*1/);
  assert.match(get("input-queue-status").textContent, /<img src=x>/);
  assert.equal(get("input-queue-status").querySelectorAll("img").length, 0);
  start("other", "pi", { steering: true, followUp: false });
  assert.doesNotMatch(get("input-queue-status").textContent, /later/);
  socket.emit({ ...receipt, status: "handled" });
  socket.emit({
    type: "input_queue",
    sessionId: "chat",
    runId: "run",
    steering: [],
    followUp: ["later"],
  });
  get("session-select").value = "chat";
  get("session-select").dispatchEvent(new window.Event("change"));
  assert.match(get("input-queue-status").textContent, /即时引导.*0.*后续跟进.*1/);
  assert.match(get("messages").querySelector(".input-control").textContent, /已交给底座/);
  assert.equal(get("messages").querySelectorAll(".input-control").length, 1);
});

void test("a fresh normal send waits for its run ID but queueing normal input preserves a truly active target", (t) => {
  const { get, socket, start, submit } = createDemo(t);
  start("chat", "pi", { steering: true, followUp: true });
  socket.emit({ type: "run_started", sessionId: "chat", runId: "old", origin: "prompt" });
  socket.emit({ type: "run_completed", sessionId: "chat", runId: "old", origin: "prompt" });
  get("message-input").value = "new work";
  submit("message-form");
  assert.equal(get("steer-btn").disabled, true);
  assert.equal(get("follow-up-btn").disabled, true);
  socket.emit({ type: "run_started", sessionId: "chat", runId: "new", origin: "prompt" });
  get("message-input").value = "ordinary queued input";
  submit("message-form");
  assert.equal(get("steer-btn").disabled, false);
  get("message-input").value = "native hint";
  get("steer-btn").click();
  assert.equal(socket.sent.at(-1).runId, "new");
});

void test("late control rejections and old run terminal events cannot stop the current parent run", (t) => {
  const { get, socket, start } = createDemo(t);
  start("chat", "pi", { steering: true, followUp: true });
  socket.emit({ type: "run_started", sessionId: "chat", runId: "old", origin: "prompt" });
  get("message-input").value = "hint";
  get("steer-btn").click();
  const { controlId } = socket.sent.at(-1);
  socket.emit({ type: "run_completed", sessionId: "chat", runId: "old", origin: "prompt" });
  socket.emit({ type: "run_started", sessionId: "chat", runId: "new", origin: "prompt" });
  socket.emit({
    type: "input_control",
    sessionId: "chat",
    runId: "old",
    controlId,
    status: "rejected",
    message: "stale run",
  });
  socket.emit({ type: "run_completed", sessionId: "chat", runId: "old", origin: "prompt" });
  assert.equal(get("cancel-btn").disabled, false);
  assert.equal(get("steer-btn").disabled, false);
  assert.match(get("messages").querySelector(".input-control").textContent, /已拒绝.*stale run/);
  assert.match(get("messages").querySelector(".input-control").textContent, /hint/);
  socket.emit({
    type: "input_queue",
    sessionId: "chat",
    runId: "new",
    steering: ["current"],
    followUp: [],
  });
  socket.emit({
    type: "input_queue",
    sessionId: "chat",
    runId: "old",
    steering: ["stale"],
    followUp: [],
  });
  assert.match(get("input-queue-status").textContent, /current/);
  assert.doesNotMatch(get("input-queue-status").textContent, /stale/);
});

void test("disconnect makes unconfirmed controls unknown without retry, while cleanup displays discarded leftovers", (t) => {
  const { get, socket, sockets, start, submit, window } = createDemo(t);
  start("chat", "pi", { steering: true, followUp: true });
  socket.emit({ type: "run_started", sessionId: "chat", runId: "run", origin: "prompt" });
  get("message-input").value = "pending hint";
  get("follow-up-btn").click();
  const { controlId } = socket.sent.at(-1);
  get("disconnect-btn").click();
  assert.match(get("messages").querySelector(".input-control").textContent, /状态未知/);
  assert.equal(get("steer-btn").disabled, true);
  submit("connect-form");
  const resumed = sockets[1];
  resumed.dispatchEvent(new window.Event("open"));
  resumed.emit({ type: "registered", runtimes: [] });
  resumed.emit({
    type: "session.started",
    sessionId: "chat",
    runtimeId: "pi",
    agentId: "main",
    capabilities: { steering: true, followUp: true },
  });
  assert.equal(
    resumed.sent.some((frame) => frame.type === "input.control"),
    false,
  );
  resumed.emit({
    type: "input_control",
    sessionId: "chat",
    runId: "run",
    controlId,
    operation: "follow_up",
    content: "pending hint",
    status: "queued",
  });
  resumed.emit({
    type: "input_queue",
    sessionId: "chat",
    runId: "run",
    steering: [],
    followUp: [],
    discardedSteering: [],
    discardedFollowUp: ["pending hint"],
    reason: "cancelled",
  });
  resumed.emit({ type: "run_cancelled", sessionId: "chat", runId: "run" });
  assert.match(get("input-queue-status").textContent, /即时引导.*0.*后续跟进.*0/);
  assert.match(get("messages").textContent, /未处理.*pending hint/);
  assert.equal(get("follow-up-btn").disabled, true);
});

void test("replayed dispatching controls become unknown when the owning run fails without an acknowledgement", (t) => {
  const { get, socket, start } = createDemo(t);
  start("recovered", "pi", { steering: true, followUp: true });
  socket.emit({
    type: "run_started",
    sessionId: "recovered",
    runId: "interrupted",
    origin: "prompt",
  });
  for (const [controlId, status] of [
    ["pending", "dispatching"],
    ["accepted", "queued"],
  ]) {
    socket.emit({
      type: "input_control",
      sessionId: "recovered",
      runId: "interrupted",
      controlId,
      operation: "steer",
      content: controlId,
      status,
    });
  }
  socket.emit({
    type: "input_queue",
    sessionId: "recovered",
    runId: "interrupted",
    steering: ["before-crash"],
    followUp: [],
  });
  socket.emit({
    type: "error",
    sessionId: "recovered",
    runId: "interrupted",
    message: "Gateway restarted; execution result unknown",
  });
  assert.match(
    get("messages").querySelector('[data-control-id="pending"]').textContent,
    /状态未知/,
  );
  assert.match(
    get("messages").querySelector('[data-control-id="accepted"]').textContent,
    /已接收.*等待处理/,
  );
  assert.equal(get("steer-btn").disabled, true);
  assert.match(get("input-queue-status").textContent, /状态未知/);
  assert.doesNotMatch(get("input-queue-status").textContent, /before-crash/);
});

for (const { name, events, expected, toolName } of [
  {
    name: "same-length snapshot correction",
    events: [
      { type: "message_start" },
      { type: "content_snapshot", text: "wrong" },
      { type: "content_snapshot", text: "right" },
      { type: "done" },
    ],
    expected: ["right"],
  },
  {
    name: "shorter snapshot correction",
    events: [
      { type: "message_start" },
      { type: "content_snapshot", text: "long draft answer" },
      { type: "content_snapshot", text: "ok" },
      { type: "done" },
    ],
    expected: ["ok"],
  },
  {
    name: "independent messages before and after a tool",
    events: [
      { type: "message_start" },
      { type: "content_snapshot", text: "before tool" },
      { type: "tool_call", toolCallId: "read-1", toolName: "read", args: { path: "marker.txt" } },
      { type: "tool_result", toolCallId: "read-1", content: "marker", isError: false },
      { type: "message_start" },
      { type: "content_snapshot", text: "after tool" },
      { type: "done" },
    ],
    expected: ["before tool", "after tool"],
    toolName: "read",
  },
  {
    name: "late authoritative snapshot corrects the same message across a tool",
    events: [
      { type: "message_start" },
      { type: "content", delta: "draft answer" },
      {
        type: "tool_call",
        toolCallId: "read-1",
        toolName: "read",
        args: { path: "marker.txt" },
      },
      { type: "tool_result", toolCallId: "read-1", content: "marker", isError: false },
      { type: "content_snapshot", text: "corrected answer" },
      { type: "done" },
    ],
    expected: ["corrected answer"],
    toolName: "read",
  },
  {
    name: "authoritative final snapshot replaces incremental draft",
    events: [
      { type: "message_start" },
      { type: "content", delta: "draft" },
      { type: "content_snapshot", text: "draft with stale ending" },
      { type: "content", delta: "!" },
      { type: "content_snapshot", text: "final answer" },
      { type: "done" },
    ],
    expected: ["final answer"],
  },
  {
    name: "equal suffix deltas are legitimate repeated text",
    events: [
      { type: "message_start" },
      { type: "content", delta: "ha" },
      { type: "content", delta: "ha" },
      { type: "done" },
    ],
    expected: ["haha"],
  },
  {
    name: "distinct native messages retain identical content",
    events: [
      { type: "message_start" },
      { type: "content_snapshot", text: "same answer" },
      { type: "message_start" },
      { type: "content_snapshot", text: "same answer" },
      { type: "done" },
    ],
    expected: ["same answer", "same answer"],
  },
]) {
  void test(`OpenClaw decoder through SDK and Demo preserves ${name}, including delivery replay`, (t) => {
    const { get, socket, start } = createDemo(t);
    start("openclaw-integrity", "openclaw");
    const project = createClientProjector();
    const deliveries = [];
    let cursor = 0;
    const decoder = createOpenClawDecoder((signal) => {
      const event = {
        ...signal,
        runtimeId: "openclaw",
        conversationId: "openclaw-integrity",
        replyId: "integrity-answer",
        cursor: ++cursor,
        eventId: `event-${cursor}`,
      };
      for (const [index, payload] of project(event).entries()) {
        const delivery = {
          ...payload,
          cursor: event.cursor,
          eventId: event.eventId,
          deliveryId: `${event.eventId}:${index}`,
        };
        deliveries.push(delivery);
        socket.emit(delivery);
      }
    });
    for (const event of events) decoder.accept(event);
    const text = () =>
      [...get("messages").querySelectorAll(".message.bot .content")].map(
        (element) => element.textContent,
      );
    assert.deepEqual(text(), expected);
    assert.equal(get("messages").querySelectorAll(".stream-active").length, 0);
    const cards = [...get("messages").querySelectorAll(".message.bot")];
    assert.equal(new Set(cards.map((card) => card.dataset.answerMessageId)).size, expected.length);
    if (toolName) {
      assert.equal(get("messages").querySelector(".tool-call .content").textContent, toolName);
      assert.equal(get("messages").querySelector(".tool-result").textContent, "marker");
    }
    for (const delivery of deliveries) socket.emit(delivery);
    assert.deepEqual(text(), expected);
    assert.equal(get("messages").querySelectorAll(".message.bot").length, expected.length);
  });
}
