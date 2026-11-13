# 网关事件协议

[业务系统接入](integration.md) · [底座配置](runtimes.md) · [贡献指南](../CONTRIBUTING.md)

## 使用版本化事件

新客户端使用 `protocolVersion: 1` 接收统一事件。默认格式仍提供 `stream_*`、`tool_*` 和 `subagent_*` 等事件。可运行的浏览器示例见[业务系统接入](integration.md)。

版本 1 使用 `onEvent` 消费统一事件；原有展示回调服务于默认格式。注册只建立连接身份，返回 `runtimes` 目录，不绑定或启动底座。`startSession(id, { runtimeId, agentId })` 返回 `session.started`，携带该会话的能力。`sendMessage` 只需会话 ID；也可在首次消息显式携带 `runtimeId`、`agentId` 创建会话。重复打开同一绑定恢复订阅，尝试改绑会报错。未知运行时返回会话错误，未知协议版本关闭连接。

`cancel(sessionId)` 只在该会话 `capabilities.cancel` 为真时可用。`onRunStarted`、`onRunCompleted`、`onRunCancelled` 区分整次运行与中间的 `streamEnd`。重连应复用 SDK 实例，再次 `connect()`；已确认的会话会重新订阅。

### 在支持的运行时中引导或跟进

Demo 中先选择 Pi 或 dsh 原生插件 runtime、开始会话，再用“发送”开始任务。收到带 `runId` 的运行开始事件后，可继续在同一输入框输入内容，选择“即时引导”或“后续跟进”。空闲时使用普通“发送”。按钮按会话能力启用：Pi RPC 和 dsh 原生插件声明 `steering: true`、`followUp: true`；当前 OpenClaw 和 DeepSeek 托管 JSON-RPC 适配器没有接入这些控制，按钮保持禁用。dsh 原生会话尚在创建时提交控制会明确拒绝，可在原生接收当前输入后再提交。

Pi 1.3.0 的即时引导在当前助手轮的工具调用结束后、下一次模型调用前处理；后续跟进在没有待处理工具调用或引导消息后处理。两者都作用于当前原生会话和网关运行，不创建第二个网关任务，也不强行中断正在执行的工具。

```javascript
client.onControlReceipt((event) => {
  const receipt = event.version === 1 ? event.data : event;
  console.log(receipt.controlId, receipt.status);
});
client.onInputQueue((event) => {
  const queue = event.version === 1 ? event.data : event;
  console.log(queue.steering, queue.followUp);
});
// sessionId、runId 来自当前会话的运行开始事件；每次独立输入使用新的 controlId。
const controlId = client.steer("先检查边界条件", { sessionId, runId });
client.followUp("完成后补充测试说明", { sessionId, runId });
```

`steer(content, { sessionId, runId, controlId? })` 和 `followUp(...)` 发送 `input.control`；会话 ID、当前运行 ID 和非空文本必填，缺少目标或正文会抛错。SDK 自动生成 `controlId`，也可由调用方显式提供。成功写入 WebSocket 时返回该 ID，未连接时返回 `false`；返回 ID 不等于底座已接受。控制 ID 独立于原请求的 `requestId`、`replyId`，不替换原聊天消息身份。同一个 ID 只用于重放同一控制；更改操作、正文或目标运行会被拒绝。过期运行、不支持的操作或未授权的会话会收到关联该控制 ID 的拒绝回执。

`onControlReceipt` 和 `onInputQueue` 保留原始信封：协议版本 1 分别收到 `input.control`、`input.queue`，载荷在 `event.data`，会话在 `event.conversationId`；默认格式分别收到 `input_control`、`input_queue`，载荷在顶层，会话在 `event.sessionId`。两种格式均带 `runId`。连接上的直接拒绝始终使用 `input_control`，即使客户端选择版本 1；它可能仅含 `controlId`、`runId`、`sessionId`、`status`、`message`，需要按 ID 关联本地正文。

回执 `dispatching` 表示正在提交，`queued` 仅表示原生队列已接收，`handled` 表示原生输入处理器已接管，`rejected` 表示明确拒绝，`unknown` 表示接收结果未知。`queued`、`handled` 都不证明模型已执行或完成。待处理内容以 `input.queue` 的 `steering`、`followUp` 文本数组为准，文本可能经 Pi 输入处理器、技能或模板转换，不按原文推算哪个控制已消费。

运行结束或停止时，若原生队列仍有剩余，网关清理并发布空快照，附带 `discardedSteering`、`discardedFollowUp` 和 `reason: "run_settled" | "cancelled"`，Demo 单独显示未处理内容。没有输入控制的运行可能不产生任何队列快照。断线时未收到回执的控制显示“状态未知”，重连只订阅并回放日志，不自动重发控制。网关重启后未确认的提交同样恢复为 `unknown`；应先核对底座结果，再决定是否发起新输入。

事件信封示例：

```json
{
  "version": 1,
  "eventId": "event-uuid",
  "runtimeId": "openclaw",
  "agentId": "main",
  "conversationId": "chat-1",
  "runId": "run-uuid",
  "origin": "prompt",
  "audience": "conversation",
  "seq": 3,
  "timestamp": 1780000000000,
  "type": "message.delta",
  "data": { "messageId": "message-uuid", "delta": "你好" }
}
```

`eventId` 唯一；`seq` 在同一次运行内递增；`runId` 每次执行或主动投递独立生成。`queryMessageId`、`answerMessageId` 分别进入可选的 `requestId`、`replyId`，用于关联现有客户端消息。`agentId` 是投递目标，子智能体身份位于 `data.agentId`，原生子会话键转换成 `data.childSessionId`。

| 事件族   | 事件                                                                                                          |
| -------- | ------------------------------------------------------------------------------------------------------------- |
| 运行     | `run.started`、`run.completed`、`run.failed`、`run.cancelled`、`run.deferred`                                 |
| 输入     | `input.accepted`：已持久化的用户输入；旧 SDK 投影为 `message_sent`                                            |
| 输入控制 | `input.control`：原生控制接收状态；`input.queue`：底座待处理队列快照；旧格式为 `input_control`、`input_queue` |
| 消息     | `message.started`、`message.delta`、`message.snapshot`、`message.completed`                                   |
| 推理展示 | `reasoning.started`、`reasoning.delta`、`reasoning.completed`                                                 |
| 工具     | `tool.started`、`tool.completed`；`activity.started/completed` 表示现有工具阶段指示                           |
| 子智能体 | `subagent.started/message/delta/result/failed/ended`、`subagent.tool.started/completed`                       |

完整载荷字段见 [TypeScript 契约](../protocol/index.d.cts)。消息增量与快照明确区分，网关不根据文本长度或前缀猜测快照。

## 运行与子智能体

同一 `clientId + runtimeId + agentId + conversationId` 的请求按完整执行周期串行处理；不同身份的队列独立。接受请求不等于执行完成，适配器必须等待原生执行周期结束才返回。SSE 缺少结束标记、无效 JSON、HTTP 错误和插件调度失败会产生 `run.failed`。

`run.deferred` 表示本次请求已转为等待后续主动通知，不表示底座任务成功结束。当前 OpenClaw 和 dsh 接入未提供单任务取消。Pi RPC 取消等待原生结束信号。客户端断线不取消已接受的任务。CLI 接收 SIGINT/SIGTERM 时关闭连接和自身持有的底座进程；底座凭证及原生历史不删除。

## 断线、重启与去重

CLI 默认持久化到 `state/push-gateway.sqlite`；程序内 `createGatewayServer()` 未传 `databasePath` 时使用内存数据库，适合测试。网关的绑定、原生 session ID、输入、运行状态和事件游标都在 SQLite。原生历史仍由 OpenClaw、DeepSeek 或 Pi 管理，恢复时必须保留原来的底座 home、工作目录及相关会话文件。

- 浏览器 `session.start` 自动携带 SDK 的 `afterCursor`；服务器包含该最后游标重新投递，SDK 按 event/delivery ID 去重。同一 canonical 事件拆成多条旧格式消息时不会漏投剩余片段。
- `queryMessageId` 是请求幂等键。同一客户端、会话、键只能接受一个执行；相同键改正文或 mentions 会报冲突。Demo 每次新输入生成唯一 ID。未提供键的原始客户端每次都是新请求。
- 网关重启后，未完成运行记为失败并注明“结果未知、未自动重跑”。这不等于底座操作已回滚；不要盲目用新 ID 重做有副作用的工作。
- 后续显式新请求可恢复原生上下文：DeepSeek 使用 `session/resume`，Pi 恢复后核对原生 session ID。缺失、损坏、被占用或身份不符直接失败，不悄悄创建空白历史。
- 进程未启动、初始化或明确接收前拒绝，不会把新会话误标为已持久化；写入后丢失回执则保守视为可能已接收。
- SQLite 日志暂不自动清理；包含聊天内容，应放在受保护持久卷并纳入容量和备份管理。保留/删除由管理员明确处理，不以过期猜测删除会话。

`mentions` 仍使用结构化数组：

```json
{
  "type": "message",
  "sessionId": "chat-1",
  "content": "请调研并给出实现建议",
  "mentions": [{ "agentId": "researcher", "label": "Researcher" }]
}
```

OpenClaw 插件启动这些专家子智能体、等待结果，再交给主智能体汇总。网关只转换并投递子智能体进度和结果。此编排语义属于 OpenClaw，未来其他适配器需声明各自能力。

### Pi 子智能体

Pi 适配器自动加载本插件维护的扩展，提供 `subagent({ task: "任务描述" })` 工具，无需新增用户配置。模型调用一次工具执行一个任务：启动独立 Pi 子进程，创建新的原生子会话，等待结果后返回给父智能体。它不是 Pi 自带的子智能体能力，也不是 `@mentions`；不会按提及名称切换到其他已注册智能体。

子会话使用所属智能体原有的 home、工作目录和当前模型，只继承父会话当前启用的原生内置工具名称集合；不恢复默认工具，也不自动增加写入或 Bash 能力。子进程使用 `--no-extensions`，不加载父会话的扩展、自定义工具、MCP 或扩展权限钩子，因此不能把它视为完整权限策略继承；子会话也没有 `subagent` 工具，不允许递归派生。

若当前 provider 的凭据只存在于父 Pi 进程的运行时（例如 `--api-key`），适配器仅将该 provider 解析后的密钥通过原生 `--api-key` 参数传给子进程，不写盘或记录该密钥。与父进程的 CLI 参数一样，该参数可被有权限的操作系统用户观察；建议优先使用环境变量或 `models.json` 中的环境变量引用。这不代表完整继承自定义 provider 或扩展认证实现。

子智能体的流文本、工具参数、工具结果与终态通过现有 `subagent.*` 协议回到父会话，Demo 复用通用卡片展示。完成、失败和取消分别显示“已完成”“失败”“已停止”；取消、失败及父会话关闭时等待子进程清理。不同子会话有独立身份，即使名称相同也不会合并；切换 Demo 会话不会把后台子智能体显示到其他会话。

这不是操作系统或文件访问沙箱。适配器不会主动使用其他已注册智能体的目录，但子会话与父会话拥有相同的操作系统权限；启用的 `read`、`bash` 等工具仍可能访问其他绝对路径。需要依赖扩展权限钩子的部署，不能假设这些钩子会约束子会话，启用前需另行评估进程和文件系统权限。
