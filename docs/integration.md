# 把智能体接入业务系统

Open Push Channel 网关可作为独立 WebSocket 服务运行。业务页面用它发送聊天消息和接收事件，业务后端用 HTTP 接口启动任务和投递通知。底座可以在网关之外运行。使用 Node.js 启动网关，或构建[仅包含网关和 Demo 的容器镜像](../deploy/gateway/README.md)。

以工单系统为例。工单页创建一个绑定到实际智能体的会话，用户在页面发送问题。业务后端在收到状态变更事件后，用工单会话 ID 发起摘要任务，页面通过同一条 WebSocket 接收运行事件。若后端已生成摘要，可以直接推送结果。

## 为业务系统配置认证

在网关的 JSON 配置文件中声明允许连接的客户端及其目标。将以下内容保存为 `admin-backend/gateway.config.local.json`。本例假定 OpenClaw 插件与网关同机运行，OpenClaw 的 `channels.push-channel.middlewareUrl` 指向 `http://127.0.0.1:3001`。插件安装见[快速开始](README.zh-CN.md#用-openclaw-发出第一条消息)。

```json
{
  "databasePath": "state/push-gateway.sqlite",
  "runtimes": [
    {
      "id": "openclaw",
      "type": "openclaw",
      "url": "http://127.0.0.1:3002/webhook"
    }
  ],
  "auth": {
    "clients": [
      {
        "id": "helpdesk",
        "tokenEnv": "PUSH_HELPDESK_CLIENT_TOKEN",
        "targets": [{ "runtimeId": "openclaw", "agentId": "main" }]
      }
    ],
    "pushTokenEnv": "PUSH_CHANNEL_PUSH_TOKEN"
  }
}
```

从密钥管理工具向网关进程注入互不相同的客户端令牌和服务端推送令牌。本机试用时，可以在当前终端生成临时令牌，再从项目根目录启动：

```bash
export PUSH_HELPDESK_CLIENT_TOKEN="$(openssl rand -hex 32)"
export PUSH_CHANNEL_PUSH_TOKEN="$(openssl rand -hex 32)"
npm --prefix admin-backend ci
npm --prefix admin-backend start -- --config gateway.config.local.json
```

如果 OpenClaw 已按快速开始配置 Webhook 令牌，在新网关进程中继续使用相同的 `PUSH_CHANNEL_WEBHOOK_TOKEN`。需要 OpenClaw 主动通知时，把同一个 `PUSH_CHANNEL_PUSH_TOKEN` 注入 OpenClaw 进程。如果它已在运行，重新启动后才会读取更新的环境变量。

| 凭据 | 由谁持有 | 用途 |
| --- | --- | --- |
| `PUSH_HELPDESK_CLIENT_TOKEN` | 网关与获授权的业务页面 | WebSocket 注册。网关按此确定客户端 ID 和可用的底座、智能体 |
| `PUSH_CHANNEL_PUSH_TOKEN` | 网关、可信业务后端及需要回调网关的 OpenClaw | `/api/tasks`、`/api/send`、`/send` 和受保护的 `/api/runtimes` |
| `PUSH_DSH_PLUGIN_TOKEN` | 网关与 dsh 原生插件 | dsh 通过独立的 `/plugins` WebSocket 接入，见[底座配置](runtimes.md#dsh-原生插件) |
| `PUSH_CHANNEL_WEBHOOK_TOKEN` | 网关与 OpenClaw 插件 | 网关向 OpenClaw webhook 发请求。非本机监听时必填，设置后本机请求也校验 |
| 模型凭据 | 对应底座 | 访问模型提供商 |

业务服务应在自己的登录和权限检查后，将相应的**客户端**令牌交给当前页面。令牌持有者可以访问该客户端身份下的会话，因此需要按实际用户隔离要求配置客户端身份。网页和 WebSocket 对外通过 HTTPS/WSS 提供。服务端和宿主令牌保留在对应服务进程中。

推送令牌可在整个网关范围内提交任务和通知，适合共享同一信任边界的业务服务和宿主。需要隔离不同组织的服务端权限时，为它们分别部署网关。

未配置 `auth` 的模式适用于本机同源 Demo。网关绑定非本机地址时要求配置 `auth`。启用认证后，实际客户端身份由令牌确定，服务端按目标白名单授权。若前端允许用户指定 OpenClaw 子智能体，每个被指定的智能体也必须出现在该客户端同一底座的目标白名单中。

## 在网页中发送消息和接收流

将下面的 HTML 保存为本地测试页面，或把其中的脚本嵌入业务页面。它使用上述本机网关、`helpdesk` 客户端和 OpenClaw 的 `main` 智能体。输入 `PUSH_HELPDESK_CLIENT_TOKEN` 的值，连接成功后就可以发送消息。实际部署时把脚本地址和网关 URL 改为你的网关地址。

```html
<label>客户端令牌 <input id="token" type="password" autocomplete="off"></label>
<button id="connect">连接</button>
<label>消息 <input id="message" value="你好"></label>
<button id="send" disabled>发送</button>
<pre id="reply" aria-live="polite"></pre>
<p id="status" role="status"></p>
<script src="http://127.0.0.1:3001/demo/sdk.js"></script>
<script>
  const gateway = "http://127.0.0.1:3001";
  const sessionId = "ticket-42";
  const target = { runtimeId: "openclaw", agentId: "main" };
  const byId = (id) => document.getElementById(id);
  const messages = new Map();

  byId("connect").onclick = () => {
    const client = new PushChannelSDK(gateway, "helpdesk", {
      token: byId("token").value,
      protocolVersion: 1,
    });
    byId("token").value = "";
    byId("connect").disabled = true;

    client.onRegistered(() => client.startSession(sessionId, target));
    client.onSessionStarted((session) => {
      if (session.sessionId === sessionId) byId("send").disabled = false;
    });
    client.onEvent((event) => {
      if (event.conversationId !== sessionId) return;
      const data = event.data;
      const key = `${event.runId}:${data.messageId}`;
      if (event.type === "message.delta") {
        messages.set(key, (messages.get(key) ?? "") + data.delta);
      } else if (event.type === "message.snapshot") {
        messages.set(key, data.text);
      }
      byId("reply").textContent = [...messages.values()].join("\n\n");
      if (event.type.startsWith("run.")) {
        byId("status").textContent = data.message ?? event.type;
      }
    });
    client.onError((error) => {
      byId("status").textContent = error?.message ?? "连接或会话操作失败";
    });
    client.onClose(() => {
      byId("send").disabled = true;
      byId("connect").disabled = false;
      byId("status").textContent = "连接已关闭";
      // The next connection creates a new SDK and replays the session.
      messages.clear();
      byId("reply").textContent = "";
    });
    byId("send").onclick = () => {
      const content = byId("message").value.trim();
      if (!content) return;
      client.sendMessage(content, sessionId, { queryMessageId: crypto.randomUUID() });
    };
    client.connect();
  };
</script>
```

`sessionId` 是业务应用使用的会话 ID，服务端事件通过 `conversationId` 返回它。网关用客户端 ID 和会话 ID 查找绑定，`runtimeId` 与 `agentId` 在首次创建时确定。后续消息沿用原绑定，切换目标时使用新的会话 ID。

实际产品可以根据 `session.started.capabilities` 显示停止、引导等操作。工具与子智能体字段见[协议参考](protocol.md)。

## 由业务后端启动任务

网页连接并创建 `helpdesk` 的 `ticket-42` 会话后，在另一个终端从同一来源设置 `PUSH_CHANNEL_PUSH_TOKEN`，然后执行：

```bash
curl --fail-with-body http://127.0.0.1:3001/api/tasks \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $PUSH_CHANNEL_PUSH_TOKEN" \
  -d '{
    "clientId": "helpdesk",
    "sessionId": "ticket-42",
    "eventId": "ticket-42-summary-v1",
    "content": "根据这个会话给出简短摘要。"
  }'
```

HTTP 202 表示网关持久化并接受了任务。用返回的 `runId` 关联 WebSocket 上 `origin: "task"` 的事件，等待运行终态判断结果。可附加 `runtimeId`、`agentId` 核对原绑定。业务服务应先验证实际操作者有权触发任务，因为执行时可调用目标底座启用的工具。

按同一业务事件生成稳定的 `eventId`。同一个任务重试会得到原 `runId`。同一 ID 改了内容或目标会报错。HTTP 404 时检查会话是否创建、客户端 ID 是否匹配，以及可选的运行时和智能体字段是否符合原绑定。

## 发送已有结果或通知

```bash
curl --fail-with-body http://127.0.0.1:3001/api/send \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $PUSH_CHANNEL_PUSH_TOKEN" \
  -d '{
    "clientId": "helpdesk",
    "agentId": "main",
    "sessionId": "ticket-42",
    "eventId": "ticket-42-status-resolved",
    "content": "工单已解决。"
  }'
```

`/api/send` 向会话投递通知，`/api/tasks` 调用智能体执行。通知返回 HTTP 200 表示网关接受了消息，浏览器收到的事件带 `origin: "push"`。同一个通知 ID 可以安全地重试相同正文。按会话发送时，消息可在离线用户重新订阅后回放。

通知载荷除了 `content`，还支持协议 [AdapterSignal](../protocol/index.d.cts) 对应的 `signal`，或 OpenClaw 旧事件格式的 `event`。三者只能提供一种。始终给业务通知带上明确的 `clientId`、`sessionId` 和 `agentId`，避免按智能体向所有在线客户端广播。

## 重连、去重和部署边界

- 同一 SDK 实例再次 `connect()` 会恢复已确认的会话订阅。SDK 用游标和事件 ID 去重。页面刷新后用原身份和原绑定重新打开会话。上面的示例关闭连接后重新创建 SDK，因此会重新回放原会话。
- 聊天重试复用同一 `queryMessageId`。任务和通知分别使用稳定的 `eventId`。三者都应保留同一 ID 对应的原内容与目标。
- 单个网关进程可服务多个客户端和会话。同一客户端、底座、智能体和会话中的运行按序执行。SQLite 文件由一个网关进程独占。将其和底座历史放在持久存储中。
- 网关重启后，未结束运行记录为结果未知，底座可能已产生效果。先核对结果，再决定是否发起新任务。
- 生产部署应保护 SQLite 内容、配置 TLS 及访问控制，并分别限制各底座的文件和工具权限。吞吐量与高可用需要按部署另行验证。多个副本需要共享状态、会话与事件归属的设计，不能共用当前 SQLite 文件。

环境变量和 WebSocket 输入控制的详细行为见[协议参考](protocol.md)。容器部署见[独立网关](../deploy/gateway/README.md)和[含 RPC 底座的运行容器](../deploy/runtime-runner/README.md)。
