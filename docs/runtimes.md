# 选择和配置底座

在[快速开始](README.zh-CN.md#用-openclaw-发出第一条消息)连接 OpenClaw，或按下文连接 dsh、Pi。各底座需先配置自己的模型和凭据。

## OpenClaw HTTP/SSE

同机部署时，插件默认监听 `127.0.0.1:3002`，网关默认向 `http://127.0.0.1:3002/webhook` 发请求。跨机部署需要配置 `channels.push-channel.listenHost`，并向 OpenClaw 进程和网关注入相同的 `PUSH_CHANNEL_WEBHOOK_TOKEN`。显式设置该令牌后，本机请求也需要校验。通过私有网络连接宿主，跨网络时使用 TLS。详见[独立网关容器](../deploy/gateway/README.md)。

OpenClaw 向网关主动推送时使用网关侧的 `PUSH_CHANNEL_PUSH_TOKEN`。这与网关调用宿主 webhook 的令牌用途不同。

连接多个 OpenClaw 实例时，为每个启用入站认证的运行时设置独立的 `webhookTokenEnv`，指向网关进程中的环境变量名，并把对应值作为该 OpenClaw 进程的 `PUSH_CHANNEL_WEBHOOK_TOKEN`。各实例使用不同的令牌。网关只有一个 OpenClaw 运行时时，可直接使用网关进程的 `PUSH_CHANNEL_WEBHOOK_TOKEN`。

## 网关托管 RPC

网关可启动本机的 Pi RPC 或 DeepSeek Harness SDK 进程。安装 `@earendil-works/pi-coding-agent@1.3.0` 或 `@deepseek-ai/dsh@0.2.1-rc.2`，确保所需命令在网关的 PATH 中。用[多底座配置示例](../admin-backend/gateway.config.example.json)创建自己的配置，只保留准备好的运行时。模型配置遵循对应底座格式，模板见 [Pi models.json](../deploy/runtime-runner/pi-models.example.json) 与 [dsh profile](../deploy/runtime-runner/dsh-profile.example.yml)。

从仓库根目录启动：

```bash
npm --prefix admin-backend ci
npm --prefix admin-backend start -- --config gateway.config.local.json
```

Pi 配置文件的位置为 `<dataRoot>/pi/<agentId>/home/.pi/agent/models.json`。DeepSeek SDK profile 放在 `<dataRoot>/dsh/<agentId>/home/.dsh/profiles/sdk/cordis.patch.yml`。密钥通过环境变量或底座认证配置提供。新会话从 Demo 选择相应运行时和智能体。

适配器依据 [DeepSeek SDK 协议](https://github.com/deepseek-ai/deepseek-harness/blob/687f282e8625c6b8d11ff58211a90524be2e4317/packages/sdk/protocol/README.md) 与 [Pi RPC 协议](https://github.com/earendil-works/pi/blob/1d01b3b1f8de2e67a3a1c1694b14c984e4edd156/packages/coding-agent/docs/rpc.md)实现。升级底座后应重新检查适配器契约。

`gateway.config.example.json` 是服务端配置，不包含密钥。`runtimes` 中每项需要唯一 `id` 和 `type`；OpenClaw 需要 `url`，工作空间仍由 OpenClaw 自己管理。Pi/DeepSeek 必须声明非空 `agents` 智能体 ID 白名单和顶层 `dataRoot`；可配置 `command`、字符串数组 `args` 和字符串映射 `env`，DeepSeek 另支持 `provider`、`model`。相对 `dataRoot`、`databasePath` 相对配置文件解析。`command` 为 `./bin/pi`、`scripts/dsh-wrapper` 等含路径分隔符的相对路径时，也相对配置文件目录解析，不随智能体工作空间变化；绝对路径保持不变，`pi`、`dsh` 等裸命令名仍通过子进程的 `PATH` 查找。

每个原生智能体固定使用 `dataRoot/dsh/<agentId>/{home,workspace}` 或 `dataRoot/pi/<agentId>/{home,workspace}`。同一个智能体的不同会话共用自己的工作空间，不同底座、不同智能体不共用工作目录、配置或历史。目录在首次运行时创建；注册连接和创建会话不启动原生进程。会话创建、恢复和任务接受前检查智能体白名单；同一底座的同名智能体只能由一个 runtime 实例声明，避免两个运行实例共用目录。

原生 `agentId` 使用最多 128 个 ASCII 小写字母、数字、下划线或连字符，以小写字母或数字开头，不允许路径分隔符或 `..`；拒绝大小写别名，避免在大小写不敏感的文件系统中共用目录。服务端派生 `HOME`、`DSH_HOME`、`DSH_AGENTS_HOME`、`PI_CODING_AGENT_DIR`、`PI_CODING_AGENT_SESSION_DIR` 及 XDG 目录，不允许 `env` 覆盖。首次准备时拒绝托管根目录、home、workspace、原生配置路径的符号链接，防止意外指向其他底座；工作文件内部的链接不受此检查限制。技能分别安装到对应智能体的原生目录，不自动共享。目录隔离不是安全沙箱，同一运行容器内的工具权限仍需独立限制。

停止网关后再迁移各底座的工作文件和原生配置。Pi 历史与工作目录有关，迁移后需验证恢复。容器构建、Secret 模板及持久卷布局见[容器部署](../deploy/runtime-runner/README.md)。

`args` 不能修改网关拥有的会话、存储或 RPC 生命周期：Pi 禁止 `--session-dir`、`--session`、`--session-id`、`--no-session`、`--continue`/`-c`、`--resume`/`-r`、`--fork`、`--mode`；dsh 禁止 `--profile`、`--patch`、`--from-default-profile` 和配置导出模式。两者也禁止帮助、版本、其他一次性退出模式、首位位置参数和 `--` 参数终止符；`--key=value` 写法同样检查。升级时显式删除冲突参数，不会自动重写。Pi 的 `--provider`、`--model`、工具选择等正常参数可以保留。原生 profile 及工具配置仍由可信部署者维护，派生路径和参数检查不构成安全沙箱，不能限制任意受信任插件自己选择其他存储路径。

本地配置可放在被 Git 忽略的 `gateway.config.local.json`；密钥优先通过环境或底座已有认证配置提供。两个原生适配器保持进程与原生会话，不将聊天降级成直接调用模型 HTTP API。

## dsh 原生插件

需要 dsh 真流式输出时，使用 `open-push-channel/dsh` 入口。原生插件订阅公开 `agent/assistant-stream`，将增量、工具和本机进程内后代事件转换成统一协议，执行、技能、模型、原生会话和子智能体调度仍归 dsh。插件不修改底座，不接管 SDK 标准输出。

同一 npm 包保留 OpenClaw 默认入口，dsh 只加载 `./dsh` 子入口。发布内容只包含宿主插件与共享传输协议，不包含网关、dsh/Pi 引擎、Demo 或其他底座依赖。独立网关仍从本项目 `admin-backend` 部署。

先准备独立 dsh profile 和工作目录；由宿主部署设置这个智能体专用的 `DSH_HOME`、`DSH_AGENTS_HOME` 和绝对 `cwd`。plugin 模式不替你创建这些目录，也不继承网关托管 SDK 模式的 home。在仓库根目录将本地包加入独立 dsh profile，再把示例加入该 profile 的 `cordis.patch.yml`。

```bash
dsh plugin --profile push-channel add "$(pwd)"
```

```yaml
- insert:
    - id: push-channel
      name: open-push-channel/dsh
      config:
        url: ws://127.0.0.1:3003/plugins
        runtimeId: dsh
        agentId: main
        tokenEnv: PUSH_DSH_PLUGIN_TOKEN
        provider: your-native-provider
        model: your-native-model
        cwd: /absolute/path/to/dsh/main/workspace
```

`provider`、`model` 使用这个 dsh profile 已配置的原生标识；技能和工具按该宿主自己的方式安装和授权，浏览器不能覆盖。示例对应 dsh `0.2.1-rc.2` 的公开服务 API；升级后需重新验证装载与事件契约。profile 必须提供 `agents`、`subagents`、`sessionPersistence` 和 `loader`，默认 dsh-base 包含这些服务。

网关使用 [插件配置示例](../admin-backend/gateway.plugins.example.json)，其中 `runtimeId`、智能体白名单和全部能力值须与宿主握手一致。给网关与 dsh 进程分别注入同名 `PUSH_DSH_PLUGIN_TOKEN`；Demo 客户端和 HTTP 推送各使用示例声明的独立 token。在一个终端启动网关，另开终端启动上述 dsh profile：

```bash
npm --prefix admin-backend ci
PORT=3003 npm --prefix admin-backend start -- --config gateway.plugins.example.json
```

```bash
dsh --profile push-channel
```

首次连接失败不会阻塞原 dsh 启动；日志会提示离线。修复网关或认证后需重新加载插件。成功握手后的连接中断会重连元数据，正在执行的任务与控制输入不会自动重发。原生插件只关联本机、可验证实时所有权的进程内后代；远端子智能体没有公开父级关联契约，因此不推测展示归属。

打开 `http://127.0.0.1:3003/demo/`，连接时输入客户端令牌 `PUSH_DEMO_CLIENT_TOKEN` 对应的值。新会话选择 `dsh` 运行时与 `main` 智能体，可查看实时正文、推理、工具、子智能体和队列；按钮依据握手能力启用。`steering`/`followUp` 只作用于当前运行的原生队列，结束时仅清理本插件提交且仍未处理的输入；宿主或其他插件的待处理输入保留。原生插件公开生命周期服务 `ctx.pushChannel.notify({ clientId, conversationId }, { eventId, content })`，供其他可信 dsh 插件或主动任务向明确绑定目标发送通知；它不能创建或改绑会话。

迁移时使用新 `runtimeId` 和新会话验收，再停用旧 SDK runtime。旧 RPC 会话、配置与原生历史不会自动迁移；先停止旧 SDK 进程，避免它与新原生插件同时使用同一 native store 或 session ID。现有工作文件和凭证按宿主部署方案分别迁移，插件不会移动已有数据。

Pi 通过 RPC 接入。另有[独立会话 SDK 插件设计](pi-sdk-plugin-design.md)。
