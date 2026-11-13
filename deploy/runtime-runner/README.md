# 独立运行容器部署

此镜像包含 Push Gateway、DeepSeek Harness 0.2.1-rc.2 和 Pi 1.3.0，不包含 OpenClaw。OpenClaw 经 HTTP 适配器连接；运行容器只需挂载 `dsh/`、`pi/`、`push-gateway/` 数据，不挂载 OpenClaw 配置和工作空间。

## 构建与配置

从 push-channel 根目录构建：

```sh
docker build -f deploy/runtime-runner/Dockerfile -t push-runtime-runner:local .
```

将配置 Secret 只读挂载到 `/etc/push-gateway`，包含以下文件：

- `gateway.json`：运行时、智能体 ID 白名单、认证环境变量名；不填真实密钥。
- `dsh-profile.yml`：原生 SDK profile 的 `cordis.patch.yml` 内容。
- `pi-models.json`：原生 Pi `models.json` 内容。

模型配置必须遵循对应版本的原生格式，使用环境变量引用；密钥用 Kubernetes Secret 的 `envFrom` 注入。这里只复制配置，不转换模型、安装技能或调整原生工具权限。模型提供方凭据仍交给底座。

自定义 OpenAI 兼容接口可以从本目录源码中的 [dsh-profile.example.yml](dsh-profile.example.yml) 和 [pi-models.example.json](pi-models.example.json) 开始。将 `model.example.com`、`your-model`、上下文和输出容量替换为实际接口的契约，再分别作为 `dsh-profile.yml`、`pi-models.json` 写入每用户自己的私有 Secret，同时提供 `PUSH_MODEL_API_KEY`。示例文件不包含在运行镜像中，不会自动发送到模型。推理模式和 `compat` 选项应按实际接口配置，不默认套用其他提供方的参数。

`gateway.json` 最小生产示例：

```json
{
  "dataRoot": "/runtime-data",
  "databasePath": "/runtime-data/push-gateway/gateway.sqlite",
  "runtimes": [
    { "id": "openclaw", "type": "openclaw", "url": "http://127.0.0.1:3002/webhook" },
    { "id": "dsh", "type": "deepseek-harness", "agents": ["researcher"], "provider": "custom", "model": "your-model" },
    { "id": "pi", "type": "pi", "agents": ["coder"], "args": ["--provider", "custom", "--model", "your-model"] }
  ],
  "auth": {
    "clients": [
      {
        "id": "demo",
        "tokenEnv": "PUSH_GATEWAY_CLIENT_TOKEN",
        "targets": [
          { "runtimeId": "openclaw", "agentId": "main" },
          { "runtimeId": "dsh", "agentId": "researcher" },
          { "runtimeId": "pi", "agentId": "coder" }
        ]
      }
    ],
    "pushTokenEnv": "PUSH_CHANNEL_PUSH_TOKEN"
  }
}
```

示例假定同 Pod 的 OpenClaw plugin webhook 监听 `127.0.0.1:3002`。跨容器网络连接时，将 `channels.push-channel.listenHost` 设为容器可访问的地址，并给 OpenClaw 与网关注入相同的 `PUSH_CHANNEL_WEBHOOK_TOKEN`。仅允许网关访问 webhook 端口，跨不受信任网络时使用 TLS。

OpenClaw 主动推送到此容器时，将其 `middlewareUrl` 指向 `http://127.0.0.1:3003` 或网关服务地址，并在 OpenClaw 和运行容器内通过 `PUSH_CHANNEL_PUSH_TOKEN` 注入相同的 Secret 值。入站 webhook token 与主动推送 token 分开配置。

## 数据布局和启动行为

```text
/runtime-data/
├── dsh/researcher/
│   ├── home/.dsh/profiles/sdk/cordis.patch.yml
│   ├── home/.agents/
│   └── workspace/
├── pi/coder/
│   ├── home/.pi/agent/models.json
│   └── workspace/
└── push-gateway/gateway.sqlite
```

启动入口先验证完整网关配置、认证和所需原生模板，再创建目录。没有现有配置时，模板分别复制到各智能体自己的 home。已有配置不覆盖；修改 Secret 后只重启不会替换旧 home 配置。需要升级原生配置时，停止该运行容器，备份并显式修改各智能体自己的配置，再启动验证。技能、历史和工作文件同样独立。

容器启动不会调用模型。收到对应智能体的第一条任务后才启动原生子进程。相同智能体的会话共用 home/workspace、原生会话 ID 互相独立；不同智能体使用不同适配器实例。

本机可只验证配置和初始化，不启动服务：

```sh
node deploy/runtime-runner/bootstrap.cjs --config /etc/push-gateway/gateway.json --prepare-only
```

入口只接受 `--config <文件>` 和 `--prepare-only`，固定启动网关脚本，不接受脚本位置参数。镜像 CMD 和 Kubernetes 容器 `args` 均只传入口参数，例如 `["--config", "/etc/push-gateway/gateway.json"]`。省略 `--config` 时使用 `/etc/push-gateway/gateway.json`；该文件必须已挂载。

## 验证与限制

容器默认监听 `0.0.0.0:3003`，强制要求网关 `auth`。只通过可信网络或 TLS 反向代理开放，不公开原生配置 Secret。`GET /healthz` 仅证明网关在线，不证明底座或模型可用。

部署后用 frontend-demo 为各 runtime/agent 创建会话，分别写入、读取工作文件，再重启验证历史恢复；检查同名文件没有出现在其他底座或智能体目录。再验证工具、子智能体（底座支持时）、通知和 `/api/tasks`。中断任务不自动重执行；不能把健康检查当作以上行为证明。

每用户只部署一个网关进程写自己的 SQLite。目录和 Kubernetes 子路径挂载降低误用风险，不是智能体间安全边界：此镜像中的原生工具仍可访问运行容器获准挂载的其他目录。强安全隔离需要独立容器/权限以及远程执行适配，不在当前本地子进程适配器范围内。

OpenClaw 现有数据的 PVC 迁移由部署项目单独执行；本入口不读取、移动或修复 OpenClaw 数据。
