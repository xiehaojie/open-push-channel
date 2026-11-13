# 参与 Open Push Channel 开发

按 [README](README.md) 启动独立网关和 Demo，按[底座配置](docs/runtimes.md)连接要验证的智能体。网关测试可以使用受控协议对端运行。

## 模块分工

| 路径 | 职责 |
| --- | --- |
| `protocol/` | 事件与原生插件传输校验、TypeScript 契约 |
| `admin-backend/src/gateway/`、`admin-backend/src/websocket/` | 会话绑定、运行队列、SQLite、认证与事件投递 |
| `admin-backend/src/adapters/` | OpenClaw HTTP/SSE、dsh SDK、Pi RPC 和通用原生插件传输 |
| `index.ts`、`src/` | OpenClaw 插件与公开 SDK 接入 |
| `plugins/dsh/`、`plugins/client.cjs` | dsh 原生插件与共享网关连接 |
| `frontend-demo/` | 浏览器 SDK 与 Demo |
| `deploy/gateway/`、`deploy/runtime-runner/` | 独立网关镜像与含 RPC 底座的可选镜像 |

各智能体底座管理自己的模型、工具、工作空间、编排与原生会话历史。网关统一会话接入、运行事件和消息投递。

## 构建和验证

使用 Node.js 22.19+（22.x）或 24+。从本仓库根目录执行：

```bash
npm ci
npm --prefix admin-backend ci
npm test
npm pack
```

`npm test` 运行独立网关、协议、适配器、Demo 与运行容器启动器测试。可单独运行目标测试，例如：

```bash
node --test admin-backend/src/adapters/pi.test.js
node --test plugins/dsh/index.test.cjs
node --test frontend-demo/sdk.test.js
```

修改 OpenClaw 插件时，在独立测试配置中安装 `npm pack` 产物并启动对应宿主。用于测试的 OpenClaw 源码检出可将本项目放在 `extensions/push-channel` 下，安装宿主的开发依赖，再用宿主提供的测试入口执行 `index.test.ts` 和 `src/*.test.ts`。运行方式遵循该宿主检出的开发指南。

报告测试时区分受控对端、产物安装和真实模型联调，写明宿主版本及覆盖的链路。修改打包或部署时，验证生成的产物可以加载和启动。

## 接入新底座

1. 阅读目标宿主的公开接口，确认消息接收、会话恢复、运行完成及取消契约。
2. 按 [Adapter 契约](protocol/index.d.cts) 在 `admin-backend/src/adapters/` 接入，并通过 `runtime-config.js` 装配服务端配置。宿主支持原生插件时，可以复用 `plugins/client.cjs` 连接网关。
3. 声明实际支持的能力，使用统一事件区分消息增量、快照、工具及子智能体。适配器负责原生结果；网关负责 `run.*` 与输入确认。
4. 为成功、明确拒绝、终态缺失、重复请求、过期运行与连接中断提供行为测试。输入控制需要验证目标运行、重复提交、接收结果未知及队列处理。
5. 更新[能力表](docs/README.zh-CN.md#连接其他底座)和[底座配置](docs/runtimes.md)。协议行为变更同步校验、类型、SDK 和示例。

`run()` 应等待原生执行周期结束。`onAccepted()` 表示底座已接收或接收结果未知；明确拒绝时不调用。用 `emit()` 发送 `AdapterSignal`，避免重复生成运行终态。凭据、工具权限和工作目录从可信宿主或部署配置加载。

## 提交问题或修改

普通问题请使用 GitHub Issue 并附上版本与复现步骤。安全问题按 [SECURITY.md](SECURITY.md) 报告。提交 Pull Request 时写明用户可见的变化、执行过的验证、兼容性影响和尚未覆盖的环境。

请把真实令牌、配置、日志、数据库和聊天记录留在本机。示例配置使用环境变量引用。使用各底座公开接口测试，无需修改已安装底座。
