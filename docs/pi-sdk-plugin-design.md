# Pi 公开 SDK 独立会话插件设计

> 状态：设计，尚未实现或通过原生联调。本文不代表已有安装包、Pi 插件入口或新运行时配置。当前 Pi RPC 适配器、流式、工具、子智能体、输入控制和工作空间行为保持不变，使用方法见 [底座配置](runtimes.md#网关托管-rpc)。

目标是在 Pi 中安装 Open Push Channel 插件，由插件通过公开 SDK 管理独立 `AgentSession`，再连接统一网关。远端聊天不注入用户正在操作的 TUI 会话，也不修改 Pi 核心或已安装依赖。本文供下一轮实现与验收使用；开发流程见 [CONTRIBUTING](../CONTRIBUTING.md)。

## 为什么不直接向当前 TUI 会话发送输入

已检查的 Pi 1.3.0 `ExtensionAPI.sendUserMessage()` 返回 `void`，异步拒绝通过扩展错误通道报告。普通事件上下文没有 `newSession()` 或 `switchSession()`；这些方法属于命令上下文，替换会话后旧上下文失效。`input.source === "extension"` 也不标识具体扩展或请求。

因此，不能用正文匹配、时间窗口或下一个 `agent_start` 推测某次远端输入已被原生接收。特别是同文请求、慢输入钩子、本机用户输入和其他扩展输入交错时，推测会把输出挂到错误的远端请求。`ReplacedSessionContext` 虽提供返回 Promise 的发送方法，但只在替换后的回调中出现，不等于普通插件自动获得了当前宿主的完整会话句柄。

独立 SDK 会话让插件直接持有创建结果，订阅、调用、排队、取消和关闭都归属于同一个确定的原生对象。它是插件内嵌 Pi SDK，不是把现有 TUI 会话变成远端可控会话。

## 拟议交付边界

```text
业务前端 / frontend-demo
          │ 现有客户端协议
Push Channel 网关 + SQLite
          │ 插件连接协议
安装到 Pi 的 Push Channel 插件
          ├── 独立 SDK 会话 A：客户端 / 智能体 / 会话 A
          └── 独立 SDK 会话 B：客户端 / 智能体 / 会话 B

Pi 原本的 TUI 会话：不接收上述远端输入，不参与其事件投递
```

- 网关负责认证、授权、会话绑定、请求去重、统一事件持久化和回放，不创建 Pi 进程或读取 Pi 私有会话文件。
- 插件负责 SDK 会话、原生资源、事件转换和关闭，模型、工具及权限执行仍由 Pi SDK 完成。
- 一个远端会话对应一个独立 `AgentSession`；同会话的普通输入串行，原生控制仍属于当前 run。不得用一个当前会话轮流服务不同 conversation。
- 每个原生会话的回调直接闭包绑定其所有者，不按输出文本、模型名、当前 UI 选择或下一条事件猜归属。
- 这不是操作系统沙箱。同进程插件和工具仍具有宿主进程权限；需要更强隔离时继续使用受控子进程及独立系统权限。

拟采用独立 Pi package 交付，复用统一协议和插件连接客户端，不捆绑第二份 Pi。按 Pi package 的现有约定，宿主提供的 Pi 包声明为 peer，而非运行时 dependencies；支持版本另用契约测试与明确的兼容范围说明。包名、入口、配置和安装步骤在实现验收后确定，本轮不添加 `pi.extensions` 或 `./pi` 导出。[Pi package 合同](https://github.com/earendil-works/pi/blob/1d01b3b1f8de2e67a3a1c1694b14c984e4edd156/packages/coding-agent/docs/packages.md)

## 已核对的 SDK 合同

核对对象为本机安装的 `@earendil-works/pi-coding-agent@1.3.0`：同时读取了包根导出、声明和实际实现。下表是源码事实，不是已经通过本项目集成测试的承诺。

| 公开表面                      | 已核对事实                                                                                                                                            | 设计使用方式                                                                    |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| 包根导出                      | 导出 `createAgentSession`、`AgentSession`、`DefaultResourceLoader`、`SessionManager`、`SettingsManager`、`ModelRuntime`、`AgentSessionRuntime`        | 运行时代码只从包根导入，不深导入 `dist/core/**`                                 |
| `createAgentSession(options)` | 可显式提供 cwd、agentDir、modelRuntime、model、settingsManager、resourceLoader、sessionManager 和工具配置；返回 session、扩展结果及可能的模型回退提示 | 所有资源归属在创建前确定；不得忽略模型回退提示后冒充原模型                      |
| `SessionManager`              | `create()`、`open()`、`inMemory()` 是公开方法；持有精确原生 session ID                                                                                | 新建与精确恢复分开；恢复失败不换一个新会话继续执行                              |
| `AgentSession.subscribe()`    | 返回取消订阅函数；包含正文、工具、队列及运行事件                                                                                                      | 先订阅，再提交输入；重连不重新创建订阅或重跑输入                                |
| `AgentSession.prompt()`       | 返回 `Promise<void>`；可拒绝，运行结束或输入被扩展处理后也可返回                                                                                      | 结合该对象的真实活动与最终事件判断结果，不能把 Promise 返回当作一次成功模型回答 |
| `steer()`、`followUp()`       | 返回原生 `"queued"` 或 `"handled"`；扩展可处理或变换输入                                                                                              | 回执只表达原生事实，不推断逐条消费或完成                                        |
| 队列 API                      | 有 `queue_update`、`getSteeringMessages()`、`getFollowUpMessages()` 和 `clearQueue()`                                                                 | 展示原生队列快照；复制只读数组，不修改原生返回值                                |
| 终态                          | `message_end` 是权威消息；`agent_end` 后仍可能有自动恢复或排队工作；`agent_settled` 才表明不会自动继续                                                | 不把工具分段、低层结束或最后一条 delta 当作 run 成功                            |
| 取消与关闭                    | `abort()` 等待 idle；`dispose()` 为同步清理。`AgentSessionRuntime.dispose()` 先等待扩展 shutdown handlers，再 dispose 当前 session                    | 关闭顺序需单独实现和验证，不能只调用同步 dispose 就宣称所有扩展资源已关闭       |

导出的 `PromptOptions.preflightResult` 注释明确为 RPC 内部 hook；本设计不依赖它，也不访问 AgentSession 私有字段。公开 SDK 概述见 [Pi SDK](https://github.com/earendil-works/pi/blob/1d01b3b1f8de2e67a3a1c1694b14c984e4edd156/packages/coding-agent/docs/sdk.md)。

## 会话、资源与权限隔离

### 精确绑定与恢复

每个 SDK session 固定绑定 `clientId + runtimeId + agentId + conversationId`，运行再关联独立 `runId`。注册连接不创建模型会话；在首次实际执行或显式恢复时准备原生对象。不同客户端、智能体或会话不能复用同一对象；连接断开不是修改绑定的理由。

已额外核对原生 ID 的创建路径：公开 `NewSessionOptions` 含 `id?: string`，`SessionManager.create(cwd, sessionDir, { id: nativeSessionId })` 将该选项传入构造器；`newSession()` 校验后使用指定 ID，不要求修改私有字段。未来实现可将网关已经分配的 native ID 作为新建 ID，并在创建后用 `getSessionId()` 再次精确确认。此处只记录公开合同，本轮不修改网关协议或为 Pi 增加配置。

指定 ID 不自动提供唯一性或持久恢复：原生文件名还含时间戳，同 ID 反复调用 create 可能产生另一份历史。因此拟在插件侧的智能体作用域 SQLite 中保留唯一 owner、native ID、受控原生文件路径与创建/派发状态；具体 schema、路径和升级方案需在实现前确定。先事务性领取唯一 owner，再创建并记录 SDK 返回的路径，最后才允许派发。不得仅用重启即丢失的内存 Map，也不得把网关 UUID 当作未经确认的原生身份。

使用现有 Pi 智能体独立 home、workspace 和原生 session 存储策略，不与 OpenClaw、dsh 或其他 Pi 智能体目录混用。同一个智能体的不同会话可以共享其工作文件，但上下文和原生会话必须独立。网关绑定和回放索引继续使用 SQLite；不新增插件 JSON/JSONL 映射侧车。Pi 自己的原生会话格式属于外部工具合同，不手工重写或复制为网关数据库。

恢复只能打开持久 owner 记录指向的精确原生会话，校验原生 ID、所属智能体及工作目录。已检查的 `SessionManager.open()` 在路径不存在时可能准备一个新会话，并不是“缺失必拒绝”的恢复 API；插件必须先检查受控路径真实存在，再调用 SDK 并核对 ID，拒绝缺失、空白或错误所有者的记录，不能把它当作新建继续执行。不能使用“最近会话”搜索，也不能允许浏览器提交任意文件路径。

新建 manager 的路径也不等于文件已经落盘：原生 SDK 在出现会话消息前可能只持有内存 setup 记录。需要覆盖“已保留 ID 但未派发”“已派发但未收到结果”“文件缺失”的分别恢复；派发结果未知时不自动重跑，也不重建同 ID 掩盖历史丢失。旧 RPC 历史不在本设计中自动迁移，不手改其 JSONL header。

### 模型和认证不能假定继承

默认 SDK 工厂会按自己的 agentDir 创建模型运行时、设置、存储和资源加载器，并不等于继承当前 TUI 内存中的所有注册内容。

- 显式指定智能体自己的 cwd、agentDir、配置来源与模型选择，拒绝使用宿主进程偶然的默认目录。
- `ctx.model` 可描述当前模型，但单独复制模型对象不能证明目标 ModelRuntime 有对应 provider、虚拟路由或认证。
- `ctx.modelRegistry` 是兼容 facade，其底层 runtime 为私有字段；不能通过强转或私有属性取出共享。
- 自定义 provider、运行时密钥、OAuth 刷新、虚拟模型、输入钩子和扩展注册的迁移/重建方式，均需针对真实配置验证。失败时明确报错，不降级到另一模型或直调模型 HTTP API。
- 不把模型凭据、网关 token 或 provider 配置放进浏览器、统一事件、日志或真实会话样本。插件连接认证不应变成模型可调用的工具参数。

是否按智能体共享一个显式管理的 ModelRuntime，还是每会话独立创建，需要先验证 provider 注册和认证变更的可见性、撤销及资源生命周期；本设计不预先宣称哪一种能完整继承 TUI 配置。

### 技能、工具和扩展必须显式装配

`DefaultResourceLoader` 可配置技能、提示模板、上下文文件和扩展来源；`CreateAgentSessionOptions` 也可声明工具允许/禁止项、自定义工具。应按服务端可信策略装配，不从浏览器接收可执行模块路径或工具配置。

CLI 内置的 MCP、codemode、tool search 不是 SDK 工厂自动提供的同等配置。若需要它们，必须通过公开工厂与生命周期显式加载，并证明权限钩子、项目信任和工具暴露与设计一致。技能加载成功也不等于所有扩展权限机制被继承。

防止递归加载 Push Channel 必须发生在扩展执行之前：

1. 建立 SDK 会话资源允许列表，排除 Channel 宿主入口；需要的子智能体工具使用单独、窄的注册组件，不重启整套 Channel。
2. 不单独依赖 `extensionsOverride` 过滤自身：已检查的加载器先执行扩展装载，再调用 override，此时 factory 副作用已经发生。
3. `noExtensions` 仍可允许显式 additional extension paths；若采用它，必须同时控制所有显式路径和 inline factories。无法用公开默认加载器证明安全时，设计自有 `ResourceLoader`，不修改原生加载器。
4. 禁止把“禁用全部扩展”描述为完整继承权限；如某项权限由扩展提供，必须明确不支持或显式装配后验证。

现有子智能体实现暂不搬移、不复制。后续若复用，需要同时证明子会话资源策略、工具权限、身份谱系与关闭；未通过前不声明新 SDK 插件具备子智能体能力。

## 拟议运行与故障合同

1. **宿主启动**：factory 只注册；宿主会话/服务启动后建立插件连接。握手不调用模型，不导入其他底座，不扫描原生历史。
2. **请求准入**：验证客户端授权、精确会话、请求幂等与当前锁。插件持有自己的 SDK session，不对用户 TUI 发消息。
3. **派发和接收**：订阅后调用该 session 的 `prompt()`，分别记录派发事实、原生接受事实和最终结果。初始化、模型或认证明确拒绝时不得标为已接受。输入被扩展 handled、不产生模型活动、慢钩子失败等情况必须在原生验证后确定非成功结果，不用超时或正文匹配伪造回执。
4. **实时输出**：正文增量、推理、工具和子智能体按真实原生身份转换；`message_end` 校准最终文本而非再次追加。重试可能更正已有预览，不能把失败草稿拼成第二份最终回复。
5. **控制与结束**：同一对象上的控制操作串行，保留原生 queued/handled。等待控制回执与运行最终静止后再释放锁；按当前队列合同处理剩余内容并明确展示。`abort()` 不代替 `clearQueue()` 的证明，取消和完成也不能互相覆盖。
6. **断线与关闭**：断线不自动重发输入；采用网关现有“结果未知、不重新执行”的恢复规则。停止新准入，等待在途输入/控制，按已批准的运行取消策略停止 SDK 工作，等待原生 idle 与扩展 shutdown，取消订阅并释放本插件拥有的对象和连接。不得 dispose 用户 TUI 或其他插件的会话。

SDK 工厂返回的裸 session 与 CLI 使用的完整生命周期包装不同；扩展 `session_start`、MCP 连接、shutdown 等需要显式绑定和关闭。优先评估公开 `AgentSessionRuntime` 是否满足独立会话 owner 的需要；具体组装、Promise 与 settled 的先后关系、失败清理仍需下一轮原生测试，不能仅凭类型宣布完成。

## 实现前必须通过的验收门槛

| 门槛       | 最小证据                                                                                                     |
| ---------- | ------------------------------------------------------------------------------------------------------------ |
| 安装与兼容 | 独立打包安装、导入包根成功；不加载第二份 Pi，不依赖宿主源码路径；卸载后 Pi 原本可用                          |
| 无模型启动 | 临时 home/workspace 中握手成功；不产生 provider 请求、不加载自身第二次，不更改真实配置                       |
| 会话归属   | 两客户端、两 conversation、同文并发输入分别得到独立 native ID 和回调；TUI 与其他扩展运行不串入远端           |
| 资源与权限 | 技能、内置/自定义工具、MCP、provider、认证与信任策略各有显式正反例；禁止项确实拒绝                           |
| 接收与拒绝 | 慢输入钩子、handled、transform、异常、缺模型/认证均有明确结果；未决输入不盲目重发                            |
| 流式与校准 | 原生首段 delta 在 settled 前可见；Unicode、最终修订、工具穿插、重试后正文与权威消息一致且只显示一次          |
| 控制与取消 | 原生 queued/handled、队列快照、迟到回执、取消清队列和同会话复用均有证据；不推断逐条消费                      |
| 恢复与回放 | 指定原生 ID 与 owner 唯一；重启保留索引；缺失历史不被 open/create 变成新会话；精确恢复与纯回放不增加执行次数 |
| 资源清理   | 正常关闭、启动失败、运行错误、断线和宿主 shutdown 后无本插件遗留连接、定时器、会话或子进程                   |
| 端到端     | 真实原生 SDK 输出、统一网关事件与 frontend-demo 三段对照；仅夹具/类型检查不算真实联调                        |

先写并执行可复现上述边界的 RED，再实现最小插件；按 [开发指南](../CONTRIBUTING.md) 选择窄测试、审查和原生验收。未通过的能力保持关闭。当前这份设计没有运行模型、容器、SDK 生命周期探针或新插件测试，也没有为新方案声明任何测试通过。

## 合同证据与维护

以下文件路径相对安装的 Pi 包根，仅用于审计；不是允许生产代码深导入的路径：

- `package.json`、`dist/index.d.ts`：包根 exports 与可用 SDK 导出。
- `dist/core/sdk.d.ts`、`dist/core/sdk.js`：显式配置边界、默认资源创建、模型恢复与回退。
- `dist/core/agent-session.d.ts`、`dist/core/agent-session.js`：prompt、输入控制、队列、终态、订阅和 dispose。
- `dist/core/extensions/types.d.ts`、`dist/core/extensions/runner.js`：普通/命令/替换上下文与失效边界。
- `dist/core/resource-loader.d.ts`、`dist/core/resource-loader.js`：扩展执行顺序、资源发现和 override 的时机。
- `dist/core/model-runtime.d.ts`、`dist/core/model-registry.d.ts`：模型运行时与认证 facade，禁止访问的私有底层字段。
- `dist/core/session-manager.d.ts`、`dist/core/agent-session-runtime.js`：精确会话存储及异步 shutdown owner。

升级 Pi、改变资源继承或安装形态时重新核对这些合同与验收门槛；在线最新文档不能替代当前安装版本的实际声明和行为。本文为中文设计记录，无对应英文运行指南；只有实现与原生验收完成后才能更新 README 的当前能力承诺。
