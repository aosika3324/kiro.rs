# kiro-rs 长上下文耗时与 Claude 原生一致性研究

> 本文档面向后续代码审阅 / 方案核查。引用代码处均附 `文件:行号`（基于当前工作副本，
> 行号可能随后续改动漂移，以函数名为准）。目标是请 Claude Opus 4.8 核查：诊断是否
> 成立、哪些优化能保持 Claude 原生一致性、哪些优化会改变语义。

## 背景问题

通过 kiro-rs 代理使用 Claude Code / Anthropic Messages API 时，输入 token 达到
`160K-215K` 后，单轮请求耗时明显变长。截图中可见：

- `4K` 级输入通常在数秒内完成。
- `160K-215K` 级输入常见十几秒到几十秒。

用户关心：

1. 是否可以在保持 Claude 原生一致性的前提下提升速度。
2. 是否能利用真实上游 prompt cache，而不是仅做本地 cache 计量。
3. 为什么 Kiro-Go / kiro-go-plus 使用体感更快。
4. 如果借鉴 Kiro-Go，会不会导致“降智”或破坏 Claude Code 原生行为。

## 当前 kiro-rs 方案

当前 kiro-rs 更偏向 **Claude 原生一致 / pass-through**：

1. **代理自身不做会话压缩 / 摘要 / 总历史截断。**
   `/v1/messages` 请求转换为 Kiro `conversationState` 后完整发给上游：
   `src/anthropic/handlers.rs:653-680`。

2. **1M 模型按 1M context window 折算。**
   `claude-sonnet-4.6/4.8`、`claude-opus-4.6/4.7/4.8` 返回 `1_000_000`：
   `src/anthropic/converter.rs:210-222`。
   因此 `160K-215K` 只占 16%-21.5%，严格按 1M 原生逻辑不会触发 auto-compact。

3. **usage 上报已改为不低于本地真实估算。**
   流式路径 `StreamContext::resolved_usage` 使用
   `max(contextUsage 折算值, 本地 count_all_tokens)`：
   `src/anthropic/stream.rs:1112-1117`。
   非流式路径 `resolve_usage_input_tokens` 同样取 max：
   `src/anthropic/handlers.rs:364-377`。
   这能防止 usage 低估导致客户端永远不 compact，但不会减少当前请求体。

4. **本地 cache_metering 是计量，不是真实上游缓存。**
   注释明确说明 Kiro 上游不下发 `cache_creation/cache_read` token 字段，项目在中转层
   自行模拟提示词缓存计量：`src/anthropic/cache_metering.rs:1-19`。

5. **只有单字段安全截断，不做总 prompt 缩短。**
   `src/text_truncate.rs` 默认仅在单字段超过约 `680_000` bytes 时截断，目的是避免
   `CONTENT_LENGTH_EXCEEDS_THRESHOLD`，不是会话 compact：
   `src/text_truncate.rs:25-29`。

## 核心判断

大 token 慢的主要原因不是账号调度，而是单请求模型侧 prefill / 长上下文处理成本。
多账号并发调度能提升吞吐，不能让一个 `200K` token 请求变成 `4K` token 的处理成本。

所以：

- 严格保持 Claude 原生完整上下文：`160K-215K` 慢属于预期。
- 要显著变快：必须减少模型实际处理的上下文，或确认并利用真实上游 prompt cache。
- 仅增加服务器带宽通常不够。带宽只影响上传 / 转发，不能消除上游模型读取 200K token
  的计算成本。

## 真实上游缓存可行性

当前代码库没有发现真实上游缓存协议入口：

1. Anthropic 入参类型接收 `cache_control`：
   `src/anthropic/types.rs:199-216`、`src/anthropic/types.rs:244-246`、
   `src/anthropic/types.rs:249-273`。

2. 但转换成 Kiro 请求时，`cache_control` 没有进入 Kiro wire format：
   - system 只拼接 `.text`：`src/anthropic/converter.rs:1031-1040`
   - tool 只保留 `name/description/input_schema`：`src/anthropic/converter.rs:968-976`

3. Kiro 请求结构只有：
   - `conversationState`
   - `profileArn`
   - `additionalModelRequestFields`

   见 `src/kiro/model/requests/kiro.rs:32-50`。没有 prompt cache 字段。

4. provider 请求头也没有 prompt cache / beta / cache-control 上游字段：
   - IDE endpoint：`src/kiro/endpoint/ide.rs:85-101`
   - CLI endpoint：`src/kiro/endpoint/cli.rs:71-88`

5. 上游 `meteringEvent` 实测只含 credit，不含 token/cache 字段：
   `src/kiro/model/events/metering.rs:1-7`。

### 结论

当前 kiro-rs 不能通过开关启用“真实上游 prompt cache”。现有缓存只是本地模拟 usage
口径。若要做真实上游缓存，需要先证明 Kiro 后端存在可用协议字段、header 或透明缓存。

### 建议实测

用同账号、同模型、同 session、同稳定大前缀，连续发两轮请求：

1. 记录第一轮和第二轮：
   - total duration
   - first-token latency
   - upstream credits
   - contextUsagePercentage
2. 对比带 / 不带 Anthropic `cache_control` 的请求。
3. 开 debug 日志核查实际上游 body/header 是否出现任何 cache 相关字段。

如果第二轮明显更快且 credits 下降，可能存在透明前缀缓存；否则不能指望 cache 解决
`200K` 长上下文耗时。

## Kiro-Go / kiro-go-plus 为什么体感更快

本机对比了：

- `/home/miku/workspack/Kiro-Go`
- `/home/miku/workspack/kiro-go-plus`

结论：Kiro-Go 快主要不是因为严格原生 + 真实上游缓存，而是它更积极地减少 / 清理发给
Kiro 的内容。

### 1. Kiro-Go 有总请求体 900KB 裁剪

`maxPayloadBytes = 900 * 1024`：
`/home/miku/workspack/Kiro-Go/proxy/translator.go:49-57`。

超过后会丢最旧历史，只保留 system priming、最近几轮、active tool turn 和当前消息，
并插入占位提示：
`/home/miku/workspack/Kiro-Go/proxy/translator.go:1624-1691`。

这会明显变快，但不是严格 Claude 原生完整上下文。

### 2. Kiro-Go 有 Claude Code system prompt 过滤 / 替换

它检测 Claude Code CLI 内置 system prompt 后，可替换成短 backend prompt：

- 入口：`/home/miku/workspack/Kiro-Go/proxy/translator.go:362-393`
- 替换文案：`/home/miku/workspack/Kiro-Go/proxy/translator.go:481-486`
- 开关：`/home/miku/workspack/Kiro-Go/config/config.go:645-653`

这能显著降 token，但会改变 Claude Code 原生系统提示。是否默认启用取决于运行时
config，不应视为严格原生行为。

### 3. Kiro-Go 会清理历史工具结构

历史里的 structured tool calls/results 会被扁平化、去重、清理空 assistant turn：
`/home/miku/workspack/Kiro-Go/proxy/translator.go:1481-1611`。

这能减少冗余、降低上游格式错误概率，但仍会改变传给模型的历史结构。

### 4. Kiro-Go 的 prompt cache 也是本地 tracker

`promptCacheTracker` 基于 fingerprint/TTL 在本地估算 `cache_read/cache_creation`：
`/home/miku/workspack/Kiro-Go/proxy/cache_tracker.go:55-69`。

handler 中 `Compute/Update` 只用于 response usage：
`/home/miku/workspack/Kiro-Go/proxy/handler.go:900-901`、
`/home/miku/workspack/Kiro-Go/proxy/handler.go:1255-1258`。

没有发现它把真实 cache 指令写入 Kiro payload/header。

### 5. Kiro-Go 同样按 1M window

Kiro-Go 对 Claude 4.6+ 也返回 `1_000_000`：
`/home/miku/workspack/Kiro-Go/proxy/kiro.go:570-583`。

所以它不是通过把 1M 降为 200K 来提前触发 compact。

## 可以借鉴且基本保持原生一致的优化

以下优化不改写 prompt，不删历史，不做代理侧摘要，因此更符合“原生一致”约束。

### 1. 连接复用 / HTTP2

当前 kiro-rs 请求上游时强制 `Connection: close`：
`src/kiro/provider.rs:505-510`。

Kiro-Go 使用可复用 HTTP transport，配置了：

- `MaxIdleConns`
- `MaxIdleConnsPerHost`
- `IdleConnTimeout`
- `ForceAttemptHTTP2`

见 `/home/miku/workspack/Kiro-Go/proxy/kiro.go:108-127`。

建议核查：kiro-rs 是否可以移除 `Connection: close`，并确保 reqwest client 池可复用
连接。该优化不改变模型输入，属于原生一致优化。预期改善网络/TLS/并发开销，但无法消除
200K token 模型计算成本。

### 2. 端点 / 区域选择

Kiro-Go 支持多个 endpoint：

- Kiro IDE
- CodeWhisperer
- AmazonQ

见 `/home/miku/workspack/Kiro-Go/proxy/kiro.go:32-51`。

kiro-rs 也有 endpoint 抽象和按账号 region 生成 URL：
`src/kiro/endpoint/ide.rs:63-79`、`src/kiro/endpoint/cli.rs:63-80`。

建议核查：

- 实际账号是否路由到最近/最快 region。
- 不同 endpoint 在同样 payload 下 first-token latency 是否差异明显。
- endpoint fallback 是否可能增加失败路径耗时。

### 3. token-aware 调度

当前账号调度已按在途负载率 / 并发 permit 优化，但一个 `4K` 请求和一个 `200K` 请求都占
一个 slot。可以考虑在不改 prompt 的前提下：

- 大 token 请求进入 long-context 队列。
- 大 token 请求占更多调度权重。
- 避免多个 200K 请求同时压同一账号或同一小账号池。

该优化只改变排队和账号选择，不改变模型输入，保持原生一致。它改善的是吞吐和队列延迟，
不是单个 200K 请求的模型计算时间。

### 4. 可观测性

建议 trace 中区分：

- 本地序列化请求体 bytes
- 本地估算 input tokens
- 上游 contextUsage 折算 input tokens
- first-token latency
- total duration
- selected endpoint / region
- 是否发生重试 / fallback

这能判断慢在传输、排队、模型 prefill 还是输出生成。

## 会破坏严格原生一致但可作为“快速模式”的优化

这些可以借鉴 Kiro-Go，但不建议作为默认原生模式：

1. 总 payload 上限裁剪旧历史。
2. 替换 Claude Code system prompt。
3. 过滤环境噪声、git 状态、边界 marker。
4. 扁平化历史 structured tool calls/results。
5. 提前触发 Claude Code compact（例如把有效窗口配置为 160K-200K）。

这些都可能提升速度，但会改变模型看到的上下文。风险包括：

- 丢失历史约束 / 文件细节 / 工具结果。
- 破坏 Claude Code 对工具使用历史的原生假设。
- 摘要或裁剪造成“降智”、误判任务状态。
- 调试时很难解释为什么代理行为不同于原生 Claude。

## 建议结论

如果目标是 **严格 Claude 原生一致**：

1. 保持完整 pass-through。
2. 不做代理侧总结 / 截断 / system prompt 替换。
3. 优先优化连接复用、HTTP2、endpoint/region、token-aware 调度。
4. 做真实上游缓存实测；没有协议证据前，不要把本地 cache_metering 当成真实性能优化。

如果目标是 **明显降低 160K-215K 请求耗时**：

1. 必须接受非严格原生策略。
2. 可做可选 Fast Mode：
   - payload 总大小上限
   - 保留最近历史
   - 插入明确 elision marker
   - 可选 Claude Code prompt filter
   - 可选提前 compact
3. Fast Mode 必须显式标注会改变上下文，不能默认伪装成原生。

## 需要 Claude Opus 4.8 核查的问题

1. 上述“当前 kiro-rs 没有真实上游 prompt cache 协议入口”的判断是否成立？
2. `Connection: close` 是否确实会妨碍 reqwest 连接复用，并值得移除？
3. Kiro-Go 的 900KB payload 裁剪 / prompt filter 是否是其体感更快的主因？
4. 是否存在 Kiro / Amazon Q Developer 私有协议中的真实缓存字段、header 或 event，本仓库尚未实现？
5. 在保持 Claude Code 原生行为的前提下，是否还有其它非语义层优化可做？
