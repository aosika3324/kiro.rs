# Design: Kiro `codewhisperer` 端点（移植 Kiro-Go 的 CodeWhisperer route）

**Date:** 2026-07-01
**Branch:** `feat/switch-codewhisperer-endpoint`
**Status:** Implemented, build green, 9/9 unit tests passing

## 背景

用户反馈 kiro.rs 的 `ide` 端点在某些账号下出错，希望参考 `C:\Users\Admin\Kiro-Go`
（Go 版 reverse proxy）的 CodeWhisperer route，让请求**不经过** `ide` 端点。

经对比两个仓库，发现 Kiro-Go 维护了三条上游 route（`proxy/kiro.go:33-52`）：

| Name | URL | Origin | AmzTarget |
|---|---|---|---|
| Kiro IDE | `q.us-east-1.amazonaws.com/generateAssistantResponse` | `AI_EDITOR` | *(无)* |
| CodeWhisperer | `codewhisperer.us-east-1.amazonaws.com/generateAssistantResponse` | `AI_EDITOR` | `AmazonCodeWhispererStreamingService.GenerateAssistantResponse` |
| AmazonQ | `q.us-east-1.amazonaws.com/generateAssistantResponse` | `AI_EDITOR` | `AmazonQDeveloperStreamingService.SendMessage` |

kiro.rs 已有 `ide`（对应 Kiro IDE route）和 `cli`（AWS JSON 协议，path/origin/protocol
都不同）。两者都不等于 CodeWhisperer route。

## 关键结论

Kiro-Go 的 CodeWhisperer route **与 kiro.rs 的 `ide` 端点仅差两处**：

| | us-east-1 host | `x-amz-target` | path / origin / content-type / profileArn 注入 / UA |
|---|---|---|---|
| `ide`（现状） | `q.us-east-1.amazonaws.com` | 不发送 | `/generateAssistantResponse`、`AI_EDITOR`、`application/json` |
| CodeWhisperer | `codewhisperer.us-east-1.amazonaws.com` | `AmazonCodeWhispererStreamingService.GenerateAssistantResponse` | 与 `ide` 完全一致 |

CodeWhisperer = **ide + 1 个 `x-amz-target` 头 + us-east-1 用 codewhisperer host**。
其余完全不变，故风险面非常窄。

## 主机区域逻辑（复用已有 helper）

host 逻辑复用 `external_idp_host(api_region)`（`src/kiro/model/credentials.rs:508`，
已有单测覆盖）：

- `us-east-1`（或空）→ `codewhisperer.us-east-1.amazonaws.com`
- 其它区域 → `q.{region}.amazonaws.com`

这与 Kiro-Go 的 `regionalizeURLForRegion` 行为一致：CodeWhisperer REST 主机族**仅存在于
us-east-1**，其余区域一律由区域化 Amazon Q 主机服务，不存在 `codewhisperer.{region}`。

`ide` 端点只对 `external_idp` 凭据这样做，其它凭据仍用 `q.{config.region}`。
新 `codewhisperer` 端点**对所有凭据类型一致**走 `external_idp_host`——这正是让
us-east-1 的 social/api_key/idc 凭据也命中 `codewhisperer.us-east-1.amazonaws.com`
主机的关键差异。

## 选择机制（「不经过 ide」的实现方式）

不修改硬编码，复用已有的按凭据/配置选择机制（`provider.rs:215`）：

- **全局**：`config.defaultEndpoint = "codewhisperer"` → 所有请求走新端点。
- **按凭据**：单条凭据设 `endpoint = "codewhisperer"`。

内置默认仍保留 `"ide"`（opt-in，便于回滚）。

## 改动文件

| 文件 | 改动 |
|---|---|
| `src/kiro/endpoint/codewhisperer.rs`（新建） | `CodewhispererEndpoint` 实现 `KiroEndpoint`，clone `ide` + `x-amz-target` + 对所有凭据用 `external_idp_host`。含 9 个单测。 |
| `src/kiro/endpoint/mod.rs` | `pub mod codewhisperer;` + `pub use codewhisperer::CodewhispererEndpoint;` |
| `src/main.rs` | 端点注册表追加 `codewhisperer`（与 ide/cli 并列）；import 更新 |
| `README.md` | 凭据字段表 + 常用字段表注明 `codewhisperer` 可选 |

## 测试

新端点单测（9 项，全部通过）覆盖：

1. `name()` 返回 `"codewhisperer"`
2. us-east-1 下**所有凭据类型**（含非 external_idp）命中 `codewhisperer.us-east-1.amazonaws.com`
3. external_idp + us-east-1 → codewhisperer host
4. 非 us-east-1 区域（eu-central-1）→ `q.eu-central-1.amazonaws.com`
5. `x-amz-target` 取值与 Kiro-Go 逐字符一致
6. `profileArn` 注入：有值 / 无值 / 覆盖 / 非法 JSON 四种情况

`cargo build` 通过，`cargo test codewhisperer` 9/9 通过。

## Non-goals（按选定的 scope A 排除）

- 不移植 Kiro-Go 的三条 route + `preferredEndpoint` / `endpointFallback` 自动失败转移
  （那是更大的 scope C，留待后续按需扩展）。
- 不修改现有 `ide` / `cli` 端点。
- 不把内置默认改为 `codewhisperer`。

## 已知风险 / 注意事项

- **CodeWhisperer ≈ ide**：若 `ide` 的报错根因**不是**缺 `x-amz-target` 头或 host 不对
  （例如 profileArn / token / 请求体校验问题），改走 codewhisperer 端点也不会修复。
  需要实际跑一次拿到上游错误 body 才能确认根因。
- 构建依赖：本仓库用 `rust-embed` 嵌入 `admin-ui/dist`，新 clone 该目录不存在
  （gitignored）会导致整个 crate 编译失败。需先 `cd admin-ui && bun install && bun run build`
  生成产物；或放置占位文件以解锁编译。
