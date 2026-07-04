# 设计文档:缓存计量改指纹模型(参考 Kiro-Go)

任务 #7。将 `src/anthropic/cache_metering.rs` 从**会话推断模型**改为 **Kiro-Go 式内容指纹 + 全局 LRU 物理命中模型**。

## 决策(已与用户对齐)
1. **去掉 R 留存阻尼**(纯 Kiro-Go 式):`read = 真实命中量`,不再压低。
2. **整体替换会话模型**:删除 `MeterGovernance`(会话表 / cold-warm / 高水位)及未提交的命中率 WIP,换纯指纹 LRU。
3. **先设计(本文档)后实现**。

---

## 1. 背景:为什么改

上游(CodeWhisperer/Kiro)**不返回真实的 cache_creation / cache_read**,这两个数字是本代理**为下游计费合成**的。当前 kiro.rs 用"会话 last_seen + cold/warm + 消息条数高水位"**逻辑推断**是否命中缓存——在 agent 工具循环、多对话共享同一 key、长短请求交错时容易偏(实测命中率卡 55%,warm 轮 creation 爆炸)。

Kiro-Go 用**内容 SHA256 指纹 + 全局 LRU(带 TTL)**:对消息前缀在每个 breakpoint 算指纹,在全局表里找**最长已命中前缀** → `read = 命中 token`,`creation = 超出的新 token`。这是**按内容物理匹配**,不猜,准。

---

## 2. 现状与目标的数据流对比

两边其实是**同一个两阶段结构**,只是"估命中"的方式不同:

- **请求阶段**:估算各消息块 token → 定 breakpoint → 判命中 → 得 `(input_est, creation_est, read_est)` + `prompt_total_est`(打包进 `CacheUsage`)。
- **响应阶段**:拿到上游真实 `input_tokens` → `split_against_total(real)` 把三桶按估算占比缩放到真实总量。

**本次只替换"请求阶段的判命中方式"**,响应阶段的比例缩放(`split_against_total`)结构保留(仅去掉 R 分支)。调用点签名尽量不动,降低爆炸半径。

---

## 3. 新数据结构(cache_metering.rs)

```
/// 单个缓存断点:前缀指纹 + 到此的累计 token + 该断点 TTL。
struct CacheBreakpoint { fingerprint: [u8;32], cumulative_tokens: i32, ttl_secs: u64 }

/// 一次请求的缓存画像(BuildProfile 产出)。
struct CachePrefixProfile {
    breakpoints: Vec<CacheBreakpoint>,  // 按前缀顺序,累计 token 单调增
    total_input_est: i32,
    model: String,
}

/// 全局 LRU 缓存表条目。
struct PrefixEntry { expires_at: i64 /*unix秒*/, ttl_secs: u64 }

/// 替代 MeterGovernance。全局共享(跨账号跨会话),带 TTL + LRU 容量上限。
pub struct PromptCacheTracker {
    inner: Mutex<TrackerInner>,   // entries: HashMap<[u8;32],PrefixEntry> + LRU order(VecDeque<[u8;32]>)
    max_entries: usize,
    default_ttl_secs: u64,        // breakpoint 无显式 cache_control.ttl 时用
    // 统计:hits/misses/evictions/expirations(AtomicI64),供 admin 可选展示
}
```

`CacheUsage`(保留,去掉 `read_ratio` 字段):
```
pub struct CacheUsage { input_est, creation_est, read_est, prompt_total_est: i32 }
```

## 4. 核心算法(对齐 Kiro-Go)

### 4.1 `build_profile(req, total_input_est) -> Option<CachePrefixProfile>`
对齐 `flattenClaudeCacheBlocks` + `BuildClaudeProfile`:
1. 展平可缓存块,顺序:**request_prelude(model+tool_choice) → tools[] → system[] → messages[]**。
2. 逐块:规范化 JSON(键排序、剥离 `cache_control`/位置键) → 滚动 SHA256 喂入 → 累加块 token(复用现有 `message_tokens`/`block_tokens`/`estimate_tokens`,口径不变)。
3. 断点判定:块自带 `cache_control` → 是断点(取其 TTL,`ephemeral 1h` vs `5m`);**首个显式断点出现后**,每个 message-end 边界成为隐式断点(继承 active TTL)——支持多轮命中更早前缀。
4. 每个断点记录 `{当前滚动哈希, 累计token, ttl}`。无断点 → 返回 None(视作无缓存)。

### 4.2 `PromptCacheTracker::compute(&profile) -> CacheUsage`
对齐 `Compute`:
1. 先 `prune_expired`(按 now 清过期条目)。
2. `last_tokens = min(最后断点累计, total_input_est)`;`max_cacheable = total_input_est * 0.85`(封顶,保证最新内容本轮不全命中),`last_tokens` 再 clamp 到 `max_cacheable`。
3. **倒序**扫断点:跳过低于 `min_cacheable(model)`(opus 阈值更高);命中未过期条目 → 刷新其 TTL + LRU 提前 → `matched = min(断点累计, last_tokens)`,break。
4. `creation_est = max(last_tokens - matched, 0)`,`read_est = matched`,`input_est = total_input_est - last_tokens`(本轮真正新问题),`prompt_total_est = total_input_est`。
5. 空表(首次):`read=0`,`creation = last_tokens`(若 ≥ 阈值,否则 0)。

### 4.3 `PromptCacheTracker::update(&profile)`
对齐 `Update`:请求后把所有 ≥ 阈值的断点指纹写入全局表(`expires_at = now + ttl`),LRU 提前,`evict_overflow` 保容量上限。**跨账号共享**(同内容不同账号复用,与 Kiro-Go C1 一致)。

### 4.4 `CacheUsage::split_against_total(real) -> (input, creation, read)`
保留现有比例缩放,**删掉 R 分支**:`read_base` 即真实命中,直接 `read = read_base`,不推回 input。

---

## 5. 调用点改造(handlers.rs)

`compute_cache_usage_for_key`(handlers.rs:469)——唯一实质改动点:
```
旧: g.observe_session(seed, now, msg_count) -> prev_n; compute_structural_cache_usage(payload, R, prev_n)
新: let profile = build_profile(payload, total_input_est)?;
    let usage = tracker.compute(&profile);
    tracker.update(&profile);   // 写回全局表
    usage
```
- 删掉 per-key `cache_read_ratio` 覆盖查询(R 已去除)。
- `tracker` 从 `AppState` 取(替换现有 `meter_governance` 注入,main.rs:282/309/336)。
- 其余调用点(stream.rs / openai)只依赖 `CacheUsage` + `split_against_total`,签名不变,**零改动**。

### 5.1 去 R 的完整清理面(评审补充:比原估大)
R(`cache_read_ratio`)不是只在 admin 层——它是一条贯穿 per-key 的完整链路,漏删会编译失败或留死代码。全部涉及:
- `model/config.rs:216/424/487` 全局 `cache_read_ratio` + default(保留字段读旧配置,标 deprecated 不再生效)
- `admin/client_keys.rs:74/248/328/386/419/464` per-key 存储/更新/查询 `cache_read_ratio_of`
- `anthropic/middleware.rs:39/170/180` 请求上下文注入 `cache_read_ratio`
- `anthropic/prompt_filter.rs:176`、`admin/service.rs:2228/2250/2275/2306/2328`、`admin/handlers.rs:1039/1151`、`admin/types.rs:518/539/880/950`
- 决策:**移除** per-key R 字段(client key 结构瘦身)还是**保留字段但计量不再消费**?建议移除,彻底。前端 client-key 编辑 UI 的 R 输入一并删。

### 5.2 与响应缓存(response_cache)的边界(评审补充:必须澄清)
`response_cache`(handlers.rs:625,`resolve_response_cache`)是**独立的一层**——缓存真实响应体、命中直接返回不打上游(我之前测代理时就被它的相同 msg_id 命中干扰过)。本次指纹计量 tracker 与它**完全无关、互不复用**:计量 tracker 只算 usage 数字,response_cache 存响应体。设计须保证两者独立:响应缓存命中时**不**跑指纹 compute/update(不打上游就无 usage 事件)。实现时在 response_cache 命中分支之后、真实上游调用路径上才 compute/update。

## 6. 可调命中率参数(前端旋钮)

去掉 R 后,命中率不再由单一阻尼系数控制,而是由指纹模型的**物理参数**决定。这些常数全部做成 **Admin 前端可调 + 运行时热更新 + 持久化**(对齐现有 `RuntimeGovernanceConfig` 范式),让运营方无需改代码即可调缓存表现:

| 参数 | 含义 | 默认 | 调大 → | 调小 → |
|---|---|---|---|---|
| `cacheMaxRatio` | 单请求可命中占总 input 的上限(85% 封顶,保证最新内容本轮不全命中) | 0.85 | 命中率↑(更激进,可能虚高) | 命中率↓(更保守) |
| `cacheMinTokens` | 断点最小可缓存 token 阈值(非 opus) | 1024 | 小块不进缓存,命中率↓ | 更多小块可缓存,命中率↑ |
| `cacheMinTokensOpus` | opus 模型的最小阈值(opus 缓存门槛更高) | 4096 | 同上(仅 opus) | 同上(仅 opus) |
| `cacheTtlSecs` | breakpoint 无显式 `cache_control.ttl` 时的默认 TTL | 300 | 缓存留存久,跨请求命中率↑ | 过期快,命中率↓ |
| `cacheMaxEntries` | 全局 LRU 容量上限(超出淘汰最旧) | 20000 | 容纳更多前缀,高并发命中率↑ | 省内存,命中率↓ |

**边界校验**:`cacheMaxRatio ∈ [0.5,1.0]`;`cacheMinTokens/Opus ≥ 0`;`cacheTtlSecs ∈ [0,86400]`;`cacheMaxEntries ∈ [100,1_000_000]`。非法值拒绝并保留旧值。

前端在设置页「缓存 / 配额治理」卡片内,把原 R 输入替换为这 5 个参数的输入框(数字 + 说明 + 保存),并可选显示实时命中率统计(hits/misses/entries/命中率%)供调参参考。

## 7. 配置 / 持久化 / Admin 集成

- **config.rs**:`cache_read_ratio` 弃用(保留字段读旧配置不报错,标 deprecated;不再生效)。新增 `cache_max_ratio` / `cache_min_tokens` / `cache_min_tokens_opus` / `cache_ttl_secs`(复用旧 `cache_meter_ttl_secs`)/ `cache_max_entries`,各带 `default_*` 与 serde 默认,兼容旧配置文件。
- **PromptCacheTracker**:上述参数存为 tracker 的运行时可变字段(`Mutex`/`Atomic`),Admin 改动即时生效(对齐 EndpointRouting/MeterGovernance 的共享 Arc 热更新范式)。
- **持久化**(对齐 Kiro-Go C3):**首版即落盘** `data/prompt_cache.json` 存指纹表——启动 Load(丢弃已过期条目)+ 定期 flush(脏标记,如 30s)+ 退出 flush,跨重启保命中率。
- **Admin 运行时治理**(service.rs / types.rs `RuntimeGovernanceConfig`):移除 `cacheReadRatio`(含 per-key);新增上述 5 个参数字段 + GET/PUT。可选暴露 tracker 命中率统计。
- **前端**(admin-ui):`api/credentials.ts` + `hooks` + `settings-page.tsx` 缓存卡片:删 R 输入,加 5 个参数旋钮 + 命中率展示。

## 9. 迁移面清单(文件:改动)

| 文件 | 改动 |
|---|---|
| `src/anthropic/cache_metering.rs` | 删 `MeterGovernance`/`observe_session`/`last_turn_creation_start`/`compute_structural_cache_usage` + 命中率 WIP;新增 `CacheBreakpoint`/`CachePrefixProfile`/`PromptCacheTracker`/`build_profile`/`compute`/`update`;`CacheUsage` 去 `read_ratio`;`split_against_total` 去 R 分支;新增 canonicalize+sha256 |
| `src/anthropic/handlers.rs` | `compute_cache_usage_for_key` 改指纹流程;删 per-key R |
| `src/main.rs` | `meter_governance` → `PromptCacheTracker` 构造/注入(含 5 个可调参数) |
| `src/model/config.rs` | `cache_read_ratio` deprecated;新增 `cache_max_ratio`/`cache_min_tokens`/`cache_min_tokens_opus`/`cache_ttl_secs`/`cache_max_entries` + 默认值 |
| `src/admin/types.rs`+`service.rs` | 运行时治理去 R 字段;加 5 个参数字段 + GET/PUT + 校验;可选命中率统计 |
| `admin-ui` `api/credentials.ts`+`hooks`+`settings-page.tsx` | 缓存卡片删 R 输入,加 5 个参数旋钮 + 命中率展示 |

## 10. 测试计划
- 单测:首请求(read=0,creation=whole)、命中最长前缀(read=matched)、部分命中(creation=delta)、过期→miss、LRU 淘汰、`cacheMaxRatio` 封顶、opus 阈值、跨账号共享命中、canonicalize 幂等(键序/cache_control 剥离不改指纹)、`split_against_total` 无 R、参数校验边界。
- 删掉/替换旧的会话/cold-warm/高水位测试(约 15 个)。
- 集成:测试服实打多轮对话,观测 `/api/admin` 命中率统计与 usage 输出,调 5 个参数看命中率变化,对比改造前。

## 11. 风险
- **持久化文件并发/损坏**:flush 用临时文件+rename 原子写;损坏/缺失时当空表启动(best-effort,不 fatal)。
- **token 估算口径**:沿用现有 estimate,保证与历史计费连续。
- **breakpoint 提取覆盖度**:必须覆盖 tool_use/tool_result/image/system 多态,否则指纹漂移;测试重点。
- **cache_metering.rs 有未提交 WIP**:整体替换会**丢弃那批命中率 WIP**(用户已确认)。实现前先 `git stash`/记录该 WIP 内容备查。
