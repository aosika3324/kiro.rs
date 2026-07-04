# 设计文档:调度整体换 Kiro-Go 账号池模型

任务 #9。取消 kiro.rs 现有的优先级 + P2C + EWMA + 账号级并发上限 + 大请求惩罚,选账号策略整体换成 Kiro-Go 的账号池模型。**保留分组隔离**。

## 决策(已与用户对齐)
1. **去掉账号级并发上限**(纯 Kiro-Go):单账号无并发上限,过载快速透传给上游,不本地排队。
2. **保留分组隔离**:API Key → 分组 → 账号子池的层级不变,只在每个分组内换成 Kiro-Go 式选账号。
3. **先设计(本文档)后实现**。
4. **保留统计持久化 + 禁用原因**(kiro.rs 已有,Kiro-Go 无——不倒退)。

## 1. 背景

kiro.rs 现调度(`token_manager.rs`)比 Kiro-Go 复杂:`ranked_available_credentials` 按 `effective_load(含EWMA错误率惩罚) → priority → id` 排序,`try_acquire_lease` 走账号级信号量,balanced 模式用 P2C 抽选,Long 档大请求走 `biased_load` 降权。用户认为 Kiro-Go 的**加权轮询 + 无并发上限 + 固定冷却**更适合高并发生产(过载快速失败而非本地排队)。

## 2. 现状 → 目标

| 维度 | kiro.rs 现状 | 目标(Kiro-Go 式) |
|---|---|---|
| 选账号 | effective_load→priority→id 排序 + P2C | 分组内加权轮询(weight 复制账号条目) |
| 并发 | 账号级 Semaphore(可 per-account 覆盖) | **无上限**,直接透传 |
| 冷却 | 可配 `throttled_until` | 固定:连续 3 次失败→1min;配额 402→1h |
| 健康度 | EWMA 错误率(0~1)连续值 | 二值:冷却中 / 正常 |
| 大请求 | large_penalty 降权 | 无差异 |
| 分组 | 保留 | **保留**(分组内轮询) |
| 统计/禁用原因 | 持久化 | **保留** |

## 3. 保留 vs 移除 vs 新增

**移除**:`CredentialRuntime` 信号量 / `try_acquire_lease` / `acquire_idle_permit` P2C / `biased_load` 大请求惩罚 / `ewma_error` 排序 / `cap_of` 并发上限 / balanced-priority 双模。相关 Admin `maxConcurrency`/`loadBalancingMode` 配置与前端 UI 一并移除。

**保留**:分组匹配 `credential_matches_request(model, group)` / `throttled_until` 字段(改由固定冷却驱动)/ `DisabledReason` 持久化 / 统计(success/failure/last_used)持久化 / 余额巡检自动恢复。

**新增**:分组内加权轮询游标(`round_robin_cursor: HashMap<groupKey, AtomicUsize>` 或按 weight 展开的候选列表);Kiro-Go 式固定冷却常量。

## 4. 选账号算法(对齐 Kiro-Go GetNextExcluding)

分组过滤后,在该分组的候选账号上加权轮询:

```
select(model, group, excluded) -> Option<id>:
  cands = entries.filter(matches(model, group) && !disabled)   # 分组隔离保留
  n = cands.len(); if n == 0 { return None }
  cursor = round_robin_cursor[group_key]                         # 每分组一个原子游标
  # 第一轮:找一个"完全可用"的账号
  for _ in 0..n:
     idx = (cursor.fetch_add(1)) % n
     acc = cands[idx]
     if excluded.contains(acc.id)      { continue }
     if now < cooldown[acc.id]         { continue }   # 冷却中跳过
     if token_expiring_soon(acc)       { continue }   # 快过期跳过
     if quota_blocked(acc)             { continue }   # 配额用尽跳过
     return Some(acc.id)
  # 第二轮兜底:全在冷却 → 返回冷却最早到期的(排除配额用尽)
  return earliest_cooldown_or_any(cands, excluded)
```

**加权轮询**:weight 通过按权重复制候选条目实现(weight=3 → 列表里出现 3 次),对齐 Kiro-Go `p.accounts` 展开。**weight 来源(决策)**:现有 `priority`(数字越小越优先)语义与 weight(越大越多流量)相反。方案 A—复用 priority 反转为 weight(如 `weight = max(1, 100/priority)`),用户既有配置不失效;方案 B—新增独立 `weight` 字段,priority 弃用,迁移时把老 priority 映射一次。**建议方案 A**(零迁移、不破坏用户既有优先级直觉),文档默认 A。**无并发上限、无 in_flight 排序、无 P2C**。

## 5. 故障 / 冷却状态机(对齐 account_failover.go + RecordError)

请求失败后按错误类型分派(对齐 Kiro-Go `handleAccountFailure`):

| 错误类型 | 判据(对齐 IsAuthFailure/IsSuspensionError) | 动作 |
|---|---|---|
| 配额 402 / OVERAGE | overage 标记 | `cooldown = now+1h`(`MarkOverLimit`) |
| 一般错误(5xx/超时/网络) | 其它 | `error_count++`;连续 ≥3 → `cooldown = now+1min`(`RecordError`) |
| Auth 失效 401/403 | bad credentials/invalid_grant/unauthorized | `DisableAccount`(持久化 `DisabledReason::AuthRevoked` + 24h 安全网冷却) |
| Suspension | temporarily_suspended / no available profile | `DisableAccount`(需人工恢复) |

成功后 `error_count[id] = 0`(清连续计数)。冷却状态存 `cooldown: HashMap<id, Instant>`(替代当前 `throttled_until` 语义,固定时长)。保留 kiro.rs 的 `DisabledReason` 持久化 + 余额巡检自动恢复(配额账号余额恢复后清 disabled)——比 Kiro-Go 二值 Enabled/Disabled 更强,不倒退。

## 5.5 重试/故障转移循环(评审补充:原计划漏了,是硬伤)

现状:`provider.rs:call_api_with_retry` 循环 `max_retries` 次,每次 `acquire_context(model,group)` 拿一个 `CallContext`(含 lease),失败调 `report_failure(id)`(返回"是否还有可用账号")驱动换账号重试。`acquire_context_sized`(token_manager.rs:2101)内部 `max_attempts = total_count_in_group × MAX_FAILURES_PER_CREDENTIAL`。

问题:Kiro-Go 的换账号靠 `GetNextExcluding(excluded map)` —— **每次重试把已失败账号加入 excluded,下一轮轮询跳过**。kiro.rs 现在没有"excluded 集合"概念,是靠信号量+排序自然错开。去掉信号量后,若 `select` 不接受 excluded,重试可能**反复选中同一个刚失败的账号**(轮询游标虽会前进,但小池/单账号场景会立刻绕回)。

**必须新增**:`select(model, group, excluded: &HashSet<u64>)`,重试循环维护 excluded 集合,对齐 Kiro-Go。`CallContext` 保留(用于统计 + 请求生命周期),但内部 `_permit: CredentialLease` 字段移除(无信号量);`acquire_context` 改为 `select`+构造无 lease 的 CallContext。`report_failure` 语义保留但不再依赖信号量。这一节不设计清楚,实现会在重试路径上卡住或死循环。

## 5.6 CallContext / in_flight 去留(评审补充)

`CallContext`(token_manager.rs:1462)当前含 `_permit: CredentialLease`,drop 时减 in_flight。去并发上限后:
- `CredentialLease`/信号量删除。
- **`in_flight` 计数是否保留?** —— dashboard 活跃账号统计(dashboard.tsx:396-402)、健康度前端(concurrency-monitor.tsx)全依赖它。**决策**:in_flight 降级为纯统计计数(acquire 时 +1、CallContext drop 时 -1,无上限约束),保留给前端展示;或彻底删除、前端相应移除"在途/活跃账号"展示。建议保留纯统计(前端改动小,仍有观测价值),文档默认此选项。

## 6. 迁移面清单(文件:改动)

| 文件 | 改动 |
|---|---|
| `token_manager.rs` | 删 `ranked_available_credentials`/`try_acquire_lease`/`acquire_idle_permit`/`biased_load`/`CredentialRuntime` 信号量/`cap_of`;新增 `select`(加权轮询)+ 每分组游标 + 固定冷却常量;`report_failure`/`report_quota_exhausted` 改按错误类型分派固定冷却;`acquire_context`/`acquire_context_sized` 简化为 `select` |
| `token_manager.rs`(字段) | `CredentialEntry` 去 `throttled_until` 可配语义 → 固定 `cooldown`;`CredMetrics` 去 `ewma_error`/`ewma_duration`(或保留仅展示不参与调度);`in_flight` 若无并发上限则降为纯统计 |
| `model/config.rs` | 弃用 `account_max_concurrency`/`load_balancing_mode`/`account_throttle_cooldown_secs`;新增固定冷却常量(或设为常量不可配) |
| `admin/types.rs`+`service.rs` | 去 `maxConcurrency`/`maxConcurrencyOverride`/`loadBalancingMode` 的 GET/PUT;`CredentialStatusItem` 去 `max_concurrency`/`in_flight`(或保留 in_flight 作展示) |
| `admin-ui` | settings-page 去负载均衡模式切换 UI;credential-card 去并发上限编辑(`ConcurrencyCapCell`)、去在途/上限展示;concurrency-monitor 汇总条重构或移除 |
| `main.rs` | 调度相关注入调整 |

**连锁风险点**:并发上限移除 → `ConcurrencyCapCell`、`CredentialScheduleMetrics`(我刚加的成功率/累计调度还能留)、`ConcurrencyMonitorSummary` 都要改。EWMA 展示(健康度前端 #8)若调度不再用 EWMA,可保留作纯观测或移除。

## 7. 测试计划
- 单测:分组内加权轮询顺序、weight 复制、冷却跳过、全冷却兜底(选最早到期)、配额跳过、连续 3 次失败进冷却、配额 402 进 1h 冷却、auth 失效禁用、成功清连续计数、分组隔离(A 组请求不选 B 组账号)。
- 删除现有并发/P2C/EWMA 排序/大请求惩罚相关测试。
- 集成:测试服并发打多请求,观测账号轮询分布 + 冷却行为。

## 8. 风险
- **触及每请求账号选择,生产在跑真实流量**——务必测试服充分验证再上。
- **去并发上限**:单账号可能被打爆(上游 429 激增),靠冷却兜底;高并发下轮询是否够分散需观测。
- **移除面广**:并发/负载均衡/EWMA 及其 Admin+前端连锁大,爆炸半径远超缓存改造。建议独立分支、独立部署验证。
- **weight 语义变更**:priority→weight 反转需明确迁移,避免用户既有优先级配置失效无提示。

