//! 客户端 API Key 管理
//!
//! 管理中转站下发的客户端 Key。生成值以 `sk-` 开头；鉴权不校验前缀，只按完整值匹配。
//!
//! 与上游 Kiro 凭据（`KiroCredentials`，`ksk_*`）相互独立：
//! - 上游凭据池：服务对接 Kiro 的"出口"
//! - 客户端 Key：中转站对外的"入口"
//!
//! 持久化为 `client_api_keys.json`（与 `credentials.json` 同目录）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Utc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

/// 单条客户端 Key
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientKey {
    pub id: u64,
    /// 明文 Key（中转站场景，校验需原值，不做 hash）
    pub key: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub disabled: bool,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<String>,
    #[serde(default)]
    pub total_calls: u64,
    #[serde(default)]
    pub total_input_tokens: u64,
    #[serde(default)]
    pub total_output_tokens: u64,
    #[serde(default)]
    pub total_cache_creation_tokens: u64,
    #[serde(default)]
    pub total_cache_read_tokens: u64,
    /// 是否启用中转层 prompt cache 计量与命中。
    ///
    /// 老数据无此字段时默认 true，避免升级后已有 Key 行为变化。
    #[serde(default = "default_cache_enabled")]
    pub cache_enabled: bool,
    /// 提示词过滤（per-key，默认关）：精简 Claude Code system prompt（检测到则整段替换）。
    /// 老数据无此字段时默认 false（不过滤，行为不变）。
    #[serde(default, skip_serializing_if = "is_false")]
    pub simplify_cc_prompt: bool,
    /// 提示词过滤（per-key，默认关）：去边界标记（删 `--- SYSTEM PROMPT ---` 等分隔行）。
    #[serde(default, skip_serializing_if = "is_false")]
    pub strip_boundary_markers: bool,
    /// 提示词过滤（per-key，默认关）：去环境噪音（删 # Environment 段、gitStatus 等行）。
    #[serde(default, skip_serializing_if = "is_false")]
    pub strip_env_noise: bool,
    /// 快速模式（per-key，默认关，首字延迟优先）。开启后对该 Key 的请求：
    /// 1) payload 截断用更小的字节上限（全局 `fastModeMaxPayloadBytes`，默认 400KB）→ 丢更多旧历史；
    /// 2) 强制开启三个提示词过滤（simplify_cc / strip_boundary / strip_env_noise），覆盖各自单独设置。
    /// 不动 web_search / usage-gated streaming / 计费。老数据无此字段时默认 false（行为不变）。
    #[serde(default, skip_serializing_if = "is_false")]
    pub fast_mode: bool,
    /// 响应缓存 per-key 覆盖（None = 跟随全局 `responseCacheEnabled`；Some(true/false) = 强制开/关）。
    /// 老数据无此字段时为 None（跟随全局，行为不变）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_cache_enabled: Option<bool>,
    /// 响应缓存 TTL per-key 覆盖（秒；None 或 0 = 跟随全局 `responseCacheTtlSecs`）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_cache_ttl_secs: Option<u32>,
    /// 缓存计量 read 留存阻尼 R per-key 覆盖 ∈ [0,1]（None = 跟随全局 `cacheReadRatio`）。
    /// 控制该 Key 的 read 桶留存比例（被砍部分推回 input，不触碰 creation）。老数据无此字段时为 None。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_ratio: Option<f64>,
    /// 缓存计量 multiplier 护栏上限 per-key 覆盖（None = 跟随默认 1.25）。`weighted/baseline` 超此
    /// 值时把 input→read 压回（不碰 creation）。收紧到 1.0 留足检测余量；老数据无此字段时为 None。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_multiplier_cap: Option<f64>,
    /// Anthropic 标准计费模式（per-key，默认关）。开启后该 Key 的 usage 走**真实互斥三桶口径**
    /// （`input + creation + read == total`，绝不超报/双重收费）；利润来自 R 挪桶（read→input）。
    /// 与关闭（默认）的唯一区别：标准模式**不施加 multiplier_cap 护栏**（接受更高检测风险换 margin）。
    /// creation 形状由 [`Self::cache_creation_ratio`] 定。老数据无此字段时默认 false。
    #[serde(default, skip_serializing_if = "is_false")]
    pub anthropic_billing_mode: bool,
    /// 标准模式 creation 占比 per-key 覆盖 ∈ [0,1]（None = 跟随默认 3%）。`creation = cacheable ×
    /// creation_ratio`，定"每轮写多少缓存"的形状;与 R 正交,二者都不破坏 sum==total。老数据无此字段时 None。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_ratio: Option<f64>,
    /// **目标缓存率 T** per-key 覆盖 ∈ [0,1]（None = 跟随全局默认，即 `cacheReadRatio`）。
    /// 面板 `cache_read/总prompt` 逼近此值；生效值在入口按全局 `cacheHitRateMax` 夹紧。
    /// 取代旧 `cache_read_ratio`（R 留存）语义——读取优先级 `cache_hit_rate ?? cache_read_ratio
    /// ?? 全局`（见 [`ClientKeyStore::cache_hit_rate_of`]）。老数据无此字段时 None。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_hit_rate: Option<f64>,
    /// **缓存热度 TTL** per-key 覆盖（秒；None 或 0 = 跟随全局 `cacheMeterTtlSecs`）。距上次请求
    /// 超此值 → 本轮转 cold（整段前缀按 creation 重写、read=0）。老数据无此字段时 None。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_ttl_secs: Option<u64>,
    /// **已废弃**（标准模式改互斥口径后不再超报，此字段被忽略）。保留仅为老配置反序列化兼容。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_inflation: Option<f64>,
    /// **已废弃**（标准模式改互斥口径后 input 由结构占比折算，不再钉常数）。保留仅为老配置反序列化兼容。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anthropic_input_tokens: Option<i32>,
    /// 累计 credit 计费量（meteringEvent.usage 累加）
    #[serde(default)]
    pub total_credits: f64,
    /// 绑定的账号分组名（可选）
    ///
    /// 设置后，用该 Key 发起的请求只会调度到 groups 包含此分组名的上游账号（严格隔离）。
    /// None 表示不绑定分组，可使用全部账号。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// 系统 Key（由 config.json apiKey 同步，不可删除、可轮换）。
    /// 老数据无此字段，默认 false。
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_system: bool,
}

/// `by_key` 仅用于判重；鉴权扫描 `entries` 并做常量时间比较。
pub struct ClientKeyManager {
    inner: RwLock<Inner>,
    path: Option<PathBuf>,
}

struct Inner {
    entries: HashMap<u64, ClientKey>,
    by_key: HashMap<String, u64>,
    next_id: u64,
}

impl ClientKeyManager {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Inner {
                entries: HashMap::new(),
                by_key: HashMap::new(),
                next_id: 1,
            }),
            path: None,
        }
    }

    /// 从文件加载（不存在时返回空管理器）
    pub fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let entries: Vec<ClientKey> = if path.exists() {
            let content = std::fs::read_to_string(&path)?;
            if content.trim().is_empty() {
                Vec::new()
            } else {
                serde_json::from_str(&content)?
            }
        } else {
            Vec::new()
        };

        let mut by_key = HashMap::with_capacity(entries.len());
        let mut by_id = HashMap::with_capacity(entries.len());
        let mut max_id = 0u64;
        for ck in entries {
            max_id = max_id.max(ck.id);
            by_key.insert(ck.key.clone(), ck.id);
            by_id.insert(ck.id, ck);
        }

        Ok(Self {
            inner: RwLock::new(Inner {
                entries: by_id,
                by_key,
                next_id: max_id + 1,
            }),
            path: Some(path),
        })
    }

    fn save_locked(&self, inner: &Inner) {
        let path = match &self.path {
            Some(p) => p,
            None => return,
        };
        let mut list: Vec<&ClientKey> = inner.entries.values().collect();
        list.sort_by_key(|k| k.id);
        match serde_json::to_string_pretty(&list) {
            Ok(json) => {
                if let Err(e) = std::fs::write(path, json) {
                    tracing::warn!("写入客户端 Key 文件失败: {}", e);
                }
            }
            Err(e) => tracing::warn!("序列化客户端 Key 失败: {}", e),
        }
    }

    /// 列表（按 id 升序）
    pub fn list(&self) -> Vec<ClientKey> {
        let inner = self.inner.read();
        let mut list: Vec<ClientKey> = inner.entries.values().cloned().collect();
        list.sort_by_key(|k| k.id);
        list
    }

    /// 生成并保存新 Key。
    pub fn create(
        &self,
        name: String,
        description: Option<String>,
        group: Option<String>,
        cache_enabled: bool,
        prompt_filters: (bool, bool, bool),
    ) -> ClientKey {
        if cache_enabled {
            self.create_with_key_full(
                name,
                description,
                group,
                generate_client_key(),
                true,
                prompt_filters,
            )
        } else {
            self.create_with_key_full(
                name,
                description,
                group,
                generate_client_key(),
                false,
                prompt_filters,
            )
        }
    }

    /// 用指定明文创建 Key（带缓存与提示词过滤设置）。bootstrap 系统密钥走 `sync_system_key`。
    fn create_with_key_full(
        &self,
        name: String,
        description: Option<String>,
        group: Option<String>,
        plaintext: String,
        cache_enabled: bool,
        prompt_filters: (bool, bool, bool),
    ) -> ClientKey {
        let mut inner = self.inner.write();
        if let Some(&id) = inner.by_key.get(&plaintext) {
            return inner
                .entries
                .get(&id)
                .cloned()
                .expect("by_key 与 entries 应一致");
        }
        let id = inner.next_id;
        inner.next_id += 1;
        let entry = ClientKey {
            id,
            key: plaintext.clone(),
            name,
            description,
            disabled: false,
            created_at: Utc::now().to_rfc3339(),
            last_used_at: None,
            total_calls: 0,
            total_input_tokens: 0,
            total_output_tokens: 0,
            total_cache_creation_tokens: 0,
            total_cache_read_tokens: 0,
            cache_enabled,
            simplify_cc_prompt: prompt_filters.0,
            strip_boundary_markers: prompt_filters.1,
            strip_env_noise: prompt_filters.2,
            fast_mode: false,
            response_cache_enabled: None,
            response_cache_ttl_secs: None,
            cache_read_ratio: None,
            cache_multiplier_cap: None,
            anthropic_billing_mode: false,
            cache_creation_ratio: None,
            cache_hit_rate: None,
            cache_ttl_secs: None,
            cache_read_inflation: None,
            anthropic_input_tokens: None,
            total_credits: 0.0,
            group: group.filter(|g| !g.trim().is_empty()),
            is_system: false,
        };
        inner.by_key.insert(plaintext, id);
        inner.entries.insert(id, entry.clone());
        self.save_locked(&inner);
        entry
    }

    /// 将 `config.apiKey` 同步为唯一的 `id=0` 系统 Key。配置值变化时保留元数据与统计、
    /// 重新启用新值，并删除与新旧明文冲突的非系统条目，使旧值立即失效。
    pub fn sync_system_key(&self, name: String, description: Option<String>, plaintext: String) {
        let mut inner = self.inner.write();
        let previous_key = inner.entries.get(&0).map(|entry| entry.key.clone());
        let mut changed = false;

        if let Some(entry) = inner.entries.get_mut(&0) {
            if entry.key != plaintext {
                entry.key = plaintext.clone();
                entry.disabled = false;
                changed = true;
            }
            if !entry.is_system {
                entry.is_system = true;
                changed = true;
            }
        } else {
            inner.entries.insert(
                0,
                ClientKey {
                    id: 0,
                    key: plaintext.clone(),
                    name,
                    description,
                    disabled: false,
                    created_at: Utc::now().to_rfc3339(),
                    last_used_at: None,
                    total_calls: 0,
                    total_input_tokens: 0,
                    total_output_tokens: 0,
                    total_cache_creation_tokens: 0,
                    total_cache_read_tokens: 0,
                    cache_enabled: true,
                    simplify_cc_prompt: false,
                    strip_boundary_markers: false,
                    strip_env_noise: false,
                    fast_mode: false,
                    response_cache_enabled: None,
                    response_cache_ttl_secs: None,
                    cache_read_ratio: None,
                    cache_multiplier_cap: None,
                    anthropic_billing_mode: false,
                    cache_creation_ratio: None,
                    cache_hit_rate: None,
                    cache_ttl_secs: None,
                    cache_read_inflation: None,
                    anthropic_input_tokens: None,
                    total_credits: 0.0,
                    group: None,
                    is_system: true,
                },
            );
            changed = true;
        }

        let entries_before = inner.entries.len();
        inner.entries.retain(|id, entry| {
            *id == 0
                || (entry.key != plaintext
                    && previous_key
                        .as_deref()
                        .map(|old_key| entry.key != old_key)
                        .unwrap_or(true))
        });
        changed |= inner.entries.len() != entries_before;

        for (id, entry) in inner.entries.iter_mut() {
            if *id != 0 && entry.is_system {
                entry.is_system = false;
                changed = true;
            }
        }

        let by_key: HashMap<String, u64> = inner
            .entries
            .iter()
            .map(|(id, entry)| (entry.key.clone(), *id))
            .collect();
        changed |= inner.by_key != by_key;
        inner.by_key = by_key;

        if changed {
            self.save_locked(&inner);
        }
    }

    pub fn delete(&self, id: u64) -> bool {
        let mut inner = self.inner.write();
        // 系统 Key 拒绝删除
        if inner.entries.get(&id).map(|e| e.is_system).unwrap_or(false) {
            return false;
        }
        let removed = match inner.entries.remove(&id) {
            Some(e) => {
                inner.by_key.remove(&e.key);
                true
            }
            None => false,
        };
        if removed {
            self.save_locked(&inner);
        }
        removed
    }

    pub fn set_disabled(&self, id: u64, disabled: bool) -> bool {
        let mut inner = self.inner.write();
        let updated = match inner.entries.get_mut(&id) {
            Some(e) => {
                e.disabled = disabled;
                true
            }
            None => false,
        };
        if updated {
            self.save_locked(&inner);
        }
        updated
    }

    pub fn update_meta(
        &self,
        id: u64,
        name: Option<String>,
        description: Option<Option<String>>,
        group: Option<Option<String>>,
        cache_enabled: Option<bool>,
        simplify_cc_prompt: Option<bool>,
        strip_boundary_markers: Option<bool>,
        strip_env_noise: Option<bool>,
        response_cache_enabled: Option<Option<bool>>,
        response_cache_ttl_secs: Option<Option<u32>>,
        cache_read_ratio: Option<Option<f64>>,
        cache_multiplier_cap: Option<Option<f64>>,
        anthropic_billing_mode: Option<bool>,
        cache_creation_ratio: Option<Option<f64>>,
        cache_hit_rate: Option<Option<f64>>,
        cache_ttl_secs: Option<Option<u64>>,
        fast_mode: Option<bool>,
    ) -> bool {
        let mut inner = self.inner.write();
        let updated = match inner.entries.get_mut(&id) {
            Some(e) => {
                if let Some(n) = name {
                    e.name = n;
                }
                if let Some(d) = description {
                    e.description = d;
                }
                if let Some(g) = group {
                    e.group = g.filter(|s| !s.trim().is_empty());
                }
                if let Some(enabled) = cache_enabled {
                    e.cache_enabled = enabled;
                }
                if let Some(v) = simplify_cc_prompt {
                    e.simplify_cc_prompt = v;
                }
                if let Some(v) = strip_boundary_markers {
                    e.strip_boundary_markers = v;
                }
                if let Some(v) = strip_env_noise {
                    e.strip_env_noise = v;
                }
                if let Some(v) = response_cache_enabled {
                    e.response_cache_enabled = v;
                }
                if let Some(v) = response_cache_ttl_secs {
                    // 0 视为"清除覆盖、跟随全局"
                    e.response_cache_ttl_secs = v.filter(|t| *t > 0);
                }
                if let Some(v) = cache_read_ratio {
                    // clamp 到 [0,1]；Some(None) 清除覆盖、跟随全局
                    e.cache_read_ratio = v.map(|r| r.clamp(0.0, 1.0));
                }
                if let Some(v) = cache_multiplier_cap {
                    // clamp 到 [0.1, 1.25]（下限=纯 read 桶权重，上限=真实 Anthropic 自然上限）；
                    // Some(None) 清除覆盖、跟随默认 1.25
                    e.cache_multiplier_cap = v.map(|r| {
                        r.clamp(
                            super::super::anthropic::cache_metering::WEIGHT_READ,
                            super::super::anthropic::cache_metering::DEFAULT_MULTIPLIER_CAP,
                        )
                    });
                }
                if let Some(v) = anthropic_billing_mode {
                    e.anthropic_billing_mode = v;
                }
                if let Some(v) = cache_creation_ratio {
                    // clamp 到 [0,1]；Some(None) 清除覆盖、跟随默认 3%
                    e.cache_creation_ratio = v.map(|r| r.clamp(0.0, 1.0));
                }
                if let Some(v) = cache_hit_rate {
                    // clamp 到 [0,1]；Some(None) 清除覆盖、跟随全局默认（生效时再按 max 夹紧）
                    e.cache_hit_rate = v.map(|r| r.clamp(0.0, 1.0));
                }
                if let Some(v) = cache_ttl_secs {
                    // 0 视为"清除覆盖、跟随全局"
                    e.cache_ttl_secs = v.filter(|t| *t > 0);
                }
                if let Some(v) = fast_mode {
                    e.fast_mode = v;
                }
                true
            }
            None => false,
        };
        if updated {
            self.save_locked(&inner);
        }
        updated
    }

    /// 返回指定 Key 绑定的分组名（None 表示未绑定或 Key 不存在）
    pub fn group_of(&self, id: u64) -> Option<String> {
        self.inner
            .read()
            .entries
            .get(&id)
            .and_then(|e| e.group.clone())
    }

    /// 返回指定 Key 是否启用 prompt cache。不存在时保守关闭。
    pub fn cache_enabled_of(&self, id: u64) -> bool {
        self.inner
            .read()
            .entries
            .get(&id)
            .map(|e| e.cache_enabled)
            .unwrap_or(false)
    }

    /// 返回指定 Key 是否启用快速模式（首字延迟优先）。不存在时保守关闭。
    pub fn fast_mode_of(&self, id: u64) -> bool {
        self.inner
            .read()
            .entries
            .get(&id)
            .map(|e| e.fast_mode)
            .unwrap_or(false)
    }

    /// 返回指定 Key 的响应缓存覆盖 `(enabled_override, ttl_secs_override)`。
    /// 两者均为 None 表示「跟随全局配置」。Key 不存在时返回 (None, None)。
    pub fn response_cache_cfg_of(&self, id: u64) -> (Option<bool>, Option<u32>) {
        self.inner
            .read()
            .entries
            .get(&id)
            .map(|e| (e.response_cache_enabled, e.response_cache_ttl_secs))
            .unwrap_or((None, None))
    }

    /// 返回指定 Key 的缓存命中率 R 覆盖（None = 跟随全局；Key 不存在时也返回 None）。
    /// **已废弃语义**：R 留存被 [`Self::cache_hit_rate_of`] 目标率取代，保留供兼容回退。
    pub fn cache_read_ratio_of(&self, id: u64) -> Option<f64> {
        self.inner
            .read()
            .entries
            .get(&id)
            .and_then(|e| e.cache_read_ratio)
    }

    /// 返回指定 Key 生效的**目标缓存率 T** 覆盖（None = 跟随全局默认）。
    /// 优先级：`cache_hit_rate ?? cache_read_ratio`（旧字段兼容）；Key 不存在时 None。
    pub fn cache_hit_rate_of(&self, id: u64) -> Option<f64> {
        self.inner
            .read()
            .entries
            .get(&id)
            .and_then(|e| e.cache_hit_rate.or(e.cache_read_ratio))
    }

    /// 返回指定 Key 的缓存热度 TTL 覆盖（秒；None 或 0 = 跟随全局；Key 不存在时 None）。
    pub fn cache_ttl_secs_of(&self, id: u64) -> Option<u64> {
        self.inner
            .read()
            .entries
            .get(&id)
            .and_then(|e| e.cache_ttl_secs)
            .filter(|v| *v > 0)
    }

    /// 返回指定 Key 的 multiplier 护栏上限覆盖（None = 跟随默认 1.25；Key 不存在时也返回 None）。
    pub fn cache_multiplier_cap_of(&self, id: u64) -> Option<f64> {
        self.inner
            .read()
            .entries
            .get(&id)
            .and_then(|e| e.cache_multiplier_cap)
    }

    /// 返回指定 Key 是否启用 Anthropic 标准计费模式（Key 不存在时 false）。
    pub fn anthropic_billing_mode_of(&self, id: u64) -> bool {
        self.inner
            .read()
            .entries
            .get(&id)
            .map(|e| e.anthropic_billing_mode)
            .unwrap_or(false)
    }

    /// 返回指定 Key 的标准模式 creation 占比覆盖（None = 跟随默认 3%；Key 不存在时也返回 None）。
    pub fn cache_creation_ratio_of(&self, id: u64) -> Option<f64> {
        self.inner
            .read()
            .entries
            .get(&id)
            .and_then(|e| e.cache_creation_ratio)
    }

    /// 返回指定 Key 的三个提示词过滤开关 (simplify_cc, strip_boundary, strip_env_noise)。
    /// Key 不存在时全 false（不过滤）。
    pub fn prompt_filters_of(&self, id: u64) -> (bool, bool, bool) {
        self.inner
            .read()
            .entries
            .get(&id)
            .map(|e| {
                (
                    e.simplify_cc_prompt,
                    e.strip_boundary_markers,
                    e.strip_env_noise,
                )
            })
            .unwrap_or((false, false, false))
    }

    /// 列出所有当前被引用的分组名（仅去重，不带计数）。
    pub fn used_group_names(&self) -> Vec<String> {
        let inner = self.inner.read();
        let mut set: std::collections::HashSet<String> = std::collections::HashSet::new();
        for e in inner.entries.values() {
            if let Some(g) = &e.group {
                set.insert(g.clone());
            }
        }
        let mut list: Vec<String> = set.into_iter().collect();
        list.sort();
        list
    }

    /// 统计指定分组被多少把 Key 绑定（用于分组管理页 / 删除前提示）。
    pub fn count_with_group(&self, group: &str) -> usize {
        self.inner
            .read()
            .entries
            .values()
            .filter(|e| e.group.as_deref() == Some(group))
            .count()
    }

    /// 指定 id 的 Key 是否为系统 Key（不存在也返回 false）。
    pub fn is_system(&self, id: u64) -> bool {
        self.inner
            .read()
            .entries
            .get(&id)
            .map(|e| e.is_system)
            .unwrap_or(false)
    }

    /// 把所有引用 `old` 的 Key 的 group 字段改为 `new`（分组改名级联用）。
    /// 返回受影响的 Key 数。
    pub fn rename_group(&self, old: &str, new: &str) -> usize {
        let mut inner = self.inner.write();
        let mut affected = 0usize;
        for entry in inner.entries.values_mut() {
            if entry.group.as_deref() == Some(old) {
                entry.group = Some(new.to_string());
                affected += 1;
            }
        }
        if affected > 0 {
            self.save_locked(&inner);
        }
        affected
    }

    /// 把所有引用 `name` 的 Key 的 group 字段清空（强删分组级联用）。
    /// 返回受影响的 Key 数。
    pub fn clear_group(&self, name: &str) -> usize {
        let mut inner = self.inner.write();
        let mut affected = 0usize;
        for entry in inner.entries.values_mut() {
            if entry.group.as_deref() == Some(name) {
                entry.group = None;
                affected += 1;
            }
        }
        if affected > 0 {
            self.save_locked(&inner);
        }
        affected
    }

    /// 生成新明文并保留 id、元数据、分组、统计及状态；旧明文立即失效。
    /// 系统 Key 的调用方必须同步 `config.apiKey`，否则重启时配置值会覆盖轮换结果。
    pub fn rotate(&self, id: u64) -> Option<ClientKey> {
        let new_key = generate_client_key();
        let mut inner = self.inner.write();
        let old_key = inner.entries.get(&id).map(|e| e.key.clone())?;
        inner.by_key.remove(&old_key);
        let entry = inner.entries.get_mut(&id)?;
        entry.key = new_key.clone();
        let snapshot = entry.clone();
        inner.by_key.insert(new_key, id);
        self.save_locked(&inner);
        Some(snapshot)
    }

    /// 重置计数（保留 Key 与名称）
    pub fn reset_stats(&self, id: u64) -> bool {
        let mut inner = self.inner.write();
        let updated = match inner.entries.get_mut(&id) {
            Some(e) => {
                e.total_calls = 0;
                e.total_input_tokens = 0;
                e.total_output_tokens = 0;
                e.total_cache_creation_tokens = 0;
                e.total_cache_read_tokens = 0;
                e.total_credits = 0.0;
                true
            }
            None => false,
        };
        if updated {
            self.save_locked(&inner);
        }
        updated
    }

    /// 不校验前缀，常量时间匹配所有启用 Key；命中后更新使用记录。
    pub fn verify_and_touch(&self, presented: &str) -> Option<u64> {
        let mut inner = self.inner.write();
        let mut hit_id: Option<u64> = None;
        for (id, ck) in inner.entries.iter() {
            if ck.disabled {
                continue;
            }
            if ck.key.as_bytes().ct_eq(presented.as_bytes()).into() {
                hit_id = Some(*id);
                // 不 break，继续完整扫描以保持常量时间
            }
        }
        let id = hit_id?;
        if let Some(entry) = inner.entries.get_mut(&id) {
            entry.total_calls += 1;
            entry.last_used_at = Some(Utc::now().to_rfc3339());
        }
        // 不在每次请求都落盘（高频写入），由 record_usage / 定期 flush 持久化
        Some(id)
    }

    /// 在请求结束时累计 Token 用量并落盘
    pub fn record_usage(
        &self,
        id: u64,
        input_tokens: u64,
        output_tokens: u64,
        cache_creation_tokens: u64,
        cache_read_tokens: u64,
        credits: f64,
    ) {
        let mut inner = self.inner.write();
        if let Some(entry) = inner.entries.get_mut(&id) {
            entry.total_input_tokens += input_tokens;
            entry.total_output_tokens += output_tokens;
            entry.total_cache_creation_tokens += cache_creation_tokens;
            entry.total_cache_read_tokens += cache_read_tokens;
            if credits.is_finite() && credits > 0.0 {
                entry.total_credits += credits;
            }
            entry.last_used_at = Some(Utc::now().to_rfc3339());
        }
        self.save_locked(&inner);
    }

    /// 获取统计后的 active Key 数（未禁用）
    pub fn active_count(&self) -> usize {
        self.inner
            .read()
            .entries
            .values()
            .filter(|e| !e.disabled)
            .count()
    }
}

impl Default for ClientKeyManager {
    fn default() -> Self {
        Self::new()
    }
}

fn is_false(b: &bool) -> bool {
    !b
}

fn default_cache_enabled() -> bool {
    true
}

/// 生成 `sk-` 前缀 + 32 位 URL-safe 随机字符串
pub fn generate_client_key() -> String {
    // OS CSPRNG（security::secure_token_urlsafe）而非 fastrand——对外分发的凭据须密码学安全。
    // 24 字节 → URL-safe base64 32 字符，熵充足。
    let body = crate::security::secure_token_urlsafe(24);
    format!("sk-{}", body)
}

/// 脱敏展示：保留前 8 个字符（含前缀）和后 4 个字符
pub fn mask_client_key(key: &str) -> String {
    let char_count = key.chars().count();
    if char_count <= 12 {
        return key.to_string();
    }
    let start: String = key.chars().take(8).collect();
    let end: String = key.chars().skip(char_count - 4).collect();
    format!("{start}...{end}")
}

pub fn default_path_in(dir: &Path) -> PathBuf {
    dir.join("client_api_keys.json")
}

pub type SharedClientKeyManager = Arc<ClientKeyManager>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_verify() {
        let mgr = ClientKeyManager::new();
        let entry = mgr.create("test".to_string(), None, None, true, (false, false, false));
        assert!(entry.key.starts_with("sk-"));
        assert_eq!(mgr.verify_and_touch(&entry.key), Some(entry.id));
        assert_eq!(mgr.verify_and_touch("nope"), None);
    }

    #[test]
    fn disabled_key_rejected() {
        let mgr = ClientKeyManager::new();
        let entry = mgr.create("test".to_string(), None, None, true, (false, false, false));
        mgr.set_disabled(entry.id, true);
        assert_eq!(mgr.verify_and_touch(&entry.key), None);
        mgr.set_disabled(entry.id, false);
        assert_eq!(mgr.verify_and_touch(&entry.key), Some(entry.id));
    }

    #[test]
    fn record_usage_accumulates() {
        let mgr = ClientKeyManager::new();
        let entry = mgr.create("test".to_string(), None, None, true, (false, false, false));
        mgr.record_usage(entry.id, 100, 50, 0, 0, 0.0);
        mgr.record_usage(entry.id, 200, 30, 5, 10, 1.5);
        let list = mgr.list();
        let e = list.iter().find(|x| x.id == entry.id).unwrap();
        assert_eq!(e.total_input_tokens, 300);
        assert_eq!(e.total_output_tokens, 80);
        assert_eq!(e.total_cache_creation_tokens, 5);
        assert_eq!(e.total_cache_read_tokens, 10);
    }

    #[test]
    fn cache_enabled_can_be_updated() {
        let mgr = ClientKeyManager::new();
        let entry = mgr.create("test".to_string(), None, None, false, (false, false, false));
        assert!(mgr.update_meta(
            entry.id,
            None,
            None,
            None,
            Some(true),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ));
        assert!(mgr.cache_enabled_of(entry.id));
    }

    #[test]
    fn fast_mode_defaults_off_and_can_be_updated() {
        let mgr = ClientKeyManager::new();
        let entry = mgr.create("test".to_string(), None, None, false, (false, false, false));
        // 默认关
        assert!(!mgr.fast_mode_of(entry.id));
        // 开启 fast_mode（最后一个参数）
        assert!(mgr.update_meta(
            entry.id,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(true),
        ));
        assert!(mgr.fast_mode_of(entry.id));
        // 关回去
        assert!(mgr.update_meta(
            entry.id,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(false),
        ));
        assert!(!mgr.fast_mode_of(entry.id));
        // 不存在的 key
        assert!(!mgr.fast_mode_of(999));
    }

    #[test]
    fn response_cache_override_can_be_updated() {
        let mgr = ClientKeyManager::new();
        let entry = mgr.create("test".to_string(), None, None, false, (false, false, false));
        // 默认无覆盖
        assert_eq!(mgr.response_cache_cfg_of(entry.id), (None, None));
        // 设置覆盖：开启 + ttl 60
        assert!(mgr.update_meta(
            entry.id,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(Some(true)),
            Some(Some(60)),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ));
        assert_eq!(mgr.response_cache_cfg_of(entry.id), (Some(true), Some(60)));
        // ttl=0 → 清除 ttl 覆盖（跟随全局）
        assert!(mgr.update_meta(
            entry.id,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(Some(0)),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ));
        assert_eq!(mgr.response_cache_cfg_of(entry.id), (Some(true), None));
    }

    #[test]
    fn mask_format() {
        assert_eq!(mask_client_key("sk-abcdefghijklmnop"), "sk-abcde...mnop");
        assert_eq!(mask_client_key("short"), "short");
        assert_eq!(mask_client_key("密钥🔐测试abcdefgh"), "密钥🔐测试abc...efgh");
    }

    #[test]
    fn cache_hit_rate_priority_and_compat() {
        // 目标缓存率 T 覆盖优先级：cache_hit_rate ?? cache_read_ratio（旧字段兼容）?? 全局(None)。
        let mgr = ClientKeyManager::new();
        let e = mgr.create("k".into(), None, None, false, (false, false, false));
        // 默认都没设 → None（跟随全局）。
        assert_eq!(mgr.cache_hit_rate_of(e.id), None, "未设 → 跟随全局");
        assert_eq!(mgr.cache_ttl_secs_of(e.id), None);

        // 只设旧 cache_read_ratio（兼容路径）→ cache_hit_rate_of 回退读它。
        let ok = mgr.update_meta(
            e.id, None, None, None, None, None, None, None, None, None,
            Some(Some(0.75)), // cache_read_ratio
            None, None, None, None, None, None,
        );
        assert!(ok);
        assert_eq!(mgr.cache_hit_rate_of(e.id), Some(0.75), "无 cache_hit_rate 时回退旧 cache_read_ratio");

        // 设新 cache_hit_rate → 覆盖旧字段（优先）。
        mgr.update_meta(
            e.id, None, None, None, None, None, None, None, None, None,
            None, None, None, None,
            Some(Some(0.9)), // cache_hit_rate
            Some(Some(120)), // cache_ttl_secs
            None,
        );
        assert_eq!(mgr.cache_hit_rate_of(e.id), Some(0.9), "cache_hit_rate 优先于 cache_read_ratio");
        assert_eq!(mgr.cache_ttl_secs_of(e.id), Some(120));

        // cache_ttl_secs=0 → 视为清除、跟随全局（cache_ttl_secs_of 过滤掉 0）。
        mgr.update_meta(
            e.id, None, None, None, None, None, None, None, None, None,
            None, None, None, None, None,
            Some(Some(0)),
            None,
        );
        assert_eq!(mgr.cache_ttl_secs_of(e.id), None, "ttl=0 → 跟随全局");
    }

    #[test]
    fn rotate_replaces_key_but_keeps_metadata_and_stats() {
        let mgr = ClientKeyManager::new();
        let entry = mgr.create(
            "kb".to_string(),
            Some("desc".into()),
            Some("groupA".into()),
            true,
            (false, false, false),
        );
        // 累计一些统计
        mgr.record_usage(entry.id, 100, 50, 5, 10, 1.5);
        let old_key = entry.key.clone();
        let rotated = mgr.rotate(entry.id).expect("rotate should succeed");
        assert_ne!(rotated.key, old_key);
        assert!(rotated.key.starts_with("sk-"));
        assert_eq!(rotated.id, entry.id);
        assert_eq!(rotated.name, "kb");
        assert_eq!(rotated.description.as_deref(), Some("desc"));
        assert_eq!(rotated.group.as_deref(), Some("groupA"));
        assert_eq!(rotated.total_input_tokens, 100);
        assert_eq!(rotated.total_output_tokens, 50);
        assert_eq!(mgr.verify_and_touch(&old_key), None);
        assert_eq!(mgr.verify_and_touch(&rotated.key), Some(entry.id));
    }

    #[test]
    fn rotate_unknown_id_returns_none() {
        let mgr = ClientKeyManager::new();
        assert!(mgr.rotate(999).is_none());
    }

    #[test]
    fn sync_system_key_uses_id_zero() {
        let mgr = ClientKeyManager::new();
        mgr.sync_system_key("默认密钥".into(), None, "custom-api-key".into());
        assert!(mgr.is_system(0));
        assert_eq!(mgr.list().first().map(|k| k.id), Some(0));
        assert_eq!(mgr.verify_and_touch("custom-api-key"), Some(0));
        mgr.sync_system_key("默认密钥".into(), None, "custom-api-key".into());
        assert_eq!(mgr.list().iter().filter(|k| k.is_system).count(), 1);
    }

    #[test]
    fn sync_system_key_replaces_config_value_and_revokes_old_key() {
        let mgr = ClientKeyManager::new();
        mgr.sync_system_key("默认密钥".into(), Some("初始描述".into()), "custom-a".into());
        mgr.update_meta(
            0,
            Some("保留名称".into()),
            Some(Some("保留描述".into())),
            Some(Some("group-a".into())),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        mgr.record_usage(0, 100, 50, 5, 10, 1.5);
        assert_eq!(mgr.verify_and_touch("custom-a"), Some(0));
        mgr.set_disabled(0, true);

        let conflicting = mgr.create_with_key_full(
            "冲突密钥".into(),
            None,
            None,
            "custom-b".into(),
            true,
            (false, false, false),
        );
        assert_ne!(conflicting.id, 0);

        mgr.sync_system_key("默认密钥".into(), None, "custom-b".into());

        assert_eq!(mgr.verify_and_touch("custom-a"), None);
        assert_eq!(mgr.verify_and_touch("custom-b"), Some(0));
        let entries = mgr.list();
        assert_eq!(entries.len(), 1);
        let system = &entries[0];
        assert_eq!(system.id, 0);
        assert!(system.is_system);
        assert_eq!(system.name, "保留名称");
        assert_eq!(system.description.as_deref(), Some("保留描述"));
        assert_eq!(system.group.as_deref(), Some("group-a"));
        assert!(!system.disabled);
        assert_eq!(system.total_input_tokens, 100);
        assert_eq!(system.total_output_tokens, 50);
    }

    #[test]
    fn system_key_cannot_be_deleted() {
        let mgr = ClientKeyManager::new();
        mgr.sync_system_key("默认密钥".into(), None, "custom-api-key".into());
        assert!(!mgr.delete(0), "系统密钥 id=0 不可删除");
        assert!(mgr.is_system(0));
    }
}
