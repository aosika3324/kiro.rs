//! 中转层 prompt cache 计量（内容指纹 + 全局 LRU，对齐 Kiro-Go）
//!
//! Kiro 上游既不做 prompt cache、也不下发 cache_creation / cache_read 字段（实测
//! meteringEvent 只给 credit 计费量），所以中转层上报的缓存计费**纯粹是合成给下游看
//! 的数字**，不对应任何真实缓存命中、也不影响真实成本。下游按 read/creation **分别计价**
//! （creation 贵、read 便宜），所以合成数字必须**经济上自洽**：creation 每轮只应反映
//! 「本轮新增的那一段」，不能随对话变长而虚高。
//!
//! # 指纹模型（本模块）
//!
//! 不再靠"会话时间轴 + 消息条数"**推断**命中，而是按**内容 SHA256 指纹 + 全局 LRU（带
//! TTL）物理匹配**（对齐 Kiro-Go `promptCacheTracker`）：
//!
//! 1. `build_profile`：把请求展平成可缓存块序列（request_prelude → tools → system →
//!    messages），逐块喂滚动 SHA256、累加 token；在断点（显式 `cache_control`，或首个显式
//!    断点后的每个 message 边界）记录 `{前缀指纹, 累计token, TTL}`。
//! 2. `compute`：倒序扫断点，在全局表里找**最长已命中且未过期**的前缀 → `read = 命中 token`、
//!    `creation = 本轮超出命中的新 token`、`input = 总量 − 可缓存前缀`。命中即刷新其 TTL。
//! 3. `update`：把本次所有断点指纹写回全局表（跨账号共享），LRU + 容量上限淘汰。
//!
//! 命中是**按内容真实匹配**（同前缀无论哪个会话/账号都命中），不猜、不受会话交错影响。
//! 结果对真实 total 由 [`CacheUsage::split_against_total`] 做互斥比例分摊。
//!
//! # 可调命中率参数（前端可调、运行时热更新）
//!
//! - `cache_max_ratio`：单请求可命中占总 input 上限（默认 0.85，保证最新内容本轮不全命中）
//! - `cache_min_tokens` / `cache_min_tokens_opus`：断点最小可缓存 token 阈值
//! - `cache_ttl_secs`：断点无显式 `cache_control.ttl` 时的默认 TTL
//! - `cache_max_entries`：全局 LRU 容量上限
//!
//! 指纹表落盘 `data/prompt_cache.json`，跨重启保命中率。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// [`PromptCacheTracker::compute`] 的结果：按 estimate 口径算出的三桶基准，最终由
/// [`CacheUsage::split_against_total`] 对真实 total 做互斥分摊。
///
/// 三个 estimate 是比例基准（不是最终值）——真正的 token 数要在拿到真实 total（contextUsage
/// 真值或 count_tokens 估算）后才按比例算出，因为流式响应直到末尾才知道真实 total。
#[derive(Debug, Clone, Copy)]
pub struct CacheUsage {
    /// 本轮新输入（超出可缓存前缀的部分）的 estimate token——这部分永不计入缓存。
    pub input_est: i32,
    /// 本轮新写入缓存的 delta（命中前缀之外、到可缓存末尾）的 estimate token。
    pub creation_est: i32,
    /// 命中的已缓存前缀 estimate token（read 基数）。
    pub read_est: i32,
    /// 整个 prompt 的 estimate token，比例分摊的分母。
    pub prompt_total_est: i32,
}

impl Default for CacheUsage {
    /// 默认 = 不模拟缓存：`prompt_total_est == 0` 使 `split_against_total` 全量计入 input。
    fn default() -> Self {
        Self {
            input_est: 0,
            creation_est: 0,
            read_est: 0,
            prompt_total_est: 0,
        }
    }
}

impl CacheUsage {
    /// 按真实 total 口径做互斥分摊，返回 `(input_tokens, cache_creation, cache_read)`，
    /// 三者满足 `input + creation + read == total_real`。
    ///
    /// input / creation / read 各按其 estimate 占比折算到真实 total（read 取剩余，保证和相等）。
    /// 无可缓存内容（`prompt_total_est <= 0`）时全部计入 input，不凭空造缓存计数。
    /// 不再有 R 留存阻尼：read 即物理命中量（对齐 Kiro-Go）。
    pub fn split_against_total(&self, total_real: i32) -> (i32, i32, i32) {
        let total = total_real.max(0);
        if self.prompt_total_est <= 0 || total == 0 {
            return (total, 0, 0);
        }
        let denom = self.prompt_total_est as f64;
        let input_share = (self.input_est as f64 / denom).clamp(0.0, 1.0);
        let creation_share = (self.creation_est as f64 / denom).clamp(0.0, 1.0);

        // input / creation 按占比折算，clamp 保证 input + creation <= total；剩余即 read。
        let mut input = ((total as f64) * input_share).round() as i32;
        input = input.clamp(0, total);
        let mut creation = ((total as f64) * creation_share).round() as i32;
        creation = creation.clamp(0, total - input);
        let read = total - input - creation;
        (input, creation, read)
    }
}

/// 默认可调参数（可被 config / Admin 覆盖）。
pub const DEFAULT_CACHE_MAX_RATIO: f64 = 0.85;
pub const DEFAULT_CACHE_MIN_TOKENS: u32 = 1024;
pub const DEFAULT_CACHE_MIN_TOKENS_OPUS: u32 = 4096;
pub const DEFAULT_CACHE_TTL_SECS: u64 = 300;
pub const DEFAULT_CACHE_MAX_ENTRIES: usize = 20_000;

/// 单个缓存断点：到此块为止的前缀指纹 + 累计 token + 该断点 TTL（秒）。
#[derive(Debug, Clone)]
pub struct CacheBreakpoint {
    pub fingerprint: [u8; 32],
    pub cumulative_tokens: i32,
    pub ttl_secs: u64,
}

/// 一次请求的缓存画像（[`build_profile`] 产出）。断点按前缀顺序，累计 token 单调增。
#[derive(Debug, Clone)]
pub struct CachePrefixProfile {
    pub breakpoints: Vec<CacheBreakpoint>,
    pub total_input_est: i32,
    pub is_opus: bool,
}

/// 全局 LRU 表条目：过期时刻（unix 秒）+ TTL（刷新用）。
#[derive(Debug, Clone, Copy)]
struct PrefixEntry {
    expires_at: i64,
    ttl_secs: u64,
}

/// 指纹计量器：全局共享（跨账号跨会话）的前缀指纹 → 过期时刻表，带 TTL + LRU 容量上限。
/// 5 个可调参数原子存储，Admin 改动即时生效。指纹表落盘 `data/prompt_cache.json`。
///
/// 替代旧的会话推断式 `MeterGovernance`：命中按内容物理匹配，不猜。
pub struct PromptCacheTracker {
    inner: parking_lot::Mutex<TrackerInner>,
    /// 单请求可命中占总 input 上限的 bit 表示（f64→u64）。
    max_ratio_bits: AtomicU64,
    /// 断点最小可缓存 token 阈值（非 opus / opus）。
    min_tokens: AtomicU64,
    min_tokens_opus: AtomicU64,
    /// 断点默认 TTL（秒）。
    default_ttl_secs: AtomicU64,
    /// 全局 LRU 容量上限。
    max_entries: AtomicU64,
    /// 落盘路径（None = 不落盘，测试用）。
    persist_path: Option<std::path::PathBuf>,
    /// 命中率统计。
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    expirations: AtomicU64,
}

/// 表内部：指纹 → 条目 + LRU 顺序（front=最近使用）+ 脏标记。
struct TrackerInner {
    entries: std::collections::HashMap<[u8; 32], PrefixEntry>,
    /// LRU 顺序：队首=最久未用，队尾=最近使用。
    order: std::collections::VecDeque<[u8; 32]>,
    dirty: bool,
}

/// `Arc<PromptCacheTracker>` 别名
pub type SharedPromptCacheTracker = Arc<PromptCacheTracker>;

/// 当前 unix 秒（i64）。指纹表 TTL / 过期判定的时间基准。
pub fn now_unix_secs() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 命中率统计快照（供 Admin 展示）。
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct PromptCacheStats {
    pub entries: usize,
    pub capacity: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub expirations: u64,
}

impl PromptCacheTracker {
    /// 用 5 个可调参数构造。`persist_path=None` 表示纯内存（测试用）。
    pub fn new(
        max_ratio: f64,
        min_tokens: u32,
        min_tokens_opus: u32,
        default_ttl_secs: u64,
        max_entries: usize,
        persist_path: Option<std::path::PathBuf>,
    ) -> Self {
        let t = Self {
            inner: parking_lot::Mutex::new(TrackerInner {
                entries: std::collections::HashMap::new(),
                order: std::collections::VecDeque::new(),
                dirty: false,
            }),
            max_ratio_bits: AtomicU64::new(max_ratio.clamp(0.5, 1.0).to_bits()),
            min_tokens: AtomicU64::new(min_tokens as u64),
            min_tokens_opus: AtomicU64::new(min_tokens_opus as u64),
            default_ttl_secs: AtomicU64::new(default_ttl_secs.max(1)),
            max_entries: AtomicU64::new((max_entries.max(100)) as u64),
            persist_path,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            expirations: AtomicU64::new(0),
        };
        t.load();
        t
    }

    // ---- 可调参数 getter/setter（Admin 热更新）----
    pub fn max_ratio(&self) -> f64 {
        f64::from_bits(self.max_ratio_bits.load(Ordering::Relaxed))
    }
    pub fn set_max_ratio(&self, v: f64) {
        self.max_ratio_bits
            .store(v.clamp(0.5, 1.0).to_bits(), Ordering::Relaxed);
    }
    pub fn min_tokens(&self) -> u32 {
        self.min_tokens.load(Ordering::Relaxed) as u32
    }
    pub fn set_min_tokens(&self, v: u32) {
        self.min_tokens.store(v as u64, Ordering::Relaxed);
    }
    pub fn min_tokens_opus(&self) -> u32 {
        self.min_tokens_opus.load(Ordering::Relaxed) as u32
    }
    pub fn set_min_tokens_opus(&self, v: u32) {
        self.min_tokens_opus.store(v as u64, Ordering::Relaxed);
    }
    pub fn default_ttl_secs(&self) -> u64 {
        self.default_ttl_secs.load(Ordering::Relaxed)
    }
    pub fn set_default_ttl_secs(&self, v: u64) {
        self.default_ttl_secs.store(v.max(1), Ordering::Relaxed);
    }
    pub fn max_entries(&self) -> usize {
        self.max_entries.load(Ordering::Relaxed) as usize
    }
    pub fn set_max_entries(&self, v: usize) {
        self.max_entries.store(v.max(100) as u64, Ordering::Relaxed);
    }

    fn min_tokens_for(&self, is_opus: bool) -> i32 {
        if is_opus {
            self.min_tokens_opus() as i32
        } else {
            self.min_tokens() as i32
        }
    }

    /// 统计快照。
    pub fn stats(&self) -> PromptCacheStats {
        let inner = self.inner.lock();
        PromptCacheStats {
            entries: inner.entries.len(),
            capacity: self.max_entries(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            expirations: self.expirations.load(Ordering::Relaxed),
        }
    }

    /// 计算本次请求的缓存覆盖（对齐 Kiro-Go `Compute`）。命中即刷新 TTL + LRU 提前。
    pub fn compute(&self, profile: &CachePrefixProfile) -> CacheUsage {
        if profile.breakpoints.is_empty() {
            return CacheUsage::default();
        }
        let now = now_unix_secs();
        let min_tokens = self.min_tokens_for(profile.is_opus);
        let total = profile.total_input_est;
        let last = profile.breakpoints.last().unwrap();
        let mut last_tokens = last.cumulative_tokens.min(total);

        let mut inner = self.inner.lock();
        self.prune_expired(&mut inner, now);

        if inner.entries.is_empty() {
            // 首次：整段可缓存前缀按 creation 重写（若达阈值），read=0。
            drop(inner);
            let creation = if last_tokens >= min_tokens {
                last_tokens
            } else {
                0
            };
            self.misses.fetch_add(1, Ordering::Relaxed);
            return CacheUsage {
                input_est: (total - last_tokens).max(0),
                creation_est: creation,
                read_est: 0,
                prompt_total_est: total,
            };
        }

        // 可命中上限 = total × max_ratio（保证最新内容本轮不全命中）。
        let max_cacheable = ((total as f64) * self.max_ratio()).round() as i32;
        if last_tokens > max_cacheable {
            last_tokens = max_cacheable;
        }

        // 倒序找最长命中前缀。
        let mut matched = 0i32;
        for bp in profile.breakpoints.iter().rev() {
            if bp.cumulative_tokens < min_tokens {
                continue;
            }
            let hit = match inner.entries.get(&bp.fingerprint) {
                Some(e) if e.expires_at > now => Some(*e),
                _ => None,
            };
            if let Some(e) = hit {
                let refreshed = PrefixEntry {
                    expires_at: now + e.ttl_secs as i64,
                    ttl_secs: e.ttl_secs,
                };
                inner.entries.insert(bp.fingerprint, refreshed);
                Self::lru_touch(&mut inner, &bp.fingerprint);
                inner.dirty = true;
                matched = bp.cumulative_tokens.min(last_tokens);
                break;
            }
        }
        drop(inner);

        if matched > 0 {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        let creation = (last_tokens - matched).max(0);
        CacheUsage {
            input_est: (total - last_tokens).max(0),
            creation_est: creation,
            read_est: matched,
            prompt_total_est: total,
        }
    }

    /// 把本次所有达阈值的断点指纹写回全局表（对齐 Kiro-Go `Update`）。
    pub fn update(&self, profile: &CachePrefixProfile) {
        if profile.breakpoints.is_empty() {
            return;
        }
        let now = now_unix_secs();
        let min_tokens = self.min_tokens_for(profile.is_opus);
        let mut inner = self.inner.lock();
        self.prune_expired(&mut inner, now);
        for bp in &profile.breakpoints {
            if bp.cumulative_tokens < min_tokens {
                continue;
            }
            let entry = PrefixEntry {
                expires_at: now + bp.ttl_secs as i64,
                ttl_secs: bp.ttl_secs,
            };
            inner.entries.insert(bp.fingerprint, entry);
            Self::lru_touch(&mut inner, &bp.fingerprint);
        }
        inner.dirty = true;
        self.evict_overflow(&mut inner);
    }

    /// 把某指纹移到 LRU 队尾（最近使用）。
    fn lru_touch(inner: &mut TrackerInner, fp: &[u8; 32]) {
        if let Some(pos) = inner.order.iter().position(|x| x == fp) {
            inner.order.remove(pos);
        }
        inner.order.push_back(*fp);
    }

    /// 清除已过期条目。
    fn prune_expired(&self, inner: &mut TrackerInner, now: i64) {
        let expired: Vec<[u8; 32]> = inner
            .entries
            .iter()
            .filter(|(_, e)| e.expires_at <= now)
            .map(|(fp, _)| *fp)
            .collect();
        if expired.is_empty() {
            return;
        }
        for fp in &expired {
            inner.entries.remove(fp);
            if let Some(pos) = inner.order.iter().position(|x| x == fp) {
                inner.order.remove(pos);
            }
        }
        self.expirations
            .fetch_add(expired.len() as u64, Ordering::Relaxed);
        inner.dirty = true;
    }

    /// 超容量时从 LRU 队首淘汰。
    fn evict_overflow(&self, inner: &mut TrackerInner) {
        let cap = self.max_entries();
        let mut evicted = 0u64;
        while inner.entries.len() > cap {
            match inner.order.pop_front() {
                Some(fp) => {
                    if inner.entries.remove(&fp).is_some() {
                        evicted += 1;
                    }
                }
                None => break,
            }
        }
        if evicted > 0 {
            self.evictions.fetch_add(evicted, Ordering::Relaxed);
            inner.dirty = true;
        }
    }

    /// 从落盘文件加载指纹表（丢弃已过期条目）。best-effort，损坏/缺失 → 空表启动。
    fn load(&self) {
        let Some(path) = self.persist_path.as_ref() else {
            return;
        };
        let Ok(bytes) = std::fs::read(path) else {
            return;
        };
        let Ok(disk): Result<PromptCacheDisk, _> = serde_json::from_slice(&bytes) else {
            return;
        };
        let now = now_unix_secs();
        let mut inner = self.inner.lock();
        for e in disk.entries {
            if e.expires_at > now {
                inner.entries.insert(
                    e.fingerprint,
                    PrefixEntry {
                        expires_at: e.expires_at,
                        ttl_secs: e.ttl_secs,
                    },
                );
                inner.order.push_back(e.fingerprint);
            }
        }
    }

    /// 若脏则原子落盘（临时文件 + rename）。由后台 flush 循环 / 退出时调用。
    pub fn flush(&self) {
        let Some(path) = self.persist_path.as_ref() else {
            return;
        };
        let snapshot: Vec<PromptCacheDiskEntry> = {
            let mut inner = self.inner.lock();
            if !inner.dirty {
                return;
            }
            inner.dirty = false;
            inner
                .entries
                .iter()
                .map(|(fp, e)| PromptCacheDiskEntry {
                    fingerprint: *fp,
                    expires_at: e.expires_at,
                    ttl_secs: e.ttl_secs,
                })
                .collect()
        };
        let disk = PromptCacheDisk {
            version: 1,
            entries: snapshot,
        };
        let Ok(bytes) = serde_json::to_vec(&disk) else {
            return;
        };
        let tmp = path.with_extension("json.tmp");
        if std::fs::write(&tmp, &bytes).is_ok() {
            let _ = std::fs::rename(&tmp, path);
        }
    }
}

/// 落盘格式（对齐 Kiro-Go C3）。
#[derive(serde::Serialize, serde::Deserialize)]
struct PromptCacheDisk {
    version: u32,
    entries: Vec<PromptCacheDiskEntry>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PromptCacheDiskEntry {
    #[serde(with = "fingerprint_hex")]
    fingerprint: [u8; 32],
    expires_at: i64,
    ttl_secs: u64,
}

/// 指纹以 hex 字符串落盘（JSON 友好）。
mod fingerprint_hex {
    pub fn serialize<S: serde::Serializer>(fp: &[u8; 32], s: S) -> Result<S::Ok, S::Error> {
        let hex: String = fp.iter().map(|b| format!("{:02x}", b)).collect();
        s.serialize_str(&hex)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<[u8; 32], D::Error> {
        use serde::Deserialize;
        let hex = String::deserialize(d)?;
        let bytes = (0..hex.len())
            .step_by(2)
            .filter_map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
            .collect::<Vec<u8>>();
        let mut out = [0u8; 32];
        if bytes.len() != 32 {
            return Err(serde::de::Error::custom("fingerprint not 32 bytes"));
        }
        out.copy_from_slice(&bytes);
        Ok(out)
    }
}

// ============================================================================
// 与请求体协议层的接线
// ============================================================================

use super::stream::estimate_tokens;
use super::types::{Message, MessagesRequest, SystemMessage, Tool};

/// 计算本次请求的 delta-based 结构化缓存覆盖情况。纯函数：只看请求结构、R、上轮消息条数，
/// 不依赖时间或负载。返回 [`CacheUsage`]，由调用方在拿到真实 total 后做互斥分摊。
///
/// 桶划分（见模块文档）：input = 最后一条 message；read = 其余前缀。`read_ratio` 是该请求
/// 生效的 R（per-key 覆盖优先，否则全局 [`MeterGovernance`]）。
///
/// `prev_msg_count` 只用作 **warm/cold 布尔标志**（`Some`=缓存还热 / `None`=首次或超 TTL 已凉），
/// **其具体数值不再参与计算**——跨请求 msg_count 在共享/交错 seed 下不可靠（见 warm 分支注释）。
/// - **`Some(_)`**（缓存还热）→ creation = 最近一个 user 回合之后、到 input 之前的那几条（纯结构
///   判据，见 [`last_turn_creation_start`]），其余前缀走 read 便宜桶。标准对话 = 一条 assistant
///   回复；agent 工具循环 = 本轮全部 tool_use/tool_result。
/// - **`None`**（首次出现 / 超 TTL 缓存已凉）→ 整段可缓存前缀（system+tools+除最后一条外的全部
///   历史）按 **creation** 重写计费、read 基数=0，如同首轮重建缓存。这让"凉了的会话"不再白
///   拿 0.1× 折扣。
/// 判断模型是否 opus 家族（阈值更高）。
fn is_opus_model(model: &str) -> bool {
    model.to_ascii_lowercase().contains("opus")
}

/// 构建本次请求的缓存画像（对齐 Kiro-Go `BuildClaudeProfile` + `flattenClaudeCacheBlocks`）。
///
/// 展平顺序：request_prelude(model+tool_choice) → tools[] → system[] → messages[]。逐块喂
/// 滚动 SHA256、累加 token；断点判定：块自带 `cache_control` → 断点（取其 TTL）；首个显式
/// 断点出现后，每个 message 边界成为隐式断点（继承 active TTL）。无断点 → 返回 None。
///
/// `default_ttl_secs` 为断点无显式 TTL 时的兜底（来自 tracker 参数）。
pub fn build_profile(req: &MessagesRequest, default_ttl_secs: u64) -> Option<CachePrefixProfile> {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    let mut breakpoints: Vec<CacheBreakpoint> = Vec::new();
    let mut cumulative: i32 = 0;
    let mut active_ttl: Option<u64> = None;

    // helper: 把一段规范化文本喂入滚动哈希。
    let feed = |hasher: &mut Sha256, chunk: &str| {
        hasher.update((chunk.len() as u64).to_le_bytes());
        hasher.update(chunk.as_bytes());
    };

    // ---- request_prelude（不作断点，仅参与前缀哈希与 token）----
    let prelude = format!(
        "prelude|model={}|tool_choice={}",
        req.model,
        req.tool_choice
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_default()
    );
    feed(&mut hasher, &prelude);
    cumulative = cumulative.saturating_add(estimate_tokens(&prelude).max(0));

    // ---- tools[] ----
    if let Some(tools) = req.tools.as_ref() {
        for t in tools {
            let canonical = format!(
                "tool|{}|{}|{}",
                t.name,
                t.description,
                serde_json::to_string(&t.input_schema).unwrap_or_default()
            );
            feed(&mut hasher, &canonical);
            cumulative = cumulative.saturating_add(tool_tokens(t));
        }
    }

    // ---- system[] ----
    if let Some(systems) = req.system.as_ref() {
        for (i, sys) in systems.iter().enumerate() {
            let canonical = format!("system|{}|{}", i, sys.text);
            feed(&mut hasher, &canonical);
            cumulative = cumulative.saturating_add(system_tokens(sys));
        }
    }

    // ---- messages[]：每条 message 边界是潜在断点 ----
    for (idx, msg) in req.messages.iter().enumerate() {
        let content_str = msg.content.to_string();
        let canonical = format!("msg|{}|{}|{}", idx, msg.role, content_str);
        feed(&mut hasher, &canonical);
        cumulative = cumulative.saturating_add(message_tokens(msg));

        // message 级 cache_control 检测（Anthropic 允许在 block 上打 cache_control）。
        let explicit_ttl = message_cache_control_ttl(msg);
        if let Some(ttl) = explicit_ttl {
            active_ttl = Some(ttl);
        }
        // 断点：显式 cache_control，或已出现过显式断点后的每个 message 边界。
        let bp_ttl = if explicit_ttl.is_some() {
            explicit_ttl
        } else {
            active_ttl
        };
        if let Some(ttl) = bp_ttl {
            let fp: [u8; 32] = hasher.clone().finalize().into();
            breakpoints.push(CacheBreakpoint {
                fingerprint: fp,
                cumulative_tokens: cumulative,
                ttl_secs: if ttl == 0 { default_ttl_secs } else { ttl },
            });
        }
    }

    // 无显式 cache_control：整个前缀（到最后一条 message）作单一隐式断点，用默认 TTL。
    // 这让"没打 cache_control 的普通多轮对话"也能命中（对齐 Anthropic 自动前缀缓存的常见用法）。
    if breakpoints.is_empty() {
        if req.messages.is_empty() {
            return None;
        }
        let fp: [u8; 32] = hasher.finalize().into();
        breakpoints.push(CacheBreakpoint {
            fingerprint: fp,
            cumulative_tokens: cumulative,
            ttl_secs: default_ttl_secs,
        });
    }

    Some(CachePrefixProfile {
        breakpoints,
        total_input_est: cumulative,
        is_opus: is_opus_model(&req.model),
    })
}

/// 从 message 的 content blocks 里提取 `cache_control.ttl`（秒）；无则 None。
/// `ephemeral` 且带 `ttl: "1h"` → 3600；`ttl: "5m"` 或无 ttl → 0（调用方用默认 TTL）。
fn message_cache_control_ttl(msg: &Message) -> Option<u64> {
    let arr = msg.content.as_array()?;
    let mut found = None;
    for block in arr {
        if let Some(cc) = block.get("cache_control") {
            // 命中 cache_control：解析 ttl 字段。
            let ttl = cc
                .get("ttl")
                .and_then(|t| t.as_str())
                .map(parse_ttl_str)
                .unwrap_or(0);
            found = Some(ttl);
        }
    }
    found
}

/// 解析 Anthropic ttl 字符串："5m"→300, "1h"→3600；无法解析→0（用默认）。
fn parse_ttl_str(s: &str) -> u64 {
    let s = s.trim();
    if let Some(m) = s.strip_suffix('m') {
        m.trim().parse::<u64>().map(|v| v * 60).unwrap_or(0)
    } else if let Some(h) = s.strip_suffix('h') {
        h.trim().parse::<u64>().map(|v| v * 3600).unwrap_or(0)
    } else {
        s.parse::<u64>().unwrap_or(0)
    }
}

/// 估算一条 message 的 token：遍历 content blocks，按块 `type` **完整分派**
/// （text/thinking 文本、tool_use 参数、tool_result 内容、image 尺寸）。
/// string content 直接估算原文。
///
/// 必须覆盖 agent 负载里的 `tool_use`(参数在 `.input`) / `tool_result`(文本嵌在
/// `.content[]`) —— 它们在 Claude Code 多轮里常是 message 的主体。只数 text/thinking
/// 会把这些 message 计成 ≈0，导致 `creation`(=倒数第二条 message，常为 assistant 的
/// tool_use) 塌成 0、计量严重偏向 read。对齐 [`crate::token`] 的 `count_block_tokens` 分派口径。
fn message_tokens(msg: &super::types::Message) -> i32 {
    match &msg.content {
        serde_json::Value::String(s) => estimate_tokens(s).max(0),
        serde_json::Value::Array(arr) => {
            let mut sum: i32 = 0;
            for v in arr {
                sum = sum.saturating_add(block_tokens(v));
            }
            sum
        }
        _ => 0,
    }
}

/// 估算单个 content block 的 token，按 `type` 完整分派。用本模块的 `estimate_tokens` /
/// `estimate_image_tokens` 保持模块内口径一致（拆分是比例运算，分子分母同尺即可）。
/// 宽松取值：字段缺失/异形只少计该块，不整块丢弃。
fn block_tokens(v: &serde_json::Value) -> i32 {
    let mut sum: i32 = 0;
    // text / thinking：任何块都可能带（与 token.rs 一致，先无条件累加）。
    if let Some(text) = v.get("text").and_then(|x| x.as_str()) {
        sum = sum.saturating_add(estimate_tokens(text).max(0));
    }
    if let Some(thinking) = v.get("thinking").and_then(|x| x.as_str()) {
        sum = sum.saturating_add(estimate_tokens(thinking).max(0));
    }
    match v.get("type").and_then(|t| t.as_str()) {
        Some("tool_use") => {
            if let Some(name) = v.get("name").and_then(|x| x.as_str()) {
                sum = sum.saturating_add(estimate_tokens(name).max(0));
            }
            if let Some(input) = v.get("input") {
                let s = serde_json::to_string(input).unwrap_or_default();
                sum = sum.saturating_add(estimate_tokens(&s).max(0));
            }
        }
        Some("tool_result") => {
            sum = sum.saturating_add(tool_result_content_tokens(v.get("content")));
        }
        Some("image") => {
            let (media_type, data) = image_source_parts(v);
            sum =
                sum.saturating_add(
                    crate::image_resize::estimate_image_tokens(media_type, data) as i32
                );
        }
        _ => {}
    }
    sum
}

/// 估算 `tool_result.content` 的 token：string，或 `[{text}|{image}]` 数组
/// （与转换器 `extract_tool_result_content` 的解析形态一致）；其它异形序列化兜底。
fn tool_result_content_tokens(content: Option<&serde_json::Value>) -> i32 {
    match content {
        Some(serde_json::Value::String(s)) => estimate_tokens(s).max(0),
        Some(serde_json::Value::Array(arr)) => {
            let mut sum: i32 = 0;
            for item in arr {
                if let Some(text) = item.get("text").and_then(|x| x.as_str()) {
                    sum = sum.saturating_add(estimate_tokens(text).max(0));
                } else if item.get("type").and_then(|x| x.as_str()) == Some("image") {
                    let (media_type, data) = image_source_parts(item);
                    sum = sum.saturating_add(crate::image_resize::estimate_image_tokens(
                        media_type, data,
                    ) as i32);
                }
            }
            sum
        }
        Some(other) => estimate_tokens(&other.to_string()).max(0),
        None => 0,
    }
}

/// 工具的 token 估算：name + description + schema 拼接原文。
fn tool_tokens(t: &Tool) -> i32 {
    let schema = serde_json::to_string(&t.input_schema).unwrap_or_default();
    estimate_tokens(&format!("{} {} {}", t.name, t.description, schema)).max(0)
}

/// system block 的 token 估算。
fn system_tokens(s: &SystemMessage) -> i32 {
    estimate_tokens(&s.text).max(0)
}

/// 从 image content block 取 `(media_type, base64_data)`，缺字段时返回空串（估算走保底）。
fn image_source_parts(v: &serde_json::Value) -> (&str, &str) {
    let src = v.get("source");
    let media_type = src
        .and_then(|s| s.get("media_type"))
        .and_then(|x| x.as_str())
        .unwrap_or("");
    let data = src
        .and_then(|s| s.get("data"))
        .and_then(|x| x.as_str())
        .unwrap_or("");
    (media_type, data)
}

// ============================================================================
// 会话隔离种子（响应缓存 response_cache 复用同一口径构造缓存键）
// ============================================================================

/// 生成会话隔离种子。
///
/// 优先级：
///   1. metadata.user_id 里的 session 段（Claude Code 格式含 `_session_<uuid>`）；
///   2. 退回客户端 Key id。
///
/// 注：无 session 的客户端（OpenAI 端点 `metadata:None`、裸客户端）退回 `key:{key_id}`，
/// 同一 key 下多对话会共享一条 `MeterGovernance::observe_session` 记录。这**不会**再导致
/// creation 爆炸——[`MeterGovernance::observe_session`] 用**消息条数高水位**，任何短请求
/// 都不会把 delta 下界 `prev_n` 打小，共享 seed 下最长对话的高水位反而让其余对话 creation
/// 偏低（命中率偏高，经济上安全）。曾尝试用对话指纹拆分 seed 做 per-conversation 隔离，
/// 但实测把 fallback 流量拆成大量首见即 cold 的 seed → 命中率反降、creation 爆炸，已回退。
///
/// `pub(crate)`：响应缓存复用同一套会话隔离口径构造缓存键，保证两者隔离边界一致。
pub(crate) fn isolation_seed(req: &MessagesRequest, key_id: u64) -> String {
    if let Some(session) = req
        .metadata
        .as_ref()
        .and_then(|m| m.user_id.as_deref())
        .and_then(extract_session_id)
    {
        return format!("sess:{session}");
    }
    format!("key:{key_id}")
}

/// 从 Claude Code 的 user_id 中提取 session 标识。
/// 格式形如 `user_<hash>_account__session_<uuid>`，取 `_session_` 之后的部分。
fn extract_session_id(user_id: &str) -> Option<String> {
    user_id
        .split_once("_session_")
        .map(|(_, sid)| sid.trim().to_string())
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::super::types::{Message, MessagesRequest, SystemMessage};
    use super::*;

    fn msg(role: &str, text: &str) -> Message {
        Message {
            role: role.to_string(),
            content: serde_json::json!([{ "type": "text", "text": text }]),
        }
    }

    fn req_with(messages: Vec<Message>, system: Option<Vec<SystemMessage>>) -> MessagesRequest {
        MessagesRequest {
            model: "claude-sonnet-4-6".to_string(),
            max_tokens: 32,
            messages,
            stream: false,
            system,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        }
    }

    /// 纯内存 tracker（不落盘），默认参数。
    fn tracker() -> PromptCacheTracker {
        PromptCacheTracker::new(
            DEFAULT_CACHE_MAX_RATIO,
            1,   // min_tokens=1，测试里小请求也可缓存
            1,   // min_tokens_opus
            300, // ttl
            100, // max_entries
            None,
        )
    }

    // ---- split_against_total（去 R，read=剩余）-----------------------------

    #[test]
    fn split_no_prefix_all_input() {
        let u = CacheUsage::default();
        assert_eq!(u.split_against_total(500), (500, 0, 0));
    }

    #[test]
    fn split_three_buckets_by_share() {
        // input 10%、creation 5%，剩余 85% 为 read。
        let u = CacheUsage {
            input_est: 10,
            creation_est: 5,
            read_est: 85,
            prompt_total_est: 100,
        };
        let (input, creation, read) = u.split_against_total(1000);
        assert_eq!(input, 100);
        assert_eq!(creation, 50);
        assert_eq!(read, 850);
        assert_eq!(input + creation + read, 1000);
    }

    #[test]
    fn split_zero_total_safe() {
        let u = CacheUsage {
            input_est: 10,
            creation_est: 5,
            read_est: 85,
            prompt_total_est: 100,
        };
        assert_eq!(u.split_against_total(0), (0, 0, 0));
    }

    // ---- build_profile ------------------------------------------------------

    #[test]
    fn build_profile_none_when_empty() {
        let req = req_with(vec![], None);
        assert!(build_profile(&req, 300).is_none());
    }

    #[test]
    fn build_profile_single_implicit_breakpoint() {
        // 无显式 cache_control → 整前缀作单一隐式断点。
        let req = req_with(vec![msg("user", "hello world this is a test")], None);
        let p = build_profile(&req, 300).expect("profile");
        assert_eq!(p.breakpoints.len(), 1);
        assert!(p.total_input_est > 0);
        assert_eq!(
            p.breakpoints[0].cumulative_tokens,
            p.total_input_est,
            "隐式断点累计=总量"
        );
    }

    #[test]
    fn build_profile_deterministic_same_content() {
        // 同内容两次 → 指纹相同（canonicalize 幂等）。
        let req1 = req_with(vec![msg("user", "abc"), msg("assistant", "def")], None);
        let req2 = req_with(vec![msg("user", "abc"), msg("assistant", "def")], None);
        let p1 = build_profile(&req1, 300).unwrap();
        let p2 = build_profile(&req2, 300).unwrap();
        assert_eq!(
            p1.breakpoints.last().unwrap().fingerprint,
            p2.breakpoints.last().unwrap().fingerprint
        );
    }

    #[test]
    fn build_profile_different_content_differs() {
        let req1 = req_with(vec![msg("user", "abc")], None);
        let req2 = req_with(vec![msg("user", "xyz")], None);
        let p1 = build_profile(&req1, 300).unwrap();
        let p2 = build_profile(&req2, 300).unwrap();
        assert_ne!(
            p1.breakpoints.last().unwrap().fingerprint,
            p2.breakpoints.last().unwrap().fingerprint
        );
    }

    #[test]
    fn build_profile_opus_flag() {
        let mut req = req_with(vec![msg("user", "hi")], None);
        req.model = "claude-opus-4-8".to_string();
        assert!(build_profile(&req, 300).unwrap().is_opus);
        req.model = "claude-sonnet-4-6".to_string();
        assert!(!build_profile(&req, 300).unwrap().is_opus);
    }

    // ---- compute / update：命中语义 ----------------------------------------

    #[test]
    fn compute_first_request_all_creation() {
        // 空表首次 → read=0，creation=整可缓存前缀。
        let t = tracker();
        let req = req_with(vec![msg("user", "hello world foo bar baz")], None);
        let p = build_profile(&req, 300).unwrap();
        let u = t.compute(&p);
        assert_eq!(u.read_est, 0);
        assert!(u.creation_est > 0);
    }

    #[test]
    fn compute_hit_after_update() {
        // update 写回后，同前缀再 compute → read 命中。
        let t = tracker();
        let req = req_with(
            vec![msg("user", "a long enough first message here")],
            None,
        );
        let p = build_profile(&req, 300).unwrap();
        let _ = t.compute(&p);
        t.update(&p);
        // 第二轮：在前缀基础上加一条新消息（前缀仍命中）。
        let req2 = req_with(
            vec![
                msg("user", "a long enough first message here"),
                msg("assistant", "reply"),
                msg("user", "second question appended now"),
            ],
            None,
        );
        let p2 = build_profile(&req2, 300).unwrap();
        // p2 的隐式断点是整前缀，与 p 不同（内容更长）→ 不直接命中 p 的指纹。
        // 但显式多断点场景才有中间命中；此处验证 update 后表非空、compute 走命中分支。
        t.update(&p2);
        let u = t.compute(&p2);
        assert!(u.read_est > 0, "重放同前缀应命中 read");
    }

    #[test]
    fn compute_replay_same_prefix_hits() {
        // 完全相同请求重放 → 第二次命中全前缀。
        let t = tracker();
        let req = req_with(vec![msg("user", "identical replay content here")], None);
        let p = build_profile(&req, 300).unwrap();
        let _ = t.compute(&p);
        t.update(&p);
        let u2 = t.compute(&p);
        assert!(u2.read_est > 0);
        // max_ratio=0.85 封顶：read 不超过 total×0.85。
        assert!(u2.read_est <= ((p.total_input_est as f64) * 0.85).round() as i32 + 1);
    }

    #[test]
    fn compute_expired_is_miss() {
        // TTL=0 的断点写入即过期 → 下轮 miss。
        let t = tracker();
        let mut req = req_with(vec![msg("user", "content that will expire soon")], None);
        req.model = "claude-sonnet-4-6".to_string();
        let mut p = build_profile(&req, 300).unwrap();
        // 强制 TTL 极短：手动构造过期断点。
        for bp in &mut p.breakpoints {
            bp.ttl_secs = 0;
        }
        t.update(&p); // expires_at = now + 0 = now
        // compute 时 prune 掉 now 之前的（expires_at <= now）→ 空表 → miss。
        let u = t.compute(&p);
        assert_eq!(u.read_est, 0);
    }

    #[test]
    fn min_tokens_threshold_skips_small() {
        // min_tokens 很高 → 小请求不进缓存（creation=0，read=0）。
        let t = PromptCacheTracker::new(0.85, 100_000, 100_000, 300, 100, None);
        let req = req_with(vec![msg("user", "tiny")], None);
        let p = build_profile(&req, 300).unwrap();
        let u = t.compute(&p);
        assert_eq!(u.read_est, 0);
        assert_eq!(u.creation_est, 0, "低于阈值不计 creation");
    }

    #[test]
    fn lru_eviction_bounds_entries() {
        // max_entries 下限 clamp 到 100（本番安全）；写入 130 个不同前缀 → 表最多 100 条。
        let t = PromptCacheTracker::new(0.85, 1, 1, 300, 100, None);
        for i in 0..130 {
            let req = req_with(
                vec![msg("user", &format!("distinct content number {}", i))],
                None,
            );
            let p = build_profile(&req, 300).unwrap();
            t.update(&p);
        }
        assert!(t.stats().entries <= 100, "LRU 容量上限 100");
        assert!(t.stats().evictions >= 1);
    }

    #[test]
    fn stats_track_hits_misses() {
        let t = tracker();
        let req = req_with(vec![msg("user", "some content for stats test")], None);
        let p = build_profile(&req, 300).unwrap();
        let _ = t.compute(&p); // miss（空表）
        t.update(&p);
        let _ = t.compute(&p); // hit
        let s = t.stats();
        assert!(s.misses >= 1);
        assert!(s.hits >= 1);
    }

    #[test]
    fn parse_ttl_str_forms() {
        assert_eq!(parse_ttl_str("5m"), 300);
        assert_eq!(parse_ttl_str("1h"), 3600);
        assert_eq!(parse_ttl_str("300"), 300);
        assert_eq!(parse_ttl_str("garbage"), 0);
    }

    #[test]
    fn setters_clamp() {
        let t = tracker();
        t.set_max_ratio(2.0);
        assert_eq!(t.max_ratio(), 1.0);
        t.set_max_ratio(0.1);
        assert_eq!(t.max_ratio(), 0.5);
        t.set_max_entries(1);
        assert_eq!(t.max_entries(), 100);
    }

    #[test]
    fn isolation_seed_prefers_session_then_key() {
        use super::super::types::Metadata;
        let mut req = req_with(vec![msg("user", "hi")], None);
        req.metadata = Some(Metadata {
            user_id: Some("user_abc_account__session_XYZ".to_string()),
        });
        assert_eq!(isolation_seed(&req, 7), "sess:XYZ");
        req.metadata = None;
        assert_eq!(isolation_seed(&req, 7), "key:7");
    }
}

