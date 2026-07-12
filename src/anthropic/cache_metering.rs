//! 中转层 prompt cache 计量（无状态、确定性、delta-based）
//!
//! Kiro 上游既不做 prompt cache、也不下发 cache_creation / cache_read 字段（实测
//! meteringEvent 只给 credit 计费量），所以中转层上报的缓存计费**纯粹是合成给下游看
//! 的数字**，不对应任何真实缓存命中、也不影响真实成本。下游按 read/creation **分别计价**
//! （creation 贵、read 便宜），所以合成数字必须**经济上自洽**：creation 每轮只应反映
//! 「本轮新增的那一段」，不能随对话变长而虚高。
//!
//! 既然底层没有真实缓存，就不该去"忠实模拟"真实缓存那套随时间/负载漂移的不确定行为。
//! 本模块按**多轮对话缓存实际怎么累积**做纯函数式、确定性的结构化拆分（delta-based）：
//!
//! ```text
//! input    = 最后一条 message（本轮新问题）              —— 未缓存
//! creation = 本会话上次请求后新增、且已进稳定前缀的消息   —— 有界，随本轮新增量走
//!            （= messages[上次条数 .. 末条)，不含 input；overhead 上轮已缓存不计）
//! read     = system + tools + 更早的全部历史              —— 上一轮已缓存
//! 首轮 / 超 TTL（cold）→ creation = system+tools+除末条外全部历史（整段重写）、read = 0
//! ```
//!
//! creation 取「**上次见到本会话后新增的那几条**」而非死板的「倒数第二条」：标准对话每轮
//! 只加一对（assistant + 新 user），两者等价；但 agent 工具循环一轮可能补进多对
//! （a1,tool_result,a2,...），此时新增的中间消息也应计 creation，不该塞进便宜的 read 桶。
//! 为此按会话记 last_seen 的 **(秒, 消息条数)**，本轮新增 = `当前条数 − 上次条数`。
//!
//! 关键性质：**creation 每轮有界（≈本轮新增的非-input 消息），read 随历史累积增长**。对话越长
//! read 越大、read:creation 比值自然往上漂——既真实又不死板，且贵的 creation 桶不会被历史规模放大。
//! 同一段对话无论何时重放、负载如何，结果**完全相同**（请求结构 + last_seen 的纯函数）。
//!
//! 命中率 `R` ∈ [0,1] 是 **read 留存阻尼**（默认 1.0）：`read_final = read × R`，被砍掉的
//! `read × (1−R)` 推回 input（相当于"假装这段前缀没命中缓存"→ 不给折扣）。R **不触碰
//! creation**，所以贵桶始终经济正确；R=1 给足缓存折扣（真实），调低则更保守。可全局设也可
//! per-key 覆盖。
//!
//! 无后台任务、无落盘、无内存增长——计量只是请求级的纯计算。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// `compute_structural_cache_usage` 的结果：按 estimate 口径算出的三桶基准 + read 留存
/// 阻尼，最终由 [`CacheUsage::split_against_total`] 对真实 total 做互斥分摊。
///
/// 三个 estimate 是比例基准（不是最终值）——真正的 token 数要在拿到真实 total（contextUsage
/// 真值或 count_tokens 估算）后才按比例算出，因为流式响应直到末尾才知道真实 total。
#[derive(Debug, Clone, Copy)]
pub struct CacheUsage {
    /// 本轮新输入（最后一条 message）的 estimate token——这部分永不计入缓存。
    pub input_est: i32,
    /// 本轮新写入缓存的 delta（倒数第二条 message；首轮为 system+tools）的 estimate token。
    pub creation_est: i32,
    /// 整个 prompt（system + tools + 全部 messages）的 estimate token，比例分摊的分母。
    pub prompt_total_est: i32,
    /// read 留存阻尼 R ∈ [0,1]：read 桶保留 `read × R`，其余推回 input（不给缓存折扣）。
    pub read_ratio: f64,
    /// Anthropic 标准计费模式开关（per-key，默认关）。开启后 [`CacheUsage::split_final`] 走
    /// [`CacheUsage::split_anthropic_standard`]：末条消息并入 creation、input 取纯余数（floor 1），
    /// 复现真实 Anthropic 暖缓存下 input≈1-2 的口径。关闭则走原 [`CacheUsage::split_against_total`]。
    pub billing_mode: bool,
    /// 利润控制器·创建回流 Cb ∈ [0,1]（仅标准模式生效，默认 0）：把便宜 read 按 Cb 升级成贵桶
    /// creation。`upgrade = read0 × Cb`；input 恒定不参与。默认 0 = 纯真实 Anthropic。
    pub creation_reflow: f64,
    /// Anthropic 标准计费模式下钉住的 input token 数（默认 2，可 per-key 覆盖）。复现真实 Anthropic
    /// 暖缓存下 input 为小常数（1/2）的口径；剩余 `total − pinned_input` 落缓存两桶。仅标准模式生效。
    pub pinned_input: i32,
}

/// 标准计费模式默认钉住的 input token 数。
pub const DEFAULT_PINNED_INPUT: i32 = 2;

impl Default for CacheUsage {
    /// 默认 = 不模拟缓存：`prompt_total_est == 0` 使 `split_against_total` 全量计入 input。
    fn default() -> Self {
        Self {
            input_est: 0,
            creation_est: 0,
            prompt_total_est: 0,
            read_ratio: 1.0,
            billing_mode: false,
            creation_reflow: 0.0,
            pinned_input: DEFAULT_PINNED_INPUT,
        }
    }
}

impl CacheUsage {
    /// 按真实 total 口径做互斥分摊，返回 `(input_tokens, cache_creation, cache_read)`，
    /// 三者满足 `input + creation + read == total_real`。
    ///
    /// `total_real` 是最终上报口径的全量 prompt token。input / creation 各按其 estimate 占比
    /// 折算到真实 total，剩余即 read；再对 read 施加留存阻尼 R（砍掉的部分推回 input）。
    /// 无可缓存内容（`prompt_total_est <= 0`）时全部计入 input，不凭空造缓存计数。
    pub fn split_against_total(&self, total_real: i32) -> (i32, i32, i32) {
        let total = total_real.max(0);
        if self.prompt_total_est <= 0 || total == 0 {
            return (total, 0, 0);
        }
        let denom = self.prompt_total_est as f64;
        let input_share = (self.input_est as f64 / denom).clamp(0.0, 1.0);
        let creation_share = (self.creation_est as f64 / denom).clamp(0.0, 1.0);

        // input / creation 按占比折算，clamp 保证 input + creation <= total。
        let mut input = ((total as f64) * input_share).round() as i32;
        input = input.clamp(0, total);
        let mut creation = ((total as f64) * creation_share).round() as i32;
        creation = creation.clamp(0, total - input);

        // 剩余即已缓存前缀（read 基数）。
        let read_base = total - input - creation;
        if read_base <= 0 {
            return (input, creation, 0);
        }
        // read 留存阻尼：保留 read_base × R，被砍部分推回 input（无缓存折扣），creation 不动。
        let r = self.read_ratio.clamp(0.0, 1.0);
        let read = ((read_base as f64) * r).round() as i32;
        let read = read.clamp(0, read_base);
        input += read_base - read;
        (input, creation, read)
    }

    /// 最终分摊入口：按 `billing_mode` 选择口径。关（默认）→ 原 [`Self::split_against_total`]
    /// （全局默认路径，零回归）；开 → [`Self::split_anthropic_standard`]（真实 Anthropic 口径 +
    /// 利润控制器）。三桶恒满足 `input + creation + read == total_real`。
    pub fn split_final(&self, total_real: i32) -> (i32, i32, i32) {
        if self.billing_mode {
            self.split_anthropic_standard(total_real)
        } else {
            self.split_against_total(total_real)
        }
    }

    /// Anthropic 标准计费口径 + 利润控制器（仅 `billing_mode` 开启时经 [`Self::split_final`] 调用）。
    ///
    /// **input 恒钉 `pinned_input`（默认 2，可 per-key 覆盖）**——复现真实 Anthropic：暖缓存下
    /// 几乎整段 prompt 都命中缓存断点，未缓存的 input 只剩小常数（1/2）。剩余 `total − pinned` 全部
    /// 落在缓存两桶（read + creation），input **不参与利润拨动**（恒定），调利润时 input 也不会变。
    ///
    /// **基线（Cb=0，纯真实 Anthropic）**：
    /// - `input = pinned_input`；
    /// - `creation0` = 本轮新增（`creation_est + input_est` 的占比折算，末条并入缓存写入）；
    /// - `read0` = 已缓存前缀 = `total − pinned − creation0`（暖缓存下吃绝大部分）。
    ///
    /// **利润控制器 Cb = `creation_reflow` ∈ [0,1]**（下游按上报 usage 付费，价 read 0.1x <
    /// input 1x < creation 1.25x）：把便宜的 read **升级**成贵的 creation——
    /// - `upgrade = read0 × Cb`；`creation_final = creation0 + upgrade`；`read_final = read0 − upgrade`。
    /// - Cb=0 → 纯真实 Anthropic（read 吃大头、利润 0 折扣）；Cb=1 → read 全部升级成 creation（利润最大）。
    /// - input 恒定不动，三桶和恒等 `total`。read_ratio(R) 在标准模式**不使用**（避免污染 input）。
    ///
    /// cold（`creation_est` 覆盖整段前缀）时 creation0≈total−pinned、read0≈0，退化为整段 creation。
    /// `total <= pinned_input` 或无可缓存内容时全部计入 input，不凭空造缓存计数。
    pub fn split_anthropic_standard(&self, total_real: i32) -> (i32, i32, i32) {
        let total = total_real.max(0);
        // pinned>=1；total<=pinned 或无可缓存内容：无法在钉 input=pinned 的同时再分缓存桶，全计 input。
        let pinned = self.pinned_input.max(1);
        if self.prompt_total_est <= 0 || total <= pinned {
            return (total, 0, 0);
        }
        let denom = self.prompt_total_est as f64;
        // 标准模式：末条消息（input_est）并入「本轮新增」= creation，复现 Anthropic 把末条也缓存。
        let creation_est = self.creation_est.saturating_add(self.input_est);
        let creation_share = (creation_est as f64 / denom).clamp(0.0, 1.0);

        // input 恒钉 pinned；剩余 (total-pinned) 在 creation / read 两桶间分配。
        let cacheable = total - pinned;
        let mut creation0 = ((cacheable as f64) * creation_share).round() as i32;
        creation0 = creation0.clamp(0, cacheable);
        let read0 = cacheable - creation0;

        // 利润控制器：把 read0 的 Cb 比例升级成贵桶 creation（read→creation，input 不动）。
        let cb = self.creation_reflow.clamp(0.0, 1.0);
        let upgrade = ((read0 as f64) * cb).round() as i32;
        let upgrade = upgrade.clamp(0, read0);
        let creation_final = creation0 + upgrade;
        let read_final = read0 - upgrade;
        (pinned, creation_final, read_final)
    }
}

/// 计量运行时治理：持有全局 read 留存阻尼 R + 缓存热度 TTL + 按会话的 last_seen 表
/// （运行时可经 Admin API 调整 R 与 TTL）。
///
/// 比旧的有状态 `CacheMeter` 轻得多：不存全前缀哈希链、不落盘，只存 `session → (上次请求秒,
/// 上次请求时的消息条数)` 一个表。秒用于判 cold/warm（见 [`Self::observe_session`]）；条数用于
/// 算「本轮新增了几条」从而界定 creation 区间（见 [`compute_structural_cache_usage`]）。
pub struct MeterGovernance {
    /// 全局 R 的 bit 表示（f64 → u64，原子读写）。per-key 未覆盖时用此值。
    read_ratio_bits: AtomicU64,
    /// 缓存热度 TTL（秒，原子）。距某会话上次请求超过此值即判 cold（缓存已凉）。
    ttl_secs: AtomicU64,
    /// 会话热度表：`isolation_seed → (上次请求 unix 秒, 上次请求的 messages 条数)`。
    last_seen: parking_lot::Mutex<std::collections::HashMap<String, (i64, usize)>>,
}

/// `last_seen` 表的清理阈值：超过此条目数时，借一次请求顺手清掉所有已过 2×TTL 的死会话，
/// 避免长期运行内存无界增长（纯防护，不影响判定语义）。
const LAST_SEEN_SWEEP_THRESHOLD: usize = 4096;

impl MeterGovernance {
    /// 用初始 R + TTL 构造（R clamp 到 [0,1]）。
    pub fn new(read_ratio: f64, ttl_secs: u64) -> Self {
        Self {
            read_ratio_bits: AtomicU64::new(read_ratio.clamp(0.0, 1.0).to_bits()),
            ttl_secs: AtomicU64::new(ttl_secs),
            last_seen: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// 当前全局 R。
    pub fn read_ratio(&self) -> f64 {
        f64::from_bits(self.read_ratio_bits.load(Ordering::Relaxed))
    }

    /// 设置全局 R（clamp 到 [0,1]），运行时立即对后续请求生效。
    pub fn set_read_ratio(&self, ratio: f64) {
        self.read_ratio_bits
            .store(ratio.clamp(0.0, 1.0).to_bits(), Ordering::Relaxed);
    }

    /// 当前缓存热度 TTL（秒）。
    pub fn ttl_secs(&self) -> u64 {
        self.ttl_secs.load(Ordering::Relaxed)
    }

    /// 设置缓存热度 TTL（秒），运行时立即对后续请求生效。
    pub fn set_ttl_secs(&self, ttl: u64) {
        self.ttl_secs.store(ttl, Ordering::Relaxed);
    }

    /// 记录本会话本次请求（时间 + 消息条数**高水位**），返回**上轮缓存还热时的消息条数高水位**。
    ///
    /// 返回 `Some(prev_n)` = warm：该会话此前出现过 **且** 距上次请求 `<= TTL`（缓存未凉），
    /// `prev_n` 是**已见过的消息条数高水位**，供调用方界定「本轮新增 = 当前条数 − prev_n」的
    /// creation 区间。返回 `None` = cold（首次出现 / 间隔超 TTL）→ 调用方把整段前缀按 creation
    /// 重写计费。`now` / `msg_count` 为本次请求的 unix 秒与 messages 条数（参数化便于测试）。
    ///
    /// **存高水位（`prev_n.max(msg_count)`）而非裸 msg_count**：`creation = msg_est[prev_n .. n-1]`
    /// 的下界依赖 prev_n，但同一 session seed 上可能出现**更小 msg_count** 的请求——OpenAI 端点回退
    /// key 级 seed 时多对话共享一条记录、Claude Code 的 title/探针/子任务复用同 session 但消息少、
    /// 历史被重截断、并发乱序。裸存会把 prev_n 打小，使下一条真实长请求算出**横跨整段历史**的巨大
    /// delta → `creation` 爆炸（吃掉本该进 read 便宜桶的历史）。取高水位后，短请求不再拉低下界，
    /// 后续长请求的 creation 恢复到「真实新增」量级。副作用只在合法 compaction/截断使条数**永久**
    /// 下降时出现：那一轮 creation 计 0（欠计新摘要）——**偏向便宜桶，经济上安全**，永不再虚高。
    /// cold（缓存已凉）则重置基线为本次条数，不保留旧高水位（前缀确实要整段重建）。
    pub fn observe_session(&self, session: &str, now: i64, msg_count: usize) -> Option<usize> {
        let ttl = self.ttl_secs.load(Ordering::Relaxed) as i64;
        let mut map = self.last_seen.lock();
        // 偶发清理：表过大时清掉死会话（超 2×TTL 没来过的）。
        if map.len() > LAST_SEEN_SWEEP_THRESHOLD {
            let dead_before = now - ttl.saturating_mul(2).max(0);
            map.retain(|_, &mut (last, _)| last >= dead_before);
        }
        let warm = match map.get(session) {
            Some(&(last, prev_n)) if now.saturating_sub(last) <= ttl => Some(prev_n),
            _ => None,
        };
        // warm：存高水位（短请求不拉低下界）；cold：重置基线为本次条数。
        let stored_n = match warm {
            Some(prev_n) => prev_n.max(msg_count),
            None => msg_count,
        };
        map.insert(session.to_string(), (now, stored_n));
        warm
    }
}

/// `Arc<MeterGovernance>` 别名
pub type SharedMeterGovernance = Arc<MeterGovernance>;

/// 当前 unix 秒（i64）。用于会话热度判定的时间基准。
pub fn now_unix_secs() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ============================================================================
// 与请求体协议层的接线
// ============================================================================

use super::stream::estimate_tokens;
use super::types::{MessagesRequest, SystemMessage, Tool};

/// 计算本次请求的 delta-based 结构化缓存覆盖情况。纯函数：只看请求结构、R、上轮消息条数，
/// 不依赖时间或负载。返回 [`CacheUsage`]，由调用方在拿到真实 total 后做互斥分摊。
///
/// 桶划分（见模块文档）：input = 最后一条 message；read = 其余前缀。`read_ratio` 是该请求
/// 生效的 R（per-key 覆盖优先，否则全局 [`MeterGovernance`]）。
///
/// `prev_msg_count` 是本会话上轮缓存还热时的上次消息条数（见 [`MeterGovernance::observe_session`]）:
/// - **`Some(prev_n)`**（缓存还热）→ creation = `messages[prev_n .. n-1]`，即上次见到后**新增、
///   且已沉为稳定前缀**的那几条（标准对话 = 倒数第二条一条；agent 工具循环可能多条），其余前缀
///   走 read 便宜桶。`prev_n` 钳到 `[0, n-1]`；若 `prev_n >= n-1`（无新增沉淀）则 creation=0。
/// - **`None`**（首次出现 / 超 TTL 缓存已凉）→ 整段可缓存前缀（system+tools+除最后一条外的全部
///   历史）按 **creation** 重写计费、read 基数=0，如同首轮重建缓存。这让"凉了的会话"不再白
///   拿 0.1× 折扣。
pub fn compute_structural_cache_usage(
    req: &MessagesRequest,
    read_ratio: f64,
    prev_msg_count: Option<usize>,
) -> CacheUsage {
    // system + tools 开销（首轮即首次写入缓存的那段）。
    let mut overhead: i32 = 0;
    if let Some(tools) = req.tools.as_ref() {
        for t in tools {
            overhead = overhead.saturating_add(tool_tokens(t));
        }
    }
    if let Some(systems) = req.system.as_ref() {
        for sys in systems {
            overhead = overhead.saturating_add(system_tokens(sys));
        }
    }

    let n = req.messages.len();
    if n == 0 {
        // 无 message：无可缓存内容，全入 input（prompt_total_est=0 触发默认分摊）。
        return CacheUsage {
            input_est: 0,
            creation_est: 0,
            prompt_total_est: 0,
            read_ratio: read_ratio.clamp(0.0, 1.0),
            ..CacheUsage::default()
        };
    }

    let msg_est: Vec<i32> = req.messages.iter().map(message_tokens).collect();
    let msgs_total: i32 = msg_est.iter().fold(0, |a, b| a.saturating_add(*b));
    let prompt_total_est = overhead.saturating_add(msgs_total);

    // input = 最后一条 message（本轮新问题），永不计入缓存。
    let input_est = msg_est[n - 1];
    // creation = 本轮"写入缓存"的部分：
    //   cold（None：首次/超TTL，缓存已凉）→ 整段可缓存前缀 = prompt_total − input，全部重写计
    //     creation（read 基数随之为 0），如同首轮；
    //   warm（Some(prev_n)）→ messages[prev_n .. n-1]：上次见到后新增、且已沉为稳定前缀的那几条。
    //     prev_n 钳到 [0, n-1]；标准对话每轮 +2 条（prev_n = n-2）→ 恰为倒数第二条；agent 工具
    //     循环一轮补进多对 → 覆盖全部新增中间消息。prev_n >= n-1（无新增沉淀，如纯重放）→ creation=0。
    let creation_est = match prev_msg_count {
        None => prompt_total_est.saturating_sub(input_est),
        Some(prev_n) => {
            let start = prev_n.min(n - 1);
            msg_est[start..n - 1].iter().fold(0i32, |a, b| a.saturating_add(*b))
        }
    };

    CacheUsage {
        input_est,
        creation_est,
        prompt_total_est,
        read_ratio: read_ratio.clamp(0.0, 1.0),
        ..CacheUsage::default()
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
            sum = sum
                .saturating_add(crate::image_resize::estimate_image_tokens(media_type, data) as i32);
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
                    sum = sum.saturating_add(
                        crate::image_resize::estimate_image_tokens(media_type, data) as i32,
                    );
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
/// 注：无 session 的客户端（OpenAI 端点 `metadata:None`、裸客户端）退回
/// `key:{key_id}:root:{hash(messages[0])}` —— **key 级 + 对话根哈希**。
///
/// 为什么加对话根哈希：单靠 `key:{key_id}` 会让同一 key 下**所有不同对话**共享一条
/// [`MeterGovernance::observe_session`] 记录。该记录存**消息条数高水位**，一旦某个长对话
/// 把水位顶高，同 key 上其余**更短对话**的 `prev_n` 就被顶到 `>= n-1` → creation 区间塌成空
/// → creation 恒为 0（216 实测 98.3% 请求 creation=0、read 占比 99.5% 的根因）。以对话根
/// （首条消息，整段对话生命周期内不变）哈希入 seed，使不同对话天然分到不同记录、各自独立
/// 高水位；同一对话的后续轮次 messages[0] 不变 → seed 不变 → 仍 warm（不会退化成每轮 cold）。
///
/// 与旧「全量对话指纹」方案的关键区别：旧方案把**整段消息**入哈希，每轮追加消息都变新 seed
/// → 永远首见即 cold → 命中率反降、creation 爆炸。这里**只哈希首条**，天然轮次稳定。
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
    // 无显式 session：key 级 + 对话根哈希，隔离同 key 下的不同对话。
    match req.messages.first() {
        Some(root) => format!("key:{key_id}:root:{:016x}", conversation_root_hash(root)),
        None => format!("key:{key_id}"),
    }
}

/// 对话根（首条消息）的稳定哈希（FNV-1a over role + 规范化文本）。
/// 只取首条：整段对话生命周期内不变 → 同一对话多轮同 seed；不同对话大概率不同 seed。
fn conversation_root_hash(root: &super::types::Message) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    let mut mix = |bytes: &[u8]| {
        for b in bytes {
            h ^= *b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
    };
    mix(root.role.as_bytes());
    mix(b"\x00");
    // content 可能是字符串或块数组；序列化为紧凑 JSON 后哈希（确定性、与结构无关的稳定串）。
    match serde_json::to_string(&root.content) {
        Ok(s) => mix(s.as_bytes()),
        Err(_) => mix(b"?"),
    }
    h
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
    use super::*;
    use super::super::types::{Message, MessagesRequest, Metadata, SystemMessage};

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

    // ---- split_against_total ------------------------------------------------

    #[test]
    fn split_no_prefix_all_input() {
        // prompt_total_est == 0（默认）→ 全量计入 input。
        let u = CacheUsage::default();
        assert_eq!(u.split_against_total(500), (500, 0, 0));
    }

    #[test]
    fn split_three_buckets_by_share() {
        // input 占比 10%、creation 占比 5%，剩余 85% 为 read（R=1 全留存）。
        let u = CacheUsage {
            input_est: 10,
            creation_est: 5,
            prompt_total_est: 100,
            read_ratio: 1.0,
            ..CacheUsage::default()
        };
        let (input, creation, read) = u.split_against_total(1000);
        assert_eq!(input, 100);
        assert_eq!(creation, 50);
        assert_eq!(read, 850);
        assert_eq!(input + creation + read, 1000);
    }

    #[test]
    fn split_creation_bounded_independent_of_history() {
        // creation 只随 creation_est 占比走，不随 read 基数（历史规模）变化——贵桶有界。
        // 短历史：total 小
        let short = CacheUsage {
            input_est: 10,
            creation_est: 20,
            prompt_total_est: 100,
            read_ratio: 1.0,
            ..CacheUsage::default()
        };
        // 长历史：同样的 input/creation 占比，但 prompt_total 大得多（read 基数暴涨）
        let long = CacheUsage {
            input_est: 10,
            creation_est: 20,
            prompt_total_est: 1000,
            read_ratio: 1.0,
            ..CacheUsage::default()
        };
        let (_, c_short, _) = short.split_against_total(300);
        let (_, c_long, r_long) = long.split_against_total(3000);
        // creation 占比相同（20/100 vs 20/1000 → 真实 total 也等比放大），关键是 read 吃掉增量
        assert_eq!(c_short, 60); // 300 × 20/100
        assert_eq!(c_long, 60); // 3000 × 20/1000 —— creation 不被历史放大
        assert!(r_long > 2000, "历史增长全进 read（便宜桶），不进 creation");
    }

    #[test]
    fn split_read_retention_pushes_to_input_not_creation() {
        // R<1：read 被砍的部分推回 input，creation 纹丝不动（贵桶经济正确）。
        let u = CacheUsage {
            input_est: 10,
            creation_est: 10,
            prompt_total_est: 100,
            read_ratio: 0.5,
            ..CacheUsage::default()
        };
        let (input, creation, read) = u.split_against_total(1000);
        // base: input=100, creation=100, read_base=800
        // R=0.5 → read=400，被砍 400 推回 input → input=500
        assert_eq!(input, 500);
        assert_eq!(creation, 100, "creation 不受 R 影响");
        assert_eq!(read, 400);
        assert_eq!(input + creation + read, 1000);
    }

    #[test]
    fn split_ratio_zero_no_read() {
        // R=0：完全不给缓存折扣，read 全部推回 input；creation 仍按其占比保留。
        let u = CacheUsage {
            input_est: 10,
            creation_est: 10,
            prompt_total_est: 100,
            read_ratio: 0.0,
            ..CacheUsage::default()
        };
        let (input, creation, read) = u.split_against_total(1000);
        assert_eq!(creation, 100);
        assert_eq!(read, 0);
        assert_eq!(input, 900);
    }

    #[test]
    fn split_pure_replay_hit_rate_equals_r() {
        // 纯重放（creation_est≈0：无新增沉淀）时命中率精确 = R。锁住此语义:
        //   R=1.0 → read≈total、input≈0 → 命中率 100%(贴近真实 Anthropic 稳态);
        //   R=0.8 → read=total×0.8、input=total×0.2 → 命中率精确 80%(实证里所有高命中样本卡 80.0% 的成因)。
        // 这是「配置能到多少」与「改码能到多少」归因的数学基准，不能悄悄漂移。
        let replay = |r: f64| CacheUsage {
            input_est: 0,
            creation_est: 0,
            prompt_total_est: 1000, // >0 触发分摊；input/creation 占比为 0 → read_base=total
            read_ratio: r,
            ..CacheUsage::default()
        };
        let hit = |(_i, _c, rd): (i32, i32, i32), tot: i32| rd as f64 / tot as f64;

        let (i1, c1, r1) = replay(1.0).split_against_total(1000);
        assert_eq!((i1, c1, r1), (0, 0, 1000), "R=1.0 纯重放 → 全 read");
        assert!((hit((i1, c1, r1), 1000) - 1.0).abs() < 1e-9, "R=1.0 → 命中率 100%");

        let (i8, c8, r8) = replay(0.8).split_against_total(1000);
        assert_eq!((i8, c8, r8), (200, 0, 800), "R=0.8 → read=800、被砍 200 推回 input");
        assert!((hit((i8, c8, r8), 1000) - 0.8).abs() < 1e-9, "R=0.8 → 命中率精确 80%");
    }

    #[test]
    fn split_is_deterministic() {
        let u = CacheUsage {
            input_est: 33,
            creation_est: 41,
            prompt_total_est: 207,
            read_ratio: 1.0,
            ..CacheUsage::default()
        };
        let a = u.split_against_total(4096);
        let b = u.split_against_total(4096);
        assert_eq!(a, b);
        assert_eq!(a.0 + a.1 + a.2, 4096, "互斥口径必须自洽");
    }

    #[test]
    fn split_zero_total_safe() {
        let u = CacheUsage {
            input_est: 10,
            creation_est: 10,
            prompt_total_est: 100,
            read_ratio: 1.0,
            ..CacheUsage::default()
        };
        assert_eq!(u.split_against_total(0), (0, 0, 0));
    }

    // ---- split_anthropic_standard（标准计费模式 + 利润控制器）-----------------

    /// 标准模式：input 恒钉 `pinned`；(total-pinned) 分到 creation/read；Cb 把 read 升级成 creation。
    /// read_ratio 在标准模式不使用（传 1.0 占位）。
    fn std_usage(
        input_est: i32,
        creation_est: i32,
        total_est: i32,
        cb: f64,
        pinned: i32,
    ) -> CacheUsage {
        CacheUsage {
            input_est,
            creation_est,
            prompt_total_est: total_est,
            read_ratio: 1.0,
            billing_mode: true,
            creation_reflow: cb,
            pinned_input: pinned,
        }
    }

    #[test]
    fn std_input_pinned_default_two_warm() {
        // 默认 pinned=2；暖缓存：creation_share=(5+15)/1000=2%。
        // cacheable=total-2=9998；creation0=9998×2%≈200；read0=余下。
        let u = std_usage(5, 15, 1000, 0.0, 2);
        let (i, c, r) = u.split_final(10000);
        assert_eq!(i, 2, "input 恒钉 pinned=2");
        assert_eq!(c, 200, "creation0 = (total-2) × 2% ≈ 200");
        assert_eq!(r, 9998 - 200, "read = cacheable − creation0");
        assert_eq!(i + c + r, 10000, "三桶和恒等 total");
    }

    #[test]
    fn std_input_pinned_configurable() {
        // pinned 可配置：设 5 → input 恒 5，cacheable=total-5。
        let u = std_usage(5, 15, 1000, 0.0, 5);
        let (i, c, r) = u.split_final(10000);
        assert_eq!(i, 5, "input 恒钉 pinned=5（可配置）");
        assert_eq!(i + c + r, 10000);
        // pinned 设 1 → input 恒 1（兼容旧行为）。
        let u1 = std_usage(5, 15, 1000, 0.0, 1);
        assert_eq!(u1.split_final(10000).0, 1, "pinned=1 → input 恒 1");
    }

    #[test]
    fn std_baseline_cb_zero_pure_anthropic() {
        // Cb=0：read 不升级，纯真实 Anthropic 口径，利润 0 折扣。pinned=2。
        let u = std_usage(2, 8, 500, 0.0, 2);
        let (i, c, r) = u.split_final(4000);
        assert_eq!(i, 2, "input 恒 pinned=2");
        // cacheable=3998；creation0=3998×10/500=80(round)；read0=3918。
        assert_eq!(c, 80);
        assert_eq!(r, 3918);
        assert_eq!(i + c + r, 4000);
    }

    #[test]
    fn std_profit_cb_upgrades_read_to_creation_input_stays_pinned() {
        // Cb=0.5：read0 的一半升级成贵桶 creation；input 仍恒 pinned（关键：调利润 input 不变）。
        let u = std_usage(2, 8, 500, 0.5, 2);
        let (i, c, r) = u.split_final(4000);
        // creation0=80, read0=3918。upgrade=3918×0.5=1959。creation=80+1959=2039, read=3918-1959=1959。
        assert_eq!(i, 2, "调利润时 input 依然恒 pinned=2");
        assert_eq!(c, 80 + 1959, "read 的一半升级进 creation");
        assert_eq!(r, 3918 - 1959);
        assert_eq!(i + c + r, 4000);
    }

    #[test]
    fn std_profit_cb_one_all_read_to_creation() {
        // Cb=1：read 全部升级成 creation（利润最大），read=0，input 仍 pinned。
        let u = std_usage(2, 8, 500, 1.0, 2);
        let (i, c, r) = u.split_final(4000);
        assert_eq!(i, 2);
        assert_eq!(r, 0, "Cb=1 → read 清零");
        assert_eq!(c, 3998, "cacheable 全进 creation（贵桶）");
        assert_eq!(i + c + r, 4000);
    }

    #[test]
    fn std_cold_whole_creation_input_pinned() {
        // cold：creation_est 覆盖整段前缀 → creation_share≈100% → creation0≈cacheable, read0≈0。
        let u = std_usage(10, 990, 1000, 0.0, 2);
        let (i, c, r) = u.split_final(1000);
        assert_eq!(i, 2, "input 恒 pinned=2");
        assert_eq!(r, 0, "cold 无 read 基数");
        assert_eq!(c, 998, "整段计 creation（cacheable=998）");
        assert_eq!(i + c + r, 1000);
    }

    #[test]
    fn std_disabled_falls_back_to_legacy_split() {
        // billing_mode=false → split_final 走原 split_against_total（零回归）。
        let u = CacheUsage {
            input_est: 10,
            creation_est: 5,
            prompt_total_est: 100,
            read_ratio: 1.0,
            billing_mode: false,
            creation_reflow: 0.0,
            pinned_input: DEFAULT_PINNED_INPUT,
        };
        assert_eq!(u.split_final(1000), u.split_against_total(1000));
    }

    #[test]
    fn std_no_cacheable_all_input() {
        // total <= pinned → 全计 input。
        let u = std_usage(0, 0, 0, 0.0, 2);
        assert_eq!(u.split_final(500), (500, 0, 0));
    }

    // ---- compute_structural_cache_usage ------------------------------------

    #[test]
    fn compute_cold_charges_whole_prefix_as_creation() {
        // cold(首次/超TTL,缓存凉了)：整段可缓存前缀(system+历史,除最后一条)按 creation 重写、
        // read=0,如同首轮。对比同请求 warm 时只把倒数第二条计 creation、其余进 read。
        let big = "the quick brown fox ".repeat(40);
        let req = req_with(
            vec![
                msg("user", &big),
                msg("assistant", &big),
                msg("user", "short new question"),
            ],
            Some(vec![SystemMessage {
                text: "you are helpful ".repeat(50),
                cache_control: None,
            }]),
        );
        let warm = compute_structural_cache_usage(&req, 1.0, Some(req.messages.len() - 2));
        let cold = compute_structural_cache_usage(&req, 1.0, None);

        // 两者 input 相同(都是最后一条),prompt_total 相同。
        assert_eq!(cold.input_est, warm.input_est);
        assert_eq!(cold.prompt_total_est, warm.prompt_total_est);
        // cold 的 creation = 整段前缀 = total − input;warm 的 creation 只一条,远小于 cold。
        assert_eq!(cold.creation_est, cold.prompt_total_est - cold.input_est);
        assert!(cold.creation_est > warm.creation_est * 2, "cold 把整段前缀都计 creation");

        let (ci, cc, cr) = cold.split_against_total(cold.prompt_total_est);
        assert_eq!(cr, 0, "cold 无 read(整段重写)");
        assert_eq!(ci + cc, cold.prompt_total_est);
        let (_, wc, wr) = warm.split_against_total(warm.prompt_total_est);
        assert!(wr > 0, "warm 有 read");
        assert!(cc > wc, "cold 的 creation(贵桶)远多于 warm");
    }

    #[test]
    fn compute_single_message_first_write() {
        // 单条 message + system：input=该 message，creation=system(首次写缓存)，read=0。
        let req = req_with(
            vec![msg("user", "hello there friend")],
            Some(vec![SystemMessage {
                text: "you are helpful ".repeat(20),
                cache_control: None,
            }]),
        );
        let u = compute_structural_cache_usage(&req, 1.0, None);
        assert!(u.input_est > 0);
        assert!(u.creation_est > 0, "首轮 system+tools 计作 creation");
        let (input, creation, read) = u.split_against_total(u.prompt_total_est);
        assert_eq!(read, 0, "首轮无 read");
        assert!(input > 0 && creation > 0);
        assert_eq!(input + creation + read, u.prompt_total_est);
    }

    #[test]
    fn compute_single_message_no_overhead_all_input() {
        // 单条 message、无 system/tools：creation_est=0 → 全入 input。
        let req = req_with(vec![msg("user", "hi")], None);
        let u = compute_structural_cache_usage(&req, 1.0, None);
        assert_eq!(u.creation_est, 0);
        assert_eq!(u.input_est, u.prompt_total_est);
        let (input, creation, read) = u.split_against_total(u.prompt_total_est.max(1));
        assert_eq!(creation, 0);
        assert_eq!(read, 0);
        assert_eq!(input, u.prompt_total_est.max(1));
    }

    #[test]
    fn compute_multi_turn_delta_creation_is_prev_message() {
        // 历史(u1,a1) + 本轮 u2：input=u2，creation=a1(倒数第二条)，read=system+tools+u1。
        let big = "the quick brown fox ".repeat(40);
        let req = req_with(
            vec![
                msg("user", &big),
                msg("assistant", &big),
                msg("user", "short new question"),
            ],
            Some(vec![SystemMessage {
                text: "you are helpful ".repeat(50),
                cache_control: None,
            }]),
        );
        let u = compute_structural_cache_usage(&req, 1.0, Some(1));
        let a1_est = message_tokens(&msg("assistant", &big));
        let u2_est = message_tokens(&msg("user", "short new question"));
        assert_eq!(u.creation_est, a1_est, "creation = 倒数第二条 message");
        assert_eq!(u.input_est, u2_est, "input = 最后一条 message");
        let (input, creation, read) = u.split_against_total(u.prompt_total_est);
        assert!(read > 0, "非首轮应有 cache_read");
        assert!(creation > 0);
        assert!(read > creation, "read（system+u1）应远大于 creation（仅 a1）");
        assert_eq!(input + creation + read, u.prompt_total_est);
    }

    #[test]
    fn compute_creation_does_not_grow_with_history() {
        // 核心经济性质：对话越长，creation 仍≈一条 message，不随历史线性增长。
        let unit = "lorem ipsum dolor sit amet ".repeat(10);
        let short = req_with(
            vec![msg("user", &unit), msg("assistant", &unit), msg("user", "q")],
            None,
        );
        // 长对话：20 条历史 + 本轮
        let mut long_msgs: Vec<Message> = Vec::new();
        for i in 0..10 {
            long_msgs.push(msg("user", &format!("{unit} {i}")));
            long_msgs.push(msg("assistant", &unit));
        }
        long_msgs.push(msg("user", "q"));
        let long = req_with(long_msgs, None);

        let cu_short = compute_structural_cache_usage(&short, 1.0, Some(short.messages.len() - 2));
        let cu_long = compute_structural_cache_usage(&long, 1.0, Some(long.messages.len() - 2));
        // creation_est 都≈一条 assistant 消息，长对话不放大
        let a_est = message_tokens(&msg("assistant", &unit));
        assert_eq!(cu_short.creation_est, a_est);
        assert_eq!(cu_long.creation_est, a_est, "长对话 creation 仍是一条 message");
        // 而 prompt_total（→read 基数）长对话远大于短对话
        assert!(cu_long.prompt_total_est > cu_short.prompt_total_est * 5);

        let (_, c_short, _) = cu_short.split_against_total(cu_short.prompt_total_est);
        let (_, c_long, r_long) = cu_long.split_against_total(cu_long.prompt_total_est);
        assert!(r_long > c_long * 5, "长对话增量几乎全进便宜的 read 桶");
        // creation 真实 token 不爆炸（两者同数量级，长对话甚至更小，因占比被摊薄）
        assert!(c_long <= c_short + 5);
    }

    #[test]
    fn compute_read_retention_controls_discount() {
        // R 越大，read 越多、input 越少；creation 不变。
        let body = "lorem ipsum dolor sit amet ".repeat(20);
        let req = req_with(
            vec![msg("user", &body), msg("assistant", &body), msg("user", "q")],
            None,
        );
        let total = compute_structural_cache_usage(&req, 1.0, Some(1)).prompt_total_est;
        let (i_lo, c_lo, r_lo) = compute_structural_cache_usage(&req, 0.5, Some(1)).split_against_total(total);
        let (i_hi, c_hi, r_hi) = compute_structural_cache_usage(&req, 1.0, Some(1)).split_against_total(total);
        assert!(r_hi > r_lo, "R 越大 read 越多");
        assert!(i_hi < i_lo, "R 越大 input 越少（折扣更足）");
        assert_eq!(c_lo, c_hi, "creation 不受 R 影响");
    }

    #[test]
    fn compute_image_message_counts_tokens() {
        let png = make_test_png(750, 750);
        let img_tokens = crate::image_resize::estimate_image_tokens("image/png", &png) as i32;
        assert!(img_tokens > 100);
        let req = req_with(
            vec![
                Message {
                    role: "user".to_string(),
                    content: serde_json::json!([
                        {"type":"image","source":{"type":"base64","media_type":"image/png","data": png}},
                        {"type":"text","text":"describe"}
                    ]),
                },
                msg("assistant", "a pixel"),
                msg("user", "and now"),
            ],
            None,
        );
        let u = compute_structural_cache_usage(&req, 1.0, Some(1));
        // 含图历史(u1)在 read 前缀里 → prompt_total 应远大于本轮纯文本新输入。
        assert!(u.prompt_total_est >= img_tokens, "prompt_total 应含图片 token");
        let (_, _, read) = u.split_against_total(u.prompt_total_est);
        assert!(read > img_tokens / 2, "含图历史进 read 桶");
    }

    #[test]
    fn compute_tool_use_message_counted_as_creation() {
        // 回归：agentic 轮里倒数第二条常是 assistant 的 tool_use（无顶层 text，参数在 .input）。
        // 修复前只数 text/thinking → 该 message≈0 → creation 塌成 0。修复后必须计入 input 参数。
        let big_args = "x".repeat(2000);
        let tool_use = Message {
            role: "assistant".to_string(),
            content: serde_json::json!([{
                "type": "tool_use", "id": "toolu_1", "name": "run_bash",
                "input": { "command": big_args }
            }]),
        };
        let toolu_est = message_tokens(&tool_use);
        assert!(toolu_est > 100, "tool_use 参数必须计入 token，实得 {toolu_est}");

        // 历史 (u1, assistant tool_use) + 本轮 user：creation = 倒数第二条 = tool_use。
        let req = req_with(
            vec![msg("user", "do something"), tool_use, msg("user", "next")],
            None,
        );
        let u = compute_structural_cache_usage(&req, 1.0, Some(1));
        assert_eq!(u.creation_est, toolu_est, "creation 应等于 tool_use message 的 token");
        let (input, creation, read) = u.split_against_total(u.prompt_total_est);
        assert!(creation > 0, "修复后 cache_creation 不再塌成 0");
        assert_eq!(input + creation + read, u.prompt_total_est);
    }

    #[test]
    fn compute_tool_result_message_counted() {
        // 回归：user 侧 tool_result 文本嵌在 .content[]（顶层无 text）。修复前整段被漏。
        let big = "result line ".repeat(300);
        let tool_result = Message {
            role: "user".to_string(),
            content: serde_json::json!([{
                "type": "tool_result", "tool_use_id": "toolu_1", "content": big
            }]),
        };
        let tr_est = message_tokens(&tool_result);
        assert!(tr_est > 100, "tool_result 内容必须计入，实得 {tr_est}");
        // tool_result 作为历史前缀 → 进 read 桶，prompt_total 应含其 token。
        let req = req_with(
            vec![tool_result, msg("assistant", "ok"), msg("user", "q")],
            None,
        );
        let u = compute_structural_cache_usage(&req, 1.0, Some(1));
        assert!(u.prompt_total_est > tr_est, "prompt_total 应含 tool_result token");
    }

    #[test]
    fn compute_empty_messages_safe() {
        let req = req_with(vec![], None);
        let u = compute_structural_cache_usage(&req, 1.0, None);
        assert_eq!(u.input_est, 0);
        assert_eq!(u.creation_est, 0);
        assert_eq!(u.split_against_total(100), (100, 0, 0));
    }

    // ---- MeterGovernance ---------------------------------------------------

    #[test]
    fn governance_get_set_and_clamp() {
        let g = MeterGovernance::new(0.8, 300);
        assert!((g.read_ratio() - 0.8).abs() < 1e-9);
        g.set_read_ratio(0.95);
        assert!((g.read_ratio() - 0.95).abs() < 1e-9);
        // clamp 到 [0,1]
        g.set_read_ratio(1.5);
        assert!((g.read_ratio() - 1.0).abs() < 1e-9);
        g.set_read_ratio(-0.2);
        assert!((g.read_ratio() - 0.0).abs() < 1e-9);
        assert!((MeterGovernance::new(2.0, 300).read_ratio() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn governance_ttl_get_set() {
        let g = MeterGovernance::new(1.0, 300);
        assert_eq!(g.ttl_secs(), 300);
        g.set_ttl_secs(60);
        assert_eq!(g.ttl_secs(), 60);
    }

    #[test]
    fn governance_warmth_cold_then_warm_then_expired() {
        let g = MeterGovernance::new(1.0, 300);
        // 首次出现 → cold(None)，本次记 5 条
        assert_eq!(g.observe_session("sess:a", 1000, 5), None, "首次出现应判 cold");
        // TTL 内再来 → warm，返回上次条数 5；本次记 7 条
        assert_eq!(g.observe_session("sess:a", 1200, 7), Some(5), "TTL(300)内应 warm 且返回上次条数");
        // 超 TTL → cold(缓存凉了)；本次记 9 条
        assert_eq!(g.observe_session("sess:a", 1600, 9), None, "距上次>300s 应判 cold");
        // 刚刷新过,紧接着再来 → warm，返回刚记的 9
        assert_eq!(g.observe_session("sess:a", 1700, 11), Some(9), "刷新后 TTL 内应 warm");
        // 不同会话互不影响 → cold
        assert_eq!(g.observe_session("sess:b", 1700, 3), None, "另一会话首次应 cold");
    }

    #[test]
    fn governance_hwm_short_request_does_not_lower_prev_n() {
        // 核心修复：同一 seed 上出现更小 msg_count 的短请求（OpenAI key 级 seed 下的另一对话、
        // title/探针/子任务、被重截断的历史），不得把 prev_n 下界打小 → 否则下一条长请求会算出
        // 横跨整段历史的巨大 creation delta。存高水位后短请求不拉低下界。
        let g = MeterGovernance::new(1.0, 300);
        // 长对话到 200 条 → 首次 cold，记高水位 200。
        assert_eq!(g.observe_session("key:42", 1000, 200), None);
        // 同 seed 冒出一条短请求（另一对话/探针，只 3 条）→ warm，返回高水位 200（不是 3）。
        assert_eq!(
            g.observe_session("key:42", 1010, 3),
            Some(200),
            "短请求应读到高水位 200,而非被自己打小"
        );
        // 长对话回来到 202 条 → warm，返回的 prev_n 仍是高水位 200（旧 bug 会返回 3）。
        assert_eq!(
            g.observe_session("key:42", 1020, 202),
            Some(200),
            "长请求应读到高水位 200 → creation 只覆盖新增 2 条,不横跨历史"
        );
        // 高水位随真实增长上移。
        assert_eq!(g.observe_session("key:42", 1030, 205), Some(202));
    }

    #[test]
    fn governance_hwm_bounds_creation_delta() {
        // 端到端证明高水位把 creation 从「横跨整段历史」压回「本轮新增」量级。
        let body = "lorem ipsum dolor sit amet ".repeat(20);
        // 构造 6 条历史 + 末条：模拟长对话某轮 n=7。
        let mut msgs: Vec<Message> = Vec::new();
        for i in 0..6 {
            msgs.push(msg(if i % 2 == 0 { "user" } else { "assistant" }, &body));
        }
        msgs.push(msg("user", "new q"));
        let req = req_with(msgs, None);
        let n = req.messages.len(); // 7

        // 旧 bug：prev_n 被短请求打成 1 → creation 横跨 msg[1..6]（5 条）。
        let exploded = compute_structural_cache_usage(&req, 1.0, Some(1));
        // 修复：高水位使 prev_n = n-2 = 5 → creation 只覆盖 msg[5..6]（1 条）。
        let bounded = compute_structural_cache_usage(&req, 1.0, Some(n - 2));
        assert!(
            exploded.creation_est > bounded.creation_est * 3,
            "打小的 prev_n 会让 creation 爆炸(exploded={} vs bounded={})",
            exploded.creation_est,
            bounded.creation_est
        );
    }

    #[test]
    fn governance_cold_resets_baseline_not_hwm() {
        // cold（超 TTL，缓存确已凉）：重置基线为本次条数，不保留旧高水位——前缀整段要重建。
        let g = MeterGovernance::new(1.0, 100);
        assert_eq!(g.observe_session("key:9", 1000, 50), None); // 首次 cold，记 50
        assert_eq!(g.observe_session("key:9", 1050, 52), Some(50), "TTL 内 warm");
        // 超 TTL → cold，基线重置为本次的 4（不因高水位 52 而保留）。
        assert_eq!(g.observe_session("key:9", 1300, 4), None, "超 TTL 应 cold");
        // 紧接着来 → warm，读到刚重置的 4（证明 cold 没保留旧高水位 52）。
        assert_eq!(
            g.observe_session("key:9", 1310, 6),
            Some(4),
            "cold 后基线应是重置值 4,不是旧高水位 52"
        );
    }

    #[test]
    fn compute_warm_multi_message_burst_creation() {
        // C 方案核心：一轮补进多对消息（agent 工具循环）时，creation 覆盖**全部新增中间消息**，
        // 而非只倒数第二条。历史 [u0,a0]（上次 prev_n=2）+ 本轮新增 [a1,tr,a2] + 末条 input。
        let body = "lorem ipsum dolor sit amet ".repeat(20);
        let req = req_with(
            vec![
                msg("user", &body),      // 0  上轮已缓存
                msg("assistant", &body), // 1  上轮已缓存（prev_n=2 → [0,1] 是上次的前缀）
                msg("assistant", &body), // 2  本轮新增 ← creation
                msg("user", &body),      // 3  本轮新增（tool_result 占位）← creation
                msg("assistant", &body), // 4  本轮新增 ← creation
                msg("user", "new q"),    // 5  本轮 input
            ],
            None,
        );
        let est = |m: &Message| message_tokens(m);
        let burst: i32 = est(&req.messages[2]) + est(&req.messages[3]) + est(&req.messages[4]);
        let u = compute_structural_cache_usage(&req, 1.0, Some(2));
        assert_eq!(u.creation_est, burst, "creation 应覆盖上次见到后新增的全部中间消息");
        assert_eq!(u.input_est, est(&req.messages[5]), "input 仍是末条");
        let (input, creation, read) = u.split_against_total(u.prompt_total_est);
        assert!(creation > 0 && read > 0);
        assert_eq!(input + creation + read, u.prompt_total_est);

        // 对比旧「倒数第二条」语义（prev_n = n-2 = 4）：creation 只一条，明显偏小。
        let old = compute_structural_cache_usage(&req, 1.0, Some(req.messages.len() - 2));
        assert_eq!(old.creation_est, est(&req.messages[4]));
        assert!(u.creation_est > old.creation_est * 2, "多消息 burst 下 C 比旧语义计入更多 creation");
    }

    #[test]
    fn compute_warm_no_new_settled_creation_zero() {
        // warm 但 prev_n >= n-1（纯重放：上次条数 == 本次条数，无新增沉淀）→ creation=0。
        let body = "lorem ipsum ".repeat(20);
        let req = req_with(
            vec![msg("user", &body), msg("assistant", &body), msg("user", "q")],
            None,
        );
        let u = compute_structural_cache_usage(&req, 1.0, Some(3)); // prev_n == n
        assert_eq!(u.creation_est, 0, "无新增沉淀时 creation 为 0");
        let u2 = compute_structural_cache_usage(&req, 1.0, Some(2)); // prev_n == n-1
        assert_eq!(u2.creation_est, 0, "prev_n==n-1（末条即新增）→ 新增全是 input，creation=0");
    }

    // ---- isolation_seed ----------------------------------------------------

    #[test]
    fn isolation_seed_prefers_session_then_key() {
        let req = req_with(vec![msg("user", "x")], None);
        // 无 session：回退 key 级 + 对话根哈希（不再是裸 key:7），前缀仍以 key:7 打头。
        let fallback = isolation_seed(&req, 7);
        assert!(
            fallback.starts_with("key:7:root:"),
            "无 session 回退应为 key:7:root:<hash>，实得 {fallback}"
        );
        // 显式 session 最高优先。
        let mut req2 = req;
        req2.metadata = Some(Metadata {
            user_id: Some("user_abc_account__session_uuid-123".to_string()),
        });
        assert_eq!(isolation_seed(&req2, 7), "sess:uuid-123");
    }

    #[test]
    fn extract_session_id_parses_claude_code_format() {
        assert_eq!(
            extract_session_id("user_xxx_account__session_0b4445e1-uuid"),
            Some("0b4445e1-uuid".to_string())
        );
        assert_eq!(extract_session_id("no-session-here"), None);
        assert_eq!(extract_session_id("trailing_session_"), None);
    }

    fn make_test_png(w: u32, h: u32) -> String {
        use base64::{Engine, engine::general_purpose::STANDARD as B64};
        use image::{ImageFormat, Rgb, RgbImage};
        use std::io::Cursor;
        let mut img = RgbImage::new(w, h);
        for y in 0..h {
            for x in 0..w {
                img.put_pixel(x, y, Rgb([(x % 256) as u8, (y % 256) as u8, 128]));
            }
        }
        let mut buf = Vec::new();
        img.write_to(&mut Cursor::new(&mut buf), ImageFormat::Png)
            .unwrap();
        B64.encode(&buf)
    }

    // ---- isolation_seed 根哈希隔离（修复目标）---------------------------------

    /// 无显式 session id 时，同一 key 下的**不同对话**必须拿到**不同 seed**（按对话根
    /// messages[0] 区分），否则它们共用一条 last_seen 记录 → 高水位互相污染 → creation 塌陷。
    /// 见 [`creation_collapses_when_conversations_share_key_seed`]。
    #[test]
    fn isolation_seed_distinguishes_conversations_under_same_key() {
        // 两个不同对话（首条 user 不同），无 metadata → 回退 key 级 seed。
        let conv_a = req_with(vec![msg("user", "help me refactor the auth module")], None);
        let conv_b = req_with(vec![msg("user", "write a poem about the sea")], None);

        let seed_a = isolation_seed(&conv_a, 0);
        let seed_b = isolation_seed(&conv_b, 0);
        assert_ne!(
            seed_a, seed_b,
            "同 key 下不同对话应得到不同 seed（当前实现都返回 key:0 → 会红）"
        );
    }

    /// 同一对话的多轮请求（messages[0] 不变、后续追加）必须拿到**相同 seed**，
    /// 否则每轮都变新 seed → 永远 cold → creation 爆炸（正是上次全量指纹方案翻车点）。
    #[test]
    fn isolation_seed_stable_across_turns_of_same_conversation() {
        let root = "help me refactor the auth module";
        let turn1 = req_with(vec![msg("user", root)], None);
        let turn2 = req_with(
            vec![
                msg("user", root),
                msg("assistant", "sure, let's start"),
                msg("user", "now add tests"),
            ],
            None,
        );
        assert_eq!(
            isolation_seed(&turn1, 0),
            isolation_seed(&turn2, 0),
            "同一对话多轮（messages[0] 不变）必须同 seed，否则永远 cold"
        );
    }

    /// 显式 session id 仍最高优先（根哈希只是无 session 时的回退隔离）。
    #[test]
    fn isolation_seed_explicit_session_takes_priority() {
        let mut req = req_with(vec![msg("user", "anything")], None);
        req.metadata = Some(Metadata {
            user_id: Some("user_abc_account__session_deadbeef".to_string()),
        });
        assert_eq!(isolation_seed(&req, 0), "sess:deadbeef");
    }

    // ---- creation 塌陷复现（seed 碰撞 + 高水位）--------------------------------

    /// 复现 216 实测病象：同一 key 下多个**不同对话**共用一条 `key:N` seed（客户端不带
    /// `_session_`，isolation_seed 回退到 key 级）。observe_session 存消息条数**高水位**，
    /// 一旦某个长对话把水位顶高，之后同 key 上任何**更短对话的请求**都满足 `prev_n >= n-1`
    /// → creation 区间 `msg_est[prev_n.min(n-1) .. n-1]` 塌成空 → creation=0。
    ///
    /// 这正是 98.3% 请求 cache_creation=0、read 占比 99.5% 的根因：短对话的合法新增被
    /// 长对话的历史高水位吞掉，全塞进便宜的 read 桶，贵的 creation 桶几乎永不产生。
    #[test]
    fn creation_collapses_when_conversations_share_key_seed() {
        // 两个 message 大小一致，便于用条数直接推断 creation 区间。
        let seed = "key:0"; // 无 _session_ 时的 fallback seed
        let g = MeterGovernance::new(1.0, 3600);

        // 对话 A：一个很长的 agent 对话，把高水位顶到 40 条。
        assert_eq!(g.observe_session(seed, 1_000, 40), None, "A 首次出现 → cold");
        // A 继续，warm，返回高水位 40。
        assert_eq!(g.observe_session(seed, 1_010, 42), Some(40), "A 第二轮 warm，prev_n=40");

        // 对话 B：一个**全新的短对话**，但共用同一 key seed。它的第 2 轮只有 4 条消息，
        // 本该把「上次后新增的中间消息」计入 creation。但 observe_session 返回的是**高水位 40**，
        // 远大于 B 的消息数 4。
        let prev_n_for_b = g
            .observe_session(seed, 1_020, 4)
            .expect("同 key 且 TTL 内 → warm");
        assert_eq!(prev_n_for_b, 42, "B 拿到的是被 A 顶高的水位，而非 B 自己的历史");

        // 用这个被污染的 prev_n 计算 B 的一轮真实对话（4 条：u,a,u,a → 末条为 input，
        // 中间的 a(索引1)、u(索引2) 本应计 creation）。
        let big = "x".repeat(4000);
        let b_req = req_with(
            vec![
                msg("user", &big),      // 0
                msg("assistant", &big), // 1  ← 本应计 creation
                msg("user", &big),      // 2  ← 本应计 creation
                msg("assistant", &big), // 3  ← input（末条）
            ],
            None,
        );
        let n = b_req.messages.len(); // 4
        assert!(prev_n_for_b >= n - 1, "被污染的 prev_n({}) >= n-1({})", prev_n_for_b, n - 1);

        let usage = compute_structural_cache_usage(&b_req, 1.0, Some(prev_n_for_b));
        // BUG：creation 区间 = msg_est[min(42, 3) .. 3] = msg_est[3..3] = 空 → 0。
        assert_eq!(
            usage.creation_est, 0,
            "复现塌陷：B 的合法新增(msg 1,2)被 A 的高水位吞掉 → creation=0"
        );

        // 对照：若 B 用自己真实的上轮条数(2)计算，creation 应覆盖 msg[2]（非零）。
        let correct = compute_structural_cache_usage(&b_req, 1.0, Some(2));
        assert!(
            correct.creation_est > 0,
            "正确隔离下 B 的新增应计入 creation（当前实现因 seed 碰撞算成 0）"
        );
    }
}
