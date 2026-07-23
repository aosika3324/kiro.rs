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
/// 阻尼，最终由 [`DeltaCacheUsage::split_against_total`] 对真实 total 做互斥分摊。
///
/// 三个 estimate 是比例基准（不是最终值）——真正的 token 数要在拿到真实 total（contextUsage
/// 真值或 count_tokens 估算）后才按比例算出，因为流式响应直到末尾才知道真实 total。
#[derive(Debug, Clone, Copy)]
pub struct DeltaCacheUsage {
    /// 本轮新输入（最后一条 message）的 estimate token——这部分永不计入缓存。
    pub input_est: i32,
    /// 本轮新写入缓存的 delta（倒数第二条 message；首轮为 system+tools）的 estimate token。
    pub creation_est: i32,
    /// 整个 prompt（system + tools + 全部 messages）的 estimate token，比例分摊的分母。
    pub prompt_total_est: i32,
    /// 上轮已沉淀、本轮可作 `cache_read` 的旧前缀 estimate token（warm 时来自
    /// [`MeterGovernance::observe_session`] 高水位；cold=0）。目标率算法据此预测「真实该读多少」，
    /// 再由 `hit_rate` 削顶。
    pub prev_cached_est: i32,
    /// 目标缓存率 T ∈ [0,1]（新语义主旋钮）：面板显示 `cache_read / 总prompt` 逼近此值。
    /// 分摊时 `read = min(prev_cached, T×total)`，超出的旧前缀挪进 creation（1.25× 贵桶，真实
    /// 「部分重写」+ 利润）。T 越低 → read 越少、creation 越多 → 面板命中率越低、加权收入越高。
    /// 生效值在入口已按全局 `hit_rate_max` 夹紧。
    pub hit_rate: f64,
    /// **已废弃**（R 留存语义被 `hit_rate` 目标率取代）。保留仅为老配置/调用点兼容，分摊不再使用。
    pub read_ratio: f64,
    /// multiplier 护栏上限（C，仅**检测安全默认模式**生效）：`weighted/baseline` 超此值时把
    /// input→read 压回（见 [`DeltaCacheUsage::apply_multiplier_cap`]）。默认 [`DEFAULT_MULTIPLIER_CAP`]=1.25。
    /// `billing_mode` 开启（标准计费）时**不施加**此护栏（标准模式故意超报，护栏会抵消利润）。
    pub multiplier_cap: f64,
    /// Anthropic 标准计费模式开关（per-key，默认关）。开启后 [`DeltaCacheUsage::split_final`] 走
    /// [`DeltaCacheUsage::split_anthropic_standard`]：**真实互斥三桶口径**（`input + creation + read
    /// == total_real`，绝不超报、不双重收费），利润来自 R 把便宜的 read（0.1×）挪回 input（1.0×）。
    /// 与默认模式 [`DeltaCacheUsage::split_against_total`] 的唯一区别：**标准模式不施加 multiplier_cap
    /// 护栏**（接受更高检测风险换 margin），且 creation 由 `creation_ratio` 旋钮决定形状。
    pub billing_mode: bool,
    /// read 膨胀系数 p（**已废弃**：标准模式改互斥口径后不再超报，此字段被忽略）。
    /// 保留仅为老配置反序列化兼容——历史上曾用 `read_final = read0 × (1+p)` 超报,现已移除
    /// （超报即双重收费,与"贴近真实 Anthropic"矛盾）。利润改由 [`Self::read_ratio`]（R 挪桶）承担。
    pub read_inflation: f64,
    /// 标准模式 creation 占比旋钮（仅标准模式生效，默认 0.03）：`creation = cacheable × creation_ratio`，
    /// 复现真实 Anthropic 每轮写入一小段缓存（自然的小值）。read0 = cacheable − creation，再经 R 挪桶。
    /// 与 R 正交：creation_ratio 定"写多少"形状，R 定"读↔输入"利润;二者都不破坏 sum==total。
    pub creation_ratio: f64,
    /// 钉住的 input token 数（**已废弃**：标准模式改互斥口径后 input 由结构占比折算，不再钉常数）。
    /// 保留仅为老配置反序列化兼容,标准模式忽略此字段。
    pub pinned_input: i32,
    /// 本轮 creation 是否记入 **1h** ephemeral 桶（默认 false = 5m）。由入站请求的 `cache_control.ttl`
    /// 决定：任一断点标 `"1h"` → true（见 [`compute_structural_cache_usage`]）。仅影响上报时
    /// creation 在 `ephemeral_5m_input_tokens` / `ephemeral_1h_input_tokens` 的归桶与计价权重
    /// （5m=1.25× / 1h=2.0×），不改变三桶 token 总数。
    pub creation_is_1h: bool,
}

/// 下游按此权重给三桶计价（对齐真实 Anthropic：input 1.0 / cache_creation 1.25 / cache_read 0.1）。
/// 护栏据此算 `weighted = Σ 桶×权重`，与检测方 `weighted/baseline` 口径一致。
pub const WEIGHT_INPUT: f64 = 1.0;
/// cache_creation 计价权重（写入缓存 5m ephemeral，贵桶）。
pub const WEIGHT_CREATION: f64 = 1.25;
/// cache_creation 计价权重（写入缓存 **1h** ephemeral，最贵桶——真实 Anthropic 1h 写入为 2.0×）。
pub const WEIGHT_CREATION_1H: f64 = 2.0;
/// cache_read 计价权重（命中缓存，便宜桶）。
pub const WEIGHT_READ: f64 = 0.1;

/// multiplier 护栏默认上限。1.25 = 真实 Anthropic 暖缓存的自然上限（round1 缓存写就是 1.25x），
/// 故默认不扭曲正常形状、仅兜底保证绝不越异常线；per-key 可收紧到 1.0 留足检测余量。
pub const DEFAULT_MULTIPLIER_CAP: f64 = 1.25;

/// 标准计费模式默认钉住的 input token 数。
pub const DEFAULT_PINNED_INPUT: i32 = 2;

/// read 膨胀系数 p 的上限（read 最多 ×(1+MAX)）。
pub const MAX_READ_INFLATION: f64 = 9.0;

/// 标准模式 read 膨胀系数默认值（+20% 利润）。
pub const DEFAULT_READ_INFLATION: f64 = 0.2;

/// 标准模式 creation 占比默认值（cacheable 的 3%，自然的小值）。
pub const DEFAULT_CREATION_RATIO: f64 = 0.03;

impl Default for DeltaCacheUsage {
    /// 默认 = 不模拟缓存：`prompt_total_est == 0` 使 `split_against_total` 全量计入 input。
    fn default() -> Self {
        Self {
            input_est: 0,
            creation_est: 0,
            prompt_total_est: 0,
            prev_cached_est: 0,
            hit_rate: 1.0,
            read_ratio: 1.0,
            multiplier_cap: DEFAULT_MULTIPLIER_CAP,
            billing_mode: false,
            read_inflation: 0.0,
            creation_ratio: DEFAULT_CREATION_RATIO,
            pinned_input: DEFAULT_PINNED_INPUT,
            creation_is_1h: false,
        }
    }
}

impl DeltaCacheUsage {
    /// 按真实 total 口径做互斥分摊，返回 `(input_tokens, cache_creation, cache_read)`，
    /// 三者满足 `input + creation + read == total_real`。
    ///
    /// `total_real` 是最终上报口径的全量 prompt token。默认（检测安全）支路：走目标率分摊
    /// [`Self::split_target_rate`] 后**施加 multiplier_cap 护栏**。无可缓存内容
    /// （`prompt_total_est <= 0`）时全部计入 input，不凭空造缓存计数。
    pub fn split_against_total(&self, total_real: i32) -> (i32, i32, i32) {
        let (input, creation, read) = self.split_target_rate(total_real);
        self.apply_multiplier_cap(total_real.max(0), input, creation, read)
    }

    /// 目标缓存率分摊核心（两支路共用）。恒满足 `input + creation + read == total_real`（互斥）。
    ///
    /// 语义：input/creation/prev_cached 各按其 estimate 占比折算到真实 total；真实预测
    /// `read* = prev`（旧前缀全命中）、`creation* = delta`（本轮新沉淀）；再按目标率 T 削顶——
    /// `read = min(prev, round(T×total))`，超目标的旧前缀 `excess` 挪进 creation（1.25× 贵桶，
    /// 真实"部分重写" + 利润）。浮点 round 余数兜底进 input，保证三桶和恒等 total。
    /// cold 时 `prev_cached_est=0` → read=0（面板 0%，真实：缓存刚建没得读）。
    pub fn split_target_rate(&self, total_real: i32) -> (i32, i32, i32) {
        let total = total_real.max(0);
        if self.prompt_total_est <= 0 || total == 0 {
            return (total, 0, 0);
        }
        let denom = self.prompt_total_est as f64;
        let totf = total as f64;
        let input_share = (self.input_est as f64 / denom).clamp(0.0, 1.0);
        let prev_share = (self.prev_cached_est as f64 / denom).clamp(0.0, 1.0);

        // input 按占比折算（本轮真新增，永不缓存）。
        let mut input = (totf * input_share).round().clamp(0.0, totf) as i32;
        // 旧前缀（预测可 read 的量）折算到真实 total，clamp 到 input 之外的空间。
        let prev = (totf * prev_share).round().clamp(0.0, (total - input) as f64) as i32;

        // 目标率削顶：read 最多 min(prev, T×total)。超出的旧前缀挪 creation。
        let t = self.hit_rate.clamp(0.0, 1.0);
        let read_cap = (totf * t).round().clamp(0.0, totf) as i32;
        let read = prev.min(read_cap).clamp(0, total - input);
        let excess = (prev - read).max(0);

        // creation = 本轮真实新沉淀 delta（= cacheable_est − prev_cached_est，占比折算）+ 削顶 excess。
        // delta 直接由 total − input − prev 得到（互斥剩余），再加 excess；最后 input 吸收 round 余数。
        let delta = (total - input - prev).max(0);
        let mut creation = (delta + excess).clamp(0, total - input - read);
        // round 余数兜底进 input，保证 input + creation + read == total。
        input += total - input - creation - read;
        // 极端负数保护（浮点边界）。
        if input < 0 {
            creation = (creation + input).max(0);
            input = 0;
        }
        (input, creation, read)
    }

    /// multiplier 护栏（C）：`weighted/baseline` 超 `multiplier_cap` 时，把 input(1.0x) 闭式挪去
    /// read(0.1x) 压回上限，**不碰 creation**（creation=本轮真实新增，挪它=伪造暖轮 read → 因果违规）。
    ///
    /// 每挪 1 token input→read，weighted 降 `WEIGHT_INPUT − WEIGHT_READ = 0.9`。需挪
    /// `x = ceil((weighted − cap·baseline) / 0.9)`，钳到 `[0, input]`。三桶和不变（仍恒等 total）。
    /// 若 creation 单独就超 cap（input 已挪空仍压不下，如手动把 cap 设到 <1.25 的纯 creation 冷轮）：
    /// 保持 creation 诚实、宁可略高于该激进 cap，也不伪造 read。默认 cap=1.25 时此路径不触发。
    fn apply_multiplier_cap(
        &self,
        total: i32,
        input: i32,
        creation: i32,
        read: i32,
    ) -> (i32, i32, i32) {
        let cap = self.multiplier_cap;
        if cap <= 0.0 || total <= 0 {
            return (input, creation, read);
        }
        let baseline = total as f64;
        let weighted = WEIGHT_INPUT * input as f64
            + self.creation_weight() * creation as f64
            + WEIGHT_READ * read as f64;
        if weighted <= cap * baseline {
            return (input, creation, read);
        }
        let excess = weighted - cap * baseline;
        let mv = (excess / (WEIGHT_INPUT - WEIGHT_READ)).ceil() as i32;
        let mv = mv.clamp(0, input);
        (input - mv, creation, read + mv)
    }

    /// 本轮 creation 桶的计价权重：1h → [`WEIGHT_CREATION_1H`]（2.0），否则 5m → [`WEIGHT_CREATION`]（1.25）。
    fn creation_weight(&self) -> f64 {
        if self.creation_is_1h {
            WEIGHT_CREATION_1H
        } else {
            WEIGHT_CREATION
        }
    }

    /// 把总 creation 归桶为 `(ephemeral_5m, ephemeral_1h)`：按 [`Self::creation_is_1h`] 整段归到
    /// 对应桶，另一个为 0。上游无真实的每断点 token，presence-based 路由是唯一诚实且可实现的选择。
    pub fn creation_split(&self, creation: i32) -> (i32, i32) {
        if self.creation_is_1h {
            (0, creation)
        } else {
            (creation, 0)
        }
    }

    /// 最终分摊入口：按 `billing_mode` 选择口径。二者都恒满足 `input + creation + read
    /// == total_real`（互斥三桶，**绝不超报、不双重收费**）。区别只在护栏：
    /// - 关（默认）→ [`Self::split_against_total`]：检测安全，含 multiplier_cap 护栏。
    /// - 开（标准计费）→ [`Self::split_anthropic_standard`]：**不施加护栏**（接受更高检测风险
    ///   换 margin），creation 由 `creation_ratio` 旋钮定形状；利润来自 R 挪桶。
    pub fn split_final(&self, total_real: i32) -> (i32, i32, i32) {
        if self.billing_mode {
            self.split_anthropic_standard(total_real)
        } else {
            self.split_against_total(total_real)
        }
    }

    /// Anthropic 标准计费口径（仅 `billing_mode` 开启时经 [`Self::split_final`] 调用）。
    ///
    /// **互斥三桶，恒满足 `input + creation + read == total_real`——绝不超报、拒绝双重收费。**
    /// 与默认 [`Self::split_against_total`] 数学**同源**（都走目标率 [`Self::split_target_rate`]），
    /// 唯一区别是**不施加 multiplier_cap 护栏**（接受更高检测风险换 margin）。利润与形状由
    /// 目标率 T（`hit_rate`）统一表达：T 越低 → read 越少、creation 越多 → 加权收入越高。
    /// output 独立按输出价计费，不在此三桶内。无可缓存内容（`prompt_total_est<=0`）时全计 input。
    pub fn split_anthropic_standard(&self, total_real: i32) -> (i32, i32, i32) {
        // 标准模式 = 目标率分摊但不施加护栏（与默认模式的唯一差异）。
        self.split_target_rate(total_real)
    }
}

/// 计量口径分派枚举：把两套互不兼容的缓存计量模型收敛到一个统一的类型，供
/// [`crate::anthropic::stream::StreamContext`] / 非流式 handler 无差别持有。
///
/// - [`CacheUsage::Delta`]：**默认检测安全模式**（`billing_mode=false`）——无状态、确定性的
///   delta-based 结构化拆分（见 [`DeltaCacheUsage`] 与 [`compute_structural_cache_usage`]）。
/// - [`CacheUsage::Cch`]：**Anthropic 标准计费模式**（`billing_mode=true`）——上游 CCH 内容
///   指纹计量（有状态最长公共前缀命中，见 [`CchResult`] 与 [`cch_compute_cache_usage`]）。
///
/// 两个变体都实现 `split_final` / `creation_split`，消费端（handlers / stream）调用形式完全一致。
#[derive(Debug, Clone, Copy)]
pub enum CacheUsage {
    /// 默认检测安全模式：delta-based 结构化拆分（不动）。
    Delta(DeltaCacheUsage),
    /// Anthropic 标准计费模式：CCH 内容指纹计量。
    Cch(CchResult),
}

impl Default for CacheUsage {
    /// 默认 = 检测安全的 delta 模式（`billing_mode=false` 支路）。
    fn default() -> Self {
        CacheUsage::Delta(DeltaCacheUsage::default())
    }
}

impl CacheUsage {
    /// 最终三桶分摊，返回 `(input, cache_creation, cache_read)`，恒满足 `sum == total_real`。
    /// - Delta → [`DeltaCacheUsage::split_final`]（默认走 split_against_total，billing_mode 走标准口径）。
    /// - Cch → [`CchResult::split_against_total`]（内容指纹命中的互斥分摊）。
    pub fn split_final(&self, total_real: i32) -> (i32, i32, i32) {
        match self {
            CacheUsage::Delta(d) => d.split_final(total_real),
            CacheUsage::Cch(c) => c.split_against_total(total_real),
        }
    }

    /// creation 归 `(ephemeral_5m, ephemeral_1h)` 桶：按 `creation_is_1h` 整段归桶（另一个为 0）。
    /// 两个变体口径一致：1h → `(0, creation)`，否则 5m → `(creation, 0)`。
    pub fn creation_split(&self, creation: i32) -> (i32, i32) {
        match self {
            CacheUsage::Delta(d) => d.creation_split(creation),
            CacheUsage::Cch(c) => {
                if c.creation_is_1h {
                    (0, creation)
                } else {
                    (creation, 0)
                }
            }
        }
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
    /// **语义迁移期**：新算法用「目标缓存率 T」（见 [`compute_structural_cache_usage`] 的
    /// `hit_rate`），此字段作为「未设 per-key `cache_hit_rate` 时的全局默认 T」回退值继续使用
    /// （旧 `cacheReadRatio` 的深轮语义 ≈ 目标命中率，数值平滑）。
    read_ratio_bits: AtomicU64,
    /// 缓存热度 TTL（秒，原子）。距某会话上次请求超过此值即判 cold（缓存已凉）。
    /// per-key `cache_ttl_secs` 未覆盖时用此全局值。
    ttl_secs: AtomicU64,
    /// 目标缓存率 T 的上限（bit 表示）。生效 T 在入口按此夹紧，防恒 ~100%（检测特征）。
    /// admin 前端可配，默认 [`DEFAULT_CACHE_HIT_RATE_MAX`]。
    hit_rate_max_bits: AtomicU64,
    /// 会话热度表：`isolation_seed → (上次请求 unix 秒, 已沉淀可缓存前缀 token 高水位估算)`。
    /// token 级（不再是 messages 条数）：warm 时该值即「上轮已缓存、本轮可 read 的前缀量」。
    last_seen: parking_lot::Mutex<std::collections::HashMap<String, (i64, u32)>>,
}

/// `last_seen` 表的清理阈值：超过此条目数时，借一次请求顺手清掉所有已过 2×TTL 的死会话，
/// 避免长期运行内存无界增长（纯防护，不影响判定语义）。
const LAST_SEEN_SWEEP_THRESHOLD: usize = 4096;

/// 目标缓存率 T 的默认上限。恒 ~100% 命中是检测特征，夹到 0.95 保留自然抖动余量。
pub const DEFAULT_CACHE_HIT_RATE_MAX: f64 = 0.95;

/// **真实性闸门（破绽 B）**：真实 Anthropic prompt caching 的最小可缓存前缀 token 门槛。
/// 可缓存前缀（`prompt_total − input`）低于此值 → 不缓存，全计 input（cache 两桶为 0）。
/// Anthropic 文档：Sonnet/Opus 1024、Haiku 2048；取保守下限 1024（低估门槛 = 更少请求被判可缓存
/// = 偏向诚实，不会把本不该缓存的短请求报成缓存）。可经 `KIRO_RS_MIN_CACHEABLE_TOKENS` 覆盖。
pub const DEFAULT_MIN_CACHEABLE_TOKENS: i32 = 1024;

/// 读取生效的最小可缓存门槛（env 覆盖，缺省 [`DEFAULT_MIN_CACHEABLE_TOKENS`]）。
fn min_cacheable_tokens() -> i32 {
    std::env::var("KIRO_RS_MIN_CACHEABLE_TOKENS")
        .ok()
        .and_then(|s| s.parse::<i32>().ok())
        .filter(|n| *n >= 0)
        .unwrap_or(DEFAULT_MIN_CACHEABLE_TOKENS)
}

impl MeterGovernance {
    /// 用初始 R（= 全局默认目标率 T）+ TTL 构造。命中率上限取默认 [`DEFAULT_CACHE_HIT_RATE_MAX`]。
    pub fn new(read_ratio: f64, ttl_secs: u64) -> Self {
        Self::new_with_max(read_ratio, ttl_secs, DEFAULT_CACHE_HIT_RATE_MAX)
    }

    /// 用初始 R + TTL + 命中率上限构造（均 clamp 到 [0,1]）。
    pub fn new_with_max(read_ratio: f64, ttl_secs: u64, hit_rate_max: f64) -> Self {
        Self {
            read_ratio_bits: AtomicU64::new(read_ratio.clamp(0.0, 1.0).to_bits()),
            ttl_secs: AtomicU64::new(ttl_secs),
            hit_rate_max_bits: AtomicU64::new(hit_rate_max.clamp(0.0, 1.0).to_bits()),
            last_seen: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// 当前全局默认目标缓存率 T（旧称 R）。
    pub fn read_ratio(&self) -> f64 {
        f64::from_bits(self.read_ratio_bits.load(Ordering::Relaxed))
    }

    /// 设置全局默认目标率 T（clamp 到 [0,1]），运行时立即对后续请求生效。
    pub fn set_read_ratio(&self, ratio: f64) {
        self.read_ratio_bits
            .store(ratio.clamp(0.0, 1.0).to_bits(), Ordering::Relaxed);
    }

    /// 当前目标缓存率上限。
    pub fn hit_rate_max(&self) -> f64 {
        f64::from_bits(self.hit_rate_max_bits.load(Ordering::Relaxed))
    }

    /// 设置目标缓存率上限（clamp 到 [0,1]），运行时立即生效。
    pub fn set_hit_rate_max(&self, v: f64) {
        self.hit_rate_max_bits
            .store(v.clamp(0.0, 1.0).to_bits(), Ordering::Relaxed);
    }

    /// 当前缓存热度 TTL（秒）。
    pub fn ttl_secs(&self) -> u64 {
        self.ttl_secs.load(Ordering::Relaxed)
    }

    /// 设置缓存热度 TTL（秒），运行时立即对后续请求生效。
    pub fn set_ttl_secs(&self, ttl: u64) {
        self.ttl_secs.store(ttl, Ordering::Relaxed);
    }

    /// 记录本会话本次请求（时间 + 可缓存前缀 token **高水位**），返回**上轮缓存还热时的
    /// 已沉淀可读前缀 token 高水位**。
    ///
    /// 返回 `Some(prev_cached)` = warm：该会话此前出现过 **且** 距上次请求 `<= ttl`（缓存未凉），
    /// `prev_cached` 是**已见过的可缓存前缀 token 高水位**，即本轮可作 `cache_read` 的旧前缀量；
    /// 调用方据此界定「本轮新写入 delta = 当前 cacheable − prev_cached」的 creation 区间。
    /// 返回 `None` = cold（首次出现 / 间隔超 ttl）→ 调用方把整段前缀按 creation 重写计费。
    ///
    /// `ttl` **由调用方传入**（per-key `cache_ttl_secs` 覆盖优先，否则全局 [`Self::ttl_secs`]）——
    /// 表按 session_key 共享，key_id 已并入 seed，天然隔离。`now` / `cacheable_est` 为本次请求的
    /// unix 秒与可缓存前缀（`prompt_total_est − input_est`）token 估算（参数化便于测试）。
    ///
    /// **存高水位（`prev.max(cacheable_est)`）而非裸值**：同一 session seed 上可能出现**更小**的
    /// 请求（OpenAI 端点回退 key 级 seed 多对话共享、Claude Code title/探针/子任务复用同 session
    /// 但消息少、历史被重截断、并发乱序）。裸存会把 prev 打小，使下一条长请求算出横跨整段历史的
    /// 巨大 delta → creation 爆炸（吃掉本该进 read 便宜桶的历史）。取高水位后短请求不拉低下界。
    /// 副作用只在合法 compaction/截断使前缀**永久**变短时出现：那轮 creation 计 0（欠计新摘要）
    /// ——偏向便宜桶、经济安全。cold 则重置基线为本次 cacheable，不留旧高水位（前缀确要整段重建）。
    pub fn observe_session(
        &self,
        session: &str,
        now: i64,
        cacheable_est: u32,
        ttl_secs: u64,
    ) -> Option<u32> {
        let ttl = ttl_secs as i64;
        let mut map = self.last_seen.lock();
        // 偶发清理：表过大时清掉死会话（超 2×TTL 没来过的）。
        if map.len() > LAST_SEEN_SWEEP_THRESHOLD {
            let dead_before = now - ttl.saturating_mul(2).max(0);
            map.retain(|_, &mut (last, _)| last >= dead_before);
        }
        let warm = match map.get(session) {
            Some(&(last, prev)) if now.saturating_sub(last) <= ttl => Some(prev),
            _ => None,
        };
        // warm：存高水位（短请求不拉低下界）；cold：重置基线为本次 cacheable。
        let stored = match warm {
            Some(prev) => prev.max(cacheable_est),
            None => cacheable_est,
        };
        map.insert(session.to_string(), (now, stored));
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

/// 计算本次请求的结构化缓存覆盖（目标缓存率口径）。纯函数：只看请求结构、目标率 T、上轮
/// 已缓存前缀 token。返回 [`DeltaCacheUsage`]，由调用方在拿到真实 total 后经
/// [`DeltaCacheUsage::split_target_rate`] 做互斥分摊。
///
/// - `hit_rate`：该请求生效的目标缓存率 T（per-key `cache_hit_rate` 覆盖优先，否则全局默认，
///   且已按 `hit_rate_max` 夹紧）。面板 `cache_read/总prompt` 逼近此值。
/// - `prev_cached`：本会话上轮缓存还热时**已沉淀可读前缀的 token 高水位**（见
///   [`MeterGovernance::observe_session`]，token 级）：
///   - **`Some(prev)`**（warm）→ `prev_cached_est = min(prev, cacheable)`（旧前缀，预测可 read），
///     `creation_est = cacheable − prev_cached_est`（本轮新沉淀 delta，写入缓存）。
///   - **`None`**（cold：首次/超 TTL）→ `prev_cached_est=0`，`creation_est=cacheable`（整段重写，
///     read=0），如首轮重建；面板显示 0%，真实。
/// 估算本次请求的**可缓存前缀** token（`system + tools + 除最后一条外的全部 messages`）。
/// 供入口在调用 [`compute_structural_cache_usage`] 前喂给 [`MeterGovernance::observe_session`]
/// 记高水位/判 cold-warm。与 `compute_structural_cache_usage` 内部口径一致（都用 estimate）。
pub fn estimate_cacheable_tokens(req: &MessagesRequest) -> i32 {
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
        return 0;
    }
    // 除最后一条（本轮新输入）外的历史前缀。
    let hist: i32 = req.messages[..n - 1]
        .iter()
        .map(message_tokens)
        .fold(0i32, |a, b| a.saturating_add(b));
    overhead.saturating_add(hist).max(0)
}

pub fn compute_structural_cache_usage(
    req: &MessagesRequest,
    hit_rate: f64,
    prev_cached: Option<i32>,
) -> DeltaCacheUsage {
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

    // 入站 cache_control.ttl 决定 creation 归 5m 还是 1h 桶（仅影响上报归桶与计价权重）。
    let creation_is_1h = request_marks_1h_cache(req);
    let t = hit_rate.clamp(0.0, 1.0);

    let n = req.messages.len();
    if n == 0 {
        // 无 message：无可缓存内容，全入 input（prompt_total_est=0 触发默认分摊）。
        return DeltaCacheUsage {
            input_est: 0,
            creation_est: 0,
            prompt_total_est: 0,
            prev_cached_est: 0,
            hit_rate: t,
            creation_is_1h,
            ..DeltaCacheUsage::default()
        };
    }

    let msg_est: Vec<i32> = req.messages.iter().map(message_tokens).collect();
    let msgs_total: i32 = msg_est.iter().fold(0, |a, b| a.saturating_add(*b));
    let prompt_total_est = overhead.saturating_add(msgs_total);

    // input = 最后一条 message（本轮新问题），永不计入缓存。
    let input_est = msg_est[n - 1];
    let cacheable = prompt_total_est.saturating_sub(input_est).max(0);

    // prev_cached / creation 由会话热度决定：
    //   cold（None）→ 无旧前缀可读，整段 cacheable 按 creation 重写；
    //   warm（Some(prev)）→ 旧前缀 min(prev, cacheable) 可 read，其余（本轮新沉淀）计 creation。
    let (prev_cached_est, creation_est) = match prev_cached {
        None => (0, cacheable),
        Some(prev) => {
            let p = prev.clamp(0, cacheable);
            (p, cacheable - p)
        }
    };

    DeltaCacheUsage {
        input_est,
        creation_est,
        prompt_total_est,
        prev_cached_est,
        hit_rate: t,
        creation_is_1h,
        ..DeltaCacheUsage::default()
    }
}

/// 请求里是否有任一 `cache_control` 断点标了 `ttl == "1h"`（大小写不敏感）。
///
/// 扫 system / tools 的强类型 `cache_control`，以及 message content blocks 里 JSON 形态的
/// `cache_control.ttl`（`Message.content` 是自由 `serde_json::Value`）。命中任一即返回 true——
/// creation 整段归 1h 桶（2.0× 权重）；否则默认 5m（1.25×）。仅影响上报归桶,不改 token 总数。
fn request_marks_1h_cache(req: &MessagesRequest) -> bool {
    fn is_1h(cc: &Option<super::types::CacheControl>) -> bool {
        cc.as_ref()
            .and_then(|c| c.ttl.as_deref())
            .is_some_and(|t| t.trim().eq_ignore_ascii_case("1h"))
    }
    if let Some(systems) = req.system.as_ref() {
        if systems.iter().any(|s| is_1h(&s.cache_control)) {
            return true;
        }
    }
    if let Some(tools) = req.tools.as_ref() {
        if tools.iter().any(|t| is_1h(&t.cache_control)) {
            return true;
        }
    }
    // message content blocks：content 为自由 JSON，扫其中对象的 cache_control.ttl。
    req.messages.iter().any(|m| json_has_1h_cache_control(&m.content))
}

/// **真实性闸门（破绽 A + B）**：本请求是否有资格产生 cache 两桶（cache_read/cache_creation）。
///
/// 真实 Anthropic 仅在①请求带 `cache_control` 断点、且②可缓存前缀（`estimate_cacheable_tokens`）
/// ≥ 最小门槛（[`DEFAULT_MIN_CACHEABLE_TOKENS`]）时才产生缓存两桶；否则整段计 `input_tokens`。
/// 不满足此条件却上报缓存 → 客户端 diff 请求即可识破转卖。计量入口据此决定：不合格 → 全 input。
pub fn cache_eligible(req: &MessagesRequest) -> bool {
    request_has_cache_control(req) && estimate_cacheable_tokens(req) >= min_cacheable_tokens()
}

/// 请求里是否**存在任一** `cache_control` 断点（不论 ttl）。
///
/// **真实性闸门（破绽 A）**：真实 Anthropic 只在请求显式带 `cache_control` 断点时才产生
/// `cache_read`/`cache_creation`；无任何断点 → 全部计 `input_tokens`、缓存两桶为 0。客户端
/// 若发不带断点的请求却看到我们上报缓存，即穿帮。故计量前先查此条件，无断点直接全 input。
/// 扫 system / tools 的强类型 `cache_control` + message content blocks 的 JSON 形态。
fn request_has_cache_control(req: &MessagesRequest) -> bool {
    if let Some(systems) = req.system.as_ref() {
        if systems.iter().any(|s| s.cache_control.is_some()) {
            return true;
        }
    }
    if let Some(tools) = req.tools.as_ref() {
        if tools.iter().any(|t| t.cache_control.is_some()) {
            return true;
        }
    }
    req.messages.iter().any(|m| json_has_cache_control(&m.content))
}

/// 递归扫 JSON 里是否存在任一 `cache_control` 键（用于 `Message.content` 的自由形态）。
fn json_has_cache_control(v: &serde_json::Value) -> bool {
    match v {
        serde_json::Value::Object(map) => {
            if map.contains_key("cache_control") {
                return true;
            }
            map.values().any(json_has_cache_control)
        }
        serde_json::Value::Array(arr) => arr.iter().any(json_has_cache_control),
        _ => false,
    }
}

/// 递归扫 JSON 里任一 `cache_control.ttl == "1h"`（用于 `Message.content` 的自由形态）。
fn json_has_1h_cache_control(v: &serde_json::Value) -> bool {
    match v {
        serde_json::Value::Object(map) => {
            if let Some(cc) = map.get("cache_control") {
                if cc
                    .get("ttl")
                    .and_then(|t| t.as_str())
                    .is_some_and(|t| t.trim().eq_ignore_ascii_case("1h"))
                {
                    return true;
                }
            }
            map.values().any(json_has_1h_cache_control)
        }
        serde_json::Value::Array(arr) => arr.iter().any(json_has_1h_cache_control),
        _ => false,
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

// ============================================================================
// CCH（Content-fingerprint Cache metering）：Anthropic 标准计费模式（billing_mode=true）专用。
//
// 移植自上游 v0.7.1 src/anthropic/cache_metering.rs（合并时被丢弃那版），全部类型 / 函数加
// `Cch` / `cch_` 前缀，与本文件既有 delta 模型（DeltaCacheUsage / MeterGovernance /
// compute_structural_cache_usage / isolation_seed）**完全隔离、互不影响**。
//
// 有状态最长公共前缀命中：把 prompt 稳定前缀按 message 边界切成递增前缀段链，跨轮命中即
// cache_read，其后到末段即 cache_creation。内存 + JSON 落盘（cache_dir/cch_cache.json）。
// ============================================================================

use parking_lot::Mutex as CchMutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap as CchHashMap;
use std::path::PathBuf;

use super::types::CacheControl;

/// CCH 默认条目上限（防止内存无限增长）
const CCH_DEFAULT_CAPACITY: usize = 4096;
/// CCH 最长 TTL（1h，与 Anthropic ttl="1h" 对齐）
const CCH_MAX_TTL_SECS: i64 = 3600;
/// CCH 默认 TTL（5min，ephemeral 默认值）
const CCH_DEFAULT_TTL_SECS: i64 = 5 * 60;

/// CCH 单个缓存条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CchCacheEntry {
    /// 该前缀段累计的估算 token 数
    pub tokens: u32,
    /// 过期时间戳（unix 秒）
    pub expires_at: i64,
    /// 上次命中时间（用于 LRU 淘汰）
    pub last_hit_at: i64,
}

/// CCH 一次查询的结果（每段一份）
#[derive(Debug, Clone, Copy)]
pub struct CchSegmentResult {
    /// 该段是否命中
    pub hit: bool,
    /// 该段累计 tokens（保留供调试 / 调用方扩展）
    #[allow(dead_code)]
    pub tokens: u32,
}

/// `cch_compute_cache_usage` 的结果：缓存计费量 + 比例分摊所需的 estimate 口径基准。
///
/// `cache_read` / `cache_covered_est` 是按 estimate 口径算出的「被缓存覆盖前缀」拆分；
/// 最终上报按 [`Self::split_against_total`] 换算到真实 total 口径（互斥三桶 sum==total）。
/// `creation_is_1h` 为 fork 新增字段（CCH 原版无）：由入站请求 cache_control.ttl 决定 creation
/// 归 5m / 1h 桶（见 [`CacheUsage::creation_split`]），不改三桶 token 总数。
#[derive(Debug, Clone, Copy, Default)]
pub struct CchResult {
    /// 缓存读取 token（estimate 口径，最深命中段累计）。
    pub cache_read: i32,
    /// 被缓存覆盖前缀的 estimate token 总量（read + creation）。
    pub cache_covered_est: i32,
    /// 整个 prompt 的 estimate token 总量（比例分摊的分母）。
    pub prompt_total_est: i32,
    /// 本轮 creation 是否记入 1h ephemeral 桶（默认 false = 5m）。fork 新增。
    pub creation_is_1h: bool,
}

impl CchResult {
    /// 按真实 total 口径做互斥分摊，返回 `(input_tokens, cache_creation, cache_read)`。
    ///
    /// `total_real` 是最终上报口径的全量 prompt token。三者满足 `input + creation + read == total_real`。
    /// 无缓存覆盖（`cache_covered_est == 0`）或基准缺失时，直接返回 `(total_real, 0, 0)`——全部
    /// 计入 input，不凭空造缓存计数。
    pub fn split_against_total(&self, total_real: i32) -> (i32, i32, i32) {
        let total = total_real.max(0);
        if self.cache_covered_est <= 0 || self.prompt_total_est <= 0 {
            return (total, 0, 0);
        }
        // 比例无量纲，跨估算器成立；clamp 到 [0, total] 防止 estimate 偏差越界。
        let ratio = (self.cache_covered_est as f64 / self.prompt_total_est as f64).clamp(0.0, 1.0);
        let cache_total = ((total as f64) * ratio).round() as i32;
        let cache_total = cache_total.min(total);
        // 在缓存覆盖部分内部，按 estimate 口径的 read/creation 占比二次拆分。
        let read = if self.cache_covered_est > 0 {
            ((cache_total as f64) * (self.cache_read as f64 / self.cache_covered_est as f64)).round()
                as i32
        } else {
            0
        };
        let read = read.clamp(0, cache_total);
        let creation = cache_total - read;
        let input = total - cache_total;
        (input, creation, read)
    }
}

/// CCH 进程内提示词缓存（有状态，内存 + JSON 落盘）。
pub struct CchCacheMeter {
    inner: CchMutex<CchInner>,
    persist_path: Option<PathBuf>,
}

#[derive(Default)]
struct CchInner {
    entries: CchHashMap<u64, CchCacheEntry>,
    /// 自上次落盘后是否有变化
    dirty: bool,
}

impl CchCacheMeter {
    /// 创建一个空 cache。`persist_path` 为 `Some` 时会自动从该文件加载历史。
    pub fn new(persist_path: Option<PathBuf>) -> Self {
        let mut inner = CchInner::default();
        if let Some(path) = persist_path.as_ref() {
            if let Ok(bytes) = std::fs::read(path) {
                if let Ok(entries) = serde_json::from_slice::<CchHashMap<u64, CchCacheEntry>>(&bytes)
                {
                    let now = cch_now_secs();
                    for (k, v) in entries {
                        if v.expires_at > now {
                            inner.entries.insert(k, v);
                        }
                    }
                    tracing::info!(
                        "CchCacheMeter 重建：从 {} 加载 {} 条有效记录",
                        path.display(),
                        inner.entries.len()
                    );
                }
            }
        }
        Self {
            inner: CchMutex::new(inner),
            persist_path,
        }
    }

    /// 查询一组前缀段哈希，返回每段命中情况；命中段会刷新 last_hit_at。
    pub fn lookup(&self, segment_hashes: &[u64], segment_tokens: &[u32]) -> Vec<CchSegmentResult> {
        debug_assert_eq!(segment_hashes.len(), segment_tokens.len());
        let now = cch_now_secs();
        let mut inner = self.inner.lock();
        let mut out = Vec::with_capacity(segment_hashes.len());
        for (h, t) in segment_hashes.iter().zip(segment_tokens.iter()) {
            let hit = match inner.entries.get_mut(h) {
                Some(entry) if entry.expires_at > now => {
                    entry.last_hit_at = now;
                    true
                }
                _ => false,
            };
            out.push(CchSegmentResult { hit, tokens: *t });
        }
        out
    }

    /// 把一组前缀段写入缓存（用于 miss 后登记 / 续期）。`ttl_secs` clip 到 [60, CCH_MAX_TTL_SECS]。
    pub fn record(&self, segment_hashes: &[u64], segment_tokens: &[u32], ttl_secs: i64) {
        debug_assert_eq!(segment_hashes.len(), segment_tokens.len());
        let ttl = ttl_secs.clamp(60, CCH_MAX_TTL_SECS);
        let now = cch_now_secs();
        let expires_at = now + ttl;
        let mut inner = self.inner.lock();
        for (h, t) in segment_hashes.iter().zip(segment_tokens.iter()) {
            inner.entries.insert(
                *h,
                CchCacheEntry {
                    tokens: *t,
                    expires_at,
                    last_hit_at: now,
                },
            );
        }
        inner.dirty = true;
        // 容量超限：按 last_hit_at 淘汰最旧的若干条
        if inner.entries.len() > CCH_DEFAULT_CAPACITY {
            let drop_n = inner.entries.len() - CCH_DEFAULT_CAPACITY;
            let mut victims: Vec<(u64, i64)> = inner
                .entries
                .iter()
                .map(|(k, v)| (*k, v.last_hit_at))
                .collect();
            victims.sort_by_key(|x| x.1);
            for (k, _) in victims.into_iter().take(drop_n) {
                inner.entries.remove(&k);
            }
        }
    }

    /// 把当前快照写到 persist_path（仅在 dirty 时实际落盘）
    pub fn flush_to_disk(&self) {
        let path = match self.persist_path.clone() {
            Some(p) => p,
            None => return,
        };
        let snapshot = {
            let mut inner = self.inner.lock();
            if !inner.dirty {
                return;
            }
            inner.dirty = false;
            inner.entries.clone()
        };
        let json = match serde_json::to_vec(&snapshot) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("CchCacheMeter 序列化失败: {}", e);
                return;
            }
        };
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Err(e) = std::fs::write(&path, json) {
            tracing::warn!("CchCacheMeter 落盘失败 {}: {}", path.display(), e);
        }
    }

    /// 启动后台周期任务：定期 flush + 清理过期条目
    pub fn spawn_background(self: Arc<Self>) {
        let weak = Arc::downgrade(&self);
        tokio::spawn(async move {
            let interval = std::time::Duration::from_secs(60);
            loop {
                tokio::time::sleep(interval).await;
                let Some(cache) = weak.upgrade() else { return };
                cache.evict_expired();
                cache.flush_to_disk();
            }
        });
    }

    /// 删除已过期条目（lookup 不命中过期时只是返回 miss，不会顺手清理；
    /// 这里在后台周期里清一次，避免内存膨胀）。
    pub fn evict_expired(&self) {
        let now = cch_now_secs();
        let mut inner = self.inner.lock();
        let before = inner.entries.len();
        inner.entries.retain(|_, v| v.expires_at > now);
        if inner.entries.len() != before {
            inner.dirty = true;
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.lock().entries.len()
    }
}

fn cch_now_secs() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 解析 cache_control 的 ttl 字符串（"5m" / "1h"）→ 秒
pub fn cch_parse_ttl(ttl: Option<&str>) -> i64 {
    match ttl {
        Some(s) if s.eq_ignore_ascii_case("1h") => 3600,
        Some(s) if s.eq_ignore_ascii_case("5m") => 300,
        _ => CCH_DEFAULT_TTL_SECS,
    }
}

/// `Arc<CchCacheMeter>` 别名
pub type SharedCchCacheMeter = Arc<CchCacheMeter>;

/// 协议层提取出来的一个"段"（segment）：从请求开头累计到本断点的所有内容。
#[derive(Debug, Clone, Copy)]
struct CchSegment {
    hash: u64,
    cumulative_tokens: u32,
    /// 该段单独的 ttl（秒）
    ttl_secs: i64,
}

/// 调用 CchCacheMeter 计算本次请求的缓存覆盖情况，并把所有断点（含命中段）记录回
/// cache、刷新 TTL。返回 [`CchResult`]，由调用方在拿到真实 total 后做互斥分摊。
///
/// 取最深命中的段索引 i*：`cache_read = segments[i*].cumulative_tokens`、
/// `cache_creation = segments.last().cumulative_tokens − segments[i*].cumulative_tokens`。
/// 全部 miss 时 cache_read = 0。没有任何可缓存前缀（空段）时返回全零 `CchResult`
/// （`split_against_total` 会把 total 全部计入 input）且不写入。
///
/// `creation_is_1h` 由入站请求 cache_control.ttl 决定（复用 delta 模型的 `request_marks_1h_cache`），
/// 仅影响 creation 5m/1h 归桶与计价权重，不改三桶 token 总数。
pub fn cch_compute_cache_usage(
    cache: &CchCacheMeter,
    req: &MessagesRequest,
    key_id: u64,
) -> CchResult {
    let creation_is_1h = request_marks_1h_cache(req);
    let (segments, prompt_total_est) = cch_extract_segments(req, key_id);
    if segments.is_empty() {
        // 无断点：仍带出 prompt_total_est，但 covered=0 → 全入 input。
        return CchResult {
            prompt_total_est: prompt_total_est as i32,
            creation_is_1h,
            ..Default::default()
        };
    }

    let hashes: Vec<u64> = segments.iter().map(|s| s.hash).collect();
    let cum_tokens: Vec<u32> = segments.iter().map(|s| s.cumulative_tokens).collect();
    let results = cache.lookup(&hashes, &cum_tokens);

    let deepest_hit = results.iter().rposition(|r| r.hit);
    let covered = *cum_tokens.last().unwrap();
    let cache_read = match deepest_hit {
        Some(i) => cum_tokens[i],
        None => 0u32,
    };

    // 把所有段一次性写回（命中段刷新 last_hit_at；未命中段插入）。所有段共用同一 ttl。
    cache.record(&hashes, &cum_tokens, segments[0].ttl_secs);

    CchResult {
        cache_read: cache_read as i32,
        cache_covered_est: covered as i32,
        prompt_total_est: prompt_total_est as i32,
        creation_is_1h,
    }
}

/// 从请求体里按顺序提取断点段：tools → system → messages（对齐 Anthropic 拼接顺序）。
/// 返回 `(segments, prompt_total_est)`。`key_id` 用于会话隔离（哈希以隔离种子起头）。
fn cch_extract_segments(req: &MessagesRequest, key_id: u64) -> (Vec<CchSegment>, u32) {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    let mut cum_tokens: u32 = 0;
    let mut segments: Vec<CchSegment> = Vec::new();
    // 被跳过的动态 system 头部 token：只计入 prompt_total 分母，不进哈希 / 缓存段。
    let mut dynamic_prefix_tokens: u32 = 0;

    // 会话隔离种子：为 None（主 Key 无 session，被多用户共享）时不模拟缓存，直接返回空段。
    let Some(seed) = cch_isolation_seed(req, key_id) else {
        return (Vec::new(), 0);
    };
    hasher.update(seed.as_bytes());

    // feed 解耦哈希与 token 估算：`hash_text` 进哈希链，`token_text` 进 token 累计。
    let feed = |hasher: &mut Sha256, hash_text: &str, token_text: &str, cum: &mut u32| {
        hasher.update(hash_text.as_bytes());
        if !token_text.is_empty() {
            *cum = cum.saturating_add(estimate_tokens(token_text).max(0) as u32);
        }
    };

    let commit = |hasher: &Sha256, cum: u32, segments: &mut Vec<CchSegment>, ttl_secs: i64| {
        let digest = hasher.clone().finalize();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&digest[..8]);
        let hash = u64::from_be_bytes(buf);
        segments.push(CchSegment {
            hash,
            cumulative_tokens: cum,
            ttl_secs,
        });
    };

    // 统一 ttl：探测整个请求里出现过的最大 cache_control.ttl，否则默认 5m。
    let ttl = cch_detect_max_ttl(req);

    // 1. tools（全部喂入，作为前缀基础的一部分；工具定义跨轮稳定）。
    if let Some(tools) = req.tools.as_ref() {
        for t in tools {
            feed(&mut hasher, &cch_tool_signature(t), &cch_tool_token_text(t), &mut cum_tokens);
        }
    }

    // 2. system —— 跳过「首个带 cache_control 的 block 之前」的动态头部（Claude Code 每轮变化
    //    的小 block 故意不打 cache_control）。若无任何 cache_control 则全部纳入。
    if let Some(systems) = req.system.as_ref() {
        let skip_until = systems
            .iter()
            .position(|s| s.cache_control.is_some())
            .unwrap_or(0);
        for sys in systems.iter().take(skip_until) {
            dynamic_prefix_tokens =
                dynamic_prefix_tokens.saturating_add(estimate_tokens(&sys.text).max(0) as u32);
        }
        for sys in systems.iter().skip(skip_until) {
            feed(&mut hasher, &cch_system_signature(sys), &sys.text, &mut cum_tokens);
        }
    }

    // tools+system 前缀作为链的第一个段（仅当确实有内容时）。
    if cum_tokens > 0 {
        commit(&hasher, cum_tokens, &mut segments, ttl);
    }

    // 3. messages：除最后一条外，每条 message 边界切一个递增前缀段。
    let last_idx = req.messages.len().saturating_sub(1);
    for (idx, msg) in req.messages.iter().enumerate() {
        // role 进哈希（区分 user/assistant 边界），但不计入 token。
        feed(&mut hasher, &msg.role, "", &mut cum_tokens);
        match &msg.content {
            serde_json::Value::String(s) => {
                feed(&mut hasher, s, s, &mut cum_tokens);
            }
            serde_json::Value::Array(arr) => {
                for v in arr {
                    if v.get("type").and_then(|t| t.as_str()) == Some("image") {
                        let (media_type, data) = image_source_parts(v);
                        hasher.update(b"block:image|");
                        hasher.update(media_type.as_bytes());
                        hasher.update(b"|");
                        hasher.update(data.as_bytes());
                        let img_tokens = crate::image_resize::estimate_image_tokens(media_type, data);
                        cum_tokens = cum_tokens.saturating_add(img_tokens);
                    } else {
                        feed(
                            &mut hasher,
                            &cch_block_signature_value(v),
                            &cch_block_token_text(v),
                            &mut cum_tokens,
                        );
                    }
                }
            }
            _ => {}
        }
        // 最后一条不切段（当前轮新输入，属 cache_creation 尾部）。
        if idx != last_idx {
            commit(&hasher, cum_tokens, &mut segments, ttl);
        }
    }

    // prompt_total 分母 = 可缓存前缀累计 + 被跳过的动态头部。
    (segments, cum_tokens.saturating_add(dynamic_prefix_tokens))
}

/// 生成 CCH 会话隔离种子。优先 metadata session；主 Key（key_id==0）无 session → None
/// （被多用户共享，不模拟缓存以免跨用户虚假命中）；其余客户端 Key 按 key 隔离。
/// 复用 delta 模型的 [`extract_session_id`]（同一 user_id 解析口径）。
fn cch_isolation_seed(req: &MessagesRequest, key_id: u64) -> Option<String> {
    if let Some(session) = req
        .metadata
        .as_ref()
        .and_then(|m| m.user_id.as_deref())
        .and_then(extract_session_id)
    {
        return Some(format!("sess:{session}"));
    }
    if key_id == 0 {
        return None;
    }
    Some(format!("key:{key_id}"))
}

/// 探测请求里出现过的最大 cache_control.ttl（"1h" 优先于 "5m"）；无任何 cache_control 时默认 5m。
fn cch_detect_max_ttl(req: &MessagesRequest) -> i64 {
    let mut ttl = CCH_DEFAULT_TTL_SECS;
    let mut bump = |cc: Option<&CacheControl>| {
        if let Some(cc) = cc {
            ttl = ttl.max(cch_parse_ttl(cc.ttl.as_deref()));
        }
    };
    if let Some(tools) = req.tools.as_ref() {
        for t in tools {
            bump(t.cache_control.as_ref());
        }
    }
    if let Some(systems) = req.system.as_ref() {
        for sys in systems {
            bump(sys.cache_control.as_ref());
        }
    }
    for msg in &req.messages {
        if let serde_json::Value::Array(arr) = &msg.content {
            for v in arr {
                if let Some(t) = v
                    .get("cache_control")
                    .and_then(|cc| cc.get("ttl"))
                    .and_then(|t| t.as_str())
                {
                    ttl = ttl.max(cch_parse_ttl(Some(t)));
                }
            }
        }
    }
    ttl
}

fn cch_tool_signature(t: &Tool) -> String {
    let schema = serde_json::to_string(&t.input_schema).unwrap_or_default();
    format!("tool:{}|{}|{}", t.name, t.description, schema)
}

/// 工具的 token 估算原文：name + description + schema 拼接，不含签名前缀 / 分隔符。
fn cch_tool_token_text(t: &Tool) -> String {
    let schema = serde_json::to_string(&t.input_schema).unwrap_or_default();
    format!("{} {} {}", t.name, t.description, schema)
}

fn cch_system_signature(s: &SystemMessage) -> String {
    format!("sys:{}", s.text)
}

/// 直接从 content block 的 JSON 值算签名，只取 type/text/thinking 三个字段。
fn cch_block_signature_value(v: &serde_json::Value) -> String {
    let s = |key: &str| v.get(key).and_then(|x| x.as_str()).unwrap_or("");
    format!("block:{}|{}|{}", s("type"), s("text"), s("thinking"))
}

/// content block 的 token 估算原文：仅 text + thinking 的纯文本，不含签名结构标记。
fn cch_block_token_text(v: &serde_json::Value) -> String {
    let s = |key: &str| v.get(key).and_then(|x| x.as_str()).unwrap_or("");
    let text = s("text");
    let thinking = s("thinking");
    if thinking.is_empty() {
        text.to_string()
    } else if text.is_empty() {
        thinking.to_string()
    } else {
        format!("{text} {thinking}")
    }
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
        let u = DeltaCacheUsage::default();
        assert_eq!(u.split_against_total(500), (500, 0, 0));
    }

    #[test]
    fn split_three_buckets_by_share() {
        // 目标率口径：input 占比 10%、旧缓存前缀占比 85%（T=1 全读）、本轮新沉淀 delta=5% 计 creation。
        let u = DeltaCacheUsage {
            input_est: 10,
            prev_cached_est: 85,
            prompt_total_est: 100,
            hit_rate: 1.0,
            ..DeltaCacheUsage::default()
        };
        let (input, creation, read) = u.split_against_total(1000);
        assert_eq!(input, 100);
        assert_eq!(creation, 50, "delta = total − input − prev = 1000−100−850");
        assert_eq!(read, 850, "T=1 → 旧前缀全命中");
        assert_eq!(input + creation + read, 1000);
    }

    #[test]
    fn split_creation_bounded_independent_of_history() {
        // 历史增长（旧缓存前缀变大）全进 read 便宜桶，creation（本轮新沉淀 delta）不被历史放大。
        // 短历史：delta 占 20%、旧前缀占 70%。
        let short = DeltaCacheUsage {
            input_est: 10,
            prev_cached_est: 70,
            prompt_total_est: 100,
            hit_rate: 1.0,
            ..DeltaCacheUsage::default()
        };
        // 长历史：同样 input(1%)/delta，旧前缀占比涨到 89%（read 基数暴涨）。
        let long = DeltaCacheUsage {
            input_est: 10,
            prev_cached_est: 890,
            prompt_total_est: 1000,
            hit_rate: 1.0,
            ..DeltaCacheUsage::default()
        };
        let (_, c_short, _) = short.split_against_total(300);
        let (_, c_long, r_long) = long.split_against_total(3000);
        // delta 占比相同(20/100 vs 100/1000=10%? 不——保持 delta = total−input−prev 等比)
        assert_eq!(c_short, 60, "300 − 30(input) − 210(prev) = 60 delta");
        assert_eq!(c_long, 300, "3000 − 30 − 2670 = 300 delta（等比放大，非被历史额外放大）");
        assert!(r_long > 2000, "历史增长主要进 read（便宜桶）");
    }

    #[test]
    fn split_target_rate_caps_read_excess_to_creation() {
        // T<1：read 被目标率削顶，超出的旧前缀挪进 creation（贵桶，真实"部分重写"+利润）。
        // input 占 10%、旧前缀占 80%，T=0.5 → read 上限=total×0.5。
        let u = DeltaCacheUsage {
            input_est: 10,
            prev_cached_est: 80,
            prompt_total_est: 100,
            hit_rate: 0.5,
            ..DeltaCacheUsage::default()
        };
        let (input, creation, read) = u.split_against_total(1000);
        // input=100, prev=800, read_cap=500 → read=500, excess=300
        // delta = 1000−100−800 = 100 → creation = 100 + 300 = 400
        assert_eq!(input, 100);
        assert_eq!(read, 500, "read 被 T=0.5 削顶到 total×0.5");
        assert_eq!(creation, 400, "被削的旧前缀 300 挪进 creation + delta 100");
        assert_eq!(input + creation + read, 1000);
        assert!((read as f64 / 1000.0 - 0.5).abs() < 1e-9, "命中率 = T");
    }

    #[test]
    fn split_target_rate_zero_no_read() {
        // T=0：不认任何缓存命中，旧前缀全挪进 creation（重写），read=0。
        let u = DeltaCacheUsage {
            input_est: 10,
            prev_cached_est: 80,
            prompt_total_est: 100,
            hit_rate: 0.0,
            ..DeltaCacheUsage::default()
        };
        let (input, creation, read) = u.split_against_total(1000);
        assert_eq!(read, 0);
        assert_eq!(creation, 900, "delta 100 + 全部旧前缀 800 挪 creation");
        assert_eq!(input, 100);
    }

    #[test]
    fn split_pure_replay_hit_rate_equals_t() {
        // 纯重放（整段前缀已缓存、无新增 delta：prev_cached_est == prompt_total_est，input=0）时
        // 命中率精确 = T。锁住此语义（面板 read/total 逼近 T）:
        //   T=1.0 → read=total、input=0、creation=0 → 命中率 100%(真实 Anthropic 稳态);
        //   T=0.8 → read=total×0.8，被削的 20% 挪 creation → 命中率精确 80%。
        let replay = |t: f64| DeltaCacheUsage {
            input_est: 0,
            prev_cached_est: 1000, // 整段前缀都是旧缓存,无新增 delta
            prompt_total_est: 1000,
            hit_rate: t,
            ..DeltaCacheUsage::default()
        };
        let hit = |(_i, _c, rd): (i32, i32, i32), tot: i32| rd as f64 / tot as f64;

        let (i1, c1, r1) = replay(1.0).split_against_total(1000);
        assert_eq!((i1, c1, r1), (0, 0, 1000), "T=1.0 纯重放 → 全 read");
        assert!((hit((i1, c1, r1), 1000) - 1.0).abs() < 1e-9, "T=1.0 → 命中率 100%");

        let (i8, c8, r8) = replay(0.8).split_against_total(1000);
        // input=0, prev=1000, read_cap=800 → read=800, excess=200 → creation=0+200
        assert_eq!((i8, c8, r8), (0, 200, 800), "T=0.8 → read=800、被削 200 挪 creation");
        assert!((hit((i8, c8, r8), 1000) - 0.8).abs() < 1e-9, "T=0.8 → 命中率精确 80%");
    }

    #[test]
    fn split_is_deterministic() {
        let u = DeltaCacheUsage {
            input_est: 33,
            creation_est: 41,
            prompt_total_est: 207,
            read_ratio: 1.0,
            ..DeltaCacheUsage::default()
        };
        let a = u.split_against_total(4096);
        let b = u.split_against_total(4096);
        assert_eq!(a, b);
        assert_eq!(a.0 + a.1 + a.2, 4096, "互斥口径必须自洽");
    }

    #[test]
    fn split_zero_total_safe() {
        let u = DeltaCacheUsage {
            input_est: 10,
            creation_est: 10,
            prompt_total_est: 100,
            read_ratio: 1.0,
            ..DeltaCacheUsage::default()
        };
        assert_eq!(u.split_against_total(0), (0, 0, 0));
    }

    #[test]
    fn split_sum_never_exceeds_total_detector_safe() {
        // 检测安全不变量：默认模式三桶和**恒等** total_real（互斥分摊），上报总量绝不 > 真实量。
        // 这是"过检测"的数学根基——检测方重新 tokenize 数出的 baseline == 我们 split 的 total_real，
        // 故 weighted/baseline 的 multiplier ≤ 1x（read 0.1x 桶越大越 < 1），永不会像超报那样冲到 7~20x。
        // 覆盖多种占比 + 各档 total，全部必须 sum == total，且各桶非负。
        for (ie, ce, pe, r) in [
            (10, 5, 100, 1.0),
            (33, 41, 207, 1.0),
            (10, 10, 100, 0.5),
            (0, 0, 1000, 0.8),
            (500, 100, 1000, 1.0),
        ] {
            let u = DeltaCacheUsage {
                input_est: ie,
                creation_est: ce,
                prompt_total_est: pe,
                read_ratio: r,
                ..DeltaCacheUsage::default()
            };
            for total in [1, 500, 4096, 10_000, 140_210] {
                let (i, c, rd) = u.split_against_total(total);
                assert!(i >= 0 && c >= 0 && rd >= 0, "桶非负");
                assert_eq!(i + c + rd, total, "三桶和恒等 total_real（multiplier≤1x，检测安全）");
            }
        }
    }

    /// 计算 multiplier = weighted/baseline（与检测方口径一致）。
    fn mult((i, c, r): (i32, i32, i32), total: i32) -> f64 {
        (WEIGHT_INPUT * i as f64 + WEIGHT_CREATION * c as f64 + WEIGHT_READ * r as f64)
            / total as f64
    }

    #[test]
    fn cap_default_1_25_is_noop_for_normal_shapes() {
        // 默认 cap=1.25 对正常形状不触发：三桶不被改写，multiplier 本就 ≤1.25。
        // input 占 10%、旧前缀占 40%、T=1；delta=50%。
        let u = DeltaCacheUsage {
            input_est: 10,
            prev_cached_est: 40,
            prompt_total_est: 100,
            hit_rate: 1.0,
            ..DeltaCacheUsage::default()
        };
        // input=100, prev=400, read=400, delta=1000−100−400=500 → creation=500
        let out = u.split_against_total(1000); // weighted=100+625+40=765 → 0.765x
        assert_eq!(out, (100, 500, 400), "默认 cap 不改写正常形状");
        assert!(mult(out, 1000) <= 1.25 + 1e-9);
    }

    #[test]
    fn cap_pushes_input_to_read_not_creation() {
        // 高 input 占比使 multiplier 逼近 1.0；收紧 cap=0.5 → 护栏把 input 挪回 read（不碰 creation）。
        // input 占 90%、无旧前缀 → input=900,creation=100(delta),read=0 → weighted=1025 → 1.025x。
        let u = DeltaCacheUsage {
            input_est: 90,
            prev_cached_est: 0,
            prompt_total_est: 100,
            hit_rate: 1.0,
            multiplier_cap: 0.5,
            ..DeltaCacheUsage::default()
        };
        let (i, c, r) = u.split_against_total(1000);
        assert_eq!(c, 100, "creation 绝不被护栏改动（诚实，不伪造暖轮 read）");
        assert_eq!(i + c + r, 1000, "三桶和仍恒等 total");
        assert!(mult((i, c, r), 1000) <= 0.5 + 1e-9, "护栏后 multiplier ≤ cap");
        assert!(r > 0 && i < 900, "input 被挪去 read 压回上限");
    }

    #[test]
    fn cap_zero_disables_guardrail() {
        // cap<=0 → 不设护栏，形状原样返回。input 占 90%、无旧前缀 → (900,100,0)。
        let u = DeltaCacheUsage {
            input_est: 90,
            prev_cached_est: 0,
            prompt_total_est: 100,
            hit_rate: 1.0,
            multiplier_cap: 0.0,
            ..DeltaCacheUsage::default()
        };
        assert_eq!(u.split_against_total(1000), (900, 100, 0), "cap=0 关闭护栏");
    }

    // ---- split_anthropic_standard（标准计费：互斥三桶，拒绝双重收费）--------------

    /// 标准模式构造（目标率口径，与默认模式同源，仅不施加护栏）：input=0（全量前缀可缓存），
    /// `prev_cached_est` 定旧缓存前缀占比、`total_est` 为分母、`t` 为目标缓存率。
    /// read=min(prev, T×total)，delta+excess 计 creation。便于单验 T 对 read/creation 形状的控制。
    fn std_usage(prev_cached_est: i32, total_est: i32, t: f64) -> DeltaCacheUsage {
        DeltaCacheUsage {
            input_est: 0,
            prev_cached_est,
            prompt_total_est: total_est,
            hit_rate: t,
            billing_mode: true,
            ..DeltaCacheUsage::default()
        }
    }

    #[test]
    fn std_sum_equals_total_never_over_reports() {
        // 拒绝双重收费的核心不变量：标准模式三桶和**恒等** total_real，绝不超报。
        // 覆盖多种 R / creation_ratio / total 组合。
        for (prev, t) in [(990, 1.0), (900, 0.5), (1000, 0.0), (950, 0.8), (0, 1.0)] {
            let u = std_usage(prev, 1000, t);
            for total in [1, 500, 4096, 10_000, 140_210] {
                let (i, c, rd) = u.split_final(total);
                assert!(i >= 0 && c >= 0 && rd >= 0, "桶非负");
                assert_eq!(i + c + rd, total, "标准模式三桶和恒等 total（不超报/不双重收费）");
            }
        }
    }

    #[test]
    fn std_delta_shapes_creation() {
        // 本轮新沉淀 delta = total − prev 计 creation；旧前缀 prev 在 T=1 时全 read。
        // prev 占 99% → delta=1% → creation=1%。
        let lo = std_usage(990, 1000, 1.0);
        let (i1, c1, r1) = lo.split_final(10000);
        assert_eq!(c1, 100, "delta 1% → creation=100");
        assert_eq!(i1, 0, "input_est=0 → input=0");
        assert_eq!(r1, 9900, "T=1 → 旧前缀全 read");
        // prev 占 95% → delta=5% → creation=500。
        let hi = std_usage(950, 1000, 1.0);
        assert_eq!(hi.split_final(10000).1, 500, "delta 5% → creation=500");
    }

    #[test]
    fn std_lower_t_shifts_read_to_creation_for_margin() {
        // 目标率 T 利润杠杆:T 越低 → read↓、creation↑（被削旧前缀挪 creation），加权收入↑,
        // 但 sum 恒等 total（不超报）。prev 占 99% → delta=1%；total=10000。
        let (_, _, r_full) = std_usage(990, 1000, 1.0).split_final(10000);
        let (i_half, c_half, r_half) = std_usage(990, 1000, 0.5).split_final(10000);
        let (i_zero, c_zero, r_zero) = std_usage(990, 1000, 0.0).split_final(10000);
        assert_eq!(r_full, 9900, "T=1 → 旧前缀全 read");
        assert_eq!(r_half, 5000, "T=0.5 → read 削顶到 total×0.5");
        assert_eq!(c_half, 5000, "被削旧前缀 4900 + delta 100 → creation=5000");
        assert_eq!(i_half, 0, "input_est=0 → input 恒 0（不再挪 input）");
        assert_eq!(r_zero, 0, "T=0 → read=0");
        assert_eq!(c_zero, 10000, "T=0 → 旧前缀+delta 全 creation");
        // 加权收入单调:T 越低越高（creation 1.25× > read 0.1×），但都不超报。
        let w = |i: i32, c: i32, rd: i32| WEIGHT_INPUT * i as f64 + WEIGHT_CREATION * c as f64 + WEIGHT_READ * rd as f64;
        assert!(w(i_zero, c_zero, r_zero) > w(i_half, c_half, r_half), "T↓ 加权收入↑");
    }

    #[test]
    fn std_no_cacheable_all_input_std() {
        // 无可缓存内容（prompt_total_est<=0）→ 全计 input。
        let u = DeltaCacheUsage { billing_mode: true, ..DeltaCacheUsage::default() };
        assert_eq!(u.split_final(2), (2, 0, 0));
    }

    #[test]
    fn std_no_guardrail_unlike_default() {
        // 标准模式不施加 multiplier_cap 护栏（与默认模式的唯一区别）。
        // 即便 multiplier_cap 设得很低,标准模式也不压 creation→read。
        // T=0 → 旧前缀全挪 creation：input=0, creation=1000, read=0 → weighted=1250(1.25x)。
        // 护栏本会想压,但标准模式忽略护栏。
        let u = DeltaCacheUsage {
            input_est: 0,
            prev_cached_est: 1000,
            prompt_total_est: 1000,
            hit_rate: 0.0, // read 全挪 creation,加权最高
            multiplier_cap: 0.5, // 激进护栏
            billing_mode: true,
            ..DeltaCacheUsage::default()
        };
        assert_eq!(u.split_final(1000), (0, 1000, 0), "标准模式无视护栏(creation 不被压)");
    }

    #[test]
    fn std_billing_mode_off_uses_safe_default() {
        // billing_mode 关（默认）→ split_final 走 split_against_total（检测安全，受护栏）。
        // 整段前缀已缓存、T=1 → 全 read。
        let u = DeltaCacheUsage {
            input_est: 0,
            prev_cached_est: 1000,
            prompt_total_est: 1000,
            hit_rate: 1.0,
            ..DeltaCacheUsage::default()
        };
        assert!(!u.billing_mode, "默认关");
        assert_eq!(u.split_final(1000), (0, 0, 1000), "默认模式 T=1 全 read");
    }

    #[test]
    fn creation_split_routes_by_ttl_flag() {
        // creation_is_1h=false → 全归 5m；true → 全归 1h。
        let u5 = DeltaCacheUsage { creation_is_1h: false, ..DeltaCacheUsage::default() };
        assert_eq!(u5.creation_split(300), (300, 0), "默认 5m");
        let u1 = DeltaCacheUsage { creation_is_1h: true, ..DeltaCacheUsage::default() };
        assert_eq!(u1.creation_split(300), (0, 300), "1h 标记 → 全归 1h");
    }

    #[test]
    fn creation_weight_by_ttl() {
        // 1h creation 计价权重 2.0，5m 为 1.25。
        assert_eq!(DeltaCacheUsage { creation_is_1h: false, ..DeltaCacheUsage::default() }.creation_weight(), WEIGHT_CREATION);
        assert_eq!(DeltaCacheUsage { creation_is_1h: true, ..DeltaCacheUsage::default() }.creation_weight(), WEIGHT_CREATION_1H);
    }

    #[test]
    fn request_1h_ttl_detected_in_system() {
        // system 断点标 ttl=1h → compute 出的 DeltaCacheUsage.creation_is_1h = true。
        let req = req_with(
            vec![msg("user", "hi")],
            Some(vec![SystemMessage {
                text: "You are helpful".to_string(),
                cache_control: Some(super::super::types::CacheControl {
                    cache_type: "ephemeral".to_string(),
                    ttl: Some("1h".to_string()),
                }),
            }]),
        );
        let u = compute_structural_cache_usage(&req, 1.0, None);
        assert!(u.creation_is_1h, "system 的 1h ttl 应被识别");
    }

    #[test]
    fn request_1h_ttl_detected_in_message_content_json() {
        // message content block 里 JSON 形态的 cache_control.ttl=1h 也应被扫到。
        let req = req_with(
            vec![Message {
                role: "user".to_string(),
                content: serde_json::json!([{
                    "type": "text",
                    "text": "big context",
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                }]),
            }],
            None,
        );
        let u = compute_structural_cache_usage(&req, 1.0, None);
        assert!(u.creation_is_1h, "message content 的 1h ttl 应被扫到");
    }

    #[test]
    fn request_default_5m_when_no_1h_marker() {
        // 无 ttl 或 ttl=5m → creation_is_1h = false（默认 5m 桶）。
        let req = req_with(vec![msg("user", "hi")], None);
        assert!(!compute_structural_cache_usage(&req, 1.0, None).creation_is_1h, "无标记默认 5m");
        let req5m = req_with(
            vec![Message {
                role: "user".to_string(),
                content: serde_json::json!([{
                    "type": "text", "text": "x",
                    "cache_control": { "type": "ephemeral", "ttl": "5m" }
                }]),
            }],
            None,
        );
        assert!(!compute_structural_cache_usage(&req5m, 1.0, None).creation_is_1h, "5m 不置 1h");
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
        let cold = compute_structural_cache_usage(&req, 1.0, None);
        // warm 的 prev_cached 是上轮已缓存前缀的 token 数（token 级）。取可缓存前缀的 3/4，
        // 使 warm.creation = cacheable − prev ≈ 1/4 cacheable（远小于 cold 的整段）、且 read>0。
        let cacheable = cold.prompt_total_est - cold.input_est;
        let warm = compute_structural_cache_usage(&req, 1.0, Some(cacheable * 3 / 4));

        // 两者 input 相同(都是最后一条),prompt_total 相同。
        assert_eq!(cold.input_est, warm.input_est);
        assert_eq!(cold.prompt_total_est, warm.prompt_total_est);
        // cold 的 creation = 整段前缀 = total − input;warm 的 creation 只 delta,远小于 cold。
        assert_eq!(cold.creation_est, cacheable);
        assert_eq!(warm.creation_est, cacheable - cacheable * 3 / 4, "warm creation = delta");
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
        // token 级：令上轮已缓存 prev = 可缓存前缀 − a1（即缓存到只差最后那条 assistant），
        // 则本轮新沉淀 delta = a1 → creation = a1，其余 (system+u1) 进 read。
        let a1_est = message_tokens(&msg("assistant", &big));
        let u2_est = message_tokens(&msg("user", "short new question"));
        let cold = compute_structural_cache_usage(&req, 1.0, None);
        let cacheable = cold.prompt_total_est - cold.input_est;
        let u = compute_structural_cache_usage(&req, 1.0, Some(cacheable - a1_est));
        assert_eq!(u.creation_est, a1_est, "creation = 本轮新沉淀 delta（倒数第二条 a1）");
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

        // token 级：令上轮已缓存到只差最后一条 assistant（prev = cacheable − a_est），则本轮新沉淀
        // delta = a_est → creation 恒 = 一条 assistant，无论历史多长，不随历史线性增长。
        let a_est = message_tokens(&msg("assistant", &unit));
        let cold_short = compute_structural_cache_usage(&short, 1.0, None);
        let cold_long = compute_structural_cache_usage(&long, 1.0, None);
        let cacheable_short = cold_short.prompt_total_est - cold_short.input_est;
        let cacheable_long = cold_long.prompt_total_est - cold_long.input_est;
        let cu_short = compute_structural_cache_usage(&short, 1.0, Some(cacheable_short - a_est));
        let cu_long = compute_structural_cache_usage(&long, 1.0, Some(cacheable_long - a_est));
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
    fn compute_target_rate_controls_read() {
        // 目标率 T 越大 → read 越多、creation 越少（削顶更松）。input 不受 T 影响（本轮新问题）。
        // 用深暖轮（prev 大）使 T 削顶真正生效：prev = cacheable，read=min(prev, T×total)=T×total。
        let body = "lorem ipsum dolor sit amet ".repeat(20);
        let req = req_with(
            vec![msg("user", &body), msg("assistant", &body), msg("user", "q")],
            None,
        );
        let cold = compute_structural_cache_usage(&req, 1.0, None);
        let total = cold.prompt_total_est;
        let cacheable = total - cold.input_est;
        // prev = 整段可缓存前缀（纯重放式深暖轮）→ delta=0，read 完全由 T 削顶决定。
        let (i_lo, c_lo, r_lo) =
            compute_structural_cache_usage(&req, 0.5, Some(cacheable)).split_against_total(total);
        let (i_hi, c_hi, r_hi) =
            compute_structural_cache_usage(&req, 1.0, Some(cacheable)).split_against_total(total);
        assert!(r_hi > r_lo, "T 越大 read 越多");
        assert!(c_hi < c_lo, "T 越大 creation 越少（削顶挪进 creation 的 excess 更少）");
        assert_eq!(i_lo, i_hi, "input 不受 T 影响（恒为本轮新问题）");
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
        // 深暖轮：prev = 整段可缓存前缀（含图历史）→ 含图前缀进 read 桶。
        let cold = compute_structural_cache_usage(&req, 1.0, None);
        let cacheable = cold.prompt_total_est - cold.input_est;
        let u = compute_structural_cache_usage(&req, 1.0, Some(cacheable));
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
        // token 级：prev = 首条 user（缓存到只差 tool_use）→ 本轮 delta = tool_use → creation=toolu_est。
        let cold = compute_structural_cache_usage(&req, 1.0, None);
        let cacheable = cold.prompt_total_est - cold.input_est;
        let u = compute_structural_cache_usage(&req, 1.0, Some(cacheable - toolu_est));
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
        // 首次出现 → cold(None)，本次记 cacheable=5
        assert_eq!(g.observe_session("sess:a", 1000, 5, 300), None, "首次出现应判 cold");
        // TTL 内再来 → warm，返回上次高水位 5；本次记 7
        assert_eq!(g.observe_session("sess:a", 1200, 7, 300), Some(5), "TTL(300)内应 warm 且返回上次高水位");
        // 超 TTL → cold(缓存凉了)；本次记 9
        assert_eq!(g.observe_session("sess:a", 1600, 9, 300), None, "距上次>300s 应判 cold");
        // 刚刷新过,紧接着再来 → warm，返回刚记的 9
        assert_eq!(g.observe_session("sess:a", 1700, 11, 300), Some(9), "刷新后 TTL 内应 warm");
        // 不同会话互不影响 → cold
        assert_eq!(g.observe_session("sess:b", 1700, 3, 300), None, "另一会话首次应 cold");
    }

    #[test]
    fn governance_hwm_short_request_does_not_lower_prev_n() {
        // 核心修复：同一 seed 上出现更小 msg_count 的短请求（OpenAI key 级 seed 下的另一对话、
        // title/探针/子任务、被重截断的历史），不得把 prev_n 下界打小 → 否则下一条长请求会算出
        // 横跨整段历史的巨大 creation delta。存高水位后短请求不拉低下界。
        let g = MeterGovernance::new(1.0, 300);
        // 长对话到 cacheable=200 → 首次 cold，记高水位 200。
        assert_eq!(g.observe_session("key:42", 1000, 200, 300), None);
        // 同 seed 冒出一条短请求（另一对话/探针，只 3）→ warm，返回高水位 200（不是 3）。
        assert_eq!(
            g.observe_session("key:42", 1010, 3, 300),
            Some(200),
            "短请求应读到高水位 200,而非被自己打小"
        );
        // 长对话回来到 202 → warm，返回的 prev 仍是高水位 200（旧 bug 会返回 3）。
        assert_eq!(
            g.observe_session("key:42", 1020, 202, 300),
            Some(200),
            "长请求应读到高水位 200 → creation 只覆盖新增 2,不横跨历史"
        );
        // 高水位随真实增长上移。
        assert_eq!(g.observe_session("key:42", 1030, 205, 300), Some(202));
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

        let _ = n;
        let cold = compute_structural_cache_usage(&req, 1.0, None);
        let cacheable = cold.prompt_total_est - cold.input_est;
        let one_msg = message_tokens(&msg("assistant", &body));
        // 打小的 prev（如被短请求污染成很小）→ creation 横跨几乎整段历史（爆炸）。
        let exploded = compute_structural_cache_usage(&req, 1.0, Some(one_msg));
        // 高水位使 prev = cacheable − 一条 → creation 只覆盖本轮新增一条（有界）。
        let bounded = compute_structural_cache_usage(&req, 1.0, Some(cacheable - one_msg));
        assert!(
            exploded.creation_est > bounded.creation_est * 3,
            "打小的 prev 会让 creation 爆炸(exploded={} vs bounded={})",
            exploded.creation_est,
            bounded.creation_est
        );
    }

    #[test]
    fn governance_cold_resets_baseline_not_hwm() {
        // cold（超 TTL，缓存确已凉）：重置基线为本次条数，不保留旧高水位——前缀整段要重建。
        let g = MeterGovernance::new(1.0, 100);
        assert_eq!(g.observe_session("key:9", 1000, 50, 100), None); // 首次 cold，记 50
        assert_eq!(g.observe_session("key:9", 1050, 52, 100), Some(50), "TTL 内 warm");
        // 超 TTL → cold，基线重置为本次的 4（不因高水位 52 而保留）。
        assert_eq!(g.observe_session("key:9", 1300, 4, 100), None, "超 TTL 应 cold");
        // 紧接着来 → warm，读到刚重置的 4（证明 cold 没保留旧高水位 52）。
        assert_eq!(
            g.observe_session("key:9", 1310, 6, 100),
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
        // token 级：上轮已缓存前缀 prev = msg[0]+msg[1]（tool 循环前的状态）。本轮新沉淀 delta =
        // msg[2..5] = burst（一轮补进多对消息）→ creation = burst，覆盖全部新增中间消息。
        let prev = est(&req.messages[0]) + est(&req.messages[1]);
        let u = compute_structural_cache_usage(&req, 1.0, Some(prev));
        assert_eq!(u.creation_est, burst, "creation 应覆盖上次见到后新增的全部中间消息");
        assert_eq!(u.input_est, est(&req.messages[5]), "input 仍是末条");
        let (input, creation, read) = u.split_against_total(u.prompt_total_est);
        assert!(creation > 0 && read > 0);
        assert_eq!(input + creation + read, u.prompt_total_est);

        // 对比：若上轮已缓存到只差最后一条 assistant（prev 更大）→ creation 只一条，明显偏小。
        let prev_deep = prev + est(&req.messages[2]) + est(&req.messages[3]);
        let old = compute_structural_cache_usage(&req, 1.0, Some(prev_deep));
        assert_eq!(old.creation_est, est(&req.messages[4]));
        assert!(u.creation_est > old.creation_est * 2, "多消息 burst 下计入更多 creation");
    }

    #[test]
    fn compute_warm_no_new_settled_creation_zero() {
        // warm 但 prev_n >= n-1（纯重放：上次条数 == 本次条数，无新增沉淀）→ creation=0。
        let body = "lorem ipsum ".repeat(20);
        let req = req_with(
            vec![msg("user", &body), msg("assistant", &body), msg("user", "q")],
            None,
        );
        let cold = compute_structural_cache_usage(&req, 1.0, None);
        let cacheable = cold.prompt_total_est - cold.input_est;
        // prev == cacheable（整段前缀已缓存，纯重放，无新增沉淀）→ creation=0。
        let u = compute_structural_cache_usage(&req, 1.0, Some(cacheable));
        assert_eq!(u.creation_est, 0, "无新增沉淀时 creation 为 0");
        // prev > cacheable（被污染的高水位）→ clamp 后仍 == cacheable → creation=0。
        let u2 = compute_structural_cache_usage(&req, 1.0, Some(cacheable + 999));
        assert_eq!(u2.creation_est, 0, "prev 超 cacheable → clamp → creation=0");
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

    // ---- 目标缓存率 T：新语义端到端 -------------------------------------------

    /// 冷轮显示 0%（缓存刚建、无得读），暖轮逐轮爬升、稳态逼近 T。
    #[test]
    fn target_rate_cold_zero_then_warm_ramps_to_t() {
        let body = "lorem ipsum dolor sit amet ".repeat(30);
        let req = req_with(
            vec![msg("user", &body), msg("assistant", &body), msg("user", "q")],
            None,
        );
        let cold = compute_structural_cache_usage(&req, 0.9, None);
        let total = cold.prompt_total_est;
        let (_, _, r_cold) = cold.split_against_total(total);
        assert_eq!(r_cold, 0, "冷轮 read=0 → 显示 0%（真实：缓存刚建）");

        let cacheable = total - cold.input_est;
        // 深暖轮（prev = 整段可缓存前缀）→ read = T×total，命中率逼近 T。
        let deep = compute_structural_cache_usage(&req, 0.9, Some(cacheable));
        let (_, _, r_deep) = deep.split_against_total(total);
        let hit = r_deep as f64 / total as f64;
        assert!((hit - 0.9).abs() < 0.05, "深暖轮命中率逼近 T=0.9，实得 {hit:.3}");
    }

    /// 生效 T 由入口按 hit_rate_max 夹紧（此处直接验证 split 对 hit_rate 的响应即 T 语义）。
    /// 夹紧逻辑在 handler；这里锁 split 侧：hit_rate 越大 read 越多，且 read/total==hit_rate（深暖轮）。
    #[test]
    fn target_rate_read_equals_t_on_deep_warm() {
        let u = |t: f64| DeltaCacheUsage {
            input_est: 0,
            prev_cached_est: 1000, // 深暖轮：整段前缀已缓存
            prompt_total_est: 1000,
            hit_rate: t,
            ..DeltaCacheUsage::default()
        };
        for t in [0.5, 0.7, 0.9, 0.95] {
            let (_, _, r) = u(t).split_against_total(1000);
            assert_eq!(r, (1000.0 * t).round() as i32, "深暖轮 read == T×total（T={t}）");
        }
    }

    /// 真实性闸门（破绽 A+B）：无 cache_control 断点 → 不合格；带断点且前缀 ≥1024 → 合格。
    #[test]
    fn cache_eligible_requires_control_and_min_prefix() {
        // 大前缀但无 cache_control → 不合格（真实 Anthropic 不会凭空产生缓存）。
        let big = "lorem ipsum dolor sit amet ".repeat(400); // 远超 1024 token
        let no_cc = req_with(
            vec![msg("user", &big), msg("assistant", &big), msg("user", "q")],
            None,
        );
        assert!(!request_has_cache_control(&no_cc), "无断点");
        assert!(!cache_eligible(&no_cc), "无 cache_control → 不合格（全 input）");

        // 带 cache_control（system 断点）且前缀够大 → 合格。
        let cached = req_with(
            vec![msg("user", &big), msg("assistant", &big), msg("user", "q")],
            Some(vec![SystemMessage {
                text: "you are helpful ".repeat(200),
                cache_control: Some(super::super::types::CacheControl {
                    cache_type: "ephemeral".to_string(),
                    ttl: None,
                }),
            }]),
        );
        assert!(request_has_cache_control(&cached), "system 带断点");
        assert!(cache_eligible(&cached), "带断点 + 大前缀 → 合格");

        // 带 cache_control 但可缓存前缀太小（< 1024）→ 不合格（真实 Anthropic 最小门槛）。
        let tiny = req_with(
            vec![msg("user", "hi"), msg("assistant", "yo"), msg("user", "q")],
            Some(vec![SystemMessage {
                text: "short".to_string(),
                cache_control: Some(super::super::types::CacheControl {
                    cache_type: "ephemeral".to_string(),
                    ttl: None,
                }),
            }]),
        );
        assert!(request_has_cache_control(&tiny), "有断点");
        assert!(!cache_eligible(&tiny), "前缀 < 1024 → 不合格（全 input）");
    }

    /// cache_control 断点在 message content block 里（JSON 形态）也能被检测到。
    #[test]
    fn cache_control_detected_in_message_content() {
        let req = req_with(
            vec![Message {
                role: "user".to_string(),
                content: serde_json::json!([{
                    "type": "text", "text": "x",
                    "cache_control": { "type": "ephemeral" }
                }]),
            }],
            None,
        );
        assert!(request_has_cache_control(&req), "message content block 的断点应被检测");
    }

    /// **端到端会话识别证明**：真实 pipeline（isolation_seed → observe_session → compute → split）
    /// 跑一个稳定 session 的多轮对话，聚合 `Σread/Σtotal` 应随对话加深逼近 T（不塌到 0）。
    /// 这是"会话识别正确 → 命中率达标"的守卫，防 observe_session/seed 归并回归。
    #[test]
    fn e2e_stable_session_aggregate_hit_rate_converges_to_t() {
        let t = 0.9_f64;
        let ttl = 3600_u64;
        let g = MeterGovernance::new_with_max(t, ttl, 0.95);
        // 稳定 session（Claude Code 显式 _session_ uuid）——多轮 messages[0] 不变、追加历史。
        let sid = "user_x_account__session_conv-1";
        let big = "the quick brown fox jumps ".repeat(60); // 每条 ~360 token
        // system 足够大（>1024 token）使**首轮**（可缓存前缀=仅 system）也过最小门槛，模拟真实
        // Claude Code 的大 system prompt。
        let sys_text = "you are a helpful coding assistant ".repeat(320); // ~2800 token
        let mut msgs: Vec<Message> = vec![msg("user", &big)];

        let (mut sum_read, mut sum_total) = (0i64, 0i64);
        let mut warm_hits: Vec<f64> = Vec::new();
        // 10 轮：每轮追加 (assistant, user)，末条为本轮 input。带 system cache_control 使合格。
        for turn in 0..10 {
            let mut req = req_with(
                msgs.clone(),
                Some(vec![SystemMessage {
                    text: sys_text.clone(),
                    cache_control: Some(super::super::types::CacheControl {
                        cache_type: "ephemeral".to_string(),
                        ttl: None,
                    }),
                }]),
            );
            req.metadata = Some(Metadata { user_id: Some(sid.to_string()) });
            assert!(cache_eligible(&req), "每轮都应合格（带断点+大前缀）");

            let seed = isolation_seed(&req, 0);
            let now = 1_000 + turn * 30; // 每轮间隔 30s < TTL → 保持 warm
            let cacheable = estimate_cacheable_tokens(&req).max(0) as u32;
            let prev = g.observe_session(&seed, now, cacheable, ttl).map(|v| v as i32);
            let usage = compute_structural_cache_usage(&req, t, prev);
            // 真实 total 用 prompt_total_est 近似（本地估算口径）。
            let total = usage.prompt_total_est;
            let (_i, _c, read) = usage.split_against_total(total);
            sum_read += read as i64;
            sum_total += total as i64;
            let _ = (cacheable, _i, _c);
            if prev.is_some() {
                warm_hits.push(read as f64 / total as f64);
            }
            // 追加一轮对话：assistant 回复 + user 新问题。
            msgs.push(msg("assistant", &big));
            msgs.push(msg("user", "next question please"));
        }

        // 深暖轮（后几轮）命中率应逼近 T=0.9。
        let last = *warm_hits.last().unwrap();
        assert!((last - t).abs() < 0.08, "深暖轮命中率应逼近 T=0.9，实得 {last:.3}");
        // 聚合命中率（含第 1 轮冷）应显著 >0 且爬向 T（不塌）。
        let agg = sum_read as f64 / sum_total as f64;
        assert!(agg > 0.6, "10 轮聚合命中率应 >0.6（只 1 轮冷），实得 {agg:.3}");
    }

    /// TTL 到期使暖轮转冷：命中率塌到 0（诚实——缓存真凉了）。锁住"TTL 是命中率杠杆"语义。
    #[test]
    fn e2e_ttl_expiry_turns_cold_zero_hit() {
        let t = 0.9_f64;
        let ttl = 300_u64;
        let g = MeterGovernance::new_with_max(t, ttl, 0.95);
        let seed = "sess:gap";
        // 第 1 轮：cold。
        assert_eq!(g.observe_session(seed, 1_000, 5000, ttl), None);
        // 30s 后：warm。
        assert_eq!(g.observe_session(seed, 1_030, 5200, ttl), Some(5000));
        // 间隔 > TTL（缓存凉）→ cold，命中率归 0（诚实）。
        assert_eq!(g.observe_session(seed, 1_030 + 400, 5300, ttl), None, "超 TTL → cold");
    }

    /// hit_rate_max 夹紧（MeterGovernance 侧的 setter/getter 语义）。
    #[test]
    fn hit_rate_max_getter_setter_clamps() {
        let g = MeterGovernance::new_with_max(0.9, 300, 0.95);
        assert!((g.hit_rate_max() - 0.95).abs() < 1e-9);
        g.set_hit_rate_max(1.5); // 超范围 → clamp 到 1.0
        assert!((g.hit_rate_max() - 1.0).abs() < 1e-9, "上限 clamp 到 1.0");
        g.set_hit_rate_max(-0.2); // 负 → clamp 到 0
        assert!(g.hit_rate_max().abs() < 1e-9, "下限 clamp 到 0");
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

        // 对话 A：一个很长的 agent 对话，把 token 高水位顶到很大（远超 B 的可缓存量）。
        assert_eq!(g.observe_session(seed, 1_000, 100_000, 3600), None, "A 首次出现 → cold");
        // A 继续，warm，返回高水位 100_000。
        assert_eq!(
            g.observe_session(seed, 1_010, 100_000, 3600),
            Some(100_000),
            "A 第二轮 warm，prev=100000 token"
        );

        // 对话 B：一个**全新的短对话**，但共用同一 key seed。它本该把新增内容计入 creation，
        // 但 observe_session 返回 A 顶高的 token 水位 100_000，远超 B 自己的可缓存量。
        let big = "x".repeat(4000);
        let b_req = req_with(
            vec![
                msg("user", &big),      // 0  ← B 的历史前缀
                msg("assistant", &big), // 1  ← 本应计 creation（B 新增沉淀）
                msg("user", &big),      // 2  ← input（末条）
            ],
            None,
        );
        let prev_for_b = g
            .observe_session(seed, 1_020, 500, 3600)
            .expect("同 key 且 TTL 内 → warm") as i32;
        assert_eq!(prev_for_b, 100_000, "B 拿到的是被 A 顶高的 token 水位，而非自己的历史");

        // token 级塌陷：prev 被 clamp 到 B 的 cacheable → creation = cacheable − cacheable = 0。
        let cold = compute_structural_cache_usage(&b_req, 1.0, None);
        let cacheable = cold.prompt_total_est - cold.input_est;
        assert!(prev_for_b >= cacheable, "被污染 prev({}) >= B 的 cacheable({})", prev_for_b, cacheable);
        let usage = compute_structural_cache_usage(&b_req, 1.0, Some(prev_for_b));
        assert_eq!(
            usage.creation_est, 0,
            "复现塌陷：B 的合法新增被 A 的高水位吞掉 → creation=0（偏向便宜桶，经济安全）"
        );

        // 对照：若 B 用自己真实的上轮前缀（仅首条 user 的 token）计算，creation 覆盖新增（非零）。
        let true_prev = message_tokens(&b_req.messages[0]);
        let correct = compute_structural_cache_usage(&b_req, 1.0, Some(true_prev));
        assert!(
            correct.creation_est > 0,
            "正确隔离下 B 的新增应计入 creation（seed 碰撞时被算成 0）"
        );
    }
}

#[cfg(test)]
mod cch_tests {
    //! CCH（Anthropic 标准计费模式）有状态计量单元测试。移植自上游 v0.7.1，测试名 / 类型 /
    //! 函数全部加 `cch_` 前缀，与上方 delta 模型测试（`mod tests`）隔离。
    use super::*;

    #[test]
    fn cch_lookup_miss_then_record_then_hit() {
        let cache = CchCacheMeter::new(None);
        let hashes = [1u64, 2u64];
        let tokens = [10u32, 25u32];
        let r1 = cache.lookup(&hashes, &tokens);
        assert!(r1.iter().all(|s| !s.hit));

        cache.record(&hashes, &tokens, 300);
        let r2 = cache.lookup(&hashes, &tokens);
        assert!(r2.iter().all(|s| s.hit));
    }

    #[test]
    fn cch_ttl_expiry_makes_entry_miss() {
        let cache = CchCacheMeter::new(None);
        cache.record(&[42], &[100], 60);
        {
            let mut inner = cache.inner.lock();
            if let Some(e) = inner.entries.get_mut(&42) {
                e.expires_at = cch_now_secs() - 1;
            }
        }
        let r = cache.lookup(&[42], &[100]);
        assert!(!r[0].hit);
    }

    #[test]
    fn cch_evict_expired_removes_dead_entries() {
        let cache = CchCacheMeter::new(None);
        cache.record(&[1, 2], &[5, 5], 60);
        {
            let mut inner = cache.inner.lock();
            for (_, v) in inner.entries.iter_mut() {
                v.expires_at = cch_now_secs() - 1;
            }
        }
        cache.evict_expired();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn cch_parse_ttl_handles_known_values() {
        assert_eq!(cch_parse_ttl(Some("1h")), 3600);
        assert_eq!(cch_parse_ttl(Some("5m")), 300);
        assert_eq!(cch_parse_ttl(None), 300);
        assert_eq!(cch_parse_ttl(Some("garbage")), 300);
    }

    #[test]
    fn cch_flush_and_reload_round_trip() {
        let tmp = std::env::temp_dir().join(format!("kiro-cch-{}.json", cch_now_secs()));
        let cache = CchCacheMeter::new(Some(tmp.clone()));
        cache.record(&[7], &[42], 600);
        cache.flush_to_disk();

        let cache2 = CchCacheMeter::new(Some(tmp.clone()));
        let r = cache2.lookup(&[7], &[42]);
        assert!(r[0].hit);

        let _ = std::fs::remove_file(&tmp);
    }

    use super::super::types::{CacheControl, Message, MessagesRequest, Metadata, SystemMessage, Tool};

    fn build_request_with_system_breakpoint() -> MessagesRequest {
        MessagesRequest {
            model: "claude-sonnet-4-5-20250929".to_string(),
            max_tokens: 32,
            messages: vec![Message {
                role: "user".to_string(),
                content: serde_json::Value::String("Hello".to_string()),
            }],
            stream: false,
            system: Some(vec![SystemMessage {
                text: "You are a helpful assistant. ".repeat(100),
                cache_control: Some(CacheControl {
                    cache_type: "ephemeral".to_string(),
                    ttl: None,
                }),
            }]),
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        }
    }

    #[test]
    fn cch_compute_cache_usage_first_miss_then_hit() {
        let cache = CchCacheMeter::new(None);
        let req = build_request_with_system_breakpoint();

        let u1 = cch_compute_cache_usage(&cache, &req, 1);
        assert!(u1.cache_covered_est > 0, "first call should cover prefix");
        assert_eq!(u1.cache_read, 0, "first call has nothing cached to read");
        let total = u1.prompt_total_est;
        let (in1, cc1, cr1) = u1.split_against_total(total);
        assert!(cc1 > 0, "first call creation>0, cc={cc1}");
        assert_eq!(cr1, 0);
        assert_eq!(in1 + cc1 + cr1, total, "互斥口径必须自洽");

        let u2 = cch_compute_cache_usage(&cache, &req, 1);
        assert!(u2.cache_read > 0, "second call should hit");
        let (in2, cc2, cr2) = u2.split_against_total(total);
        assert_eq!(cc2, 0, "second call creation should be 0, got {cc2}");
        assert!(cr2 > 0, "second call read>0, cr={cr2}");
        assert_eq!(in2 + cc2 + cr2, total, "互斥口径必须自洽");
        assert_eq!(cc1, cr2);
    }

    #[test]
    fn cch_split_against_total_is_mutually_exclusive() {
        let u = CchResult {
            cache_read: 30,
            cache_covered_est: 80,
            prompt_total_est: 100,
            creation_is_1h: false,
        };
        let (input, creation, read) = u.split_against_total(1000);
        assert_eq!(input + creation + read, 1000);
        assert_eq!(input, 200, "尾部 20% 是未缓存 input");
        assert_eq!(read, 300);
        assert_eq!(creation, 500);
    }

    #[test]
    fn cch_split_against_total_no_cache_all_input() {
        let u = CchResult {
            cache_read: 0,
            cache_covered_est: 0,
            prompt_total_est: 100,
            creation_is_1h: false,
        };
        assert_eq!(u.split_against_total(500), (500, 0, 0));
    }

    #[test]
    fn cch_compute_cache_usage_single_message_no_prefix() {
        let cache = CchCacheMeter::new(None);
        let req = MessagesRequest {
            model: "x".to_string(),
            max_tokens: 8,
            messages: vec![Message {
                role: "user".to_string(),
                content: serde_json::Value::String("Hello".to_string()),
            }],
            stream: false,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };
        let u = cch_compute_cache_usage(&cache, &req, 1);
        assert_eq!(u.cache_covered_est, 0);
        assert_eq!(u.split_against_total(123), (123, 0, 0));
    }

    fn build_tool_with_schema_order(insert_required_first: bool) -> Tool {
        let mut schema = std::collections::BTreeMap::new();
        if insert_required_first {
            schema.insert("required".to_string(), serde_json::json!([]));
            schema.insert("properties".to_string(), serde_json::json!({}));
            schema.insert("type".to_string(), serde_json::json!("object"));
        } else {
            schema.insert("type".to_string(), serde_json::json!("object"));
            schema.insert("properties".to_string(), serde_json::json!({}));
            schema.insert("required".to_string(), serde_json::json!([]));
        }
        Tool {
            tool_type: None,
            name: "my_tool".to_string(),
            description: "desc".to_string(),
            input_schema: schema,
            max_uses: None,
            cache_control: None,
        }
    }

    #[test]
    fn cch_tool_signature_stable_across_insert_order() {
        let a = build_tool_with_schema_order(true);
        let b = build_tool_with_schema_order(false);
        assert_eq!(cch_tool_signature(&a), cch_tool_signature(&b));
    }

    #[test]
    fn cch_compute_cache_usage_tools_hit_regardless_of_schema_order() {
        let make_req = |insert_required_first: bool| {
            let mut tool = build_tool_with_schema_order(insert_required_first);
            tool.cache_control = Some(CacheControl {
                cache_type: "ephemeral".to_string(),
                ttl: None,
            });
            MessagesRequest {
                model: "claude-sonnet-4-5-20250929".to_string(),
                max_tokens: 32,
                messages: vec![Message {
                    role: "user".to_string(),
                    content: serde_json::Value::String("Hello".to_string()),
                }],
                stream: false,
                system: None,
                tools: Some(vec![tool]),
                tool_choice: None,
                thinking: None,
                output_config: None,
                metadata: None,
            }
        };

        let cache = CchCacheMeter::new(None);
        let u1 = cch_compute_cache_usage(&cache, &make_req(false), 1);
        assert!(u1.cache_covered_est > 0, "first call should cover prefix");
        assert_eq!(u1.cache_read, 0);

        let u2 = cch_compute_cache_usage(&cache, &make_req(true), 1);
        assert_eq!(
            u2.cache_read, u1.cache_covered_est,
            "schema 顺序不应影响命中：second read 应等于 first covered"
        );
    }

    fn msg_with_cc(role: &str, text: &str, with_cc: bool) -> Message {
        let block = if with_cc {
            serde_json::json!({
                "type": "text",
                "text": text,
                "cache_control": {"type": "ephemeral"}
            })
        } else {
            serde_json::json!({"type": "text", "text": text})
        };
        Message {
            role: role.to_string(),
            content: serde_json::Value::Array(vec![block]),
        }
    }

    fn req_with_messages(messages: Vec<Message>) -> MessagesRequest {
        MessagesRequest {
            model: "claude-sonnet-4-5-20250929".to_string(),
            max_tokens: 32,
            messages,
            stream: false,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        }
    }

    #[test]
    fn cch_tool_call_history_still_hits_despite_id_drift() {
        let body = "analyze the repository structure carefully ".repeat(15);
        let assistant_tool = |id: &str| Message {
            role: "assistant".to_string(),
            content: serde_json::json!([
                {"type": "text", "text": body},
                {"type": "tool_use", "id": id, "name": "bash", "input": {"cmd": "ls"}}
            ]),
        };
        let user_result = |id: &str| Message {
            role: "user".to_string(),
            content: serde_json::json!([
                {"type": "tool_result", "tool_use_id": id, "content": body}
            ]),
        };
        let user_text = |t: &str| msg_with_cc("user", t, false);

        let cache = CchCacheMeter::new(None);
        let turn1 = req_with_messages(vec![
            user_text(&body),
            assistant_tool("toolu_aaa"),
            user_result("toolu_aaa"),
            msg_with_cc("assistant", &body, false),
            user_text("next question one"),
        ]);
        let u1 = cch_compute_cache_usage(&cache, &turn1, 1);
        assert!(u1.cache_covered_est > 0);
        assert_eq!(u1.cache_read, 0, "turn1 无历史可命中");

        let turn2 = req_with_messages(vec![
            user_text(&body),
            assistant_tool("toolu_aaa"),
            user_result("toolu_aaa"),
            msg_with_cc("assistant", &body, false),
            user_text("next question one"),
            msg_with_cc("assistant", &body, false),
            user_text("next question two"),
        ]);
        let u2 = cch_compute_cache_usage(&cache, &turn2, 1);
        assert!(u2.cache_read > 0, "turn2 应命中 turn1 的历史前缀（即便工具块带 id）");
        assert_eq!(
            u2.cache_read, u1.cache_covered_est,
            "命中的最深前缀应等于上一轮 covered"
        );
    }

    #[test]
    fn cch_multi_turn_prefix_chain_produces_read_hit() {
        let cache = CchCacheMeter::new(None);
        let body = "the quick brown fox jumps over the lazy dog ".repeat(20);

        let turn3 = req_with_messages(vec![
            msg_with_cc("user", &body, false),
            msg_with_cc("assistant", &body, false),
            msg_with_cc("user", &body, false),
            msg_with_cc("assistant", &body, false),
            msg_with_cc("user", &body, true),
        ]);
        let u3 = cch_compute_cache_usage(&cache, &turn3, 1);
        assert!(u3.cache_covered_est > 0, "turn3 should create cache");
        assert_eq!(u3.cache_read, 0, "turn3 has no prior cache to read");

        let turn4 = req_with_messages(vec![
            msg_with_cc("user", &body, false),
            msg_with_cc("assistant", &body, false),
            msg_with_cc("user", &body, false),
            msg_with_cc("assistant", &body, false),
            msg_with_cc("user", &body, false),
            msg_with_cc("assistant", &body, false),
            msg_with_cc("user", &body, true),
        ]);
        let u4 = cch_compute_cache_usage(&cache, &turn4, 1);
        assert!(u4.cache_read > 0, "turn4 should hit a prior-turn prefix");
        assert_eq!(
            u4.cache_read, u3.cache_covered_est,
            "read 应等于上一轮写入的最深历史前缀"
        );
        assert!(
            u4.cache_covered_est > u4.cache_read,
            "turn4 仍会为新增的历史前缀创建缓存"
        );
    }

    #[test]
    fn cch_prefix_chain_works_without_any_cache_control() {
        let cache = CchCacheMeter::new(None);
        let body = "lorem ipsum dolor sit amet ".repeat(20);
        let turn1 = req_with_messages(vec![
            msg_with_cc("user", &body, false),
            msg_with_cc("assistant", &body, false),
            msg_with_cc("user", &body, false),
        ]);
        let u1 = cch_compute_cache_usage(&cache, &turn1, 1);
        assert!(u1.cache_covered_est > 0, "应为历史前缀创建缓存段");
        assert_eq!(u1.cache_read, 0);

        let turn2 = req_with_messages(vec![
            msg_with_cc("user", &body, false),
            msg_with_cc("assistant", &body, false),
            msg_with_cc("user", &body, false),
            msg_with_cc("assistant", &body, false),
            msg_with_cc("user", &body, false),
        ]);
        let u2 = cch_compute_cache_usage(&cache, &turn2, 1);
        assert!(u2.cache_read > 0, "无 cache_control 也应跨轮命中历史前缀");
    }

    #[test]
    fn cch_dynamic_system_header_does_not_break_cache_hit() {
        let stable_sys = "You are a coding assistant. ".repeat(200);
        let body = "implement the feature step by step ".repeat(15);

        let make_req = |dyn_header: &str, msgs: Vec<Message>| MessagesRequest {
            model: "claude-opus-4-8".to_string(),
            max_tokens: 64,
            messages: msgs,
            stream: false,
            system: Some(vec![
                SystemMessage {
                    text: dyn_header.to_string(),
                    cache_control: None,
                },
                SystemMessage {
                    text: stable_sys.clone(),
                    cache_control: Some(CacheControl {
                        cache_type: "ephemeral".to_string(),
                        ttl: None,
                    }),
                },
            ]),
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };

        let cache = CchCacheMeter::new(None);
        let u1 = cch_compute_cache_usage(
            &cache,
            &make_req(
                "now=1001",
                vec![
                    msg_with_cc("user", &body, false),
                    msg_with_cc("assistant", &body, false),
                    msg_with_cc("user", &body, false),
                ],
            ),
            1,
        );
        assert!(u1.cache_covered_est > 0);
        assert_eq!(u1.cache_read, 0, "turn1 无历史可命中");

        let u2 = cch_compute_cache_usage(
            &cache,
            &make_req(
                "now=2002",
                vec![
                    msg_with_cc("user", &body, false),
                    msg_with_cc("assistant", &body, false),
                    msg_with_cc("user", &body, false),
                    msg_with_cc("assistant", &body, false),
                    msg_with_cc("user", &body, false),
                ],
            ),
            1,
        );
        assert!(
            u2.cache_read > 0,
            "动态 system 头变化不应破坏稳定前缀命中（实测根因）"
        );
    }

    #[test]
    fn cch_different_key_id_does_not_cross_hit() {
        let cache = CchCacheMeter::new(None);
        let body = "shared system prompt and history ".repeat(20);
        let msgs = || {
            vec![
                msg_with_cc("user", &body, false),
                msg_with_cc("assistant", &body, false),
                msg_with_cc("user", &body, false),
            ]
        };
        let a = cch_compute_cache_usage(&cache, &req_with_messages(msgs()), 1);
        assert!(a.cache_covered_est > 0);
        assert_eq!(a.cache_read, 0);
        let b = cch_compute_cache_usage(&cache, &req_with_messages(msgs()), 2);
        assert_eq!(b.cache_read, 0, "不同 key_id 不应命中彼此的前缀");
        let c = cch_compute_cache_usage(&cache, &req_with_messages(msgs()), 1);
        assert!(c.cache_read > 0, "同一 key_id 应命中自己的前缀");
    }

    #[test]
    fn cch_metadata_session_scopes_cache() {
        let body = "conversation prefix that stays stable ".repeat(20);
        let make = |session: &str| MessagesRequest {
            model: "claude-opus-4-8".to_string(),
            max_tokens: 64,
            messages: vec![
                Message { role: "user".into(), content: serde_json::json!([{"type":"text","text":body}]) },
                Message { role: "assistant".into(), content: serde_json::json!([{"type":"text","text":body}]) },
                Message { role: "user".into(), content: serde_json::json!([{"type":"text","text":body}]) },
            ],
            stream: false,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: Some(Metadata {
                user_id: Some(format!("user_abc_account__session_{session}")),
            }),
        };
        let cache = CchCacheMeter::new(None);
        let s1a = cch_compute_cache_usage(&cache, &make("aaa"), 0);
        assert_eq!(s1a.cache_read, 0);
        let s2 = cch_compute_cache_usage(&cache, &make("bbb"), 0);
        assert_eq!(s2.cache_read, 0, "不同 session 不应命中");
        let s1b = cch_compute_cache_usage(&cache, &make("aaa"), 0);
        assert!(s1b.cache_read > 0, "相同 session 应命中");
    }

    #[test]
    fn cch_master_key_without_session_does_not_simulate_cross_user_cache_hit() {
        let cache = CchCacheMeter::new(None);
        let body = "shared master-key prompt without any session ".repeat(20);
        let msgs = || {
            vec![
                msg_with_cc("user", &body, false),
                msg_with_cc("assistant", &body, false),
                msg_with_cc("user", &body, false),
            ]
        };
        let a = cch_compute_cache_usage(&cache, &req_with_messages(msgs()), 0);
        assert_eq!(a.cache_read, 0);
        assert_eq!(a.cache_covered_est, 0, "主 Key 无 session 不应产生缓存覆盖");
        let b = cch_compute_cache_usage(&cache, &req_with_messages(msgs()), 0);
        assert_eq!(b.cache_read, 0, "共享主 Key 无 session 时不得复用全局模拟缓存");
        assert_eq!(b.cache_covered_est, 0);
    }

    #[test]
    fn cch_skipped_dynamic_system_prefix_counts_toward_prompt_total() {
        let dynamic = "runtime clock and cwd marker ".repeat(40);
        let stable_sys = "You are a coding assistant. ".repeat(200);
        let body = "conversation body ".repeat(15);
        let req = MessagesRequest {
            model: "claude-opus-4-8".to_string(),
            max_tokens: 64,
            messages: vec![
                msg_with_cc("user", &body, false),
                msg_with_cc("assistant", &body, false),
                msg_with_cc("user", &body, false),
            ],
            stream: false,
            system: Some(vec![
                SystemMessage { text: dynamic.clone(), cache_control: None },
                SystemMessage {
                    text: stable_sys,
                    cache_control: Some(CacheControl {
                        cache_type: "ephemeral".to_string(),
                        ttl: None,
                    }),
                },
            ]),
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };
        let u = cch_compute_cache_usage(&CchCacheMeter::new(None), &req, 1);
        assert!(u.cache_covered_est > 0, "稳定前缀应可缓存");
        assert!(
            u.prompt_total_est >= u.cache_covered_est + estimate_tokens(&dynamic),
            "被跳过的动态 system 前缀必须计入 prompt_total 分母：total={} covered={} dyn={}",
            u.prompt_total_est,
            u.cache_covered_est,
            estimate_tokens(&dynamic)
        );
    }

    #[test]
    fn cch_extract_session_id_parses_claude_code_format() {
        assert_eq!(
            extract_session_id("user_xxx_account__session_0b4445e1-uuid"),
            Some("0b4445e1-uuid".to_string())
        );
        assert_eq!(extract_session_id("no-session-here"), None);
    }

    #[test]
    fn cch_token_count_excludes_signature_noise() {
        let history_text = "the quick brown fox jumps over the lazy dog";
        let req = MessagesRequest {
            model: "m".to_string(),
            max_tokens: 8,
            messages: vec![
                Message {
                    role: "user".to_string(),
                    content: serde_json::json!([{"type": "text", "text": history_text}]),
                },
                Message {
                    role: "assistant".to_string(),
                    content: serde_json::Value::String("ok".to_string()),
                },
            ],
            stream: false,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };
        let u = cch_compute_cache_usage(&CchCacheMeter::new(None), &req, 1);
        let pure = estimate_tokens(history_text) as i32;
        assert_eq!(u.cache_covered_est, pure, "covered 应只算原文 token");
    }

    #[test]
    fn cch_creation_is_1h_detected_from_request() {
        // fork 新增字段：入站 1h cache_control → CchResult.creation_is_1h = true。
        let req = MessagesRequest {
            model: "m".to_string(),
            max_tokens: 8,
            messages: vec![Message {
                role: "user".to_string(),
                content: serde_json::json!([{
                    "type": "text", "text": "big context",
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                }]),
            }],
            stream: false,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };
        let u = cch_compute_cache_usage(&CchCacheMeter::new(None), &req, 1);
        assert!(u.creation_is_1h, "1h ttl 应被识别");
        // 无 1h 标记 → 默认 5m。
        let req5m = req_with_messages(vec![msg_with_cc("user", "hi", false)]);
        let u5 = cch_compute_cache_usage(&CchCacheMeter::new(None), &req5m, 1);
        assert!(!u5.creation_is_1h, "无 1h 标记默认 5m");
    }
}
