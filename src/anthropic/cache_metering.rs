//! 中转层 prompt cache 计量（无状态、确定性）
//!
//! Kiro 上游既不做 prompt cache、也不下发 cache_creation / cache_read 字段（实测
//! meteringEvent 只给 credit 计费量），所以中转层上报的缓存计费**纯粹是合成给下游看
//! 的数字**，不对应任何真实缓存命中、也不影响真实成本。
//!
//! 既然底层没有真实缓存，就没有必要去"忠实模拟"真实缓存那套随时间 / 负载漂移的不确定
//! 行为。本模块用一个**纯函数式、确定性**的结构化拆分取而代之：
//!
//! ```text
//! last_input_est   = estimate(最后一条 message)            // 本轮新输入
//! prompt_total_est = estimate(system + tools + 全部 messages)
//! first_turn       = messages.len() <= 1                   // 无历史 → 全新建
//! R                = cache_read_ratio（运行时旋钮，可 per-key 覆盖）
//!
//! // 对真实 total（contextUsage 真值优先，否则 count_tokens 估算）分摊：
//! input     = round(total_real × last_input_est / prompt_total_est)
//! cacheable = total_real − input
//! first_turn → read = 0、creation = cacheable             // 首轮写缓存
//! 否则       → read = round(cacheable × R)、creation = cacheable − read
//! // 恒等：input + creation + read == total_real
//! ```
//!
//! 比例 `last_input_est / prompt_total_est` 是无量纲量，跨估算器成立，所以即便 estimate
//! 口径与真实 total 口径不同，分摊仍自洽。命中率 `R` 是直接旋钮：想呈现 95% 缓存折扣就
//! 设 0.95，想关就设 0。同一段对话无论何时重放、负载如何，结果**完全相同**。
//!
//! 无后台任务、无落盘、无内存增长——计量只是请求级的纯计算。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// `compute_structural_cache_usage` 的结果：按 estimate 口径算出的结构化比例基准 +
/// 命中率，最终由 [`CacheUsage::split_against_total`] 对真实 total 做互斥分摊。
///
/// 三个数共同决定 `(input, cache_creation, cache_read)` 的拆分，但都不是最终值——
/// 真正的 token 数要在拿到真实 total（contextUsage 真值或 count_tokens 估算）后才算出，
/// 因为流式响应直到末尾才知道真实 total。
#[derive(Debug, Clone, Copy)]
pub struct CacheUsage {
    /// 本轮新输入（最后一条 message）的 estimate token——这部分永不计入缓存。
    pub last_input_est: i32,
    /// 整个 prompt（system + tools + 全部 messages）的 estimate token，比例分摊的分母。
    pub prompt_total_est: i32,
    /// 缓存命中率 R ∈ [0,1]：可缓存前缀里有多大比例计作 cache_read（其余计 creation）。
    pub read_ratio: f64,
    /// 是否首轮（无历史）：首轮强制 read = 0、可缓存前缀全部计作 cache_creation。
    pub first_turn: bool,
}

impl Default for CacheUsage {
    /// 默认 = 不模拟缓存：`prompt_total_est == 0` 使 `split_against_total` 全量计入 input。
    fn default() -> Self {
        Self {
            last_input_est: 0,
            prompt_total_est: 0,
            read_ratio: 0.0,
            first_turn: true,
        }
    }
}

impl CacheUsage {
    /// 按真实 total 口径做互斥分摊，返回 `(input_tokens, cache_creation, cache_read)`，
    /// 三者满足 `input + creation + read == total_real`。
    ///
    /// `total_real` 是最终上报口径的全量 prompt token。无可缓存前缀（`prompt_total_est <= 0`
    /// 或 estimate 显示整个 prompt 都是本轮新输入）时，全部计入 input，不凭空造缓存计数。
    pub fn split_against_total(&self, total_real: i32) -> (i32, i32, i32) {
        let total = total_real.max(0);
        if self.prompt_total_est <= 0 || total == 0 {
            return (total, 0, 0);
        }
        // 本轮新输入占比（无量纲，跨估算器成立）；clamp 防 estimate 偏差越界。
        let input_ratio =
            (self.last_input_est as f64 / self.prompt_total_est as f64).clamp(0.0, 1.0);
        let input = ((total as f64) * input_ratio).round() as i32;
        let input = input.clamp(0, total);
        let cacheable = total - input;
        if cacheable <= 0 {
            return (total, 0, 0);
        }
        // 首轮：可缓存前缀全部计作 creation（模拟"第一次把 system / 历史写进缓存"）。
        if self.first_turn {
            return (input, cacheable, 0);
        }
        let r = self.read_ratio.clamp(0.0, 1.0);
        let read = ((cacheable as f64) * r).round() as i32;
        let read = read.clamp(0, cacheable);
        let creation = cacheable - read;
        (input, creation, read)
    }
}

/// 计量运行时治理：持有全局缓存命中率 R（运行时可经 Admin API 调整）。
///
/// 取代旧的有状态 `CacheMeter`——不再需要容量 / TTL / 落盘 / 会话级开关，只剩一个原子旋钮。
pub struct MeterGovernance {
    /// 全局缓存命中率 R 的 bit 表示（f64 → u64，原子读写）。per-key 未覆盖时用此值。
    read_ratio_bits: AtomicU64,
}

impl MeterGovernance {
    /// 用初始命中率构造（clamp 到 [0,1]）。
    pub fn new(read_ratio: f64) -> Self {
        Self {
            read_ratio_bits: AtomicU64::new(read_ratio.clamp(0.0, 1.0).to_bits()),
        }
    }

    /// 当前全局命中率 R。
    pub fn read_ratio(&self) -> f64 {
        f64::from_bits(self.read_ratio_bits.load(Ordering::Relaxed))
    }

    /// 设置全局命中率 R（clamp 到 [0,1]），运行时立即对后续请求生效。
    pub fn set_read_ratio(&self, ratio: f64) {
        self.read_ratio_bits
            .store(ratio.clamp(0.0, 1.0).to_bits(), Ordering::Relaxed);
    }
}

/// `Arc<MeterGovernance>` 别名
pub type SharedMeterGovernance = Arc<MeterGovernance>;

// ============================================================================
// 与请求体协议层的接线
// ============================================================================

use super::stream::estimate_tokens;
use super::types::{MessagesRequest, SystemMessage, Tool};

/// 计算本次请求的结构化缓存覆盖情况。纯函数：只看请求结构与命中率，不依赖任何跨请求
/// 状态、时间、负载。返回 [`CacheUsage`]，由调用方在拿到真实 total 后做互斥分摊。
///
/// `read_ratio` 是该请求生效的命中率（per-key 覆盖优先，否则全局 [`MeterGovernance`]）。
pub fn compute_structural_cache_usage(req: &MessagesRequest, read_ratio: f64) -> CacheUsage {
    // 整个 prompt 的 estimate：tools + system + 全部 messages。
    let mut prompt_total_est: i32 = 0;
    if let Some(tools) = req.tools.as_ref() {
        for t in tools {
            prompt_total_est = prompt_total_est.saturating_add(tool_tokens(t));
        }
    }
    if let Some(systems) = req.system.as_ref() {
        for sys in systems {
            prompt_total_est = prompt_total_est.saturating_add(system_tokens(sys));
        }
    }
    for msg in &req.messages {
        prompt_total_est = prompt_total_est.saturating_add(message_tokens(msg));
    }

    // 本轮新输入 = 最后一条 message 的 estimate（无 message 时为 0）。
    let last_input_est = req
        .messages
        .last()
        .map(message_tokens)
        .unwrap_or(0);

    CacheUsage {
        last_input_est,
        prompt_total_est,
        read_ratio: read_ratio.clamp(0.0, 1.0),
        // 首轮 = 没有历史可缓存（messages 只有本轮这一条，或为空）。
        first_turn: req.messages.len() <= 1,
    }
}

/// 估算一条 message 的 token：遍历 content blocks，文本 / thinking 走文本估算，
/// 图片走 Anthropic 口径尺寸估算。string content 直接估算原文。
fn message_tokens(msg: &super::types::Message) -> i32 {
    match &msg.content {
        serde_json::Value::String(s) => estimate_tokens(s).max(0),
        serde_json::Value::Array(arr) => {
            let mut sum: i32 = 0;
            for v in arr {
                if v.get("type").and_then(|t| t.as_str()) == Some("image") {
                    let (media_type, data) = image_source_parts(v);
                    sum = sum
                        .saturating_add(crate::image_resize::estimate_image_tokens(media_type, data) as i32);
                } else {
                    let text = block_token_text(v);
                    if !text.is_empty() {
                        sum = sum.saturating_add(estimate_tokens(&text).max(0));
                    }
                }
            }
            sum
        }
        _ => 0,
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

/// content block 的 token 估算原文：仅 text + thinking 的纯文本。
fn block_token_text(v: &serde_json::Value) -> String {
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
    fn split_first_turn_all_creation() {
        // 首轮：input 占比 60%，剩余 40% 全部 creation、read = 0。
        let u = CacheUsage {
            last_input_est: 60,
            prompt_total_est: 100,
            read_ratio: 0.8,
            first_turn: true,
        };
        let (input, creation, read) = u.split_against_total(1000);
        assert_eq!(input, 600);
        assert_eq!(creation, 400);
        assert_eq!(read, 0);
        assert_eq!(input + creation + read, 1000);
    }

    #[test]
    fn split_subsequent_turn_applies_ratio() {
        // 非首轮：input 占比 20%，可缓存 80%（=800）内按 R=0.8 拆 → read=640、creation=160。
        let u = CacheUsage {
            last_input_est: 20,
            prompt_total_est: 100,
            read_ratio: 0.8,
            first_turn: false,
        };
        let (input, creation, read) = u.split_against_total(1000);
        assert_eq!(input, 200);
        assert_eq!(read, 640);
        assert_eq!(creation, 160);
        assert_eq!(input + creation + read, 1000);
    }

    #[test]
    fn split_ratio_zero_all_creation() {
        // R=0：可缓存前缀全部计作 creation（等价于"从不命中"）。
        let u = CacheUsage {
            last_input_est: 10,
            prompt_total_est: 100,
            read_ratio: 0.0,
            first_turn: false,
        };
        let (input, creation, read) = u.split_against_total(1000);
        assert_eq!(input, 100);
        assert_eq!(creation, 900);
        assert_eq!(read, 0);
    }

    #[test]
    fn split_ratio_one_all_read() {
        // R=1：可缓存前缀全部计作 read。
        let u = CacheUsage {
            last_input_est: 10,
            prompt_total_est: 100,
            read_ratio: 1.0,
            first_turn: false,
        };
        let (input, creation, read) = u.split_against_total(1000);
        assert_eq!(input, 100);
        assert_eq!(creation, 0);
        assert_eq!(read, 900);
    }

    #[test]
    fn split_is_deterministic() {
        // 同输入多次调用结果完全一致（无状态）。
        let u = CacheUsage {
            last_input_est: 33,
            prompt_total_est: 207,
            read_ratio: 0.8,
            first_turn: false,
        };
        let a = u.split_against_total(4096);
        let b = u.split_against_total(4096);
        assert_eq!(a, b);
        assert_eq!(a.0 + a.1 + a.2, 4096, "互斥口径必须自洽");
    }

    #[test]
    fn split_zero_total_safe() {
        let u = CacheUsage {
            last_input_est: 10,
            prompt_total_est: 100,
            read_ratio: 0.8,
            first_turn: false,
        };
        assert_eq!(u.split_against_total(0), (0, 0, 0));
    }

    // ---- compute_structural_cache_usage ------------------------------------

    #[test]
    fn compute_single_message_is_first_turn() {
        let req = req_with(vec![msg("user", "hello there")], None);
        let u = compute_structural_cache_usage(&req, 0.8);
        assert!(u.first_turn, "单条消息 = 首轮");
        // 本轮新输入 == 整个 prompt（无 system/tools/历史）→ split 全入 input。
        assert_eq!(u.last_input_est, u.prompt_total_est);
        let total = u.prompt_total_est.max(1);
        let (input, creation, read) = u.split_against_total(total);
        assert_eq!(input, total);
        assert_eq!(creation, 0);
        assert_eq!(read, 0);
    }

    #[test]
    fn compute_multi_turn_has_cacheable_prefix() {
        // 历史(user,assistant) + 本轮(user)：本轮新输入 < 整个 prompt → 有可缓存前缀。
        let body = "the quick brown fox ".repeat(20);
        let req = req_with(
            vec![
                msg("user", &body),
                msg("assistant", &body),
                msg("user", "short new question"),
            ],
            Some(vec![SystemMessage {
                text: "you are helpful ".repeat(50),
                cache_control: None,
            }]),
        );
        let u = compute_structural_cache_usage(&req, 0.8);
        assert!(!u.first_turn);
        assert!(u.last_input_est < u.prompt_total_est, "本轮新输入应小于全量");
        let (input, creation, read) = u.split_against_total(u.prompt_total_est);
        assert!(read > 0, "非首轮应有 cache_read");
        assert!(creation > 0, "R<1 时仍有少量 creation");
        assert_eq!(input + creation + read, u.prompt_total_est);
    }

    #[test]
    fn compute_ratio_directly_controls_read_share() {
        // 命中率旋钮线性可调：同一请求，R 越大 read 越多、creation 越少。
        let body = "lorem ipsum dolor sit amet ".repeat(20);
        let req = req_with(
            vec![
                msg("user", &body),
                msg("assistant", &body),
                msg("user", "q"),
            ],
            None,
        );
        let total = compute_structural_cache_usage(&req, 0.5).prompt_total_est;
        let (_, c_low, r_low) = compute_structural_cache_usage(&req, 0.5).split_against_total(total);
        let (_, c_high, r_high) =
            compute_structural_cache_usage(&req, 0.9).split_against_total(total);
        assert!(r_high > r_low, "R 越大 read 越多");
        assert!(c_high < c_low, "R 越大 creation 越少");
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
        let u = compute_structural_cache_usage(&req, 0.8);
        // 含图历史在前缀里 → prompt_total 应远大于本轮纯文本新输入。
        assert!(u.prompt_total_est >= img_tokens, "prompt_total 应含图片 token");
        assert!(!u.first_turn);
        let (_, _, read) = u.split_against_total(u.prompt_total_est);
        assert!(read > 0);
    }

    #[test]
    fn compute_empty_messages_safe() {
        let req = req_with(vec![], None);
        let u = compute_structural_cache_usage(&req, 0.8);
        assert!(u.first_turn);
        assert_eq!(u.last_input_est, 0);
        assert_eq!(u.split_against_total(100), (100, 0, 0));
    }

    // ---- MeterGovernance ---------------------------------------------------

    #[test]
    fn governance_get_set_and_clamp() {
        let g = MeterGovernance::new(0.8);
        assert!((g.read_ratio() - 0.8).abs() < 1e-9);
        g.set_read_ratio(0.95);
        assert!((g.read_ratio() - 0.95).abs() < 1e-9);
        // clamp 到 [0,1]
        g.set_read_ratio(1.5);
        assert!((g.read_ratio() - 1.0).abs() < 1e-9);
        g.set_read_ratio(-0.2);
        assert!((g.read_ratio() - 0.0).abs() < 1e-9);
        assert!((MeterGovernance::new(2.0).read_ratio() - 1.0).abs() < 1e-9);
    }

    // ---- isolation_seed ----------------------------------------------------

    #[test]
    fn isolation_seed_prefers_session_then_key() {
        let mut req = req_with(vec![msg("user", "x")], None);
        assert_eq!(isolation_seed(&req, 7), "key:7");
        req.metadata = Some(Metadata {
            user_id: Some("user_abc_account__session_uuid-123".to_string()),
        });
        assert_eq!(isolation_seed(&req, 7), "sess:uuid-123");
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
}
