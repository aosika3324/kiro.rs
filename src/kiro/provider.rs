//! Kiro API Provider
//!
//! 核心组件，负责与 Kiro API 通信
//! 支持流式和非流式请求
//! 支持多凭据故障转移和重试
//! 支持按凭据级 endpoint 切换不同 Kiro API 端点

use reqwest::Client;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::time::sleep;

use crate::admin::trace_db::{TraceAttempt, TraceSink, outcome, truncate_snippet};
use crate::fingerprint_client::UpstreamResponse;
#[cfg(feature = "tls-fingerprint")]
use crate::fingerprint_client::{build_fingerprint_client, send_via_wreq};
use crate::http_client::{ProxyConfig, build_client, build_streaming_client, http_shard_count};
use crate::kiro::endpoint::amazonq::AMAZONQ_ENDPOINT_NAME;
use crate::kiro::endpoint::cli::CLI_ENDPOINT_NAME;
use crate::kiro::endpoint::codewhisperer::CODEWHISPERER_ENDPOINT_NAME;
use crate::kiro::endpoint::ide::IDE_ENDPOINT_NAME;
use crate::kiro::endpoint::runtime::RUNTIME_ENDPOINT_NAME;
use crate::kiro::endpoint::{KiroEndpoint, RequestContext};
use crate::kiro::error::UpstreamRateLimitError;
use crate::kiro::machine_id;
use crate::kiro::model::credentials::KiroCredentials;
use crate::kiro::token_manager::{CallContext, MultiTokenManager};
use crate::model::config::TlsBackend;
use parking_lot::Mutex;

/// debug 日志下请求体最大打印字节数。超过则只打印头部 + 省略提示,
/// 避免 RUST_LOG=debug 时 200K 级请求体被完整格式化拖垮代理。
const LOG_BODY_MAX_BYTES: usize = 2048;

/// 为 debug 日志截断请求体（UTF-8 安全,只在字符边界切），并中和其中的控制标记。
///
/// 仅用于 `tracing::debug!` 输出，**不影响实际发送内容**。除截断外，还调用
/// `security::neutralize_control_markers` 把客户端内容里可能被恶意塞入的 `<system-reminder>`
/// 等标记转成惰性全角形态——防止操作者/取证 agent 回读日志时被间接提示词注入操纵
/// （kiro.rs 是代理，会搬运任意客户端对话内容）。这是输出/展示侧防御，转发路径不受影响。
pub(crate) fn truncate_for_log(body: &str) -> std::borrow::Cow<'_, str> {
    if body.len() <= LOG_BODY_MAX_BYTES {
        return std::borrow::Cow::Owned(crate::security::neutralize_control_markers(body));
    }
    let mut end = LOG_BODY_MAX_BYTES;
    while end > 0 && !body.is_char_boundary(end) {
        end -= 1;
    }
    let neutral = crate::security::neutralize_control_markers(&body[..end]);
    std::borrow::Cow::Owned(format!("{}…[截断,完整 {} bytes]", neutral, body.len()))
}

/// 结构化诊断抓包：已写出的样本数（进程级上限，避免刷爆磁盘/日志）。
static BODY_SHAPE_CAPTURES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// 诊断用：当上游返回 400 REQUEST_BODY_INVALID 时，对已转换的 conversationState
/// 只导出**结构形状**（角色、块类型、tool_use/tool_result 配对、thinking 是否存在、
/// 各字段长度），**不含任何 prompt 文本**（文本一律以长度/哈希占位）。
///
/// 仅当环境变量 `KIRO_CAPTURE_BODY_SHAPE=<dir>` 设置时启用，且进程内最多写 `KIRO_CAPTURE_BODY_SHAPE_MAX`
/// （默认 8）个样本。用于定位 opus-4-8/sonnet-5 专属的 "Improperly formed request"。
fn capture_body_shape_on_400(request_body: &str, model: Option<&str>, error_body: &str) {
    let dir = match std::env::var("KIRO_CAPTURE_BODY_SHAPE") {
        Ok(d) if !d.is_empty() => d,
        _ => return,
    };
    // 抓两类可诊断的结构性 400：
    // - REQUEST_BODY_INVALID（"Improperly formed" / "Invalid tool use"）
    // - TOOL_USE_RESULT_MISMATCH（tool_use/tool_result 配对/邻接断裂，如
    //   "unexpected tool_use_id" / "toolResult blocks exceeds toolUse blocks"）
    if !error_body.contains("REQUEST_BODY_INVALID")
        && !error_body.contains("TOOL_USE_RESULT_MISMATCH")
    {
        return;
    }
    let max = std::env::var("KIRO_CAPTURE_BODY_SHAPE_MAX")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8);
    let n = BODY_SHAPE_CAPTURES.fetch_add(1, Ordering::Relaxed);
    if n >= max {
        return;
    }

    let shape = build_body_shape(request_body, model, error_body);
    let ts = chrono::Utc::now().format("%Y%m%d-%H%M%S%.3f");
    let path = format!("{}/shape-{}-{}.json", dir.trim_end_matches('/'), ts, n);
    if let Ok(pretty) = serde_json::to_string_pretty(&shape) {
        let _ = std::fs::write(&path, pretty);
        tracing::warn!("已写出请求体结构诊断样本 #{}: {}", n, path);
    }
}

/// 从转换后的 conversationState JSON 中抽取「形状」，文本内容全部脱敏为长度。
fn build_body_shape(
    request_body: &str,
    model: Option<&str>,
    error_body: &str,
) -> serde_json::Value {
    use serde_json::json;
    let root: serde_json::Value = match serde_json::from_str(request_body) {
        Ok(v) => v,
        Err(e) => return json!({"parse_error": e.to_string(), "raw_len": request_body.len()}),
    };
    let cs = root.get("conversationState").unwrap_or(&serde_json::Value::Null);

    // tool_use_id 的稳定短哈希：id 是不透明句柄（如 call_xxx / toolu_xxx），非 prompt 文本，
    // 但要诊断配对/邻接必须能跨轮次对齐同一 id。故仅导出 6 位十六进制哈希（FNV-1a），
    // 既不泄露原文、又能在样本内判断「某 tool_result 的 id 是否等于某轮 tool_use 的 id」。
    let id_hash = |id: Option<&str>| -> Option<String> {
        id.map(|s| {
            let mut h: u64 = 0xcbf29ce484222325;
            for b in s.as_bytes() {
                h ^= *b as u64;
                h = h.wrapping_mul(0x100000001b3);
            }
            format!("{:06x}", (h & 0xffffff))
        })
    };

    // 单条 userInputMessage / assistantResponseMessage 的形状。
    let shape_user = |uim: &serde_json::Value| -> serde_json::Value {
        let content = uim.get("content").and_then(|c| c.as_str()).unwrap_or("");
        let ctx = uim.get("userInputMessageContext");
        let tool_results: Vec<serde_json::Value> = ctx
            .and_then(|c| c.get("toolResults"))
            .and_then(|t| t.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|r| {
                        json!({
                            "toolUseId_len": r.get("toolUseId").and_then(|v| v.as_str()).map(|s| s.len()),
                            "toolUseId_hash": id_hash(r.get("toolUseId").and_then(|v| v.as_str())),
                            "status": r.get("status").and_then(|v| v.as_str()),
                            "content_blocks": r.get("content").and_then(|v| v.as_array()).map(|a| a.len()),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        let tools_arr = ctx.and_then(|c| c.get("tools")).and_then(|t| t.as_array());
        let tools_n = tools_arr.map(|a| a.len());
        // Bug B 诊断：记录 tools 定义的工具名 + 是否含非法字符（引号/空格等），
        // 用于确认「Invalid tool use format」是否源自工具「定义」本身的畸形 name。
        let is_valid_name =
            |n: &str| !n.is_empty() && n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
        let tool_def_names: Vec<serde_json::Value> = tools_arr
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        t.get("toolSpecification")
                            .and_then(|s| s.get("name"))
                            .and_then(|v| v.as_str())
                    })
                    .map(|n| json!({"name": n, "valid": is_valid_name(n)}))
                    .collect()
            })
            .unwrap_or_default();
        json!({
            "kind": "user",
            "content_len": content.len(),
            "content_has_thinking_tag": content.contains("<thinking"),
            "content_empty": content.is_empty(),
            "images": uim.get("images").and_then(|i| i.as_array()).map(|a| a.len()),
            "tool_results": tool_results,
            "tools_count": tools_n,
            "tool_def_names": tool_def_names,
            "modelId": uim.get("modelId").and_then(|v| v.as_str()),
            "origin": uim.get("origin").and_then(|v| v.as_str()),
        })
    };
    let shape_assistant = |arm: &serde_json::Value| -> serde_json::Value {
        let content = arm.get("content").and_then(|c| c.as_str()).unwrap_or("");
        let tool_uses: Vec<serde_json::Value> = arm
            .get("toolUses")
            .and_then(|t| t.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|tu| {
                        let input = tu.get("input");
                        json!({
                            "toolUseId_len": tu.get("toolUseId").and_then(|v| v.as_str()).map(|s| s.len()),
                            "toolUseId_hash": id_hash(tu.get("toolUseId").and_then(|v| v.as_str())),
                            "name": tu.get("name").and_then(|v| v.as_str()),
                            "input_is_object": input.map(|v| v.is_object()),
                            "input_type": input.map(|v| match v {
                                serde_json::Value::Object(_) => "object",
                                serde_json::Value::Array(_) => "array",
                                serde_json::Value::String(_) => "string",
                                serde_json::Value::Number(_) => "number",
                                serde_json::Value::Bool(_) => "bool",
                                serde_json::Value::Null => "null",
                            }),
                            "input_keys": input.and_then(|v| v.as_object()).map(|o| o.len()),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        json!({
            "kind": "assistant",
            "content_len": content.len(),
            "content_empty": content.is_empty(),
            "content_is_single_space": content == " ",
            "content_has_thinking_tag": content.contains("<thinking"),
            "tool_uses": tool_uses,
        })
    };

    let history_shape: Vec<serde_json::Value> = cs
        .get("history")
        .and_then(|h| h.as_array())
        .map(|arr| {
            arr.iter()
                .map(|m| {
                    if let Some(uim) = m.get("userInputMessage") {
                        shape_user(uim)
                    } else if let Some(arm) = m.get("assistantResponseMessage") {
                        shape_assistant(arm)
                    } else {
                        json!({"kind": "unknown", "keys": m.as_object().map(|o| o.keys().cloned().collect::<Vec<_>>())})
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    let current = cs
        .get("currentMessage")
        .and_then(|c| c.get("userInputMessage"))
        .map(shape_user);

    json!({
        "model": model,
        "error_body": error_body,
        "request_body_len": request_body.len(),
        "chatTriggerType": cs.get("chatTriggerType").and_then(|v| v.as_str()),
        "agentTaskType": cs.get("agentTaskType").and_then(|v| v.as_str()),
        "history_len": history_shape.len(),
        "history": history_shape,
        "current_message": current,
    })
}

/// 每个凭据的最大重试次数
const MAX_RETRIES_PER_CREDENTIAL: usize = 3;

/// 总重试次数硬上限（避免无限重试）
///
/// 注：上游 429 多为账号级速率配额（SERVICE_REQUEST_RATE_EXCEEDED），高峰期
/// 多账号同时触顶时，过多重试会在账号间连环撞墙、放大限流。故上限取较小值，
/// 配合 429 专用长退避（见 retry_delay_throttle），被限时尽早返回而非耗尽配额。
const MAX_TOTAL_RETRIES: usize = 4;

/// A4a 大请求字节阈值（~512KB）。请求体超此值视为 Long 档（约对应 128K token 的
/// Anthropic JSON 体量），触发调度排序惩罚以分散大请求、避免扎堆同账号拖垮首字节。
const LARGE_REQUEST_BYTES: usize = 512 * 1024;

/// HTTP Client 缓存容量上限（不含常驻的全局代理 client）。
/// 代理池条目较多时，避免每个不同代理都常驻一个 reqwest::Client 导致内存无界增长。
const CLIENT_CACHE_CAP: usize = 64;

const AUTO_ENDPOINT_NAME: &str = "auto";
const KIRO_ENDPOINT_ALIAS: &str = "kiro";

/// 端点路由运行时配置（首选端点 + fallback 开关）。
///
/// 与 [`MeterGovernance`](crate::anthropic::cache_metering::MeterGovernance) 等治理组件同构：
/// 以 `Arc<EndpointRouting>` 在 `KiroProvider` 与 `AdminService` 间共享，Admin 面板改动后
/// **运行时立即对后续请求生效**、无需重启。`default_endpoint` 与端点注册表仍是构造期不可变量
/// （注册表关系到 client 预热与断言），此处只承载两个可热更的旋钮。
pub struct EndpointRouting {
    /// 首选端点名（`auto` / `kiro` / `ide` / `cli` / `codewhisperer` / `amazonq` / `runtime`）。
    /// `None` 表示未设置——回退到凭据级 `endpoint` 或 `default_endpoint`。
    preferred: Mutex<Option<String>>,
    /// 是否在同一凭据上尝试其余 Kiro-Go 兼容端点（对齐 Kiro-Go auto 路由，并含 runtime）。
    fallback: AtomicBool,
}

impl EndpointRouting {
    /// 用初始首选端点 + fallback 开关构造。
    pub fn new(preferred: Option<String>, fallback: bool) -> Self {
        Self {
            preferred: Mutex::new(preferred),
            fallback: AtomicBool::new(fallback),
        }
    }

    /// 当前首选端点名（`None` 表示未设置）。
    pub fn preferred(&self) -> Option<String> {
        self.preferred.lock().clone()
    }

    /// 设置首选端点名（`None` 清除），运行时立即对后续请求生效。
    pub fn set_preferred(&self, preferred: Option<String>) {
        *self.preferred.lock() = preferred;
    }

    /// 当前 fallback 开关。
    pub fn fallback(&self) -> bool {
        self.fallback.load(Ordering::Relaxed)
    }

    /// 设置 fallback 开关，运行时立即对后续请求生效。
    pub fn set_fallback(&self, enabled: bool) {
        self.fallback.store(enabled, Ordering::Relaxed);
    }
}

/// 一个账户（一个 effective proxy）的 HTTP Client **分片集**：N 个独立 `Client`，每个各自一条
/// 到上游 host 的 HTTP/2 连接。`pick()` 按原子游标 round-robin 选一个，把同账户的并发请求摊到
/// N 条独立连接上——复现"多进程各自一条连接"的并行度，根治单 H2 连接多路复用的首字节瓶颈。
/// 见 [`crate::http_client::http_shard_count`]。
struct ShardSet {
    clients: Vec<Client>,
    cursor: AtomicUsize,
}

impl ShardSet {
    fn new(clients: Vec<Client>) -> Self {
        debug_assert!(!clients.is_empty(), "ShardSet 至少要有一个 client");
        Self {
            clients,
            cursor: AtomicUsize::new(0),
        }
    }

    /// round-robin 取一个 client（clone 廉价，内部是 Arc）。
    fn pick(&self) -> Client {
        let n = self.clients.len().max(1);
        let i = self.cursor.fetch_add(1, Ordering::Relaxed) % n;
        self.clients[i].clone()
    }
}

/// 带容量上限的 HTTP Client 缓存。
///
/// - key 为 effective proxy 配置（None = 直连/全局回退）
/// - value 为该账户的 [`ShardSet`]（N 个独立 Client，round-robin 摊连接）
/// - 受保护 key（全局代理对应的 effective 配置）永不被淘汰
/// - 超出容量时按插入顺序淘汰最旧的「非受保护」条目
struct ClientCache {
    map: HashMap<Option<ProxyConfig>, ShardSet>,
    /// 插入顺序（仅记录可淘汰的非受保护 key）
    order: std::collections::VecDeque<Option<ProxyConfig>>,
    /// 受保护、不参与淘汰的 key（全局代理）
    protected: Option<ProxyConfig>,
    cap: usize,
}

impl ClientCache {
    fn new(protected: Option<ProxyConfig>, initial: ShardSet, cap: usize) -> Self {
        let mut map = HashMap::new();
        map.insert(protected.clone(), initial);
        Self {
            map,
            order: std::collections::VecDeque::new(),
            protected,
            cap,
        }
    }

    /// round-robin 取该 key 分片集里的一个 client。
    fn get(&self, key: &Option<ProxyConfig>) -> Option<Client> {
        self.map.get(key).map(|s| s.pick())
    }

    /// 插入新分片集，必要时淘汰最旧的非受保护条目
    fn insert(&mut self, key: Option<ProxyConfig>, shard: ShardSet) {
        if key == self.protected || self.map.contains_key(&key) {
            self.map.insert(key, shard);
            return;
        }
        while self.order.len() >= self.cap {
            if let Some(evict) = self.order.pop_front() {
                self.map.remove(&evict);
            } else {
                break;
            }
        }
        self.order.push_back(key.clone());
        self.map.insert(key, shard);
    }
}

/// 构建 N 个分片 client（`build` 为单个 client 的构造闭包），组装成 [`ShardSet`]。
fn build_shard_set<F>(mut build: F) -> anyhow::Result<ShardSet>
where
    F: FnMut() -> anyhow::Result<Client>,
{
    let n = http_shard_count();
    let mut clients = Vec::with_capacity(n);
    for _ in 0..n {
        clients.push(build()?);
    }
    Ok(ShardSet::new(clients))
}

/// [`ShardSet`] 的 wreq（TLS 指纹）版：N 个独立 wreq::Client，round-robin 摊连接。语义同 ShardSet。
#[cfg(feature = "tls-fingerprint")]
struct WreqShardSet {
    clients: Vec<wreq::Client>,
    cursor: AtomicUsize,
}

#[cfg(feature = "tls-fingerprint")]
impl WreqShardSet {
    fn new(clients: Vec<wreq::Client>) -> Self {
        debug_assert!(!clients.is_empty(), "WreqShardSet 至少要有一个 client");
        Self {
            clients,
            cursor: AtomicUsize::new(0),
        }
    }

    fn pick(&self) -> wreq::Client {
        let n = self.clients.len().max(1);
        let i = self.cursor.fetch_add(1, Ordering::Relaxed) % n;
        self.clients[i].clone()
    }
}

/// API 调用结果，附带本次实际命中的上游凭据 ID（用于用量统计）
///
/// # 并发特性（重要）
///
/// `account_guard`（[`CallContext`]，内含 `CredentialLease`）持有该凭据的一个
/// Semaphore 并发 permit。在流式路径中，它被移入流处理闭包，**直到流的最后一个 chunk
/// 发出后才 drop**，permit 才归还。这意味着：慢速上游、或被下游客户端拖住读取速度的流，
/// 会长期占用该账号的并发槽位。高并发下大量慢流可能把所有账号的槽位耗尽，触发
/// `acquire_idle_permit` 的"池忙"效应（非阻塞模式下由 provider 重试链退避后换 attempt）。
///
/// 这是设计上的权衡（permit 生命周期 == 流生命周期，保证在途计数准确），不是 bug——
/// 但它使实际并发行为部分取决于下游客户端的读速度。调优点：`account_acquire_timeout_secs`
/// 控制阻塞模式的最长等待，账号并发上限由各凭据的 Semaphore 容量决定。
pub struct KiroCallResult {
    pub response: UpstreamResponse,
    pub credential_id: u64,
    pub account_guard: CallContext,
}

/// Kiro API Provider
///
/// 核心组件，负责与 Kiro API 通信
/// 支持多凭据故障转移和重试机制
/// 按凭据 `endpoint` 字段选择 [`KiroEndpoint`] 实现
pub struct KiroProvider {
    token_manager: Arc<MultiTokenManager>,
    /// Client 缓存：key = effective proxy config, value = reqwest::Client
    /// 不同代理配置的凭据使用不同的 Client，共享相同代理的凭据复用 Client。
    /// 带容量上限淘汰（全局代理 client 常驻），避免代理数量增长导致内存无界增长。
    client_cache: Mutex<ClientCache>,
    /// 流式专用 Client 缓存（小连接池 + H2 keepalive，见 [`build_streaming_client`]）。
    /// 与 `client_cache` 同构、同样按 effective proxy 缓存；连接池复用省 TLS 握手,
    /// 避免长流被上游中途掐断导致的"断流"。
    streaming_client_cache: Mutex<ClientCache>,
    /// TLS 指纹 (wreq) 客户端缓存。仅当 `config.tls_fingerprint_enabled` 开启时才会建/用。
    /// key = (effective proxy, 是否流式禁复用, profile 名)；三者任一不同即用独立 client。
    /// 惰性构建（默认关闭时不产生任何 wreq client / BoringSSL 开销），带容量上限简单淘汰。
    #[cfg(feature = "tls-fingerprint")]
    wreq_client_cache: Mutex<HashMap<(Option<ProxyConfig>, bool, String), WreqShardSet>>,
    /// TLS 后端配置
    tls_backend: TlsBackend,
    /// 端点实现注册表（key: endpoint 名称）
    endpoints: HashMap<String, Arc<dyn KiroEndpoint>>,
    /// 默认端点名称（凭据未指定 endpoint 时使用）
    default_endpoint: String,
    /// 端点路由运行时配置（首选端点 + fallback 开关）。与 `AdminService` 共享同一
    /// `Arc<EndpointRouting>`，Admin 面板改动后对后续请求立即生效、无需重启。
    routing: Arc<EndpointRouting>,
    /// 已尝试过 profileArn 解析的凭据 ID（进程内）。
    ///
    /// 避免对「无 Enterprise profile」的账号（如纯 BuilderID）在每次请求都重复调用
    /// `ListAvailableProfiles`。命中真实 ARN 的账号会把 ARN 持久化进凭据，之后
    /// 通过 `streaming_profile_arn()` 直接命中，不再进入解析路径。
    profile_resolution_attempted: Mutex<HashSet<u64>>,
}

impl KiroProvider {
    fn endpoint_alias(name: &str) -> &str {
        if name == KIRO_ENDPOINT_ALIAS {
            IDE_ENDPOINT_NAME
        } else {
            name
        }
    }

    fn is_auto_endpoint(name: &str) -> bool {
        name == AUTO_ENDPOINT_NAME
    }

    fn is_known_config_endpoint(
        endpoints: &HashMap<String, Arc<dyn KiroEndpoint>>,
        name: &str,
    ) -> bool {
        Self::is_auto_endpoint(name) || endpoints.contains_key(Self::endpoint_alias(name))
    }

    fn next_trace_step(trace_step: &mut usize) -> usize {
        let step = *trace_step;
        *trace_step = (*trace_step).saturating_add(1);
        step
    }

    /// 创建带代理配置和端点注册表的 KiroProvider 实例
    ///
    /// # Arguments
    /// * `token_manager` - 多凭据 Token 管理器
    /// * `proxy` - 全局代理配置
    /// * `endpoints` - 端点名 → 实现的注册表（至少包含 `default_endpoint` 对应条目）
    /// * `default_endpoint` - 凭据未显式指定 endpoint 时使用的名称
    pub fn with_proxy(
        token_manager: Arc<MultiTokenManager>,
        proxy: Option<ProxyConfig>,
        endpoints: HashMap<String, Arc<dyn KiroEndpoint>>,
        default_endpoint: String,
        routing: Arc<EndpointRouting>,
    ) -> Self {
        assert!(
            Self::is_known_config_endpoint(&endpoints, &default_endpoint),
            "默认端点 {} 未在 endpoints 注册表中",
            default_endpoint
        );
        if let Some(preferred) = routing.preferred() {
            assert!(
                Self::is_known_config_endpoint(&endpoints, &preferred),
                "preferred endpoint {} 未在 endpoints 注册表中",
                preferred
            );
        }
        let tls_backend = token_manager.config().tls_backend;
        // 预热：构建全局代理对应的分片集（N 条独立连接，作为受保护的常驻条目）。
        let initial_shard =
            build_shard_set(|| build_client(proxy.as_ref(), 720, tls_backend))
                .expect("创建 HTTP 客户端失败");
        let client_cache = ClientCache::new(proxy.clone(), initial_shard, CLIENT_CACHE_CAP);
        // 流式专用分片集同样预热全局代理条目。
        let initial_streaming_shard =
            build_shard_set(|| build_streaming_client(proxy.as_ref(), 720, tls_backend))
                .expect("创建流式 HTTP 客户端失败");
        let streaming_client_cache =
            ClientCache::new(proxy.clone(), initial_streaming_shard, CLIENT_CACHE_CAP);

        Self {
            token_manager,
            client_cache: Mutex::new(client_cache),
            streaming_client_cache: Mutex::new(streaming_client_cache),
            #[cfg(feature = "tls-fingerprint")]
            wreq_client_cache: Mutex::new(HashMap::new()),
            tls_backend,
            endpoints,
            default_endpoint,
            routing,
            profile_resolution_attempted: Mutex::new(HashSet::new()),
        }
    }

    /// 根据凭据的代理配置获取（或创建并缓存）对应的 reqwest::Client
    fn client_for(&self, credentials: &KiroCredentials) -> anyhow::Result<Client> {
        // 全局代理动态读自 token_manager（单一数据源），Admin 改代理后立即生效、无需重启。
        let global_proxy = self.token_manager.proxy();
        let effective = credentials.effective_proxy(global_proxy.as_ref());
        let mut cache = self.client_cache.lock();
        if let Some(client) = cache.get(&effective) {
            return Ok(client);
        }
        let shard = build_shard_set(|| build_client(effective.as_ref(), 720, self.tls_backend))?;
        let client = shard.pick();
        cache.insert(effective, shard);
        Ok(client)
    }

    /// 获取流式专用 Client（禁用空闲连接复用，见 [`build_streaming_client`]）。
    /// 与 [`Self::client_for`] 同构，但走独立的 `streaming_client_cache`。
    fn streaming_client_for(&self, credentials: &KiroCredentials) -> anyhow::Result<Client> {
        // 同 client_for：全局代理动态读自 token_manager，避免 provider 持有会过期的拷贝。
        let global_proxy = self.token_manager.proxy();
        let effective = credentials.effective_proxy(global_proxy.as_ref());
        let mut cache = self.streaming_client_cache.lock();
        if let Some(client) = cache.get(&effective) {
            return Ok(client);
        }
        let shard =
            build_shard_set(|| build_streaming_client(effective.as_ref(), 720, self.tls_backend))?;
        let client = shard.pick();
        cache.insert(effective, shard);
        Ok(client)
    }

    /// 获取（或惰性创建并缓存）TLS 指纹 wreq 客户端。
    ///
    /// `streaming` 为 true 时禁用空闲连接复用（同 [`build_streaming_client`] 语义）。
    /// 缓存 key 含 profile，改预设后自动用新 client；简单容量上限，超限清空重来（预设/代理
    /// 组合数很小，不会频繁触发）。
    #[cfg(feature = "tls-fingerprint")]
    fn wreq_client_for(
        &self,
        credentials: &KiroCredentials,
        streaming: bool,
        profile: &str,
    ) -> anyhow::Result<wreq::Client> {
        const WREQ_CACHE_CAP: usize = 64;
        let global_proxy = self.token_manager.proxy();
        let effective = credentials.effective_proxy(global_proxy.as_ref());
        let key = (effective.clone(), streaming, profile.to_string());
        let mut cache = self.wreq_client_cache.lock();
        if let Some(shard) = cache.get(&key) {
            return Ok(shard.pick());
        }
        let pool = if streaming { 0 } else { 8 };
        // N 个独立 wreq client（各自一条 H2 连接），round-robin 摊同账户并发。
        let n = http_shard_count();
        let mut clients = Vec::with_capacity(n);
        for _ in 0..n {
            clients.push(build_fingerprint_client(effective.as_ref(), 720, profile, pool)?);
        }
        let shard = WreqShardSet::new(clients);
        let client = shard.pick();
        if cache.len() >= WREQ_CACHE_CAP {
            cache.clear();
        }
        cache.insert(key, shard);
        Ok(client)
    }

    /// 发送已装饰好的上游请求，按 TLS 指纹开关选择 reqwest 或 wreq 路径。
    ///
    /// 关闭时（默认）：走传入的 reqwest `http_client`，与历史行为逐字节一致。
    /// 开启时：把该 `reqwest::Request` 重放到带浏览器指纹的 wreq 客户端。两路的响应
    /// 统一封装为 [`UpstreamResponse`]，下游流/非流处理无需分叉。错误统一为 `anyhow`
    /// （调用点仅 `.to_string()` 记录并计入 `last_error`，网络错误可重试性由既有策略判定）。
    #[allow(unused_variables)] // credentials/streaming 仅在 tls-fingerprint 特性下使用
    async fn execute_upstream(
        &self,
        http_client: &Client,
        credentials: &KiroCredentials,
        streaming: bool,
        request: reqwest::Request,
    ) -> anyhow::Result<UpstreamResponse> {
        #[cfg(feature = "tls-fingerprint")]
        {
            let (fp_enabled, fp_profile) = self.token_manager.tls_fingerprint();
            if fp_enabled {
                // SOCKS 代理不能走 wreq(BoringSSL)：其 `socks` 特性未启用（见 Cargo.toml），
                // 交给它会建连失败或静默直连、泄露真实服务器 IP。识别到 SOCKS 时回退到支持
                // socks 的 reqwest 主路径——保代理有效（IP 不泄露），仅本次牺牲指纹伪装。
                let global_proxy = self.token_manager.proxy();
                let effective = credentials.effective_proxy(global_proxy.as_ref());
                let is_socks = effective.as_ref().is_some_and(|p| p.is_socks());
                if is_socks {
                    // 每进程只告警一次，避免高频刷屏；代理仍生效，仅指纹被跳过。
                    static WARNED: AtomicBool = AtomicBool::new(false);
                    if !WARNED.swap(true, Ordering::Relaxed) {
                        tracing::warn!(
                            "TLS 指纹已开启，但生效代理为 SOCKS；wreq 未启用 socks 特性，\
                             本次请求回退到 reqwest 路径（代理正常生效，跳过指纹伪装）。\
                             如需在 SOCKS 下也伪装指纹，请改用 http/https 代理。"
                        );
                    }
                } else {
                    let client = self.wreq_client_for(credentials, streaming, &fp_profile)?;
                    let resp = send_via_wreq(&client, request).await?;
                    return Ok(UpstreamResponse::Wreq(resp));
                }
            }
        }
        let resp = http_client.execute(request).await?;
        Ok(UpstreamResponse::Reqwest(resp))
    }

    /// 根据凭据选择 endpoint 实现
    fn endpoint_for(&self, credentials: &KiroCredentials) -> anyhow::Result<Arc<dyn KiroEndpoint>> {
        let configured = credentials
            .endpoint
            .as_deref()
            .unwrap_or(&self.default_endpoint);
        let name = if Self::is_auto_endpoint(configured) {
            IDE_ENDPOINT_NAME
        } else {
            Self::endpoint_alias(configured)
        };
        self.endpoints
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("未知端点: {}", name))
    }

    /// Build endpoint order for API calls. When endpoint fallback is disabled,
    /// returns the single selected endpoint; when enabled, primary first then the
    /// remaining Kiro-Go-compatible streaming endpoints.
    fn endpoint_order_for(
        &self,
        credentials: &KiroCredentials,
    ) -> anyhow::Result<Vec<Arc<dyn KiroEndpoint>>> {
        // 首选端点为运行时热更值（Admin 面板可改），取其快照后再借用。
        let preferred = self.routing.preferred();
        let configured = credentials
            .endpoint
            .as_deref()
            .or(preferred.as_deref())
            .unwrap_or(&self.default_endpoint);
        // 自动模式 fallback 顺序:IDE → CodeWhisperer → Amazon Q → Runtime。
        // 此数组同时决定 auto 全序与 primary 指定时的补序。cli 不在链内(独立路由,不参与
        // auto fallback)。
        //
        // 为何 IDE 打头、Runtime 垫底(而非按首字延迟把 Runtime 提前):
        // 1. IDE 端点对 external_idp(企业/Entra)凭据会切到 codewhisperer host,Runtime
        //    不做此切换——Runtime 当主端点会让企业账号路由到错误 host。
        // 2. Runtime 走 runtime.*.kiro.dev,其后端模型/调优与 q.*.amazonaws.com 未必一致,
        //    实测把 Runtime 提前会引发工具调用类异常;保持 IDE 主端点是已验证的稳妥序。
        // 首字优化应走连接池/握手层(见 build_streaming_client),不靠切换主端点。
        const FALLBACK_ORDER: [&str; 4] = [
            IDE_ENDPOINT_NAME,
            CODEWHISPERER_ENDPOINT_NAME,
            AMAZONQ_ENDPOINT_NAME,
            RUNTIME_ENDPOINT_NAME,
        ];
        if Self::is_auto_endpoint(configured) {
            return FALLBACK_ORDER
                .into_iter()
                .map(|name| {
                    self.endpoints
                        .get(name)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("未知端点: {}", name))
                })
                .collect();
        }
        let primary = Self::endpoint_alias(configured);
        if !self.routing.fallback() || primary == CLI_ENDPOINT_NAME {
            return self
                .endpoints
                .get(primary)
                .cloned()
                .map(|endpoint| vec![endpoint])
                .ok_or_else(|| anyhow::anyhow!("未知端点: {}", primary));
        }
        if !FALLBACK_ORDER.contains(&primary) {
            return self
                .endpoints
                .get(primary)
                .cloned()
                .map(|endpoint| vec![endpoint])
                .ok_or_else(|| anyhow::anyhow!("未知端点: {}", primary));
        }
        let mut names = Vec::with_capacity(FALLBACK_ORDER.len());
        names.push(primary);
        for name in FALLBACK_ORDER {
            if name != primary {
                names.push(name);
            }
        }
        names
            .into_iter()
            .map(|name| {
                self.endpoints
                    .get(name)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("未知端点: {}", name))
            })
            .collect()
    }

    fn is_fallbackable_status(status: reqwest::StatusCode) -> bool {
        // 对齐 Kiro-Go (proxy/kiro.go:401-419): 除账号级状态(401/402/403)外,
        // 其余非 200 一律 fallback 到下一个端点(含 524 网关超时、400 客户端校验错误)。
        // 401/403/402 由下方账号级状态处理分支负责,在同一账号上换端点无意义。
        !matches!(status.as_u16(), 401 | 402 | 403)
    }

    fn is_fallbackable_network_error() -> bool {
        true
    }

    /// 在发起请求前，确保 Enterprise / IdC 账号的真实 profileArn 已解析并写入 `ctx`。
    ///
    /// 流式端点强制要求 profileArn；Enterprise / IdC 账号必须先把 BuilderID
    /// 占位符解析为真实 ARN，纯 BuilderID 账号则回退占位符。
    /// 仅对「OAuth 凭据 + profileArn 缺失或为占位符」的账号触发一次上游
    /// `ListAvailableProfiles` 查询（进程内去重）：
    /// - 命中真实 ARN → 写回 `ctx.credentials.profile_arn` 并由 token_manager 持久化；
    ///   之后该凭据的 `streaming_profile_arn()` 直接命中，不再进入此路径。
    /// - 无 Enterprise profile（纯 BuilderID 等）→ 保持占位符回退逻辑，并标记已尝试，
    ///   避免每次请求重复查询。
    async fn ensure_profile_arn(
        &self,
        ctx: &mut crate::kiro::token_manager::CallContext,
    ) -> anyhow::Result<()> {
        use crate::kiro::model::credentials::is_placeholder_profile_arn;

        if ctx.credentials.is_api_key_credential() {
            return Ok(());
        }
        let needs = match ctx.credentials.profile_arn.as_deref() {
            None => true,
            Some(arn) => is_placeholder_profile_arn(arn),
        };
        if !needs {
            return Ok(());
        }
        // 进程内去重：仅在「拿到上游确定结果」后才标记已尝试，避免一次网络抖动
        // 把账号永久卡在占位符上（重启前不再重试）。
        if self.profile_resolution_attempted.lock().contains(&ctx.id) {
            return Ok(());
        }
        match self
            .token_manager
            .resolve_profile_arn_for(ctx.id, &ctx.token)
            .await
        {
            Ok(Some(arn)) => {
                ctx.credentials.profile_arn = Some(arn);
                self.profile_resolution_attempted.lock().insert(ctx.id);
            }
            Ok(None) => {
                // 上游确认该账号无 Enterprise profile（纯 BuilderID 等）：标记已尝试，
                // 后续请求回退到占位符逻辑，不再重复查询。
                self.profile_resolution_attempted.lock().insert(ctx.id);
            }
            Err(e) => {
                if is_rate_limit_error(&e) {
                    return Err(e);
                }
                // 网络/瞬态错误：不标记，下次请求再试；本次按原 profileArn 继续
                tracing::warn!(
                    "凭据 #{} 解析真实 profileArn 失败（按原 profileArn 继续）: {}",
                    ctx.id,
                    e
                );
            }
        }
        Ok(())
    }

    /// 发送非流式 API 请求
    ///
    /// 支持多凭据故障转移（见 [`Self::call_api_with_retry`]）。
    /// `sink` 可选，用于逐跳上报链路追踪。
    pub async fn call_api(
        &self,
        request_body: &str,
        sink: Option<&dyn TraceSink>,
        group: Option<&str>,
        session_seed: Option<&str>,
    ) -> anyhow::Result<KiroCallResult> {
        self.call_api_with_retry(request_body, false, sink, group, session_seed)
            .await
    }

    /// 发送流式 API 请求
    ///
    /// `session_seed`（见 `cache_metering::isolation_seed`）用于会话粘性调度：同会话优先复用
    /// 上次成功命中的凭据以提升上游前缀缓存命中率；`None` 时粘性不生效。
    pub async fn call_api_stream(
        &self,
        request_body: &str,
        sink: Option<&dyn TraceSink>,
        group: Option<&str>,
        session_seed: Option<&str>,
    ) -> anyhow::Result<KiroCallResult> {
        self.call_api_with_retry(request_body, true, sink, group, session_seed)
            .await
    }

    /// 发送 MCP API 请求（WebSearch 等工具调用）
    pub async fn call_mcp(
        &self,
        request_body: &str,
        group: Option<&str>,
    ) -> anyhow::Result<UpstreamResponse> {
        self.call_mcp_with_retry(request_body, group).await
    }

    /// 内部方法：带重试逻辑的 MCP API 调用
    async fn call_mcp_with_retry(
        &self,
        request_body: &str,
        group: Option<&str>,
    ) -> anyhow::Result<UpstreamResponse> {
        let total_credentials = self.token_manager.total_count_in_group(group).max(1);
        let max_retries = (total_credentials * MAX_RETRIES_PER_CREDENTIAL).min(MAX_TOTAL_RETRIES);
        let mut last_error: Option<anyhow::Error> = None;
        let mut force_refreshed: HashSet<u64> = HashSet::new();

        for attempt in 0..max_retries {
            // MCP 调用（WebSearch 等工具）不涉及模型选择，但必须遵守客户端 Key 的凭据分组隔离。
            let ctx = match self.token_manager.acquire_context(None, group).await {
                Ok(c) => c,
                Err(e) => {
                    if is_rate_limit_error(&e) {
                        return Err(e);
                    }
                    if let Some(rate_limit) = take_rate_limit_error(&mut last_error) {
                        return Err(rate_limit);
                    }
                    last_error = Some(e);
                    continue;
                }
            };

            let config = self.token_manager.config();
            let machine_id = machine_id::generate_from_credentials(&ctx.credentials, config);

            let endpoint = match self.endpoint_for(&ctx.credentials) {
                Ok(e) => e,
                Err(e) => {
                    last_error = Some(e);
                    // endpoint 解析失败：记为失败，换下一张凭据
                    self.token_manager.report_failure(ctx.id);
                    continue;
                }
            };

            let rctx = RequestContext {
                credentials: &ctx.credentials,
                token: &ctx.token,
                machine_id: &machine_id,
                config,
            };

            let url = endpoint.mcp_url(&rctx);
            let body = endpoint.transform_mcp_body(request_body, &rctx);

            let http_client = self.client_for(&ctx.credentials)?;
            let base = http_client
                .post(&url)
                .body(body)
                .header("content-type", endpoint.content_type());
            let request = match endpoint.decorate_mcp(base, &rctx).build() {
                Ok(r) => r,
                Err(e) => {
                    last_error = Some(anyhow::anyhow!("构建 MCP 请求失败: {}", e));
                    continue;
                }
            };

            // MCP 为非流式；TLS 指纹开启时同样经 wreq 发出。
            let response = match self
                .execute_upstream(&http_client, &ctx.credentials, false, request)
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::warn!(
                        "MCP 请求发送失败（尝试 {}/{}）: {}",
                        attempt + 1,
                        max_retries,
                        e
                    );
                    last_error = Some(e);
                    if attempt + 1 < max_retries {
                        sleep(Self::retry_delay(attempt)).await;
                    }
                    continue;
                }
            };

            let status = response.status();
            let rate_limit_error = (status.as_u16() == 429)
                .then(|| UpstreamRateLimitError::from_headers(response.headers()));

            // 成功响应
            if status.is_success() {
                self.token_manager.report_success(ctx.id);
                return Ok(response);
            }

            // 失败响应
            let body = response.text().await.unwrap_or_default();

            // 402 额度用尽
            if status.as_u16() == 402 && endpoint.is_monthly_request_limit(&body) {
                let has_available = self.token_manager.report_quota_exhausted(ctx.id);
                if !has_available {
                    anyhow::bail!("MCP 请求失败（所有凭据已用尽）: {} {}", status, body);
                }
                last_error = Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body));
                continue;
            }

            // 400 Bad Request
            if status.as_u16() == 400 {
                anyhow::bail!("MCP 请求失败: {} {}", status, body);
            }

            // 401/403 凭据问题
            if matches!(status.as_u16(), 401 | 403) {
                // token 被上游失效：先尝试 force-refresh，每凭据仅一次机会
                if endpoint.is_bearer_token_invalid(&body) && !force_refreshed.contains(&ctx.id) {
                    force_refreshed.insert(ctx.id);
                    tracing::info!("凭据 #{} token 疑似被上游失效，尝试强制刷新", ctx.id);
                    if Self::handle_force_refresh_result(
                        self.token_manager.force_refresh_token_for(ctx.id).await,
                    )? {
                        tracing::info!("凭据 #{} token 强制刷新成功，重试请求", ctx.id);
                        continue;
                    }
                    tracing::warn!("凭据 #{} token 强制刷新失败，计入失败", ctx.id);
                }

                let has_available = self.token_manager.report_failure(ctx.id);
                if !has_available {
                    anyhow::bail!("MCP 请求失败（所有凭据已用尽）: {} {}", status, body);
                }
                last_error = Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body));
                continue;
            }

            // 瞬态错误
            if matches!(status.as_u16(), 408 | 429) || status.is_server_error() {
                tracing::warn!(
                    "MCP 请求失败（上游瞬态错误，尝试 {}/{}）: {} {}",
                    attempt + 1,
                    max_retries,
                    status,
                    body
                );
                last_error = if let Some(rate_limit) = rate_limit_error {
                    if !rate_limit.should_retry_locally() {
                        return Err(rate_limit.into());
                    }
                    Some(rate_limit.into())
                } else {
                    Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body))
                };
                if attempt + 1 < max_retries {
                    // 429 限流用更长退避；408/5xx 仍用通用快速退避
                    let delay = if status.as_u16() == 429 {
                        Self::retry_delay_throttle(attempt)
                    } else {
                        Self::retry_delay(attempt)
                    };
                    sleep(delay).await;
                }
                continue;
            }

            // 其他 4xx
            if status.is_client_error() {
                anyhow::bail!("MCP 请求失败: {} {}", status, body);
            }

            // 兜底
            last_error = Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body));
            if attempt + 1 < max_retries {
                sleep(Self::retry_delay(attempt)).await;
            }
        }

        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!("MCP 请求失败：已达到最大重试次数（{}次）", max_retries)
        }))
    }

    /// 内部方法：带重试逻辑的 API 调用
    ///
    /// 重试策略：
    /// - 每个凭据最多重试 MAX_RETRIES_PER_CREDENTIAL 次
    /// - 总重试次数 = min(凭据数量 × 每凭据重试次数, MAX_TOTAL_RETRIES)
    /// - 硬上限 9 次，避免无限重试
    async fn call_api_with_retry(
        &self,
        request_body: &str,
        is_stream: bool,
        sink: Option<&dyn TraceSink>,
        group: Option<&str>,
        session_seed: Option<&str>,
    ) -> anyhow::Result<KiroCallResult> {
        // 重试预算按当前请求所属分组的账号数计算，避免小分组按全局账号数获得过多无效重试
        let total_credentials = self.token_manager.total_count_in_group(group).max(1);
        let max_retries = (total_credentials * MAX_RETRIES_PER_CREDENTIAL).min(MAX_TOTAL_RETRIES);
        let mut last_error: Option<anyhow::Error> = None;
        let mut force_refreshed: HashSet<u64> = HashSet::new();
        let api_type = if is_stream { "流式" } else { "非流式" };
        let mut trace_step = 0usize;

        // 尝试从请求体中提取模型信息
        let model = Self::extract_model_from_request(request_body);

        // A4a 大请求调度惩罚：请求体 ≥ LARGE_REQUEST_BYTES(~512KB,约对应 128K token 的
        // Anthropic JSON 体量)视为 Long 档,按命中账号在途数加排序惩罚,避免大请求扎堆同账号。
        // 普通请求 large_penalty=0,调度行为与历史完全一致。
        let large_penalty = if request_body.len() >= LARGE_REQUEST_BYTES {
            self.token_manager.config().large_request_rank_penalty
        } else {
            0
        };

        for attempt in 0..max_retries {
            let attempt_start = Instant::now();
            // 获取调用上下文（绑定 index、credentials、token）
            let mut ctx = match self
                .token_manager
                .acquire_context_sized(model.as_deref(), group, large_penalty, session_seed)
                .await
            {
                Ok(c) => {
                    // 首次成功拿到凭据：标记"等账号槽"结束（幂等，sink 内部只记第一次）。
                    if let Some(s) = sink {
                        s.on_credential_acquired();
                    }
                    c
                }
                Err(e) => {
                    Self::emit_attempt(
                        sink,
                        Self::next_trace_step(&mut trace_step),
                        0,
                        "",
                        None,
                        outcome::UNKNOWN,
                        Some(&e.to_string()),
                        attempt_start,
                    );
                    if is_rate_limit_error(&e) {
                        return Err(e);
                    }
                    if let Some(rate_limit) = take_rate_limit_error(&mut last_error) {
                        return Err(rate_limit);
                    }
                    last_error = Some(e);
                    // acquire 默认非阻塞返回"池忙"。attempt 之间加短退避(指数 200ms 起),
                    // 既防 busy-spin、又给账号释放并发槽的窗口。MAX_TOTAL_RETRIES=4 兜底,
                    // 全忙时总延迟 ~1.5s 内返回。account_acquire_blocking=true 时 acquire 自身
                    // 已阻塞等待,这里的退避不会过度叠加(下一轮 acquire 仍受 30s 上限约束)。
                    if attempt + 1 < max_retries {
                        sleep(Self::retry_delay(attempt)).await;
                    }
                    continue;
                }
            };

            // 确保 Enterprise / IdC 账号的真实 profileArn 已解析（流式端点强制要求）
            self.ensure_profile_arn(&mut ctx).await?;

            let config = self.token_manager.config();
            let machine_id = machine_id::generate_from_credentials(&ctx.credentials, config);

            let endpoints = match self.endpoint_order_for(&ctx.credentials) {
                Ok(e) => e,
                Err(e) => {
                    Self::emit_attempt(
                        sink,
                        Self::next_trace_step(&mut trace_step),
                        ctx.id,
                        "",
                        None,
                        outcome::UNKNOWN,
                        Some(&e.to_string()),
                        attempt_start,
                    );
                    last_error = Some(e);
                    self.token_manager.report_failure(ctx.id);
                    continue;
                }
            };

            let stream_reuse = config.stream_conn_reuse_enabled;
            let http_client = match if is_stream && !stream_reuse {
                self.streaming_client_for(&ctx.credentials)
            } else {
                self.client_for(&ctx.credentials)
            } {
                Ok(c) => c,
                Err(e) => {
                    Self::emit_attempt(
                        sink,
                        Self::next_trace_step(&mut trace_step),
                        ctx.id,
                        "",
                        None,
                        outcome::NETWORK_ERROR,
                        Some(&e.to_string()),
                        attempt_start,
                    );
                    last_error = Some(e);
                    if attempt + 1 < max_retries {
                        sleep(Self::retry_delay(attempt)).await;
                    }
                    continue;
                }
            };

            let endpoint_count = endpoints.len();
            // `rate_limit_error` 承接上游 429 的 Retry-After（rate-limit 全链路透传）：
            // 由端点循环内命中失败的那次响应填充，并随 `selected_failure` 一起带出循环。
            let mut selected_failure: Option<(
                Arc<dyn KiroEndpoint>,
                reqwest::StatusCode,
                String,
                Instant,
                Option<UpstreamRateLimitError>,
            )> = None;

            for (endpoint_index, endpoint) in endpoints.into_iter().enumerate() {
                let endpoint_name = endpoint.name();
                let endpoint_attempt_start = if endpoint_index == 0 {
                    attempt_start
                } else {
                    Instant::now()
                };
                let rctx = RequestContext {
                    credentials: &ctx.credentials,
                    token: &ctx.token,
                    machine_id: &machine_id,
                    config,
                };
                let url = endpoint.api_url(&rctx);
                let body = endpoint.transform_api_body(request_body, &rctx);
                tracing::debug!("使用端点 [{}] POST {}", endpoint.name(), url);
                if tracing::enabled!(tracing::Level::DEBUG) {
                    tracing::debug!("实际发送请求体: {}", truncate_for_log(&body));
                }
                let base = http_client
                    .post(&url)
                    .body(body)
                    .header("content-type", endpoint.content_type());
                let request = endpoint.decorate_api(base, &rctx);
                let request = request
                    .build()
                    .map_err(|e| anyhow::anyhow!("构建请求失败: {}", e))?;
                if tracing::enabled!(tracing::Level::DEBUG) {
                    for (k, v) in request.headers() {
                        let val = v.to_str().unwrap_or("<binary>");
                        tracing::debug!(
                            "  header {}: {}",
                            k,
                            crate::security::redact_header_value(k.as_str(), val)
                        );
                    }
                }
                let response = match self
                    .execute_upstream(&http_client, &ctx.credentials, is_stream && !stream_reuse, request)
                    .await
                {
                    Ok(resp) => resp,
                    Err(e) => {
                        tracing::warn!(
                            "API 请求发送失败（端点 {}, 尝试 {}/{}）: {}",
                            endpoint_name,
                            attempt + 1,
                            max_retries,
                            e
                        );
                        Self::emit_attempt(
                            sink,
                            Self::next_trace_step(&mut trace_step),
                            ctx.id,
                            endpoint_name,
                            None,
                            outcome::NETWORK_ERROR,
                            Some(&e.to_string()),
                            endpoint_attempt_start,
                        );
                        if Self::is_fallbackable_network_error()
                            && endpoint_index + 1 < endpoint_count
                        {
                            tracing::warn!(
                                "Endpoint {} network error; trying next fallback on #{}",
                                endpoint_name,
                                ctx.id
                            );
                            continue;
                        }
                        last_error = Some(e.into());
                        if attempt + 1 < max_retries {
                            sleep(Self::retry_delay(attempt)).await;
                        }
                        break;
                    }
                };
                let status = response.status();
                if status.is_success() {
                    Self::emit_attempt(
                        sink,
                        Self::next_trace_step(&mut trace_step),
                        ctx.id,
                        endpoint_name,
                        Some(status.as_u16()),
                        outcome::SUCCESS,
                        None,
                        endpoint_attempt_start,
                    );
                    self.token_manager.report_success(ctx.id);
                    return Ok(KiroCallResult {
                        response,
                        credential_id: ctx.id,
                        account_guard: ctx,
                    });
                }
                // 上游 429 的 Retry-After 需在消费响应体之前从响应头取出（rate-limit 透传）。
                let rate_limit_error = (status.as_u16() == 429)
                    .then(|| UpstreamRateLimitError::from_headers(response.headers()));
                let body = response.text().await.unwrap_or_default();
                if Self::is_fallbackable_status(status) && endpoint_index + 1 < endpoint_count {
                    tracing::warn!(
                        "Endpoint {} returned {}; trying next fallback on #{}",
                        endpoint_name,
                        status,
                        ctx.id
                    );
                    Self::emit_attempt(
                        sink,
                        Self::next_trace_step(&mut trace_step),
                        ctx.id,
                        endpoint_name,
                        Some(status.as_u16()),
                        outcome::TRANSIENT,
                        Some(&body),
                        endpoint_attempt_start,
                    );
                    continue;
                }
                selected_failure =
                    Some((endpoint, status, body, endpoint_attempt_start, rate_limit_error));
                break;
            }

            let Some((endpoint, status, body, endpoint_attempt_start, rate_limit_error)) =
                selected_failure
            else {
                continue;
            };
            let endpoint_name = endpoint.name();

            // 402 Payment Required 且额度用尽：禁用凭据并故障转移
            if status.as_u16() == 402 && endpoint.is_monthly_request_limit(&body) {
                tracing::warn!(
                    "API 请求失败（额度已用尽，禁用凭据并切换，尝试 {}/{}）: {} {}",
                    attempt + 1,
                    max_retries,
                    status,
                    body
                );
                Self::emit_attempt(
                    sink,
                    Self::next_trace_step(&mut trace_step),
                    ctx.id,
                    endpoint_name,
                    Some(status.as_u16()),
                    outcome::QUOTA_EXHAUSTED,
                    Some(&body),
                    endpoint_attempt_start,
                );

                let has_available = self.token_manager.report_quota_exhausted(ctx.id);
                if !has_available {
                    anyhow::bail!(
                        "{} API 请求失败（所有凭据已用尽）: {} {}",
                        api_type,
                        status,
                        body
                    );
                }

                last_error = Some(anyhow::anyhow!(
                    "{} API 请求失败: {} {}",
                    api_type,
                    status,
                    body
                ));
                continue;
            }

            // 400 Bad Request - 请求问题，重试/切换凭据无意义
            if status.as_u16() == 400 {
                // 诊断：REQUEST_BODY_INVALID 时导出转换后 conversationState 的结构形状
                // （脱敏，无 prompt 文本），用于定位 opus-4-8/sonnet-5 专属 "Improperly formed"。
                capture_body_shape_on_400(request_body, model.as_deref(), &body);
                Self::emit_attempt(
                    sink,
                    Self::next_trace_step(&mut trace_step),
                    ctx.id,
                    endpoint_name,
                    Some(400),
                    outcome::BAD_REQUEST,
                    Some(&body),
                    endpoint_attempt_start,
                );
                anyhow::bail!("{} API 请求失败: {} {}", api_type, status, body);
            }

            // 401/403 - 更可能是凭据/权限问题：计入失败并允许故障转移
            if matches!(status.as_u16(), 401 | 403) {
                // 403 账号级临时风控（"temporarily suspended / unusual user activity"）：
                // 这是会自动恢复的临时封锁，按长冷却处理、到期自动回池，
                // 不计入连续失败——否则会被自愈反复复活、持续捅一个正在被风控的账号。
                if status.as_u16() == 403
                    && self.token_manager.get_account_throttle_failover()
                    && endpoint.is_account_throttled(&body)
                {
                    let cooldown_secs = self
                        .token_manager
                        .get_account_throttle_cooldown_secs()
                        .max(1);
                    let cooldown = std::time::Duration::from_secs(cooldown_secs);
                    tracing::warn!(
                        "API 请求失败（账号级临时封锁 403，凭据 #{} 冷却 {}s 并切换，尝试 {}/{}）: {}",
                        ctx.id,
                        cooldown_secs,
                        attempt + 1,
                        max_retries,
                        body
                    );
                    let remaining = self.token_manager.report_account_throttled_for_request(
                        ctx.id,
                        cooldown,
                        model.as_deref(),
                        group,
                    );
                    Self::emit_attempt(
                        sink,
                        Self::next_trace_step(&mut trace_step),
                        ctx.id,
                        endpoint_name,
                        Some(403),
                        outcome::ACCOUNT_THROTTLED,
                        Some(&body),
                        endpoint_attempt_start,
                    );
                    last_error = Some(anyhow::anyhow!(
                        "{} API 请求失败（账号级临时封锁，凭据 #{} 已冷却 {} 分钟）: {} {}",
                        api_type,
                        ctx.id,
                        cooldown_secs / 60,
                        status,
                        body
                    ));
                    if remaining == 0 {
                        anyhow::bail!(
                            "{} API 请求失败：所有凭据都处于账号风控冷却或已禁用状态。\
                             上游对凭据 #{} 的账号触发了临时封锁（temporarily suspended / unusual \
                             user activity）。建议：(1) 增加更多不同 AWS 账号的凭据；\
                             (2) 降低请求并发，避免触发上游反滥用；\
                             (3) 在管理面板降低冷却时长或手动解除冷却以重试。原始响应: {} {}",
                            api_type,
                            ctx.id,
                            status,
                            body
                        );
                    }
                    continue;
                }

                tracing::warn!(
                    "API 请求失败（可能为凭据错误，尝试 {}/{}）: {} {}",
                    attempt + 1,
                    max_retries,
                    status,
                    body
                );
                Self::emit_attempt(
                    sink,
                    Self::next_trace_step(&mut trace_step),
                    ctx.id,
                    endpoint_name,
                    Some(status.as_u16()),
                    outcome::AUTH_FAILED,
                    Some(&body),
                    endpoint_attempt_start,
                );

                // token 被上游失效：先尝试 force-refresh，每凭据仅一次机会
                if endpoint.is_bearer_token_invalid(&body) && !force_refreshed.contains(&ctx.id) {
                    force_refreshed.insert(ctx.id);
                    tracing::info!("凭据 #{} token 疑似被上游失效，尝试强制刷新", ctx.id);
                    if Self::handle_force_refresh_result(
                        self.token_manager.force_refresh_token_for(ctx.id).await,
                    )? {
                        tracing::info!("凭据 #{} token 强制刷新成功，重试请求", ctx.id);
                        continue;
                    }
                    tracing::warn!("凭据 #{} token 强制刷新失败，计入失败", ctx.id);
                }

                let has_available = self.token_manager.report_failure(ctx.id);
                if !has_available {
                    anyhow::bail!(
                        "{} API 请求失败（所有凭据已用尽）: {} {}",
                        api_type,
                        status,
                        body
                    );
                }

                last_error = Some(anyhow::anyhow!(
                    "{} API 请求失败: {} {}",
                    api_type,
                    status,
                    body
                ));
                continue;
            }

            // 429 + suspicious activity = 账号级临时风控
            // 仅当前凭据被针对，故障转移到其它凭据可立即恢复（受配置开关控制）。
            if status.as_u16() == 429
                && self.token_manager.get_account_throttle_failover()
                && endpoint.is_account_throttled(&body)
            {
                let cooldown_secs = self
                    .token_manager
                    .get_account_throttle_cooldown_secs()
                    .max(1);
                let cooldown = std::time::Duration::from_secs(cooldown_secs);
                tracing::warn!(
                    "API 请求失败（账号级风控，凭据 #{} 冷却 {}s 并切换，尝试 {}/{}）: {}",
                    ctx.id,
                    cooldown_secs,
                    attempt + 1,
                    max_retries,
                    body
                );

                let remaining = self
                    .token_manager
                    .report_account_throttled_for_request(
                        ctx.id,
                        cooldown,
                        model.as_deref(),
                        group,
                    );
                Self::emit_attempt(
                    sink,
                    Self::next_trace_step(&mut trace_step),
                    ctx.id,
                    endpoint_name,
                    Some(429),
                    outcome::ACCOUNT_THROTTLED,
                    Some(&body),
                    endpoint_attempt_start,
                );
                // 账号级风控通常不返回 Retry-After；此时使用本地实际冷却时间，
                // 让下游网关在同一时段内也停止调度该虚拟账号。
                let (rate_limit_error, must_wait_for_upstream) =
                    account_rate_limit_with_fallback(rate_limit_error, cooldown_secs);

                // 上游给出明确等待时间时必须立即交给客户端遵守，不能在同一请求中
                // 提前换号重试。无有效 Retry-After 时仍允许按既有策略故障转移。
                if must_wait_for_upstream {
                    return Err(rate_limit_error.into());
                }

                if remaining == 0 {
                    return Err(rate_limit_error.into());
                }
                last_error = Some(rate_limit_error.into());
                continue;
            }

            // 客户端请求格式错误（messages 数组违反协议）：根因在调用方，重试无意义
            // 上游常以 5xx 返回，必须在下方"瞬态错误重试"分支之前拦截，否则会被当作
            // 上游故障重试 max_retries 次，把一个坏请求放大成多次 503（503 风暴）。
            // 直接终止：不重试、不切换凭据、不计入凭据失败。
            if endpoint.is_client_validation_error(&body) {
                tracing::warn!(
                    "API 请求失败（客户端请求格式错误，不重试）: {} {}",
                    status,
                    body
                );
                Self::emit_attempt(
                    sink,
                    Self::next_trace_step(&mut trace_step),
                    ctx.id,
                    endpoint_name,
                    Some(status.as_u16()),
                    outcome::BAD_REQUEST,
                    Some(&body),
                    endpoint_attempt_start,
                );
                anyhow::bail!("{} API 请求失败: {} {}", api_type, status, body);
            }

            // 524 / gateway timeout：上游边缘层超时，继续在本次请求内重试通常只会
            // 放大客户端等待时间和 Claude 端 Retrying 轮数；快速返回，让客户端下一次调用
            // 重新建连。
            if status.as_u16() == 524 || endpoint.is_gateway_timeout(&body) {
                tracing::warn!("API 请求失败（上游网关超时，不重试）: {} {}", status, body);
                Self::emit_attempt(
                    sink,
                    Self::next_trace_step(&mut trace_step),
                    ctx.id,
                    endpoint_name,
                    Some(status.as_u16()),
                    outcome::TRANSIENT,
                    Some(&body),
                    endpoint_attempt_start,
                );
                anyhow::bail!("{} API 请求失败: {} {}", api_type, status, body);
            }

            // 429 + USER_REQUEST_RATE_EXCEEDED = 单账号请求速率超限。
            // 必须在下方通用瞬态分支之前拦截：通用分支会 sleep 后重试，而重试时
            // acquire_context 按在途数排序、并不排除刚超限的账号，常把同一账号重复选中
            // （实测每账号 >6 req/min 时 429 显著上升、平均重试 4 次、白占槽 ~26s）。
            // 这里对该账号施加短冷却（不计失败统计），让本次重试 acquire_context 自然
            // 跳过它、切换到有余量的账号；速率窗口在冷却后恢复。
            if status.as_u16() == 429 && endpoint.is_user_rate_limited(&body) {
                let cooldown_secs = self.token_manager.config().rate_limit_cooldown_secs.max(1);
                let cooldown = std::time::Duration::from_secs(cooldown_secs);
                let remaining = self.token_manager.report_rate_limited(ctx.id, cooldown);
                tracing::warn!(
                    "API 请求失败（账号 #{} 请求速率超限，短冷却 {}s 并切换，尝试 {}/{}，剩余可用 {}）: {}",
                    ctx.id,
                    cooldown_secs,
                    attempt + 1,
                    max_retries,
                    remaining,
                    body
                );
                Self::emit_attempt(
                    sink,
                    Self::next_trace_step(&mut trace_step),
                    ctx.id,
                    endpoint_name,
                    Some(429),
                    outcome::RATE_LIMITED,
                    Some(&body),
                    endpoint_attempt_start,
                );
                last_error = Some(anyhow::anyhow!(
                    "{} API 请求失败（账号 #{} 请求速率超限）: {} {}",
                    api_type,
                    ctx.id,
                    status,
                    body
                ));
                // 所有账号都在冷却时，短暂等待让最早的冷却到期，而非立即失败
                // （速率冷却很短，等待远比放弃划算）。
                if remaining == 0 && attempt + 1 < max_retries {
                    sleep(cooldown).await;
                }
                continue;
            }

            // 429/408/5xx - 瞬态上游错误：重试但不禁用或切换凭据
            // （避免 429 high traffic / 502 high load 等瞬态错误把所有凭据锁死）
            if matches!(status.as_u16(), 408 | 429) || status.is_server_error() {
                tracing::warn!(
                    "API 请求失败（上游瞬态错误，尝试 {}/{}）: {} {}",
                    attempt + 1,
                    max_retries,
                    status,
                    body
                );
                Self::emit_attempt(
                    sink,
                    Self::next_trace_step(&mut trace_step),
                    ctx.id,
                    endpoint_name,
                    Some(status.as_u16()),
                    outcome::TRANSIENT,
                    Some(&body),
                    endpoint_attempt_start,
                );
                last_error = if let Some(rate_limit) = rate_limit_error {
                    if !rate_limit.should_retry_locally() {
                        return Err(rate_limit.into());
                    }
                    Some(rate_limit.into())
                } else {
                    Some(anyhow::anyhow!(
                        "{} API 请求失败: {} {}",
                        api_type,
                        status,
                        body
                    ))
                };
                if attempt + 1 < max_retries {
                    // 429 限流用更长退避给账号配额恢复时间；408/5xx 仍用通用快速退避
                    let mut delay = if status.as_u16() == 429 {
                        Self::retry_delay_throttle(attempt)
                    } else {
                        Self::retry_delay(attempt)
                    };
                    // 事故熔断（A4）：全池错误率飙高时判定上游全局事故，瞬态错误 3× 退避，
                    // 避免重试风暴把错误率进一步放大、把稀缺并发槽耗在注定失败的重试上。
                    let threshold = config.circuit_breaker_threshold;
                    if threshold < 1.0 && self.token_manager.pool_ewma_error() > threshold {
                        delay *= 3;
                        tracing::warn!(
                            "全池错误率超阈值（>{:.2}），判定上游事故，瞬态错误退避 3×（{}ms）",
                            threshold,
                            delay.as_millis()
                        );
                    }
                    sleep(delay).await;
                }
                continue;
            }

            // 其他 4xx - 通常为请求/配置问题：直接返回，不计入凭据失败
            if status.is_client_error() {
                Self::emit_attempt(
                    sink,
                    Self::next_trace_step(&mut trace_step),
                    ctx.id,
                    endpoint_name,
                    Some(status.as_u16()),
                    outcome::BAD_REQUEST,
                    Some(&body),
                    endpoint_attempt_start,
                );
                anyhow::bail!("{} API 请求失败: {} {}", api_type, status, body);
            }

            // 兜底：当作可重试的瞬态错误处理（不切换凭据）
            tracing::warn!(
                "API 请求失败（未知错误，尝试 {}/{}）: {} {}",
                attempt + 1,
                max_retries,
                status,
                body
            );
            Self::emit_attempt(
                sink,
                Self::next_trace_step(&mut trace_step),
                ctx.id,
                endpoint_name,
                Some(status.as_u16()),
                outcome::UNKNOWN,
                Some(&body),
                endpoint_attempt_start,
            );
            last_error = Some(anyhow::anyhow!(
                "{} API 请求失败: {} {}",
                api_type,
                status,
                body
            ));
            if attempt + 1 < max_retries {
                sleep(Self::retry_delay(attempt)).await;
            }
        }

        // 所有重试都失败
        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!(
                "{} API 请求失败：已达到最大重试次数（{}次）",
                api_type,
                max_retries
            )
        }))
    }

    /// 向 trace sink 上报一跳结果（sink 为 None 时无开销）
    #[allow(clippy::too_many_arguments)]
    fn emit_attempt(
        sink: Option<&dyn TraceSink>,
        attempt: usize,
        credential_id: u64,
        endpoint: &str,
        http_status: Option<u16>,
        outcome: &str,
        error_body: Option<&str>,
        started: Instant,
    ) {
        let Some(sink) = sink else { return };
        sink.on_attempt(TraceAttempt {
            attempt: attempt as u32,
            credential_id,
            endpoint: endpoint.to_string(),
            http_status,
            outcome: outcome.to_string(),
            error_snippet: error_body.and_then(truncate_snippet),
            duration_ms: started.elapsed().as_millis() as u64,
        });
    }

    /// 从请求体中提取模型信息
    ///
    /// 尝试解析 JSON 请求体，提取 conversationState.currentMessage.userInputMessage.modelId
    fn extract_model_from_request(request_body: &str) -> Option<String> {
        use serde_json::Value;

        let json: Value = serde_json::from_str(request_body).ok()?;

        json.get("conversationState")?
            .get("currentMessage")?
            .get("userInputMessage")?
            .get("modelId")?
            .as_str()
            .map(|s| s.to_string())
    }

    fn retry_delay(attempt: usize) -> Duration {
        // 指数退避 + 少量抖动，避免上游抖动时放大故障
        const BASE_MS: u64 = 200;
        const MAX_MS: u64 = 2_000;
        let exp = BASE_MS.saturating_mul(2u64.saturating_pow(attempt.min(6) as u32));
        let backoff = exp.min(MAX_MS);
        let jitter_max = (backoff / 4).max(1);
        let jitter = fastrand::u64(0..=jitter_max);
        Duration::from_millis(backoff.saturating_add(jitter))
    }

    /// 429 限流专用退避：比通用退避更长。
    ///
    /// 上游 429（SERVICE_REQUEST_RATE_EXCEEDED）是账号级速率配额耗尽，需要更长
    /// 时间恢复；用通用的 ≤2s 快速退避只会让请求在配额恢复前反复撞墙、持续触顶。
    /// 这里 base 1s、封顶 8s，给账号配额留出恢复窗口。
    fn retry_delay_throttle(attempt: usize) -> Duration {
        const BASE_MS: u64 = 1_000;
        const MAX_MS: u64 = 8_000;
        let exp = BASE_MS.saturating_mul(2u64.saturating_pow(attempt.min(6) as u32));
        let backoff = exp.min(MAX_MS);
        let jitter_max = (backoff / 4).max(1);
        let jitter = fastrand::u64(0..=jitter_max);
        Duration::from_millis(backoff.saturating_add(jitter))
    }

    /// 返回是否刷新成功；类型化刷新 429 原样传播，其他刷新失败交回调用方按认证失败处理。
    fn handle_force_refresh_result(result: anyhow::Result<()>) -> anyhow::Result<bool> {
        match result {
            Ok(()) => Ok(true),
            Err(error) if is_rate_limit_error(&error) => Err(error),
            Err(_) => Ok(false),
        }
    }
}

fn is_rate_limit_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<UpstreamRateLimitError>().is_some()
}

fn take_rate_limit_error(last_error: &mut Option<anyhow::Error>) -> Option<anyhow::Error> {
    if last_error
        .as_ref()
        .is_some_and(|error| error.downcast_ref::<UpstreamRateLimitError>().is_some())
    {
        last_error.take()
    } else {
        None
    }
}

/// 为账号风控 429 补齐本地冷却时间，并区分上游是否明确要求等待。
fn account_rate_limit_with_fallback(
    rate_limit: Option<UpstreamRateLimitError>,
    cooldown_secs: u64,
) -> (UpstreamRateLimitError, bool) {
    let must_wait_for_upstream = rate_limit
        .as_ref()
        .is_some_and(|error| !error.should_retry_locally());
    let error = match rate_limit {
        Some(error) if error.retry_after().is_some() => error,
        _ => UpstreamRateLimitError::new(Some(cooldown_secs.to_string())),
    };
    (error, must_wait_for_upstream)
}

#[cfg(test)]
mod rate_limit_tests {
    use super::*;

    #[test]
    fn preserves_typed_rate_limit_when_later_credential_selection_fails() {
        let mut last_error = Some(anyhow::Error::new(UpstreamRateLimitError::new(Some(
            "45".to_string(),
        ))));

        // A concurrent request may cool the final credential before the next acquisition.
        // The earlier upstream 429 must win over the later generic selection failure.
        let returned = take_rate_limit_error(&mut last_error)
            .unwrap_or_else(|| anyhow::anyhow!("所有凭据均已禁用"));

        let rate_limit = returned
            .downcast_ref::<UpstreamRateLimitError>()
            .expect("应保留最初的类型化 429");
        assert_eq!(rate_limit.retry_after(), Some("45"));
        assert!(last_error.is_none());
    }

    #[test]
    fn does_not_relabel_generic_error_as_rate_limit() {
        let mut last_error = Some(anyhow::anyhow!("所有凭据均已禁用"));
        assert!(take_rate_limit_error(&mut last_error).is_none());
        assert!(last_error.is_some());
    }

    #[test]
    fn account_rate_limit_uses_cooldown_when_retry_after_is_missing() {
        let (error, must_wait) = account_rate_limit_with_fallback(
            Some(UpstreamRateLimitError::new(None)),
            300,
        );

        assert_eq!(error.retry_after(), Some("300"));
        assert!(!must_wait, "无上游等待值时仍可按账号冷却策略故障转移");
    }

    #[test]
    fn account_rate_limit_honors_explicit_upstream_retry_after() {
        let (error, must_wait) = account_rate_limit_with_fallback(
            Some(UpstreamRateLimitError::new(Some("90".to_string()))),
            300,
        );

        assert_eq!(error.retry_after(), Some("90"));
        assert!(must_wait, "上游明确要求等待时不得在内部提前重试");
    }

    #[test]
    fn force_refresh_rate_limit_is_propagated_instead_of_counted_as_auth_failure() {
        let error = anyhow::Error::new(UpstreamRateLimitError::new(Some("60".to_string())));
        let returned = KiroProvider::handle_force_refresh_result(Err(error))
            .expect_err("强制刷新 429 应立即传播");

        let rate_limit = returned
            .downcast_ref::<UpstreamRateLimitError>()
            .expect("应保留类型化 429");
        assert_eq!(rate_limit.retry_after(), Some("60"));
    }

    #[test]
    fn generic_force_refresh_failure_remains_an_auth_failure() {
        let outcome = KiroProvider::handle_force_refresh_result(Err(anyhow::anyhow!(
            "invalid refresh token",
        )))
        .unwrap();
        assert!(!outcome);
    }

    #[test]
    fn current_acquire_rate_limit_is_detected_before_outer_retry() {
        let error = anyhow::Error::new(UpstreamRateLimitError::new(Some("30".to_string())));
        assert!(is_rate_limit_error(&error));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ShardSet::pick 严格 round-robin 轮询 N 个 client（游标取模），保证并发被均匀摊到各分片。
    #[test]
    fn shard_set_round_robins() {
        let mk = || build_client(None, 30, TlsBackend::Rustls).unwrap();
        let shard = ShardSet::new(vec![mk(), mk(), mk()]);
        // 连续 pick 的游标序列应为 0,1,2,0,1,2...（用 cursor 前后差验证严格递增取模）。
        let start = shard.cursor.load(Ordering::Relaxed);
        for _ in 0..7 {
            let _ = shard.pick();
        }
        let end = shard.cursor.load(Ordering::Relaxed);
        assert_eq!(end - start, 7, "每次 pick 游标 +1（round-robin 依据）");
        // 单分片集也能正常工作（关闭分片 = 退化为单 client）。
        let one = ShardSet::new(vec![mk()]);
        let _ = one.pick();
        let _ = one.pick();
    }

    #[test]
    fn test_body_shape_tool_id_hash_correlates_across_turns() {
        // 抓包形状必须能跨轮次对齐同一 tool_use_id（用短哈希），否则无法诊断
        // TOOL_USE_RESULT_MISMATCH（哪个 tool_result 的 id 找不到对应 tool_use）。
        let body = serde_json::json!({
            "conversationState": {
                "history": [
                    {"assistantResponseMessage": {
                        "content": "",
                        "toolUses": [{"toolUseId": "call_ABC", "name": "do_it", "input": {}}]
                    }},
                    {"userInputMessage": {
                        "content": "",
                        "userInputMessageContext": {
                            "toolResults": [{"toolUseId": "call_ABC", "status": "success", "content": []}]
                        }
                    }}
                ],
                "currentMessage": {"userInputMessage": {
                    "content": "",
                    "userInputMessageContext": {
                        "toolResults": [{"toolUseId": "call_ORPHAN", "status": "success", "content": []}]
                    }
                }}
            }
        })
        .to_string();

        let shape = build_body_shape(&body, Some("claude-opus-4-8"), "TOOL_USE_RESULT_MISMATCH");
        let hist = shape["history"].as_array().unwrap();
        let use_hash = hist[0]["tool_uses"][0]["toolUseId_hash"].as_str().unwrap();
        let res_hash = hist[1]["tool_results"][0]["toolUseId_hash"].as_str().unwrap();
        // 同一 id（call_ABC）→ 历史里 tool_use 与 tool_result 哈希一致（可判定为已配对）。
        assert_eq!(use_hash, res_hash, "同一 tool_use_id 的哈希应一致，才能判定配对");

        // 当前消息的孤儿 tool_result（call_ORPHAN）哈希与任何历史 tool_use 都不同。
        let cur_hash = shape["current_message"]["tool_results"][0]["toolUseId_hash"]
            .as_str()
            .unwrap();
        assert_ne!(cur_hash, use_hash, "孤儿 tool_result 的哈希应与历史 tool_use 不同");
        // 哈希为 6 位十六进制、且不泄露原文长度以外的信息。
        assert_eq!(cur_hash.len(), 6);
    }

    #[test]
    fn test_endpoint_routing_initial_values() {
        let r = EndpointRouting::new(Some("codewhisperer".to_string()), false);
        assert_eq!(r.preferred().as_deref(), Some("codewhisperer"));
        assert!(!r.fallback());

        let empty = EndpointRouting::new(None, true);
        assert_eq!(empty.preferred(), None);
        assert!(empty.fallback());
    }

    #[test]
    fn test_endpoint_routing_hot_update() {
        let r = EndpointRouting::new(None, true);
        // 首选端点热更新：None → Some → None（清除）
        r.set_preferred(Some("amazonq".to_string()));
        assert_eq!(r.preferred().as_deref(), Some("amazonq"));
        r.set_preferred(None);
        assert_eq!(r.preferred(), None);
        // fallback 开关热更新
        r.set_fallback(false);
        assert!(!r.fallback());
        r.set_fallback(true);
        assert!(r.fallback());
    }

    #[test]
    fn test_endpoint_routing_shared_arc_reflects_updates() {
        // 模拟 provider 与 admin 共享同一 Arc：一侧改，另一侧立即可见。
        let routing = Arc::new(EndpointRouting::new(Some("ide".to_string()), true));
        let admin_view = routing.clone();

        admin_view.set_preferred(Some("runtime".to_string()));
        admin_view.set_fallback(false);

        // provider 侧读到的即是 admin 侧写入的最新值（无需重启/重建）。
        assert_eq!(routing.preferred().as_deref(), Some("runtime"));
        assert!(!routing.fallback());
    }

    // ---- 核心重试/故障转移决策纯函数（此前零覆盖，是整个代理执行路径的关键判据）----

    #[test]
    fn fallbackable_status_excludes_account_level_codes() {
        use reqwest::StatusCode;
        // 账号级状态（401/402/403）不应触发同账号换端点——由账号级分支处理。
        assert!(!KiroProvider::is_fallbackable_status(StatusCode::UNAUTHORIZED)); // 401
        assert!(!KiroProvider::is_fallbackable_status(StatusCode::PAYMENT_REQUIRED)); // 402
        assert!(!KiroProvider::is_fallbackable_status(StatusCode::FORBIDDEN)); // 403
        // 其余非 200 一律 fallback：含 400 校验错、429 限流、5xx、524 网关超时。
        assert!(KiroProvider::is_fallbackable_status(StatusCode::BAD_REQUEST)); // 400
        assert!(KiroProvider::is_fallbackable_status(StatusCode::TOO_MANY_REQUESTS)); // 429
        assert!(KiroProvider::is_fallbackable_status(StatusCode::INTERNAL_SERVER_ERROR)); // 500
        assert!(KiroProvider::is_fallbackable_status(
            StatusCode::from_u16(524).unwrap()
        ));
        // 网络错误恒可 fallback。
        assert!(KiroProvider::is_fallbackable_network_error());
    }

    #[test]
    fn endpoint_alias_maps_kiro_to_ide_only() {
        // "kiro" 是 "ide" 的别名，其余名字原样返回。
        assert_eq!(KiroProvider::endpoint_alias("kiro"), "ide");
        assert_eq!(KiroProvider::endpoint_alias("ide"), "ide");
        assert_eq!(KiroProvider::endpoint_alias("codewhisperer"), "codewhisperer");
        assert_eq!(KiroProvider::endpoint_alias("runtime"), "runtime");
    }

    #[test]
    fn auto_endpoint_detection() {
        assert!(KiroProvider::is_auto_endpoint("auto"));
        assert!(!KiroProvider::is_auto_endpoint("ide"));
        assert!(!KiroProvider::is_auto_endpoint("kiro"));
        assert!(!KiroProvider::is_auto_endpoint(""));
    }

    #[test]
    fn retry_delay_is_bounded_and_monotone_in_expectation() {
        // 通用退避封顶 2s（含 ≤25% 抖动，故上界 2500ms）；到达封顶后不再增长。
        for attempt in 0..10usize {
            let d = KiroProvider::retry_delay(attempt).as_millis() as u64;
            assert!(d <= 2_500, "attempt={attempt} delay={d} 超过 2s+jitter 上界");
            assert!(d >= 200, "attempt={attempt} delay={d} 低于 base");
        }
        // 大 attempt 收敛到封顶区间，不溢出（saturating_pow 保护）。
        let big = KiroProvider::retry_delay(usize::MAX).as_millis() as u64;
        assert!((2_000..=2_500).contains(&big), "大 attempt 应收敛到封顶：{big}");
    }

    #[test]
    fn throttle_delay_is_longer_than_generic() {
        // 429 专用退避 base 1s、封顶 8s，明显长于通用退避，给账号配额恢复窗口。
        for attempt in 0..10usize {
            let d = KiroProvider::retry_delay_throttle(attempt).as_millis() as u64;
            assert!(d >= 1_000, "attempt={attempt} throttle delay={d} 低于 1s base");
            assert!(d <= 10_000, "attempt={attempt} throttle delay={d} 超过 8s+jitter 上界");
        }
        let big = KiroProvider::retry_delay_throttle(usize::MAX).as_millis() as u64;
        assert!((8_000..=10_000).contains(&big), "大 attempt 应收敛到 8s 封顶：{big}");
    }

    #[test]
    fn extract_model_from_request_reads_nested_model_id() {
        let body = r#"{"conversationState":{"currentMessage":{"userInputMessage":{"modelId":"claude-opus-4-8","content":"hi"}}}}"#;
        assert_eq!(
            KiroProvider::extract_model_from_request(body).as_deref(),
            Some("claude-opus-4-8")
        );
        // 缺字段/非法 JSON 一律返回 None，绝不 panic。
        assert_eq!(KiroProvider::extract_model_from_request("{}"), None);
        assert_eq!(KiroProvider::extract_model_from_request("not json"), None);
        assert_eq!(
            KiroProvider::extract_model_from_request(
                r#"{"conversationState":{"currentMessage":{}}}"#
            ),
            None
        );
    }

    #[test]
    fn next_trace_step_increments_and_saturates() {
        let mut step = 0usize;
        assert_eq!(KiroProvider::next_trace_step(&mut step), 0);
        assert_eq!(KiroProvider::next_trace_step(&mut step), 1);
        assert_eq!(KiroProvider::next_trace_step(&mut step), 2);
        assert_eq!(step, 3);
        // saturating：接近上界不 panic。
        let mut max = usize::MAX;
        assert_eq!(KiroProvider::next_trace_step(&mut max), usize::MAX);
        assert_eq!(max, usize::MAX);
    }
}
