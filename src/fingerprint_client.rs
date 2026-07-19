//! TLS 指纹伪装客户端 + 统一上游响应封装。
//!
//! 默认关闭。开启后（`config.tls_fingerprint_enabled`），上游请求改由 [`wreq`]（基于
//! BoringSSL）发出，模拟浏览器的 JA3/JA4 + HTTP2 指纹，用于绕过对 TLS 指纹校验严格的
//! 目标（如 Grok/Cloudflare 返回的 403）。AWS(codewhisperer/q.*.amazonaws.com) **不**做
//! 指纹封锁，因此这是一个 opt-in 能力，关闭时 [`crate::http_client`] 的 reqwest 路径原样不变。
//!
//! 设计：不改动任何 `KiroEndpoint`（仍按 reqwest 构建完整装饰后的请求），只在唯一的
//! execute 处把已构建的 `reqwest::Request` 重放到 wreq 客户端；请求/响应两端由本模块桥接。

//! 特性开关：整套 wreq/BoringSSL 相关能力由 `tls-fingerprint` 特性门控（默认开）。
//! 关闭该特性时（例如改用 `native-tls` 的构建——它与 BoringSSL 符号冲突，两者互斥），
//! 本模块仍编译：`UpstreamResponse` 只保留 `Reqwest` 分支，`build_fingerprint_client` /
//! `send_via_wreq` 不存在，上游一律走 reqwest 路径。

use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::pin::Pin;

#[cfg(feature = "tls-fingerprint")]
use crate::http_client::{resolve_http_timeouts, ProxyConfig};

/// 统一的流错误类型（消费侧只做 `.to_string()`，故装箱即可）。
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
/// 统一的字节流类型（reqwest 与 wreq 的 `bytes_stream` 归一到这里）。
pub type BoxByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, BoxError>> + Send>>;

/// 把配置里的 profile 字符串映射到 wreq 的 [`Emulation`](wreq_util::Emulation) 预设。
#[cfg(feature = "tls-fingerprint")]
///
/// 只暴露每个浏览器家族的最新一档（够用且省得维护全表）；无法识别时回退最新 Chrome。
pub fn profile_to_emulation(profile: &str) -> wreq_util::Emulation {
    use wreq_util::Emulation;
    match profile.trim().to_ascii_lowercase().as_str() {
        "" | "chrome" | "chrome137" => Emulation::Chrome137,
        "chrome136" => Emulation::Chrome136,
        "chrome135" => Emulation::Chrome135,
        "firefox" | "firefox139" => Emulation::Firefox139,
        "firefox136" => Emulation::Firefox136,
        "safari" | "safari18" | "safari18_5" => Emulation::Safari18_5,
        "safari_ios" | "safari_ios18" => Emulation::SafariIos18_1_1,
        "edge" | "edge134" => Emulation::Edge134,
        "okhttp" | "okhttp5" => Emulation::OkHttp5,
        "opera" | "opera119" => Emulation::Opera119,
        _ => Emulation::Chrome137,
    }
}
/// 构建一个 TLS 指纹伪装的 wreq 客户端。
///
/// 超时/保活/连接池语义与 [`crate::http_client`] 共用 [`resolve_http_timeouts`]（单一数据源）。
/// `pool_max_idle_per_host = 0` 时禁用空闲连接复用（流式专用，理由同 `build_streaming_client`）。
#[cfg(feature = "tls-fingerprint")]
pub fn build_fingerprint_client(
    proxy: Option<&ProxyConfig>,
    timeout_secs: u64,
    profile: &str,
    pool_max_idle_per_host: usize,
) -> anyhow::Result<wreq::Client> {
    use std::time::Duration;
    let t = resolve_http_timeouts();
    // 注意：`.emulation()` 会覆盖 HTTP/1、HTTP/2、TLS 配置，必须在其它调优之前设置。
    let mut builder = wreq::Client::builder()
        .emulation(profile_to_emulation(profile))
        .timeout(Duration::from_secs(timeout_secs))
        .connect_timeout(Duration::from_secs(t.connect))
        .read_timeout(Duration::from_secs(t.read))
        .tcp_keepalive(Duration::from_secs(t.keepalive))
        .pool_idle_timeout(Duration::from_secs(t.pool_idle))
        .pool_max_idle_per_host(pool_max_idle_per_host);

    if let Some(pc) = proxy {
        let mut p = wreq::Proxy::all(&pc.url)?;
        if let (Some(username), Some(password)) = (&pc.username, &pc.password) {
            p = p.basic_auth(username, password);
        }
        builder = builder.proxy(p);
        tracing::debug!(
            "TLS 指纹 Client 使用代理: {}",
            crate::security::redact_proxy_url(&pc.url)
        );
    }

    Ok(builder.build()?)
}
/// 统一的上游响应封装：桥接 reqwest 与 wreq 两条客户端路径，使下游流/非流处理无需分叉。
///
/// 消费面很小：`status()` / `text()` / `bytes()` / `bytes_stream()`（无 `headers()` 调用点）。
pub enum UpstreamResponse {
    /// 普通路径（TLS 指纹关闭）。
    Reqwest(reqwest::Response),
    /// TLS 指纹路径（wreq/BoringSSL）。仅在 `tls-fingerprint` 特性下存在。
    #[cfg(feature = "tls-fingerprint")]
    Wreq(wreq::Response),
}

impl UpstreamResponse {
    /// 上游 HTTP 状态码。两条路径底层都是 `http::StatusCode`，统一暴露为 `reqwest::StatusCode`
    /// （与既有调用点 `is_success()` / `as_u16()` / `is_fallbackable_status` 完全兼容）。
    pub fn status(&self) -> reqwest::StatusCode {
        match self {
            UpstreamResponse::Reqwest(r) => r.status(),
            // 两侧同为 http 1.x 的 StatusCode，u16 往返必成功。
            #[cfg(feature = "tls-fingerprint")]
            UpstreamResponse::Wreq(r) => reqwest::StatusCode::from_u16(r.status().as_u16())
                .unwrap_or(reqwest::StatusCode::INTERNAL_SERVER_ERROR),
        }
    }

    /// 读取整个响应体为字符串（错误统一为 `anyhow`）。
    pub async fn text(self) -> anyhow::Result<String> {
        match self {
            UpstreamResponse::Reqwest(r) => Ok(r.text().await?),
            #[cfg(feature = "tls-fingerprint")]
            UpstreamResponse::Wreq(r) => Ok(r.text().await?),
        }
    }

    /// 读取整个响应体为字节（错误统一为 `anyhow`）。
    pub async fn bytes(self) -> anyhow::Result<Bytes> {
        match self {
            UpstreamResponse::Reqwest(r) => Ok(r.bytes().await?),
            #[cfg(feature = "tls-fingerprint")]
            UpstreamResponse::Wreq(r) => Ok(r.bytes().await?),
        }
    }

    /// 响应体字节流。两条路径的 chunk 错误装箱为 [`BoxError`]（下游仅 `.to_string()`）。
    pub fn bytes_stream(self) -> BoxByteStream {
        match self {
            UpstreamResponse::Reqwest(r) => {
                Box::pin(r.bytes_stream().map(|res| res.map_err(|e| Box::new(e) as BoxError)))
            }
            #[cfg(feature = "tls-fingerprint")]
            UpstreamResponse::Wreq(r) => {
                Box::pin(r.bytes_stream().map(|res| res.map_err(|e| Box::new(e) as BoxError)))
            }
        }
    }
}
/// 把一个已由 `KiroEndpoint` 完整装饰好的 `reqwest::Request` 重放到 wreq 客户端并发送。
///
/// 这样可复用现有全部端点装饰逻辑（auth / user-agent / x-amz-* / content-type / body），
/// 只把「实际建连+发送」换成带指纹的 wreq。method/url/headers 均为共享的 `http` 1.x / `url`
/// 2.x 类型；body 为内存字节（源自 String），直接透传。
#[cfg(feature = "tls-fingerprint")]
pub async fn send_via_wreq(
    client: &wreq::Client,
    request: reqwest::Request,
) -> anyhow::Result<wreq::Response> {
    let method = request.method().clone();
    let url = request.url().clone();
    // 仅内存 body（本项目所有上游请求体都由 String 构建），非流式 body 取不到字节时视为空。
    let body_bytes: Option<Bytes> = request
        .body()
        .and_then(|b| b.as_bytes())
        .map(Bytes::copy_from_slice);

    let mut rb = client.request(method, url);
    for (name, value) in request.headers() {
        rb = rb.header(name, value);
    }
    if let Some(bytes) = body_bytes {
        rb = rb.body(bytes);
    }

    Ok(rb.send().await?)
}

#[cfg(all(test, feature = "tls-fingerprint"))]
mod tests {
    use super::*;
    use crate::http_client::ProxyConfig;

    #[test]
    fn profile_maps_known_and_defaults() {
        use wreq_util::Emulation;
        assert_eq!(profile_to_emulation("chrome"), Emulation::Chrome137);
        assert_eq!(profile_to_emulation("Firefox"), Emulation::Firefox139);
        assert_eq!(profile_to_emulation("  SAFARI  "), Emulation::Safari18_5);
        assert_eq!(profile_to_emulation(""), Emulation::Chrome137);
        assert_eq!(profile_to_emulation("nonsense"), Emulation::Chrome137);
    }

    #[test]
    fn builds_client_with_and_without_proxy() {
        assert!(build_fingerprint_client(None, 720, "chrome", 0).is_ok());
        let pc = ProxyConfig::new("http://127.0.0.1:7890");
        assert!(build_fingerprint_client(Some(&pc), 720, "firefox", 8).is_ok());
    }
}
