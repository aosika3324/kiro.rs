//! HTTP Client 构建模块
//!
//! 提供统一的 HTTP Client 构建功能，支持代理配置

use reqwest::{Client, Proxy};
use std::time::Duration;

use crate::model::config::TlsBackend;

/// 读取一个以秒为单位的环境变量，缺失或非法时回退到 `default`。值为 0 也视为非法（回退默认）。
fn env_secs(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

/// 分层超时配置（秒），由 `KIRO_RS_HTTP_*` 环境变量覆盖。作为 reqwest 与 TLS 指纹
/// (wreq) 两条客户端构建路径的**单一数据源**，保证两者超时/保活/连接池语义一致。
#[derive(Debug, Clone, Copy)]
pub struct HttpTimeouts {
    pub connect: u64,
    pub read: u64,
    pub keepalive: u64,
    pub pool_idle: u64,
}

/// 解析分层超时（见各字段在 [`build_client_inner`] 的语义说明）。
pub fn resolve_http_timeouts() -> HttpTimeouts {
    HttpTimeouts {
        connect: env_secs("KIRO_RS_HTTP_CONNECT_TIMEOUT_SECS", 10),
        read: env_secs("KIRO_RS_HTTP_READ_TIMEOUT_SECS", 300),
        keepalive: env_secs("KIRO_RS_HTTP_TCP_KEEPALIVE_SECS", 60),
        pool_idle: env_secs("KIRO_RS_HTTP_POOL_IDLE_TIMEOUT_SECS", 15),
    }
}

/// 代理配置
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ProxyConfig {
    /// 代理地址，支持 http/https/socks5
    pub url: String,
    /// 代理认证用户名
    pub username: Option<String>,
    /// 代理认证密码
    pub password: Option<String>,
}

impl ProxyConfig {
    /// 从 url 创建代理配置
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            username: None,
            password: None,
        }
    }

    /// 设置认证信息
    pub fn with_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    /// URL 是否为 SOCKS 代理（`socks4://` / `socks5://`，大小写不敏感）。
    ///
    /// 用于 TLS 指纹路径的选路：wreq(BoringSSL) 客户端未启用 `socks` 特性（见 Cargo.toml），
    /// 把 `socks://` 交给它会建连失败或静默直连（泄露真实 IP）。识别到 SOCKS 时应回退到
    /// 支持 socks 的 reqwest 主路径。reqwest 侧启用了 `socks` 特性，代理正常生效。
    pub fn is_socks(&self) -> bool {
        let u = self.url.trim_start().to_ascii_lowercase();
        u.starts_with("socks4://") || u.starts_with("socks5://")
    }
}

/// 构建 HTTP Client
///
/// # Arguments
/// * `proxy` - 可选的代理配置
/// * `timeout_secs` - 超时时间（秒）
///
/// # Returns
/// 配置好的 reqwest::Client
pub fn build_client(
    proxy: Option<&ProxyConfig>,
    timeout_secs: u64,
    tls_backend: TlsBackend,
) -> anyhow::Result<Client> {
    build_client_inner(proxy, timeout_secs, tls_backend, 8)
}

/// 构建流式上游专用 HTTP Client（**启用小连接池 + H2 keepalive 探活**）。
///
/// 历史上此路径曾用 `pool_max_idle_per_host = 0`（禁用复用），理由是"AWS ALB 在长
/// prefill 静默期掐断空闲/复用连接导致断流"。**2026-07-21 对上游 `codewhisperer.
/// us-east-1.amazonaws.com` 实测推翻了这个归因**：H2 空闲连接晾到 130s（远超所谓
/// 60s ALB timeout）AWS 未掐断（无 GOAWAY/FIN/RST，30s 还回 SETTINGS 保活）。而池=0
/// 的实测代价是**每条流多付 ~128ms TLS 握手**（冷连首字 163ms vs 复用 35ms），直接
/// 拖首字。故改为小池（默认 4）复用连接省握手；配合 [`build_client_inner`] 里
/// pool>0 时启用的 H2 keepalive PING，在**复用前**淘汰真死连接。万一仍取到中途被掐
/// 的连接，由上层重试循环 + 断流 `error` 信号兜底（活跃流中途断连本就与池设置无关，
/// 单个长请求独占一条连，池大小不影响它）。可经 `KIRO_RS_HTTP_STREAM_POOL_MAX_IDLE`
/// 覆盖；设 0 可回退到旧的禁用复用行为。
pub fn build_streaming_client(
    proxy: Option<&ProxyConfig>,
    timeout_secs: u64,
    tls_backend: TlsBackend,
) -> anyhow::Result<Client> {
    let stream_pool = std::env::var("KIRO_RS_HTTP_STREAM_POOL_MAX_IDLE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(4);
    build_client_inner(proxy, timeout_secs, tls_backend, stream_pool)
}

/// 构建 HTTP Client（内部实现）
///
/// # Arguments
/// * `proxy` - 可选的代理配置
/// * `timeout_secs` - 总超时时间（秒）
/// * `pool_max_idle_per_host` - 每 host 最大空闲连接数；0 = 禁用空闲连接复用
fn build_client_inner(
    proxy: Option<&ProxyConfig>,
    timeout_secs: u64,
    tls_backend: TlsBackend,
    pool_max_idle_per_host: usize,
) -> anyhow::Result<Client> {
    // 分层超时（可经 KIRO_RS_HTTP_* 覆盖）：
    // - connect_timeout：仅 TCP+TLS 建连阶段。坏/挂死连接秒级失败重试，不再拖到总超时。
    // - read_timeout：每次读操作超时，**成功读一次即重置**。用于探测"建连后迟迟不吐字节"
    //   的挂死连接；首字节一到即重置，因此大上下文的长 prefill 与长生成都不会被误杀。
    // 这是高并发下的关键：避免少数挂死请求长时间霸占稀缺的账号并发槽，拖垮整个池子的首 token。
    // 连接池空闲超时**必须短于上游服务端的空闲关闭时间**(AWS ALB 默认 ~60s),
    // 否则池里会留存已被服务端 RST/FIN 的"半死"连接,下一个请求取到它直接
    // "socket closed unexpectedly"。取 15s 远低于 60s,使陈旧连接在被复用前先被淘汰;
    // 取连接瞬间仍可能撞上服务端刚关闭的竞态,由上层重试循环兜底(execute 失败即重试)。
    let HttpTimeouts {
        connect: connect_timeout,
        read: read_timeout,
        keepalive,
        pool_idle,
    } = resolve_http_timeouts();

    let mut builder = Client::builder()
        // 总超时仍保留为大兜底（含完整流式响应）；read_timeout 才是挂死探测主力。
        .timeout(Duration::from_secs(timeout_secs))
        .connect_timeout(Duration::from_secs(connect_timeout))
        .read_timeout(Duration::from_secs(read_timeout))
        .tcp_keepalive(Duration::from_secs(keepalive))
        // 复用空闲连接省掉重复 TCP+TLS 握手；但空闲超时短于上游关闭时间,避免取到死连接。
        // pool_max_idle_per_host=0 时 reqwest 禁用空闲连接复用(流式专用,见 build_streaming_client)。
        .pool_idle_timeout(Duration::from_secs(pool_idle))
        .pool_max_idle_per_host(pool_max_idle_per_host);

    // H2 keepalive：仅对**复用连接**的非流式路径（pool_max_idle_per_host > 0）启用。
    // 上游走 ALPN 协商的 HTTP/2；主动 PING 探测让被 AWS ALB 静默掐断的僵尸连接在**复用前**
    // 被发现并淘汰，避免辅助请求（MCP/刷新/balance/profile）取到死连接后干等到超时——这类
    // 尾部延迟毛刺会拖慢首 token 前的准备阶段。流式路径 pool=0（无空闲复用，见
    // build_streaming_client 的断流防线），本就每条新连接，加 keepalive 无意义，故跳过。
    if pool_max_idle_per_host > 0 {
        builder = builder
            .http2_keep_alive_interval(Duration::from_secs(30))
            .http2_keep_alive_timeout(Duration::from_secs(20))
            .http2_keep_alive_while_idle(true)
            .http2_adaptive_window(true);
    }

    match tls_backend {
        TlsBackend::Rustls => {
            builder = builder.use_rustls_tls();
        }
        TlsBackend::NativeTls => {
            #[cfg(feature = "native-tls")]
            {
                builder = builder.use_native_tls();
            }
            #[cfg(not(feature = "native-tls"))]
            {
                anyhow::bail!("此构建版本未包含 native-tls 后端，请在配置中改用 rustls");
            }
        }
    }

    if let Some(proxy_config) = proxy {
        let mut proxy = Proxy::all(&proxy_config.url)?;

        // 设置代理认证
        if let (Some(username), Some(password)) = (&proxy_config.username, &proxy_config.password) {
            proxy = proxy.basic_auth(username, password);
        }

        builder = builder.proxy(proxy);
        tracing::debug!(
            "HTTP Client 使用代理: {}",
            crate::security::redact_proxy_url(&proxy_config.url)
        );
    }

    Ok(builder.build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proxy_config_new() {
        let config = ProxyConfig::new("http://127.0.0.1:7890");
        assert_eq!(config.url, "http://127.0.0.1:7890");
        assert!(config.username.is_none());
        assert!(config.password.is_none());
    }

    #[test]
    fn test_proxy_config_with_auth() {
        let config = ProxyConfig::new("socks5://127.0.0.1:1080").with_auth("user", "pass");
        assert_eq!(config.url, "socks5://127.0.0.1:1080");
        assert_eq!(config.username, Some("user".to_string()));
        assert_eq!(config.password, Some("pass".to_string()));
    }

    #[test]
    fn test_proxy_config_is_socks() {
        // socks4/socks5 视为 SOCKS，大小写不敏感、容忍前导空白。
        assert!(ProxyConfig::new("socks5://127.0.0.1:1080").is_socks());
        assert!(ProxyConfig::new("socks4://127.0.0.1:1080").is_socks());
        assert!(ProxyConfig::new("SOCKS5://127.0.0.1:1080").is_socks());
        assert!(ProxyConfig::new("  socks5://h:1080").is_socks());
        // http/https 不是 SOCKS。
        assert!(!ProxyConfig::new("http://127.0.0.1:7890").is_socks());
        assert!(!ProxyConfig::new("https://127.0.0.1:7890").is_socks());
        // 子串误匹配防护：host 里含 "socks" 不算。
        assert!(!ProxyConfig::new("http://socks.example.com:8080").is_socks());
    }

    #[test]
    fn test_build_client_without_proxy() {
        let client = build_client(None, 30, TlsBackend::Rustls);
        assert!(client.is_ok());
    }

    #[test]
    fn test_build_client_with_proxy() {
        let config = ProxyConfig::new("http://127.0.0.1:7890");
        let client = build_client(Some(&config), 30, TlsBackend::Rustls);
        assert!(client.is_ok());
    }

    #[test]
    fn test_build_streaming_client_builds() {
        // 流式专用 Client(禁用空闲连接复用)应能正常构建,带/不带代理都行。
        assert!(build_streaming_client(None, 720, TlsBackend::Rustls).is_ok());
        let config = ProxyConfig::new("http://127.0.0.1:7890");
        assert!(build_streaming_client(Some(&config), 720, TlsBackend::Rustls).is_ok());
    }
}
