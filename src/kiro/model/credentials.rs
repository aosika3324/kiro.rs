//! Kiro OAuth 凭证数据模型
//!
//! 支持从 Kiro IDE 的凭证文件加载，使用 Social 认证方式
//! 支持单凭据和多凭据配置格式

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

use crate::http_client::ProxyConfig;
use crate::model::config::Config;

pub const BUILDER_ID_PROFILE_ARN: &str =
    "arn:aws:codewhisperer:us-east-1:638616132270:profile/AAAACCCCXXXX";
pub const SOCIAL_PROFILE_ARN: &str =
    "arn:aws:codewhisperer:us-east-1:699475941385:profile/EHGA3GRVQMUK";

/// Kiro OAuth 凭证
///
/// `Debug` 输出经过脱敏处理：access_token / refresh_token / client_secret /
/// kiro_api_key / proxy_password 等敏感字段只显示长度，不会泄露明文。
#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct KiroCredentials {
    /// 凭据唯一标识符（自增 ID）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,

    /// 访问令牌
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_token: Option<String>,

    /// 刷新令牌
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,

    /// Profile ARN
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_arn: Option<String>,

    /// 过期时间 (RFC3339 格式)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,

    /// 认证方式 (social / idc)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_method: Option<String>,

    /// 身份提供商（BuilderId / Enterprise / Github / Google / IAM_SSO）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,

    /// OIDC Client ID (IdC 认证需要)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,

    /// OIDC Client Secret (IdC 认证需要)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,

    /// SSO Start URL（Enterprise / IAM Identity Center 账号专用）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_url: Option<String>,

    /// 企业 SSO (external_idp, 如 Microsoft 365 / Entra ID / Azure AD) 的 OAuth2 Token 端点。
    ///
    /// 当 `auth_method == "external_idp"` 时，Token 通过 refresh_token grant 打到该端点刷新
    /// （public client，无 client_secret），而非 AWS SSO OIDC 端点。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_endpoint: Option<String>,

    /// 企业 SSO 的 OIDC Issuer URL（端点的发现来源，纯记录用途）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issuer_url: Option<String>,

    /// 企业 SSO 授予的 scopes（空格分隔）。刷新时作为 `scope` 参数回传，
    /// 其中的 `offline_access` 是拿到 refresh_token 的前提。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scopes: Option<String>,

    /// 凭据优先级（数字越小优先级越高，默认为 0）
    #[serde(default)]
    #[serde(skip_serializing_if = "is_zero")]
    pub priority: u32,

    /// 单账号并发覆盖（None = 用全局 accountMaxConcurrency；Some(n) = 该账号专属并发上限）
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrency: Option<usize>,

    /// 凭据级 Region 配置（用于 OIDC token 刷新）
    /// 未配置时回退到 config.json 的全局 region
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,

    /// 凭据级 Auth Region（用于 Token 刷新）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_region: Option<String>,

    /// 凭据级 API Region（用于 API 请求）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_region: Option<String>,

    /// 凭据级 Machine ID 配置（可选）
    /// 未配置时回退到 config.json 的 machineId；都未配置时由 refreshToken 派生
    #[serde(skip_serializing_if = "Option::is_none")]
    pub machine_id: Option<String>,

    /// 用户邮箱（从 Anthropic API 获取）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,

    /// 订阅等级（KIRO PRO+ / KIRO FREE 等）
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub subscription_title: Option<String>,

    /// 凭据级代理 URL（可选）
    /// 支持 http/https/socks5 协议
    /// 特殊值 "direct" 表示显式不使用代理（即使全局配置了代理）
    /// 未配置时回退到全局代理配置
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,

    /// 凭据级代理认证用户名（可选）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_username: Option<String>,

    /// 凭据级代理认证密码（可选）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_password: Option<String>,

    /// 凭据是否被禁用（默认为 false）
    #[serde(default)]
    pub disabled: bool,

    /// 禁用原因（持久化，用于重启后区分手动禁用 vs 自动禁用）。
    /// 取值与运行时 `DisabledReason` 对齐的短标识：manual / too_many_failures /
    /// too_many_refresh_failures / quota_exceeded / invalid_refresh_token / invalid_config。
    /// 关键用途：让「配额自动禁用」的账号在进程重启后仍能被余额巡检自动恢复
    /// （否则会被一律当作 manual，永久卡死）。`disabled=false` 时应为 None。
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disabled_reason: Option<String>,

    /// Kiro API Key（headless 模式）
    /// 格式: ksk_xxxxxxxx
    /// 设置后直接作为 Bearer Token 使用，无需 refreshToken
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kiro_api_key: Option<String>,

    /// 端点名称（可选）
    ///
    /// 决定该凭据走哪套 Kiro API。未配置时回退到 `config.defaultEndpoint`（默认 "ide"）。
    /// 端点名必须在启动时注册的端点 registry 中存在；兼容别名 "auto" / "kiro"。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    /// 账号所属分组（可属于多个分组）
    ///
    /// 客户端 Key 绑定某个分组后，用该 Key 发起的请求只会调度到 groups 包含该分组名的账号。
    /// 空数组表示该账号不属于任何分组（仅未绑定分组的 Key / master apiKey 可使用）。
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<String>,

    /// 账号来源渠道（纯备注）
    ///
    /// 标记该账号的购买来源/渠道，便于运营追踪。不参与调度、导出或筛选。
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_channel: Option<String>,
}

/// 判断是否为零（用于跳过序列化）
fn is_zero(value: &u32) -> bool {
    *value == 0
}

/// 仅显示长度，不暴露明文。例如 `Some(42 chars)` 或 `None`。
fn fmt_redacted(value: &Option<String>) -> String {
    match value {
        Some(s) if !s.is_empty() => format!("Some({} chars)", s.chars().count()),
        Some(_) => "Some(<empty>)".to_string(),
        None => "None".to_string(),
    }
}

impl std::fmt::Debug for KiroCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 单独脱敏所有可能含密钥/Token 的字段，其他元数据正常打印
        f.debug_struct("KiroCredentials")
            .field("id", &self.id)
            .field("access_token", &fmt_redacted(&self.access_token))
            .field("refresh_token", &fmt_redacted(&self.refresh_token))
            .field("profile_arn", &self.profile_arn)
            .field("expires_at", &self.expires_at)
            .field("auth_method", &self.auth_method)
            .field("provider", &self.provider)
            .field("client_id", &fmt_redacted(&self.client_id))
            .field("client_secret", &fmt_redacted(&self.client_secret))
            .field("start_url", &self.start_url)
            .field("token_endpoint", &self.token_endpoint)
            .field("issuer_url", &self.issuer_url)
            .field("scopes", &self.scopes)
            .field("priority", &self.priority)
            .field("region", &self.region)
            .field("auth_region", &self.auth_region)
            .field("api_region", &self.api_region)
            .field("machine_id", &fmt_redacted(&self.machine_id))
            .field("email", &self.email)
            .field("subscription_title", &self.subscription_title)
            .field("proxy_url", &self.proxy_url)
            .field("proxy_username", &self.proxy_username)
            .field("proxy_password", &fmt_redacted(&self.proxy_password))
            .field("disabled", &self.disabled)
            .field("kiro_api_key", &fmt_redacted(&self.kiro_api_key))
            .field("endpoint", &self.endpoint)
            .field("groups", &self.groups)
            .field("source_channel", &self.source_channel)
            .finish()
    }
}

/// 企业 SSO (external_idp) 的 auth_method 别名。凭据来源多样（Kiro 导出、Azure 门户、
/// 手工），统一归一到规范值 `external_idp`。
const EXTERNAL_IDP_ALIASES: &[&str] = &[
    "external_idp",
    "azuread",
    "azure",
    "entra",
    "entra-id",
    "microsoft",
    "m365",
    "office365",
    "external",
];

pub(crate) fn canonicalize_auth_method_value(value: &str) -> &str {
    if value.eq_ignore_ascii_case("builder-id") || value.eq_ignore_ascii_case("iam") {
        "idc"
    } else if value.eq_ignore_ascii_case("external-idp")
        || value.eq_ignore_ascii_case("externalidp")
        || value.eq_ignore_ascii_case("external_idp")
    {
        "external_idp"
    } else if value.eq_ignore_ascii_case("api_key") || value.eq_ignore_ascii_case("apikey") {
        "api_key"
    } else if EXTERNAL_IDP_ALIASES
        .iter()
        .any(|a| value.eq_ignore_ascii_case(a))
    {
        "external_idp"
    } else {
        value
    }
}

/// 导入路径的 auth_method 归一化。
///
/// 在别名规范化之外，额外做一步推断：若显式声明的方式不是企业 SSO，但携带了
/// `tokenEndpoint`（social/idc 均无此字段），则判定为 `external_idp`。这样即便粘贴的
/// JSON 未写 authMethod，只要带 tokenEndpoint 就能被正确识别。
pub(crate) fn normalize_import_auth_method(raw: &str, token_endpoint: Option<&str>) -> String {
    let canonical = canonicalize_auth_method_value(raw.trim());
    if canonical.eq_ignore_ascii_case("external_idp") {
        return "external_idp".to_string();
    }
    if token_endpoint.is_some_and(|e| !e.trim().is_empty()) {
        return "external_idp".to_string();
    }
    canonical.to_string()
}

/// 企业 SSO IdP 端点允许列表（后缀锚定）。
///
/// `tokenEndpoint` 是外发 refreshToken 的目标，属新的信任边界；导入的凭据可能来自不可信
/// 来源（如共享账号包），若指向内网/攻击者控制的主机会导致 refreshToken 泄露。故限制到
/// 已知企业 IdP 主机（Microsoft Entra / Azure AD）。前导点锚定到真实子域边界，
/// `evil-microsoftonline.com` 无法命中。新增其它 IdP 时扩展此列表。
pub const ALLOWED_EXTERNAL_IDP_SUFFIXES: &[&str] = &[
    ".microsoftonline.com",
    ".microsoftonline.us",
    ".microsoftonline.cn",
];

/// 校验企业 SSO IdP 端点 URL 是否可安全外发。
///
/// 要求：可解析、必须 https、host 非 IP 字面量、host 命中 [`ALLOWED_EXTERNAL_IDP_SUFFIXES`]。
/// 用于 Token 刷新（外发 refreshToken 前）与导入校验两处，防 SSRF / 凭据外泄。
pub fn validate_external_idp_endpoint(raw_url: &str) -> Result<(), String> {
    let url = reqwest::Url::parse(raw_url.trim())
        .map_err(|e| format!("IdP 端点 URL 无法解析: {}", e))?;

    if !url.scheme().eq_ignore_ascii_case("https") {
        return Err("IdP 端点 URL 必须为 https".to_string());
    }

    let host = match url.host_str() {
        Some(h) if !h.is_empty() => h.to_ascii_lowercase(),
        _ => return Err("IdP 端点 URL 缺少 host".to_string()),
    };

    // 拒绝 IP 字面量（含 IPv6，url 的 host_str 对 IPv6 返回不带方括号的形式）
    if host.parse::<std::net::IpAddr>().is_ok() {
        return Err("IdP 端点 host 不能是 IP 字面量".to_string());
    }

    if ALLOWED_EXTERNAL_IDP_SUFFIXES
        .iter()
        .any(|suffix| host.ends_with(suffix))
    {
        Ok(())
    } else {
        Err(format!("IdP 端点 host {:?} 不在允许列表内", host))
    }
}

/// 凭据配置（支持单对象或数组格式）
///
/// 自动识别配置文件格式：
/// - 单对象格式（旧格式，向后兼容）
/// - 数组格式（新格式，支持多凭据）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CredentialsConfig {
    /// 单个凭据（旧格式）
    Single(KiroCredentials),
    /// 多凭据数组（新格式）
    Multiple(Vec<KiroCredentials>),
}

impl CredentialsConfig {
    /// 从文件加载凭据配置
    ///
    /// - 如果文件不存在，返回空数组
    /// - 如果文件内容为空，返回空数组
    /// - 支持单对象或数组格式
    pub fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path = path.as_ref();

        // 文件不存在时返回空数组
        if !path.exists() {
            return Ok(CredentialsConfig::Multiple(vec![]));
        }

        let content = fs::read_to_string(path)?;

        // 文件为空时返回空数组
        if content.trim().is_empty() {
            return Ok(CredentialsConfig::Multiple(vec![]));
        }

        let config = serde_json::from_str(&content)?;
        Ok(config)
    }

    /// 转换为按优先级排序的凭据列表
    pub fn into_sorted_credentials(self) -> Vec<KiroCredentials> {
        match self {
            CredentialsConfig::Single(mut cred) => {
                cred.canonicalize_auth_method();
                vec![cred]
            }
            CredentialsConfig::Multiple(mut creds) => {
                // 按优先级排序（数字越小优先级越高）
                creds.sort_by_key(|c| c.priority);
                for cred in &mut creds {
                    cred.canonicalize_auth_method();
                }
                creds
            }
        }
    }

    /// 判断是否为多凭据格式（数组格式）
    pub fn is_multiple(&self) -> bool {
        matches!(self, CredentialsConfig::Multiple(_))
    }
}

impl KiroCredentials {
    /// 特殊值：显式不使用代理
    pub const PROXY_DIRECT: &'static str = "direct";

    /// 获取默认凭证文件路径
    pub fn default_credentials_path() -> &'static str {
        "credentials.json"
    }

    /// 获取有效的 Auth Region（用于 Token 刷新）
    /// 优先级：凭据.auth_region > 凭据.region > config.auth_region > config.region
    pub fn effective_auth_region<'a>(&'a self, config: &'a Config) -> &'a str {
        self.auth_region
            .as_deref()
            .or(self.region.as_deref())
            .unwrap_or(config.effective_auth_region())
    }

    /// 获取有效的 API Region（用于 API 请求）
    /// 优先级：凭据.api_region > config.api_region > config.region
    pub fn effective_api_region<'a>(&'a self, config: &'a Config) -> &'a str {
        self.api_region
            .as_deref()
            .unwrap_or(config.effective_api_region())
    }

    /// 获取有效的代理配置
    /// 优先级：凭据代理 > 全局代理 > 无代理
    /// 特殊值 "direct" 表示显式不使用代理（即使全局配置了代理）
    pub fn effective_proxy(&self, global_proxy: Option<&ProxyConfig>) -> Option<ProxyConfig> {
        match self.proxy_url.as_deref() {
            Some(url) if url.eq_ignore_ascii_case(Self::PROXY_DIRECT) => None,
            Some(url) => {
                let mut proxy = ProxyConfig::new(url);
                if let (Some(username), Some(password)) =
                    (&self.proxy_username, &self.proxy_password)
                {
                    proxy = proxy.with_auth(username, password);
                }
                Some(proxy)
            }
            None => global_proxy.cloned(),
        }
    }

    pub fn canonicalize_auth_method(&mut self) {
        let auth_method = match &self.auth_method {
            Some(m) => m,
            None => return,
        };

        let canonical = canonicalize_auth_method_value(auth_method);
        if canonical != auth_method {
            self.auth_method = Some(canonical.to_string());
        }
    }

    pub fn fill_default_profile_arn(&mut self) -> bool {
        if self.profile_arn.is_some() || self.is_api_key_credential() {
            return false;
        }

        self.profile_arn = Some(self.default_profile_arn().to_string());
        true
    }

    /// 是否为 Social 登录（Github / Google）。
    fn is_social_login(&self) -> bool {
        self.auth_method
            .as_deref()
            .map(|m| m.eq_ignore_ascii_case("social"))
            .unwrap_or(false)
            || self
                .provider
                .as_deref()
                .map(|p| p.eq_ignore_ascii_case("github") || p.eq_ignore_ascii_case("google"))
                .unwrap_or(false)
    }

    /// 是否为外部 IdP（企业 SSO，如 Azure AD）登录。
    pub fn is_external_idp(&self) -> bool {
        self.auth_method
            .as_deref()
            .map(|m| {
                let norm = m.replace('-', "_");
                norm.eq_ignore_ascii_case("external_idp")
                    || norm.eq_ignore_ascii_case("externalidp")
            })
            .unwrap_or(false)
            || self
                .provider
                .as_deref()
                .map(|p| p.eq_ignore_ascii_case("externalidp"))
                .unwrap_or(false)
    }

    /// 凭据缺少显式 profileArn 时应使用的默认 ARN：
    /// Social 登录用共享 Social ARN，其余（BuilderID 等）用 BuilderID 占位符。
    fn default_profile_arn(&self) -> &'static str {
        if self.is_social_login() {
            SOCIAL_PROFILE_ARN
        } else {
            BUILDER_ID_PROFILE_ARN
        }
    }

    /// 检查凭据是否支持 Opus 模型
    ///
    /// Free 账号不支持 Opus 模型，需要 PRO 或更高等级订阅
    pub fn supports_opus(&self) -> bool {
        match &self.subscription_title {
            Some(title) => {
                let title_upper = title.to_uppercase();
                // 如果包含 FREE，则不支持 Opus
                !title_upper.contains("FREE")
            }
            // 如果还没有获取订阅信息，暂时允许（首次使用时会获取）
            None => true,
        }
    }

    /// 检查是否为 API Key 凭据
    ///
    /// API Key 凭据直接使用 kiro_api_key 作为 Bearer Token，无需 refreshToken
    pub fn is_api_key_credential(&self) -> bool {
        self.kiro_api_key.is_some()
            || self
                .auth_method
                .as_deref()
                .map(|m| m.eq_ignore_ascii_case("api_key") || m.eq_ignore_ascii_case("apikey"))
                .unwrap_or(false)
    }

    /// 是否为企业 SSO (external_idp) 凭据。
    ///
    /// 容忍未规范化的别名（azuread/entra/... ），统一按规范值判断。
    pub fn is_external_idp_credential(&self) -> bool {
        self.auth_method
            .as_deref()
            .map(|m| canonicalize_auth_method_value(m).eq_ignore_ascii_case("external_idp"))
            .unwrap_or(false)
    }

    /// 返回该凭据在 CodeWhisperer 调用上应携带的 `tokentype` 头值（无则 None）。
    ///
    /// - API Key 凭据 → `"API_KEY"`
    /// - 企业 SSO 凭据 → `"EXTERNAL_IDP"`（缺此头上游会静默返回空 profile 列表并拒绝数据面调用）
    /// - social / idc → None
    pub fn token_type_header(&self) -> Option<&'static str> {
        if self.is_api_key_credential() {
            Some("API_KEY")
        } else if self.is_external_idp_credential() {
            Some("EXTERNAL_IDP")
        } else {
            None
        }
    }

    /// 返回「可发送给上游」的真实 profileArn（跳过 BuilderID 占位符）。
    ///
    /// - 真实 ARN（含 Social 共享 ARN）→ 原样返回；
    /// - [`BUILDER_ID_PROFILE_ARN`] 占位符 → 返回 `None`（非流式/头部类调用不应发送
    ///   BuilderID 占位符；流式请求请使用 [`Self::streaming_profile_arn`]）。
    pub fn effective_profile_arn(&self) -> Option<&str> {
        match self.profile_arn.as_deref() {
            Some(arn) if !is_placeholder_profile_arn(arn) => Some(arn),
            _ => None,
        }
    }

    /// 返回流式聊天端点（`generateAssistantResponse` / `SendMessageStreaming`）
    /// 应发送的 profileArn。
    ///
    /// 新版上游对流式端点强制要求 `profileArn`，缺失会返回
    /// `400 {"message":"profileArn is required for this request."}`。Enterprise/IdC
    /// 账号的真实 ARN 会先由 `resolve_profile_arn_for` 回填；纯 BuilderID 账号没有
    /// 可解析的真实 profile，按官方 IDE 行为发送 BuilderID 占位符。
    ///
    /// - 已有显式 profileArn（真实 ARN / Social ARN / BuilderID 占位符）→ 原样返回；
    /// - 尚未填充 → 按登录方式推断默认 ARN（Social → Social ARN，其余 → BuilderID）；
    /// - API Key 凭据无 profileArn 概念 → 返回 `None`。
    pub fn streaming_profile_arn(&self) -> Option<String> {
        if self.is_api_key_credential() {
            return None;
        }
        Some(
            self.profile_arn
                .clone()
                .unwrap_or_else(|| self.default_profile_arn().to_string()),
        )
    }

    /// external_idp 数据面区域：以已解析的 `profile_arn` 为准（profileArn 的区域段
    /// 才是数据面区域，可能是 `eu-central-1` 等）；profileArn 尚未解析时回落到凭据
    /// 自身 `region`，仍无则最终回落 `us-east-1`（对齐 Kiro-Go `kiroRegionForProfile`：
    /// profileArn 区域 > account.Region > 默认）。
    ///
    /// 仅用于 external_idp 流式端点的主机/区域选择；其余凭据类型沿用 config 区域。
    pub fn data_plane_region(&self) -> &str {
        self.profile_arn
            .as_deref()
            .and_then(region_from_profile_arn)
            .or(self.region.as_deref())
            .unwrap_or("us-east-1")
    }

    /// 统一数据面区域（对齐 Kiro-Go `kiroRegionForProfile`）：**任何凭据类型**，
    /// 只要已解析出 profileArn 的区域段就以它为准（IAM / IdC / Social / external_idp
    /// 的 profile 都可能落在 `eu-central-1` 等非 us-east-1 区域），其次是凭据显式
    /// `api_region` / `region`，最后回落 config 的 `effective_api_region`。
    ///
    /// 修复点：此前流式端点仅对 `external_idp` 走 profileArn 区域，IAM / IdC 账号的
    /// eu-central-1 profile 被错误地路由到 config 区域（默认 `us-east-1`）——主机算成
    /// `q.us-east-1` 却带 eu-central-1 的 profileArn → 区域/profile 不匹配 → 上游
    /// `400 Improperly formed request` → 调度层判失败 → 表现为「无法调度使用」。
    ///
    /// 对既有 us-east-1 账号零影响（profileArn 区域即 us-east-1，或回落 config 默认）。
    pub fn effective_data_plane_region<'a>(&'a self, config: &'a Config) -> &'a str {
        self.profile_arn
            .as_deref()
            .and_then(region_from_profile_arn)
            .or(self.api_region.as_deref())
            .or(self.region.as_deref())
            .unwrap_or(config.effective_api_region())
    }
}

/// 判断给定 profileArn 是否为 BuilderID 占位符（非真实可用的 profile）。
pub fn is_placeholder_profile_arn(arn: &str) -> bool {
    arn == BUILDER_ID_PROFILE_ARN
}

/// 从 profileArn 解析数据面区域。
///
/// ARN 形如 `arn:aws:codewhisperer:<REGION>:<ACCOUNT>:profile/<ID>`，
/// 第 4 段（下标 3）即数据面区域（如 `us-east-1` / `eu-central-1`）。
/// `account.region` 是 auth/OIDC 区域，可能与 profile 区域不同，故数据面路由
/// 必须以 profileArn 为准（对齐 Kiro-Go `regionFromProfileArn`）。
pub fn region_from_profile_arn(arn: &str) -> Option<&str> {
    let mut parts = arn.trim().splitn(6, ':');
    match (
        parts.next(), // "arn"
        parts.next(), // "aws"
        parts.next(), // "codewhisperer"
        parts.next(), // <REGION>
    ) {
        (Some("arn"), Some("aws"), Some("codewhisperer"), Some(region)) => {
            let region = region.trim();
            if region.is_empty() {
                None
            } else {
                Some(region)
            }
        }
        _ => None,
    }
}

/// 计算 external_idp 数据面主机名。
///
/// 上游事实（见 Kiro-Go `regionalizeURLForRegion`）：CodeWhisperer REST 主机族
/// **仅在 `us-east-1` 存在**；其余区域一律由区域化的 Amazon Q 主机
/// `q.{region}.amazonaws.com` 提供服务，**不存在 `codewhisperer.{region}`**。
/// 因此：
/// - `us-east-1`（或空）→ `codewhisperer.us-east-1.amazonaws.com`
/// - 其它区域 → `q.{region}.amazonaws.com`
pub fn external_idp_host(region: &str) -> String {
    let region = region.trim();
    if region.is_empty() || region == "us-east-1" {
        "codewhisperer.us-east-1.amazonaws.com".to_string()
    } else {
        format!("q.{}.amazonaws.com", region)
    }
}

/// external_idp 端点白名单主机后缀（SSRF 防护）。
///
/// 只有 Microsoft / Entra（Azure AD）的登录主机族允许作为刷新端点，避免把任意
/// issuer 当作 token 端点访问。对齐 Kiro-Go `allowedExternalIdpIssuerSuffixes`。
const ALLOWED_EXTERNAL_IDP_HOST_SUFFIXES: [&str; 3] = [
    ".microsoftonline.com",
    ".microsoftonline.us",
    ".microsoftonline.cn",
];

/// 判断主机是否落在 external_idp 白名单内（大小写不敏感，忽略端口）。
fn is_allowed_external_idp_host(host: &str) -> bool {
    let host = host.split(':').next().unwrap_or(host).to_ascii_lowercase();
    ALLOWED_EXTERNAL_IDP_HOST_SUFFIXES
        .iter()
        .any(|suffix| host.ends_with(suffix))
}

/// 校验一个完整的 external_idp token 端点 URL 是否可安全访问（SSRF 防护）。
///
/// 强制 `https://`，且主机必须落在 [`ALLOWED_EXTERNAL_IDP_HOST_SUFFIXES`] 白名单内。
/// 用于**刷新请求前的二次校验**（导入派生时已校验一次，此处防止 credentials 文件被
/// 篡改后把 refreshToken 泄露到任意主机）——对齐 Kiro-Go 刷新期 `ValidateExternalIdpEndpoint`。
pub fn is_allowed_external_idp_endpoint(url: &str) -> bool {
    let rest = match url.trim().strip_prefix("https://") {
        Some(r) => r,
        None => return false,
    };
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    if host.is_empty() {
        return false;
    }
    is_allowed_external_idp_host(host)
}

/// 从形如 `https://<host>/<tenant>/...` 的 URL 解析出 (host, tenant)。
///
/// 强制 https；host 必须落在白名单内（拒绝 IP 字面量与非 Microsoft 主机）；
/// tenant 取路径首段。任一条件不满足返回 `None`。
fn parse_ms_tenant(src: &str) -> Option<(String, String)> {
    let rest = src.trim().strip_prefix("https://")?;
    let (host, path) = rest.split_once('/')?;
    if !is_allowed_external_idp_host(host) {
        return None;
    }
    let host = host.split(':').next().unwrap_or(host).to_ascii_lowercase();
    let tenant = path.split('/').next()?.trim();
    if tenant.is_empty() {
        return None;
    }
    Some((host, tenant.to_string()))
}

/// 从 JWT access_token 解出 `iss`（issuer）claim。解析失败返回 `None`，不做签名校验。
fn jwt_iss(access_token: &str) -> Option<String> {
    use base64::Engine;
    let payload_b64 = access_token.trim().split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64)
        .ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    value
        .get("iss")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// 为 KAM 导出的 external_idp 账号派生 `tokenEndpoint` / `issuerUrl` / `scopes`。
///
/// KAM 导出的企业 SSO（Azure AD / Entra）账号通常只带 `clientId` + `refreshToken`
/// + 账号级 `userId`，缺 `tokenEndpoint`/`issuerUrl`/`scopes`；而 external_idp 刷新
/// （见 [`refresh_external_idp_token`](crate::kiro::token_manager)）硬要求
/// `tokenEndpoint`。本函数据此重建（对齐 Kiro-Go `DeriveExternalIdpEndpoints`）。
///
/// 租户来源：优先 `userId`（形如 `https://login.microsoftonline.com/<tenant>/v2.0.<oid>`），
/// 缺失时回落到 `accessToken` JWT 的 `iss` claim。二者都拿不到可用租户即返回 `None`，
/// 调用方回落原有「需要 clientId 和 tokenEndpoint」报错。
///
/// - `tokenEndpoint` = `https://<host>/<tenant>/oauth2/v2.0/token`
/// - `issuerUrl`     = `https://<host>/<tenant>/v2.0`
/// - `scopes`        = `api://<clientId>/codewhisperer:conversations api://<clientId>/codewhisperer:completions offline_access`
///   （`clientId` 缺失时为空串）
///
/// host 经白名单校验（`.microsoftonline.com/.us/.cn`），非白名单主机返回 `None`（SSRF 防护）。
pub fn derive_external_idp_endpoints(
    user_id: Option<&str>,
    access_token: Option<&str>,
    client_id: Option<&str>,
) -> Option<(String, String, String)> {
    let src = user_id
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .or_else(|| access_token.and_then(jwt_iss))?;
    let (host, tenant) = parse_ms_tenant(&src)?;
    let token_endpoint = format!("https://{}/{}/oauth2/v2.0/token", host, tenant);
    let issuer_url = format!("https://{}/{}/v2.0", host, tenant);
    let scopes = match client_id.map(str::trim).filter(|s| !s.is_empty()) {
        Some(cid) => format!(
            "api://{cid}/codewhisperer:conversations \
             api://{cid}/codewhisperer:completions offline_access"
        ),
        None => String::new(),
    };
    Some((token_endpoint, issuer_url, scopes))
}

#[cfg(test)]
impl KiroCredentials {
    fn from_json(json_string: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json_string)
    }

    fn to_pretty_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::config::Config;

    #[test]
    fn test_from_json() {
        let json = r#"{
            "accessToken": "test_token",
            "refreshToken": "test_refresh",
            "profileArn": "arn:aws:test",
            "expiresAt": "2024-01-01T00:00:00Z",
            "authMethod": "social"
        }"#;

        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.access_token, Some("test_token".to_string()));
        assert_eq!(creds.refresh_token, Some("test_refresh".to_string()));
        assert_eq!(creds.profile_arn, Some("arn:aws:test".to_string()));
        assert_eq!(creds.expires_at, Some("2024-01-01T00:00:00Z".to_string()));
        assert_eq!(creds.auth_method, Some("social".to_string()));
    }

    #[test]
    fn test_from_json_with_unknown_keys() {
        let json = r#"{
            "accessToken": "test_token",
            "unknownField": "should be ignored"
        }"#;

        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.access_token, Some("test_token".to_string()));
    }

    #[test]
    fn test_to_json() {
        let creds = KiroCredentials {
            id: None,
            access_token: Some("token".to_string()),
            refresh_token: None,
            profile_arn: None,
            expires_at: None,
            auth_method: Some("social".to_string()),
            provider: None,
            client_id: None,
            client_secret: None,
            start_url: None,
            token_endpoint: None,
            issuer_url: None,
            scopes: None,
            priority: 0,
            max_concurrency: None,
            region: None,
            auth_region: None,
            api_region: None,
            machine_id: None,
            email: None,
            subscription_title: None,
            proxy_url: None,
            proxy_username: None,
            proxy_password: None,
            disabled: false,
            disabled_reason: None,
            kiro_api_key: None,
            endpoint: None,
            groups: vec![],
            source_channel: None,
        };

        let json = creds.to_pretty_json().unwrap();
        assert!(json.contains("accessToken"));
        assert!(json.contains("authMethod"));
        assert!(!json.contains("refreshToken"));
        // priority 为 0 时不序列化
        assert!(!json.contains("priority"));
    }

    #[test]
    fn test_default_credentials_path() {
        assert_eq!(
            KiroCredentials::default_credentials_path(),
            "credentials.json"
        );
    }

    #[test]
    fn test_is_placeholder_profile_arn() {
        assert!(is_placeholder_profile_arn(BUILDER_ID_PROFILE_ARN));
        assert!(!is_placeholder_profile_arn(SOCIAL_PROFILE_ARN));
        assert!(!is_placeholder_profile_arn(
            "arn:aws:codewhisperer:us-east-1:123456789012:profile/REAL123"
        ));
    }

    #[test]
    fn test_region_from_profile_arn() {
        assert_eq!(
            region_from_profile_arn("arn:aws:codewhisperer:eu-central-1:123456789012:profile/ABC"),
            Some("eu-central-1")
        );
        assert_eq!(
            region_from_profile_arn("arn:aws:codewhisperer:us-east-1:638616132270:profile/XYZ"),
            Some("us-east-1")
        );
        // 含前后空白
        assert_eq!(
            region_from_profile_arn("  arn:aws:codewhisperer:eu-west-1:1:profile/P  "),
            Some("eu-west-1")
        );
        // BuilderID 占位符也是合法 ARN，区域为 us-east-1
        assert_eq!(
            region_from_profile_arn(BUILDER_ID_PROFILE_ARN),
            Some("us-east-1")
        );
        // 非法 / 缺段
        assert_eq!(region_from_profile_arn(""), None);
        assert_eq!(region_from_profile_arn("not-an-arn"), None);
        assert_eq!(region_from_profile_arn("arn:aws:s3:::bucket"), None);
        assert_eq!(
            region_from_profile_arn("arn:aws:codewhisperer::1:profile/P"),
            None
        );
    }

    #[test]
    fn test_external_idp_host() {
        // us-east-1（或空）→ codewhisperer 主机
        assert_eq!(
            external_idp_host("us-east-1"),
            "codewhisperer.us-east-1.amazonaws.com"
        );
        assert_eq!(
            external_idp_host(""),
            "codewhisperer.us-east-1.amazonaws.com"
        );
        assert_eq!(
            external_idp_host("  "),
            "codewhisperer.us-east-1.amazonaws.com"
        );
        // 其它区域 → 区域化 q 主机（不存在 codewhisperer.{region}）
        assert_eq!(
            external_idp_host("eu-central-1"),
            "q.eu-central-1.amazonaws.com"
        );
        assert_eq!(
            external_idp_host("ap-southeast-1"),
            "q.ap-southeast-1.amazonaws.com"
        );
    }

    #[test]
    fn test_derive_external_idp_from_user_id() {
        // 标准 KAM userId：https://login.microsoftonline.com/<tenant>/v2.0.<oid>
        let user_id =
            "https://login.microsoftonline.com/1f44574f-f8aa-40cf-8e43-e6bff9b4298a/v2.0.abc";
        let (token_endpoint, issuer_url, scopes) =
            derive_external_idp_endpoints(Some(user_id), None, Some("client-xyz"))
                .expect("应从 userId 派生出端点");
        assert_eq!(
            token_endpoint,
            "https://login.microsoftonline.com/1f44574f-f8aa-40cf-8e43-e6bff9b4298a/oauth2/v2.0/token"
        );
        assert_eq!(
            issuer_url,
            "https://login.microsoftonline.com/1f44574f-f8aa-40cf-8e43-e6bff9b4298a/v2.0"
        );
        assert_eq!(
            scopes,
            "api://client-xyz/codewhisperer:conversations \
             api://client-xyz/codewhisperer:completions offline_access"
        );
    }

    #[test]
    fn test_derive_external_idp_scopes_empty_without_client_id() {
        let user_id = "https://login.microsoftonline.com/tenant-1/v2.0.oid";
        let (_, _, scopes) = derive_external_idp_endpoints(Some(user_id), None, None)
            .expect("clientId 缺失也应派生出端点");
        assert_eq!(scopes, "");
    }

    #[test]
    fn test_derive_external_idp_rejects_non_whitelisted_host() {
        // 非 Microsoft 主机 → 拒绝（SSRF 防护）
        assert!(
            derive_external_idp_endpoints(
                Some("https://evil.example.com/tenant/v2.0.oid"),
                None,
                Some("c")
            )
            .is_none()
        );
        // IP 字面量 → 拒绝
        assert!(
            derive_external_idp_endpoints(Some("https://127.0.0.1/tenant/v2.0"), None, Some("c"))
                .is_none()
        );
        // 非 https → 拒绝
        assert!(
            derive_external_idp_endpoints(
                Some("http://login.microsoftonline.com/tenant/v2.0"),
                None,
                Some("c")
            )
            .is_none()
        );
    }

    #[test]
    fn test_derive_external_idp_fallback_to_jwt_iss() {
        // userId 缺失 → 从 accessToken JWT 的 iss 兜底。
        // 构造一个 payload 为 {"iss":"https://login.microsoftonline.com/tenant-jwt/v2.0"} 的假 JWT。
        use base64::Engine;
        let payload = serde_json::json!({
            "iss": "https://login.microsoftonline.com/tenant-jwt/v2.0"
        });
        let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&payload).unwrap());
        let fake_jwt = format!("header.{}.sig", payload_b64);
        let (token_endpoint, _, _) =
            derive_external_idp_endpoints(None, Some(&fake_jwt), Some("c"))
                .expect("应从 JWT iss 派生出端点");
        assert_eq!(
            token_endpoint,
            "https://login.microsoftonline.com/tenant-jwt/oauth2/v2.0/token"
        );
    }

    #[test]
    fn test_derive_external_idp_none_when_no_source() {
        // 既无 userId 也无可解析的 accessToken → None
        assert!(derive_external_idp_endpoints(None, None, Some("c")).is_none());
        assert!(derive_external_idp_endpoints(Some("   "), Some("not-a-jwt"), Some("c")).is_none());
    }

    #[test]
    fn test_is_allowed_external_idp_endpoint() {
        // 白名单主机 + https → 允许
        assert!(is_allowed_external_idp_endpoint(
            "https://login.microsoftonline.com/tenant/oauth2/v2.0/token"
        ));
        assert!(is_allowed_external_idp_endpoint(
            "https://login.microsoftonline.us/tenant/oauth2/v2.0/token"
        ));
        // 非 https → 拒绝
        assert!(!is_allowed_external_idp_endpoint(
            "http://login.microsoftonline.com/tenant/oauth2/v2.0/token"
        ));
        // 非白名单主机（篡改成攻击者域名）→ 拒绝
        assert!(!is_allowed_external_idp_endpoint(
            "https://evil.example.com/tenant/oauth2/v2.0/token"
        ));
        // 白名单后缀被伪造成子串（非真正后缀）→ 拒绝
        assert!(!is_allowed_external_idp_endpoint(
            "https://login.microsoftonline.com.evil.com/t/token"
        ));
        // 空 / 畸形 → 拒绝
        assert!(!is_allowed_external_idp_endpoint("https://"));
        assert!(!is_allowed_external_idp_endpoint(""));
    }

    #[test]
    fn test_data_plane_region() {
        // 以已解析的 profileArn 区域为准
        let mut cred = KiroCredentials::default();
        cred.profile_arn = Some("arn:aws:codewhisperer:eu-central-1:123:profile/REAL".to_string());
        assert_eq!(cred.data_plane_region(), "eu-central-1");

        // 无 profileArn → 回落 us-east-1
        let plain = KiroCredentials::default();
        assert_eq!(plain.data_plane_region(), "us-east-1");

        // profileArn 尚未解析，但凭据自带 region → 用 region 兜底（对齐 Kiro-Go）
        let mut region_only = KiroCredentials::default();
        region_only.region = Some("eu-central-1".to_string());
        assert_eq!(region_only.data_plane_region(), "eu-central-1");

        // profileArn 区域优先于凭据 region（profileArn 才是真实数据面区域）
        let mut both = KiroCredentials::default();
        both.profile_arn = Some("arn:aws:codewhisperer:eu-central-1:123:profile/REAL".to_string());
        both.region = Some("us-east-1".to_string());
        assert_eq!(both.data_plane_region(), "eu-central-1");

        // 占位符 profileArn（us-east-1）
        let mut placeholder = KiroCredentials::default();
        placeholder.profile_arn = Some(BUILDER_ID_PROFILE_ARN.to_string());
        assert_eq!(placeholder.data_plane_region(), "us-east-1");
    }

    #[test]
    fn test_effective_data_plane_region_all_auth_types() {
        let config = Config::default(); // region 默认 us-east-1

        // IAM（归一为 idc）+ eu-central-1 profileArn → 取 profileArn 区域（核心修复）
        let mut iam = KiroCredentials::default();
        iam.auth_method = Some("iam".to_string());
        iam.canonicalize_auth_method();
        iam.profile_arn = Some("arn:aws:codewhisperer:eu-central-1:123:profile/REAL".to_string());
        assert!(!iam.is_external_idp());
        assert_eq!(iam.effective_data_plane_region(&config), "eu-central-1");

        // 无 profileArn 的普通凭据 → 回落 config 区域（保持既有行为）
        let plain = KiroCredentials::default();
        assert_eq!(plain.effective_data_plane_region(&config), "us-east-1");

        // 凭据显式 api_region 优先于 config，但低于 profileArn
        let mut api_only = KiroCredentials::default();
        api_only.api_region = Some("ap-southeast-1".to_string());
        assert_eq!(
            api_only.effective_data_plane_region(&config),
            "ap-southeast-1"
        );

        // profileArn 区域优先于凭据 api_region
        let mut both = KiroCredentials::default();
        both.profile_arn = Some("arn:aws:codewhisperer:eu-central-1:123:profile/REAL".to_string());
        both.api_region = Some("us-east-1".to_string());
        assert_eq!(both.effective_data_plane_region(&config), "eu-central-1");

        // external_idp 仍取 profileArn 区域（行为不变）
        let mut ext = KiroCredentials::default();
        ext.auth_method = Some("external_idp".to_string());
        ext.profile_arn = Some("arn:aws:codewhisperer:eu-central-1:123:profile/REAL".to_string());
        assert_eq!(ext.effective_data_plane_region(&config), "eu-central-1");
    }

    #[test]
    fn test_effective_profile_arn_skips_placeholder() {
        // BuilderID 占位符 → None（不发送给上游）
        let mut cred = KiroCredentials::default();
        cred.profile_arn = Some(BUILDER_ID_PROFILE_ARN.to_string());
        assert_eq!(cred.effective_profile_arn(), None);

        // Social 共享 ARN → 原样返回
        cred.profile_arn = Some(SOCIAL_PROFILE_ARN.to_string());
        assert_eq!(cred.effective_profile_arn(), Some(SOCIAL_PROFILE_ARN));

        // 真实 Enterprise ARN → 原样返回
        let real = "arn:aws:codewhisperer:us-east-1:123456789012:profile/REAL123";
        cred.profile_arn = Some(real.to_string());
        assert_eq!(cred.effective_profile_arn(), Some(real));

        // 无 ARN → None
        cred.profile_arn = None;
        assert_eq!(cred.effective_profile_arn(), None);
    }

    #[test]
    fn test_streaming_profile_arn_includes_placeholder() {
        // 流式端点：显式 BuilderID 占位符原样发送，缺失会被上游以 400 拒绝
        let mut cred = KiroCredentials::default();
        cred.profile_arn = Some(BUILDER_ID_PROFILE_ARN.to_string());
        assert_eq!(
            cred.streaming_profile_arn().as_deref(),
            Some(BUILDER_ID_PROFILE_ARN)
        );

        // 真实 ARN 原样发送
        let real = "arn:aws:codewhisperer:us-east-1:123456789012:profile/REAL123";
        cred.profile_arn = Some(real.to_string());
        assert_eq!(cred.streaming_profile_arn().as_deref(), Some(real));

        // 未填充 + 非 social（BuilderID 账号）→ 回退 BuilderID 占位符
        let mut builder = KiroCredentials::default();
        builder.profile_arn = None;
        builder.refresh_token = Some("r".to_string());
        assert_eq!(
            builder.streaming_profile_arn().as_deref(),
            Some(BUILDER_ID_PROFILE_ARN)
        );

        // 未填充 + social → 回退 Social 共享 ARN（非占位符，原样发送）
        let mut social = KiroCredentials::default();
        social.profile_arn = None;
        social.auth_method = Some("social".to_string());
        assert_eq!(
            social.streaming_profile_arn().as_deref(),
            Some(SOCIAL_PROFILE_ARN)
        );

        // API Key 凭据无 profileArn 概念 → None
        let mut api = KiroCredentials::default();
        api.kiro_api_key = Some("ksk_xxx".to_string());
        assert_eq!(api.streaming_profile_arn(), None);
    }

    #[test]
    fn test_priority_default() {
        let json = r#"{"refreshToken": "test"}"#;
        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.priority, 0);
    }

    #[test]
    fn test_priority_explicit() {
        let json = r#"{"refreshToken": "test", "priority": 5}"#;
        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.priority, 5);
    }

    #[test]
    fn test_credentials_config_single() {
        let json = r#"{"refreshToken": "test", "expiresAt": "2025-12-31T00:00:00Z"}"#;
        let config: CredentialsConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, CredentialsConfig::Single(_)));
    }

    #[test]
    fn test_credentials_config_multiple() {
        let json = r#"[
            {"refreshToken": "test1", "priority": 1},
            {"refreshToken": "test2", "priority": 0}
        ]"#;
        let config: CredentialsConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, CredentialsConfig::Multiple(_)));
        assert_eq!(config.into_sorted_credentials().len(), 2);
    }

    #[test]
    fn test_credentials_config_priority_sorting() {
        let json = r#"[
            {"refreshToken": "t1", "priority": 2},
            {"refreshToken": "t2", "priority": 0},
            {"refreshToken": "t3", "priority": 1}
        ]"#;
        let config: CredentialsConfig = serde_json::from_str(json).unwrap();
        let list = config.into_sorted_credentials();

        // 验证按优先级排序
        assert_eq!(list[0].refresh_token, Some("t2".to_string())); // priority 0
        assert_eq!(list[1].refresh_token, Some("t3".to_string())); // priority 1
        assert_eq!(list[2].refresh_token, Some("t1".to_string())); // priority 2
    }

    #[test]
    fn test_external_idp_fields_roundtrip_and_canonicalize() {
        let json = r#"{
            "refreshToken": "refresh",
            "authMethod": "external-idp",
            "clientId": "client",
            "tokenEndpoint": "https://idp.example.com/oauth/token",
            "issuerUrl": "https://idp.example.com/",
            "scopes": "openid profile"
        }"#;
        let config: CredentialsConfig = serde_json::from_str(json).unwrap();
        let creds = config.into_sorted_credentials().remove(0);
        assert_eq!(creds.auth_method.as_deref(), Some("external_idp"));
        assert_eq!(
            creds.token_endpoint.as_deref(),
            Some("https://idp.example.com/oauth/token")
        );
        assert_eq!(
            creds.issuer_url.as_deref(),
            Some("https://idp.example.com/")
        );
        assert_eq!(creds.scopes.as_deref(), Some("openid profile"));
    }

    // ============ Region 字段测试 ============

    #[test]
    fn test_region_field_parsing() {
        // 测试解析包含 region 字段的 JSON
        let json = r#"{
            "refreshToken": "test_refresh",
            "region": "us-east-1"
        }"#;

        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.refresh_token, Some("test_refresh".to_string()));
        assert_eq!(creds.region, Some("us-east-1".to_string()));
    }

    #[test]
    fn test_region_field_missing_backward_compat() {
        // 测试向后兼容：不包含 region 字段的旧格式 JSON
        let json = r#"{
            "refreshToken": "test_refresh",
            "authMethod": "social"
        }"#;

        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.refresh_token, Some("test_refresh".to_string()));
        assert_eq!(creds.region, None);
    }

    #[test]
    fn test_region_field_serialization() {
        let creds = KiroCredentials {
            id: None,
            access_token: None,
            refresh_token: Some("test".to_string()),
            profile_arn: None,
            expires_at: None,
            auth_method: None,
            provider: None,
            client_id: None,
            client_secret: None,
            start_url: None,
            token_endpoint: None,
            issuer_url: None,
            scopes: None,
            priority: 0,
            max_concurrency: None,
            region: Some("eu-west-1".to_string()),
            auth_region: None,
            api_region: None,
            machine_id: None,
            email: None,
            subscription_title: None,
            proxy_url: None,
            proxy_username: None,
            proxy_password: None,
            disabled: false,
            disabled_reason: None,
            kiro_api_key: None,
            endpoint: None,
            groups: vec![],
            source_channel: None,
        };

        let json = creds.to_pretty_json().unwrap();
        assert!(json.contains("region"));
        assert!(json.contains("eu-west-1"));
    }

    #[test]
    fn test_region_field_none_not_serialized() {
        let creds = KiroCredentials {
            id: None,
            access_token: None,
            refresh_token: Some("test".to_string()),
            profile_arn: None,
            expires_at: None,
            auth_method: None,
            provider: None,
            client_id: None,
            client_secret: None,
            start_url: None,
            token_endpoint: None,
            issuer_url: None,
            scopes: None,
            priority: 0,
            max_concurrency: None,
            region: None,
            auth_region: None,
            api_region: None,
            machine_id: None,
            email: None,
            subscription_title: None,
            proxy_url: None,
            proxy_username: None,
            proxy_password: None,
            disabled: false,
            disabled_reason: None,
            kiro_api_key: None,
            endpoint: None,
            groups: vec![],
            source_channel: None,
        };

        let json = creds.to_pretty_json().unwrap();
        assert!(!json.contains("region"));
    }

    // ============ MachineId 字段测试 ============

    #[test]
    fn test_machine_id_field_parsing() {
        let machine_id = "a".repeat(64);
        let json = format!(
            r#"{{
                "refreshToken": "test_refresh",
                "machineId": "{machine_id}"
            }}"#
        );

        let creds = KiroCredentials::from_json(&json).unwrap();
        assert_eq!(creds.refresh_token, Some("test_refresh".to_string()));
        assert_eq!(creds.machine_id, Some(machine_id));
    }

    #[test]
    fn test_machine_id_field_serialization() {
        let mut creds = KiroCredentials::default();
        creds.refresh_token = Some("test".to_string());
        creds.machine_id = Some("b".repeat(64));

        let json = creds.to_pretty_json().unwrap();
        assert!(json.contains("machineId"));
    }

    #[test]
    fn test_machine_id_field_none_not_serialized() {
        let mut creds = KiroCredentials::default();
        creds.refresh_token = Some("test".to_string());
        creds.machine_id = None;

        let json = creds.to_pretty_json().unwrap();
        assert!(!json.contains("machineId"));
    }

    #[test]
    fn test_multiple_credentials_with_different_regions() {
        // 测试多凭据场景下不同凭据使用各自的 region
        let json = r#"[
            {"refreshToken": "t1", "region": "us-east-1"},
            {"refreshToken": "t2", "region": "eu-west-1"},
            {"refreshToken": "t3"}
        ]"#;

        let config: CredentialsConfig = serde_json::from_str(json).unwrap();
        let list = config.into_sorted_credentials();

        assert_eq!(list[0].region, Some("us-east-1".to_string()));
        assert_eq!(list[1].region, Some("eu-west-1".to_string()));
        assert_eq!(list[2].region, None);
    }

    #[test]
    fn test_region_field_with_all_fields() {
        // 测试包含所有字段的完整 JSON
        let json = r#"{
            "id": 1,
            "accessToken": "access",
            "refreshToken": "refresh",
            "profileArn": "arn:aws:test",
            "expiresAt": "2025-12-31T00:00:00Z",
            "authMethod": "idc",
            "clientId": "client123",
            "clientSecret": "secret456",
            "priority": 5,
            "region": "ap-northeast-1"
        }"#;

        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.id, Some(1));
        assert_eq!(creds.access_token, Some("access".to_string()));
        assert_eq!(creds.refresh_token, Some("refresh".to_string()));
        assert_eq!(creds.profile_arn, Some("arn:aws:test".to_string()));
        assert_eq!(creds.expires_at, Some("2025-12-31T00:00:00Z".to_string()));
        assert_eq!(creds.auth_method, Some("idc".to_string()));
        assert_eq!(creds.client_id, Some("client123".to_string()));
        assert_eq!(creds.client_secret, Some("secret456".to_string()));
        assert_eq!(creds.priority, 5);
        assert_eq!(creds.region, Some("ap-northeast-1".to_string()));
    }

    #[test]
    fn test_region_roundtrip() {
        // 测试序列化和反序列化的往返一致性
        let original = KiroCredentials {
            id: Some(42),
            access_token: Some("token".to_string()),
            refresh_token: Some("refresh".to_string()),
            profile_arn: None,
            expires_at: None,
            auth_method: Some("social".to_string()),
            provider: None,
            client_id: None,
            client_secret: None,
            start_url: None,
            token_endpoint: None,
            issuer_url: None,
            scopes: None,
            priority: 3,
            max_concurrency: None,
            region: Some("us-west-2".to_string()),
            auth_region: None,
            api_region: None,
            machine_id: Some("c".repeat(64)),
            email: None,
            subscription_title: None,
            proxy_url: None,
            proxy_username: None,
            proxy_password: None,
            disabled: false,
            disabled_reason: None,
            kiro_api_key: None,
            endpoint: None,
            groups: vec![],
            source_channel: None,
        };

        let json = original.to_pretty_json().unwrap();
        let parsed = KiroCredentials::from_json(&json).unwrap();

        assert_eq!(parsed.id, original.id);
        assert_eq!(parsed.access_token, original.access_token);
        assert_eq!(parsed.refresh_token, original.refresh_token);
        assert_eq!(parsed.priority, original.priority);
        assert_eq!(parsed.region, original.region);
        assert_eq!(parsed.machine_id, original.machine_id);
    }

    // ============ auth_region / api_region 字段测试 ============

    #[test]
    fn test_auth_region_field_parsing() {
        let json = r#"{
            "refreshToken": "test_refresh",
            "authRegion": "eu-central-1"
        }"#;
        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.auth_region, Some("eu-central-1".to_string()));
        assert_eq!(creds.api_region, None);
    }

    #[test]
    fn test_api_region_field_parsing() {
        let json = r#"{
            "refreshToken": "test_refresh",
            "apiRegion": "ap-southeast-1"
        }"#;
        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.api_region, Some("ap-southeast-1".to_string()));
        assert_eq!(creds.auth_region, None);
    }

    #[test]
    fn test_auth_api_region_serialization() {
        let mut creds = KiroCredentials::default();
        creds.refresh_token = Some("test".to_string());
        creds.auth_region = Some("eu-west-1".to_string());
        creds.api_region = Some("us-west-2".to_string());

        let json = creds.to_pretty_json().unwrap();
        assert!(json.contains("authRegion"));
        assert!(json.contains("eu-west-1"));
        assert!(json.contains("apiRegion"));
        assert!(json.contains("us-west-2"));
    }

    #[test]
    fn test_auth_api_region_none_not_serialized() {
        let mut creds = KiroCredentials::default();
        creds.refresh_token = Some("test".to_string());
        creds.auth_region = None;
        creds.api_region = None;

        let json = creds.to_pretty_json().unwrap();
        assert!(!json.contains("authRegion"));
        assert!(!json.contains("apiRegion"));
    }

    #[test]
    fn test_auth_api_region_roundtrip() {
        let mut original = KiroCredentials::default();
        original.refresh_token = Some("refresh".to_string());
        original.region = Some("us-east-1".to_string());
        original.auth_region = Some("eu-west-1".to_string());
        original.api_region = Some("ap-northeast-1".to_string());

        let json = original.to_pretty_json().unwrap();
        let parsed = KiroCredentials::from_json(&json).unwrap();

        assert_eq!(parsed.region, original.region);
        assert_eq!(parsed.auth_region, original.auth_region);
        assert_eq!(parsed.api_region, original.api_region);
    }

    #[test]
    fn test_backward_compat_no_auth_api_region() {
        // 旧格式 JSON 不包含 authRegion/apiRegion，应正常解析
        let json = r#"{
            "refreshToken": "test_refresh",
            "region": "us-east-1"
        }"#;
        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.region, Some("us-east-1".to_string()));
        assert_eq!(creds.auth_region, None);
        assert_eq!(creds.api_region, None);
    }

    // ============ effective_auth_region / effective_api_region 优先级测试 ============

    #[test]
    fn test_effective_auth_region_credential_auth_region_highest() {
        // 凭据.auth_region > 凭据.region > config.auth_region > config.region
        let mut config = Config::default();
        config.region = "config-region".to_string();
        config.auth_region = Some("config-auth-region".to_string());

        let mut creds = KiroCredentials::default();
        creds.region = Some("cred-region".to_string());
        creds.auth_region = Some("cred-auth-region".to_string());

        assert_eq!(creds.effective_auth_region(&config), "cred-auth-region");
    }

    #[test]
    fn test_effective_auth_region_fallback_to_credential_region() {
        let mut config = Config::default();
        config.region = "config-region".to_string();
        config.auth_region = Some("config-auth-region".to_string());

        let mut creds = KiroCredentials::default();
        creds.region = Some("cred-region".to_string());
        // auth_region 未设置

        assert_eq!(creds.effective_auth_region(&config), "cred-region");
    }

    #[test]
    fn test_effective_auth_region_fallback_to_config_auth_region() {
        let mut config = Config::default();
        config.region = "config-region".to_string();
        config.auth_region = Some("config-auth-region".to_string());

        let creds = KiroCredentials::default();
        // auth_region 和 region 均未设置

        assert_eq!(creds.effective_auth_region(&config), "config-auth-region");
    }

    #[test]
    fn test_effective_auth_region_fallback_to_config_region() {
        let mut config = Config::default();
        config.region = "config-region".to_string();
        // config.auth_region 未设置

        let creds = KiroCredentials::default();

        assert_eq!(creds.effective_auth_region(&config), "config-region");
    }

    #[test]
    fn test_effective_api_region_credential_api_region_highest() {
        // 凭据.api_region > config.api_region > config.region
        let mut config = Config::default();
        config.region = "config-region".to_string();
        config.api_region = Some("config-api-region".to_string());

        let mut creds = KiroCredentials::default();
        creds.api_region = Some("cred-api-region".to_string());

        assert_eq!(creds.effective_api_region(&config), "cred-api-region");
    }

    #[test]
    fn test_effective_api_region_fallback_to_config_api_region() {
        let mut config = Config::default();
        config.region = "config-region".to_string();
        config.api_region = Some("config-api-region".to_string());

        let creds = KiroCredentials::default();

        assert_eq!(creds.effective_api_region(&config), "config-api-region");
    }

    #[test]
    fn test_effective_api_region_fallback_to_config_region() {
        let mut config = Config::default();
        config.region = "config-region".to_string();

        let creds = KiroCredentials::default();

        assert_eq!(creds.effective_api_region(&config), "config-region");
    }

    #[test]
    fn test_effective_api_region_ignores_credential_region() {
        // 凭据.region 不参与 api_region 的回退链
        let mut config = Config::default();
        config.region = "config-region".to_string();

        let mut creds = KiroCredentials::default();
        creds.region = Some("cred-region".to_string());

        assert_eq!(creds.effective_api_region(&config), "config-region");
    }

    #[test]
    fn test_auth_and_api_region_independent() {
        // auth_region 和 api_region 互不影响
        let mut config = Config::default();
        config.region = "default".to_string();

        let mut creds = KiroCredentials::default();
        creds.auth_region = Some("auth-only".to_string());
        creds.api_region = Some("api-only".to_string());

        assert_eq!(creds.effective_auth_region(&config), "auth-only");
        assert_eq!(creds.effective_api_region(&config), "api-only");
    }

    // ============ 凭据级代理优先级测试 ============

    #[test]
    fn test_effective_proxy_credential_overrides_global() {
        let global = ProxyConfig::new("http://global:8080");
        let mut creds = KiroCredentials::default();
        creds.proxy_url = Some("socks5://cred:1080".to_string());

        let result = creds.effective_proxy(Some(&global));
        assert_eq!(result, Some(ProxyConfig::new("socks5://cred:1080")));
    }

    #[test]
    fn test_effective_proxy_credential_with_auth() {
        let global = ProxyConfig::new("http://global:8080");
        let mut creds = KiroCredentials::default();
        creds.proxy_url = Some("http://proxy:3128".to_string());
        creds.proxy_username = Some("user".to_string());
        creds.proxy_password = Some("pass".to_string());

        let result = creds.effective_proxy(Some(&global));
        let expected = ProxyConfig::new("http://proxy:3128").with_auth("user", "pass");
        assert_eq!(result, Some(expected));
    }

    #[test]
    fn test_effective_proxy_direct_bypasses_global() {
        let global = ProxyConfig::new("http://global:8080");
        let mut creds = KiroCredentials::default();
        creds.proxy_url = Some("direct".to_string());

        let result = creds.effective_proxy(Some(&global));
        assert_eq!(result, None);
    }

    #[test]
    fn test_effective_proxy_direct_case_insensitive() {
        let global = ProxyConfig::new("http://global:8080");
        let mut creds = KiroCredentials::default();
        creds.proxy_url = Some("DIRECT".to_string());

        let result = creds.effective_proxy(Some(&global));
        assert_eq!(result, None);
    }

    #[test]
    fn test_effective_proxy_fallback_to_global() {
        let global = ProxyConfig::new("http://global:8080");
        let creds = KiroCredentials::default();

        let result = creds.effective_proxy(Some(&global));
        assert_eq!(result, Some(ProxyConfig::new("http://global:8080")));
    }

    #[test]
    fn test_effective_proxy_none_when_no_proxy() {
        let creds = KiroCredentials::default();
        let result = creds.effective_proxy(None);
        assert_eq!(result, None);
    }

    // ============ 企业 SSO (external_idp) 测试 ============

    #[test]
    fn test_canonicalize_external_idp_aliases() {
        for alias in [
            "external_idp",
            "AzureAD",
            "azure",
            "Entra",
            "entra-id",
            "microsoft",
            "M365",
            "office365",
            "external",
        ] {
            assert_eq!(
                canonicalize_auth_method_value(alias),
                "external_idp",
                "别名 {:?} 应规范化为 external_idp",
                alias
            );
        }
        // 不误伤其它方式
        assert_eq!(canonicalize_auth_method_value("social"), "social");
        assert_eq!(canonicalize_auth_method_value("builder-id"), "idc");
        assert_eq!(canonicalize_auth_method_value("apikey"), "api_key");
    }

    #[test]
    fn test_normalize_import_auth_method_inference() {
        // 显式别名 → external_idp
        assert_eq!(normalize_import_auth_method("azuread", None), "external_idp");
        // 带 tokenEndpoint 但未声明（默认 social）→ 推断 external_idp
        assert_eq!(
            normalize_import_auth_method(
                "social",
                Some("https://login.microsoftonline.com/t/oauth2/v2.0/token")
            ),
            "external_idp"
        );
        // 空 tokenEndpoint 不触发推断
        assert_eq!(normalize_import_auth_method("social", Some("   ")), "social");
        assert_eq!(normalize_import_auth_method("social", None), "social");
        // idc 保持
        assert_eq!(normalize_import_auth_method("idc", None), "idc");
    }

    #[test]
    fn test_validate_external_idp_endpoint() {
        // 合法 Microsoft 主机
        assert!(
            validate_external_idp_endpoint(
                "https://login.microsoftonline.com/tenant/oauth2/v2.0/token"
            )
            .is_ok()
        );
        assert!(validate_external_idp_endpoint("https://login.microsoftonline.us/t/token").is_ok());
        // 非 https 拒绝
        assert!(validate_external_idp_endpoint("http://login.microsoftonline.com/t").is_err());
        // IP 字面量拒绝
        assert!(validate_external_idp_endpoint("https://127.0.0.1/token").is_err());
        assert!(validate_external_idp_endpoint("https://[::1]/token").is_err());
        // 允许列表外拒绝
        assert!(validate_external_idp_endpoint("https://evil.example.com/token").is_err());
        // 前导点锚定：evil-microsoftonline.com 不应命中
        assert!(validate_external_idp_endpoint("https://evil-microsoftonline.com/token").is_err());
        // 裸域（无子域）不应命中 .microsoftonline.com 后缀
        assert!(validate_external_idp_endpoint("https://microsoftonline.com/token").is_err());
    }

    #[test]
    fn test_is_external_idp_and_token_type_header() {
        let mut cred = KiroCredentials {
            auth_method: Some("azuread".to_string()), // 别名也应识别
            ..Default::default()
        };
        assert!(cred.is_external_idp_credential());
        assert_eq!(cred.token_type_header(), Some("EXTERNAL_IDP"));

        cred.auth_method = Some("social".to_string());
        assert!(!cred.is_external_idp_credential());
        assert_eq!(cred.token_type_header(), None);

        cred.auth_method = Some("api_key".to_string());
        assert_eq!(cred.token_type_header(), Some("API_KEY"));
    }

    #[test]
    fn test_external_idp_credentials_serde_roundtrip() {
        let json = r#"{
            "authMethod": "external_idp",
            "refreshToken": "rt",
            "clientId": "fa6d79bf-xxxx",
            "tokenEndpoint": "https://login.microsoftonline.com/t/oauth2/v2.0/token",
            "issuerUrl": "https://login.microsoftonline.com/t/v2.0",
            "scopes": "openid profile offline_access",
            "region": "eu-central-1"
        }"#;
        let cred = KiroCredentials::from_json(json).unwrap();
        assert_eq!(cred.auth_method.as_deref(), Some("external_idp"));
        assert_eq!(
            cred.token_endpoint.as_deref(),
            Some("https://login.microsoftonline.com/t/oauth2/v2.0/token")
        );
        assert_eq!(cred.scopes.as_deref(), Some("openid profile offline_access"));

        // 序列化后应保留新字段（camelCase）
        let serialized = cred.to_pretty_json().unwrap();
        assert!(serialized.contains("\"tokenEndpoint\""));
        assert!(serialized.contains("\"issuerUrl\""));
        assert!(serialized.contains("\"scopes\""));

        // 空字段不应出现在序列化结果中
        let empty = KiroCredentials::default();
        let empty_json = empty.to_pretty_json().unwrap();
        assert!(!empty_json.contains("tokenEndpoint"));
    }
}
