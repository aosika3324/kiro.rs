//! Kiro CodeWhisperer 端点（移植自 Kiro-Go 的 "CodeWhisperer" route）
//!
//! 与 `ide` 端点几乎一致：同样的 path (`/generateAssistantResponse`)、origin (`AI_EDITOR`)、
//! content-type (`application/json`)、profileArn 注入与 KiroIDE 风格 User-Agent。
//! 两点差异（对齐 Kiro-Go `proxy/kiro.go` 的 CodeWhisperer route）：
//!
//! 1. **主机族**：一律走 [`external_idp_host`]——`us-east-1`（或空）→
//!    `codewhisperer.us-east-1.amazonaws.com`，其余区域 → `q.{region}.amazonaws.com`
//!    （CodeWhisperer REST 主机族仅存在于 us-east-1，其余区域由区域化 Amazon Q 主机提供服务）。
//!    `ide` 只对 `external_idp` 凭据这样做，其它凭据仍用 `q.{config.region}`；本端点对所有凭据类型一致。
//!
//! 2. **`x-amz-target` 头**：追加
//!    `AmazonCodeWhispererStreamingService.GenerateAssistantResponse`。
//!    这是与 `ide` 端点的核心区别——部分账号在 `ide`（无此头）下被上游拒绝，
//!    而 CodeWhisperer route（带此头）可用。
//!
//! 选择方式：`credential.endpoint = "codewhisperer"` 或 `config.defaultEndpoint = "codewhisperer"`。

use reqwest::RequestBuilder;
use uuid::Uuid;

use super::{KiroEndpoint, RequestContext};
use crate::kiro::kiro_version;
use crate::kiro::model::credentials::external_idp_host;

/// Kiro CodeWhisperer 端点名称
pub const CODEWHISPERER_ENDPOINT_NAME: &str = "codewhisperer";

/// CodeWhisperer route 专用的 `x-amz-target` 取值（对齐 Kiro-Go）
const X_AMZ_TARGET: &str = "AmazonCodeWhispererStreamingService.GenerateAssistantResponse";

/// Kiro CodeWhisperer 端点
pub struct CodewhispererEndpoint;

impl CodewhispererEndpoint {
    pub fn new() -> Self {
        Self
    }

    /// 数据面区域：`external_idp` 以已解析的 profileArn 区域为准，其余凭据沿用 config 区域。
    /// 与 `ide` 端点逻辑一致；差异仅在 [`Self::host`] 把该区域交给 `external_idp_host`。
    fn api_region<'a>(&self, ctx: &'a RequestContext<'_>) -> &'a str {
        if ctx.credentials.is_external_idp() {
            ctx.credentials.data_plane_region()
        } else {
            ctx.credentials.effective_api_region(ctx.config)
        }
    }

    /// 与 `ide` 的关键差异：对所有凭据类型一致走 `external_idp_host`，
    /// 因此 us-east-1 凭据命中 `codewhisperer.us-east-1.amazonaws.com` 主机。
    fn host(&self, ctx: &RequestContext<'_>) -> String {
        external_idp_host(self.api_region(ctx))
    }

    fn x_amz_user_agent(&self, ctx: &RequestContext<'_>) -> String {
        format!(
            "aws-sdk-js/1.0.34 KiroIDE-{}-{}",
            kiro_version::effective(&ctx.config.kiro_version),
            ctx.machine_id
        )
    }

    fn user_agent(&self, ctx: &RequestContext<'_>) -> String {
        format!(
            "aws-sdk-js/1.0.34 ua/2.1 os/{} lang/js md/nodejs#{} api/codewhispererstreaming#1.0.34 m/E KiroIDE-{}-{}",
            ctx.config.system_version,
            ctx.config.node_version,
            kiro_version::effective(&ctx.config.kiro_version),
            ctx.machine_id
        )
    }
}

impl Default for CodewhispererEndpoint {
    fn default() -> Self {
        Self::new()
    }
}

impl KiroEndpoint for CodewhispererEndpoint {
    fn name(&self) -> &'static str {
        CODEWHISPERER_ENDPOINT_NAME
    }

    fn api_url(&self, ctx: &RequestContext<'_>) -> String {
        format!(
            "https://{}/generateAssistantResponse",
            external_idp_host(self.api_region(ctx))
        )
    }

    fn mcp_url(&self, ctx: &RequestContext<'_>) -> String {
        format!("https://{}/mcp", external_idp_host(self.api_region(ctx)))
    }

    fn decorate_api(&self, req: RequestBuilder, ctx: &RequestContext<'_>) -> RequestBuilder {
        // 与 ide 一致的 header 集合，外加 CodeWhisperer route 专用的 x-amz-target 头。
        let mut req = req
            .header("x-amz-target", X_AMZ_TARGET)
            .header("x-amzn-codewhisperer-optout", "true")
            .header("x-amzn-kiro-agent-mode", "vibe")
            .header("x-amz-user-agent", self.x_amz_user_agent(ctx))
            .header("user-agent", self.user_agent(ctx))
            .header("host", self.host(ctx))
            .header("amz-sdk-invocation-id", Uuid::new_v4().to_string())
            .header("amz-sdk-request", "attempt=1; max=3")
            .header("Authorization", format!("Bearer {}", ctx.token));

        if ctx.credentials.is_api_key_credential() {
            req = req.header("tokentype", "API_KEY");
        } else if ctx.credentials.is_external_idp() {
            req = req.header("TokenType", "EXTERNAL_IDP");
        }
        req
    }

    fn decorate_mcp(&self, req: RequestBuilder, ctx: &RequestContext<'_>) -> RequestBuilder {
        let mut req = req
            .header("x-amz-user-agent", self.x_amz_user_agent(ctx))
            .header("user-agent", self.user_agent(ctx))
            .header("host", self.host(ctx))
            .header("amz-sdk-invocation-id", Uuid::new_v4().to_string())
            .header("amz-sdk-request", "attempt=1; max=3")
            .header("Authorization", format!("Bearer {}", ctx.token));

        if let Some(arn) = ctx.credentials.effective_profile_arn() {
            req = req.header("x-amzn-kiro-profile-arn", arn);
        }
        if ctx.credentials.is_api_key_credential() {
            req = req.header("tokentype", "API_KEY");
        } else if ctx.credentials.is_external_idp() {
            req = req.header("TokenType", "EXTERNAL_IDP");
        }
        req
    }

    fn transform_api_body(&self, body: &str, ctx: &RequestContext<'_>) -> String {
        inject_profile_arn(body, ctx.credentials.streaming_profile_arn().as_deref())
    }
}

/// 将 profile_arn 注入到请求体 JSON 根对象（与 `ide` 端点同实现）
fn inject_profile_arn(request_body: &str, profile_arn: Option<&str>) -> String {
    if let Some(arn) = profile_arn {
        if let Ok(mut json) = serde_json::from_str::<serde_json::Value>(request_body) {
            json["profileArn"] = serde_json::Value::String(arn.to_string());
            if let Ok(body) = serde_json::to_string(&json) {
                return body;
            }
        }
    }
    request_body.to_string()
}

#[cfg(test)]
mod tests {
    use super::inject_profile_arn;
    use serde_json::Value;

    #[test]
    fn test_inject_profile_arn_with_some() {
        let body = r#"{"conversationState":{"conversationId":"c1"}}"#;
        let arn = Some("arn:aws:codewhisperer:us-east-1:123:profile/ABC".to_string());
        let result = inject_profile_arn(body, arn.as_deref());
        let json: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(
            json["profileArn"],
            "arn:aws:codewhisperer:us-east-1:123:profile/ABC"
        );
        assert_eq!(json["conversationState"]["conversationId"], "c1");
    }

    use super::{CodewhispererEndpoint, KiroEndpoint, RequestContext, X_AMZ_TARGET};
    use crate::kiro::model::credentials::KiroCredentials;
    use crate::model::config::Config;

    fn external_idp_cred(profile_arn: Option<&str>) -> KiroCredentials {
        let mut c = KiroCredentials::default();
        c.auth_method = Some("external_idp".to_string());
        c.provider = Some("ExternalIdp".to_string());
        c.profile_arn = profile_arn.map(|s| s.to_string());
        c
    }

    #[test]
    fn test_name() {
        assert_eq!(CodewhispererEndpoint::new().name(), "codewhisperer");
    }

    #[test]
    fn test_us_east_1_uses_codewhisperer_host_for_all_cred_types() {
        // 与 ide 的关键差异：非 external_idp 凭据在 us-east-1 也走 codewhisperer 主机
        let ep = CodewhispererEndpoint::new();
        let mut cred = KiroCredentials::default();
        cred.auth_method = Some("social".to_string());
        let config = Config::default();
        let ctx = RequestContext {
            credentials: &cred,
            token: "t",
            machine_id: "m",
            config: &config,
        };
        assert_eq!(
            ep.api_url(&ctx),
            "https://codewhisperer.us-east-1.amazonaws.com/generateAssistantResponse"
        );
        assert_eq!(ep.host(&ctx), "codewhisperer.us-east-1.amazonaws.com");
        assert_eq!(
            ep.mcp_url(&ctx),
            "https://codewhisperer.us-east-1.amazonaws.com/mcp"
        );
    }

    #[test]
    fn test_external_idp_us_east_1_codewhisperer_host() {
        let ep = CodewhispererEndpoint::new();
        let cred = external_idp_cred(None);
        let config = Config::default();
        let ctx = RequestContext {
            credentials: &cred,
            token: "t",
            machine_id: "m",
            config: &config,
        };
        assert_eq!(
            ep.api_url(&ctx),
            "https://codewhisperer.us-east-1.amazonaws.com/generateAssistantResponse"
        );
        assert_eq!(ep.host(&ctx), "codewhisperer.us-east-1.amazonaws.com");
    }

    #[test]
    fn test_non_us_east_1_collapses_to_q_regional_host() {
        // 非 us-east-1 区域 → q.{region}（不存在 codewhisperer.{region}）
        let ep = CodewhispererEndpoint::new();
        let cred =
            external_idp_cred(Some("arn:aws:codewhisperer:eu-central-1:123:profile/REAL"));
        let config = Config::default();
        let ctx = RequestContext {
            credentials: &cred,
            token: "t",
            machine_id: "m",
            config: &config,
        };
        assert_eq!(
            ep.api_url(&ctx),
            "https://q.eu-central-1.amazonaws.com/generateAssistantResponse"
        );
        assert_eq!(ep.host(&ctx), "q.eu-central-1.amazonaws.com");
    }

    #[test]
    fn test_x_amz_target_value_matches_kiro_go() {
        assert_eq!(
            X_AMZ_TARGET,
            "AmazonCodeWhispererStreamingService.GenerateAssistantResponse"
        );
    }

    #[test]
    fn test_inject_profile_arn_with_none() {
        let body = r#"{"conversationState":{"conversationId":"c1"}}"#;
        let result = inject_profile_arn(body, None);
        let json: Value = serde_json::from_str(&result).unwrap();
        assert!(json.get("profileArn").is_none());
    }

    #[test]
    fn test_inject_profile_arn_overwrites_existing() {
        let body = r#"{"conversationState":{},"profileArn":"old-arn"}"#;
        let arn = Some("new-arn".to_string());
        let result = inject_profile_arn(body, arn.as_deref());
        let json: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(json["profileArn"], "new-arn");
    }

    #[test]
    fn test_inject_profile_arn_invalid_json() {
        let body = "not-valid-json";
        let arn = Some("arn:test".to_string());
        let result = inject_profile_arn(body, arn.as_deref());
        assert_eq!(result, "not-valid-json");
    }
}
