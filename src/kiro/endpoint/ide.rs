//! Kiro IDE 端点
//!
//! 对应 Kiro IDE 客户端目前使用的 AWS CodeWhisperer 端点：
//! - API: `https://q.{api_region}.amazonaws.com/generateAssistantResponse`
//! - MCP: `https://q.{api_region}.amazonaws.com/mcp`
//!
//! 请求头使用 aws-sdk-js User-Agent 标识。请求体会在根对象上注入 `profileArn`。

use reqwest::RequestBuilder;
use uuid::Uuid;

use super::{KiroEndpoint, RequestContext};
use crate::kiro::kiro_version;

/// Kiro IDE 端点名称
pub const IDE_ENDPOINT_NAME: &str = "ide";

/// Kiro IDE 端点
pub struct IdeEndpoint;

impl IdeEndpoint {
    pub fn new() -> Self {
        Self
    }

    fn api_region<'a>(&self, ctx: &'a RequestContext<'_>) -> &'a str {
        // external_idp：数据面区域以已解析的 profileArn 为准（可能是 eu-central-1 等），
        // 不能用 config 区域；其余凭据沿用 config 的 effective_api_region。
        if ctx.credentials.is_external_idp() {
            ctx.credentials.data_plane_region()
        } else {
            ctx.credentials.effective_api_region(ctx.config)
        }
    }

    fn host(&self, ctx: &RequestContext<'_>) -> String {
        if ctx.credentials.is_external_idp() {
            crate::kiro::model::credentials::external_idp_host(self.api_region(ctx))
        } else {
            format!("q.{}.amazonaws.com", self.api_region(ctx))
        }
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

impl Default for IdeEndpoint {
    fn default() -> Self {
        Self::new()
    }
}

impl KiroEndpoint for IdeEndpoint {
    fn name(&self) -> &'static str {
        IDE_ENDPOINT_NAME
    }

    fn api_url(&self, ctx: &RequestContext<'_>) -> String {
        if ctx.credentials.is_external_idp() {
            return format!(
                "https://{}/generateAssistantResponse",
                crate::kiro::model::credentials::external_idp_host(self.api_region(ctx))
            );
        }
        format!(
            "https://q.{}.amazonaws.com/generateAssistantResponse",
            self.api_region(ctx)
        )
    }

    fn mcp_url(&self, ctx: &RequestContext<'_>) -> String {
        if ctx.credentials.is_external_idp() {
            return format!(
                "https://{}/mcp",
                crate::kiro::model::credentials::external_idp_host(self.api_region(ctx))
            );
        }
        format!("https://q.{}.amazonaws.com/mcp", self.api_region(ctx))
    }

    fn decorate_api(&self, req: RequestBuilder, ctx: &RequestContext<'_>) -> RequestBuilder {
        let mut req = req
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

/// 将 profile_arn 注入到请求体 JSON 根对象
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

    #[test]
    fn test_inject_profile_arn_with_none() {
        let body = r#"{"conversationState":{"conversationId":"c1"}}"#;
        let result = inject_profile_arn(body, None);
        let json: Value = serde_json::from_str(&result).unwrap();
        assert!(json.get("profileArn").is_none());
        assert_eq!(json["conversationState"]["conversationId"], "c1");
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

    use super::{IdeEndpoint, KiroEndpoint, RequestContext};
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
    fn test_external_idp_streaming_url_us_east_1() {
        // 占位符 / 无 profileArn → us-east-1 → codewhisperer 主机（不再是 runtime.*.kiro.dev）
        let ep = IdeEndpoint::new();
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
        assert_eq!(ep.mcp_url(&ctx), "https://codewhisperer.us-east-1.amazonaws.com/mcp");
    }

    #[test]
    fn test_external_idp_streaming_url_eu_central_1() {
        // profileArn 区域为 eu-central-1 → q.eu-central-1 主机（数据面区域取自 profileArn）
        let ep = IdeEndpoint::new();
        let cred =
            external_idp_cred(Some("arn:aws:codewhisperer:eu-central-1:123:profile/REAL"));
        let config = Config::default(); // 默认 region=us-east-1，必须被 profileArn 区域覆盖
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
        assert_eq!(ep.mcp_url(&ctx), "https://q.eu-central-1.amazonaws.com/mcp");
    }

    #[test]
    fn test_non_external_idp_streaming_url_unchanged() {
        // 非 external_idp 行为保持不变：q.{config.region}.amazonaws.com
        let ep = IdeEndpoint::new();
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
            "https://q.us-east-1.amazonaws.com/generateAssistantResponse"
        );
        assert_eq!(ep.host(&ctx), "q.us-east-1.amazonaws.com");
        assert_eq!(ep.mcp_url(&ctx), "https://q.us-east-1.amazonaws.com/mcp");
    }
}
