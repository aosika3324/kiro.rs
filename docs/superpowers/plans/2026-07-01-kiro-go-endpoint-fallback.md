# Kiro-Go Endpoint Fallback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port Kiro-Go's endpoint fallback behavior into kiro.rs by adding an `amazonq` endpoint and optional same-account endpoint fallback for upstream 429/transient failures.

**Architecture:** Add `amazonq` as a third Kiro-Go-compatible streaming endpoint alongside `ide` and `codewhisperer`. Add config fields `preferredEndpoint` and `endpointFallback`, then teach `KiroProvider` to build an endpoint order and try fallback endpoints on the same credential before account-level cooldown/failover.

**Tech Stack:** Rust 2024, Tokio, reqwest, axum, serde, anyhow, tracing, existing `KiroEndpoint` abstraction.

## Global Constraints

- Keep `endpointFallback` default `false` for backward compatibility.
- Preserve current single-endpoint behavior when `endpointFallback=false`.
- Fallback set is only `ide`, `codewhisperer`, `amazonq`; exclude `cli`.
- If primary is `cli`, fallback order remains `cli` only.
- Fallback same account only for HTTP `429`, HTTP `408`, HTTP `5xx`, and network errors.
- Do not fallback for HTTP `400`, `401`, `402`, `403`, `524`, or client validation errors.
- `429 USER_REQUEST_RATE_EXCEEDED` should cooldown the account only after all fallback endpoints are exhausted.
- Do not redesign account scheduler, queueing, token bucket, streaming parser, or Anthropic conversion.
- Do not commit `admin-ui/bun.lock` unless a later explicit task says so; it is currently dirty from Admin UI build and outside this scope.

---

## File Map

- Create `src/kiro/endpoint/amazonq.rs`
  - Owns the AmazonQ streaming route matching Kiro-Go's `AmazonQ` endpoint.
  - Implements `KiroEndpoint`.
  - Adds focused unit tests for URL/header/profileArn behavior.

- Modify `src/kiro/endpoint/mod.rs`
  - Exports `AmazonqEndpoint` and `AMAZONQ_ENDPOINT_NAME`.

- Modify `src/main.rs`
  - Registers `amazonq` in endpoint registry.
  - Validates `preferredEndpoint` when configured.
  - Passes new config values into `KiroProvider::with_proxy`.

- Modify `src/model/config.rs`
  - Adds serde fields `preferred_endpoint: Option<String>` and `endpoint_fallback: bool`.
  - Adds defaults and `Config::default()` initialization.

- Modify `src/kiro/provider.rs`
  - Stores `preferred_endpoint` and `endpoint_fallback`.
  - Adds endpoint order helper.
  - Adds fallback classification helper.
  - Refactors one upstream attempt into a small internal helper.
  - Updates `call_api_with_retry` to use fallback order.

- Modify `config.example.json`
  - Documents opt-in fallback config.

- Modify `README.md`
  - Documents `amazonq`, `preferredEndpoint`, and `endpointFallback`.

---

### Task 1: Add Config Fields for Preferred Endpoint and Fallback Toggle

**Files:**
- Modify: `src/model/config.rs:170-173`
- Modify: `src/model/config.rs:332-386`
- Test: existing config serde/default tests if present; otherwise use `cargo test config`

**Interfaces:**
- Produces: `Config.preferred_endpoint: Option<String>`
- Produces: `Config.endpoint_fallback: bool`
- Produces: `default_endpoint_fallback() -> bool`
- Consumed by: Task 4 provider constructor and endpoint-order logic.

- [ ] **Step 1: Add fields to `Config` after `default_endpoint`**

Add this block in `src/model/config.rs` immediately after `pub default_endpoint: String,`:

```rust
    /// Preferred endpoint name when endpoint fallback is enabled.
    ///
    /// If unset, fallback order starts from `default_endpoint`. Per-credential
    /// `endpoint` still has highest priority.
    #[serde(default)]
    pub preferred_endpoint: Option<String>,

    /// Whether to try Kiro-Go-compatible fallback endpoints on the same credential.
    ///
    /// Default false preserves existing single-endpoint behavior.
    #[serde(default = "default_endpoint_fallback")]
    pub endpoint_fallback: bool,
```

- [ ] **Step 2: Add default function**

Add this near `default_endpoint()`:

```rust
fn default_endpoint_fallback() -> bool {
    false
}
```

- [ ] **Step 3: Update `Config::default()`**

In `impl Default for Config`, after `default_endpoint: default_endpoint(),`, add:

```rust
            preferred_endpoint: None,
            endpoint_fallback: default_endpoint_fallback(),
```

- [ ] **Step 4: Run focused config compile check**

Run:

```bash
cd E:/kiro.rs
cargo test config --no-fail-fast
```

Expected: compile succeeds. Some unrelated tests may run; no compile errors for `Config` missing fields.

- [ ] **Step 5: Commit**

```bash
cd E:/kiro.rs
git add src/model/config.rs
git commit -m "feat: add endpoint fallback config"
```

---

### Task 2: Add AmazonQ Endpoint

**Files:**
- Create: `src/kiro/endpoint/amazonq.rs`
- Modify: `src/kiro/endpoint/mod.rs:14-18`
- Test: `cargo test amazonq`

**Interfaces:**
- Produces: `pub const AMAZONQ_ENDPOINT_NAME: &str = "amazonq"`
- Produces: `pub struct AmazonqEndpoint`
- Produces: `impl KiroEndpoint for AmazonqEndpoint`
- Consumed by: Task 3 registry, Task 4 endpoint-order helper.

- [ ] **Step 1: Create `src/kiro/endpoint/amazonq.rs`**

Use this complete file:

```rust
//! Kiro AmazonQ endpoint (ported from Kiro-Go's "AmazonQ" route).
//!
//! This is a KiroIDE-style streaming endpoint, not the existing `cli` endpoint.
//! It uses `/generateAssistantResponse`, `AI_EDITOR` body origin, JSON content,
//! profileArn injection, and KiroIDE user-agent headers.
//! The distinguishing header is:
//! `x-amz-target: AmazonQDeveloperStreamingService.SendMessage`.

use reqwest::RequestBuilder;
use uuid::Uuid;

use super::{KiroEndpoint, RequestContext};
use crate::kiro::kiro_version;

pub const AMAZONQ_ENDPOINT_NAME: &str = "amazonq";
const X_AMZ_TARGET: &str = "AmazonQDeveloperStreamingService.SendMessage";

pub struct AmazonqEndpoint;

impl AmazonqEndpoint {
    pub fn new() -> Self {
        Self
    }

    fn api_region<'a>(&self, ctx: &'a RequestContext<'_>) -> &'a str {
        if ctx.credentials.is_external_idp() {
            ctx.credentials.data_plane_region()
        } else {
            ctx.credentials.effective_api_region(ctx.config)
        }
    }

    fn host(&self, ctx: &RequestContext<'_>) -> String {
        format!("q.{}.amazonaws.com", self.api_region(ctx))
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

impl Default for AmazonqEndpoint {
    fn default() -> Self {
        Self::new()
    }
}

impl KiroEndpoint for AmazonqEndpoint {
    fn name(&self) -> &'static str {
        AMAZONQ_ENDPOINT_NAME
    }

    fn api_url(&self, ctx: &RequestContext<'_>) -> String {
        format!("https://{}/generateAssistantResponse", self.host(ctx))
    }

    fn mcp_url(&self, ctx: &RequestContext<'_>) -> String {
        format!("https://{}/mcp", self.host(ctx))
    }

    fn decorate_api(&self, req: RequestBuilder, ctx: &RequestContext<'_>) -> RequestBuilder {
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
    use super::{inject_profile_arn, AmazonqEndpoint, KiroEndpoint, RequestContext, X_AMZ_TARGET};
    use crate::kiro::model::credentials::KiroCredentials;
    use crate::model::config::Config;
    use serde_json::Value;

    fn external_idp_cred(profile_arn: Option<&str>) -> KiroCredentials {
        let mut c = KiroCredentials::default();
        c.auth_method = Some("external_idp".to_string());
        c.provider = Some("ExternalIdp".to_string());
        c.profile_arn = profile_arn.map(|s| s.to_string());
        c
    }

    #[test]
    fn test_name() {
        assert_eq!(AmazonqEndpoint::new().name(), "amazonq");
    }

    #[test]
    fn test_us_east_1_q_host() {
        let ep = AmazonqEndpoint::new();
        let cred = external_idp_cred(None);
        let config = Config::default();
        let ctx = RequestContext { credentials: &cred, token: "t", machine_id: "m", config: &config };
        assert_eq!(ep.api_url(&ctx), "https://q.us-east-1.amazonaws.com/generateAssistantResponse");
        assert_eq!(ep.mcp_url(&ctx), "https://q.us-east-1.amazonaws.com/mcp");
        assert_eq!(ep.host(&ctx), "q.us-east-1.amazonaws.com");
    }

    #[test]
    fn test_external_idp_profile_region_q_host() {
        let ep = AmazonqEndpoint::new();
        let cred = external_idp_cred(Some("arn:aws:codewhisperer:eu-central-1:123:profile/REAL"));
        let config = Config::default();
        let ctx = RequestContext { credentials: &cred, token: "t", machine_id: "m", config: &config };
        assert_eq!(ep.api_url(&ctx), "https://q.eu-central-1.amazonaws.com/generateAssistantResponse");
        assert_eq!(ep.mcp_url(&ctx), "https://q.eu-central-1.amazonaws.com/mcp");
        assert_eq!(ep.host(&ctx), "q.eu-central-1.amazonaws.com");
    }

    #[test]
    fn test_x_amz_target_matches_kiro_go() {
        assert_eq!(X_AMZ_TARGET, "AmazonQDeveloperStreamingService.SendMessage");
    }

    #[test]
    fn test_inject_profile_arn_with_some() {
        let body = r#"{"conversationState":{"conversationId":"c1"}}"#;
        let result = inject_profile_arn(body, Some("arn:test"));
        let json: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(json["profileArn"], "arn:test");
        assert_eq!(json["conversationState"]["conversationId"], "c1");
    }

    #[test]
    fn test_inject_profile_arn_with_none() {
        let body = r#"{"conversationState":{"conversationId":"c1"}}"#;
        let result = inject_profile_arn(body, None);
        let json: Value = serde_json::from_str(&result).unwrap();
        assert!(json.get("profileArn").is_none());
    }
}
```

- [ ] **Step 2: Export module in `src/kiro/endpoint/mod.rs`**

Change the module/export block to include AmazonQ:

```rust
pub mod amazonq;
pub mod cli;
pub mod codewhisperer;
pub mod ide;

pub use amazonq::AmazonqEndpoint;
pub use cli::CliEndpoint;
pub use codewhisperer::CodewhispererEndpoint;
pub use ide::IdeEndpoint;
```

- [ ] **Step 3: Run AmazonQ tests**

```bash
cd E:/kiro.rs
cargo test amazonq --no-fail-fast
```

Expected: AmazonQ endpoint tests pass.

- [ ] **Step 4: Commit**

```bash
cd E:/kiro.rs
git add src/kiro/endpoint/amazonq.rs src/kiro/endpoint/mod.rs
git commit -m "feat: add amazonq endpoint"
```

---

### Task 3: Register AmazonQ Endpoint in Main

**Files:**
- Modify: `src/main.rs:17`
- Modify: `src/main.rs:120-129`
- Test: `cargo build`

**Interfaces:**
- Consumes: `AmazonqEndpoint::new()` from Task 2.
- Produces: endpoint registry contains `amazonq`.

- [ ] **Step 1: Update import**

Replace current endpoint import with:

```rust
use kiro::endpoint::{AmazonqEndpoint, CliEndpoint, CodewhispererEndpoint, IdeEndpoint, KiroEndpoint};
```

- [ ] **Step 2: Register endpoint in registry block**

Change the registry block to:

```rust
    // 构建端点注册表
    let mut endpoints: HashMap<String, Arc<dyn KiroEndpoint>> = HashMap::new();
    {
        let ide = IdeEndpoint::new();
        endpoints.insert(ide.name().to_string(), Arc::new(ide));
        let cli = CliEndpoint::new();
        endpoints.insert(cli.name().to_string(), Arc::new(cli));
        let codewhisperer = CodewhispererEndpoint::new();
        endpoints.insert(codewhisperer.name().to_string(), Arc::new(codewhisperer));
        let amazonq = AmazonqEndpoint::new();
        endpoints.insert(amazonq.name().to_string(), Arc::new(amazonq));
    }
```

- [ ] **Step 3: Build**

```bash
cd E:/kiro.rs
cargo build
```

Expected: build succeeds.

- [ ] **Step 4: Commit**

```bash
cd E:/kiro.rs
git add src/main.rs
git commit -m "feat: register amazonq endpoint"
```

---

### Task 4: Add Endpoint Order and Fallback Classification Helpers

**Files:**
- Modify: `src/kiro/provider.rs:16`
- Modify: `src/kiro/provider.rs:121-187`
- Test: `cargo test endpoint_order fallbackable`

**Interfaces:**
- Consumes: config fields from Task 1.
- Consumes: registered endpoint names from Tasks 2-3.
- Produces: `KiroProvider::endpoint_order_for(&self, credentials: &KiroCredentials) -> anyhow::Result<Vec<Arc<dyn KiroEndpoint>>>`
- Produces: `KiroProvider::is_fallbackable_status(status: reqwest::StatusCode, body: &str, endpoint: &dyn KiroEndpoint) -> bool`
- Produces: `KiroProvider::is_fallbackable_network_error() -> bool`

- [ ] **Step 1: Add endpoint name imports**

At the top of `src/kiro/provider.rs`, replace:

```rust
use crate::kiro::endpoint::{KiroEndpoint, RequestContext};
```

with:

```rust
use crate::kiro::endpoint::{KiroEndpoint, RequestContext};
use crate::kiro::endpoint::amazonq::AMAZONQ_ENDPOINT_NAME;
use crate::kiro::endpoint::cli::CLI_ENDPOINT_NAME;
use crate::kiro::endpoint::codewhisperer::CODEWHISPERER_ENDPOINT_NAME;
use crate::kiro::endpoint::ide::IDE_ENDPOINT_NAME;
```

- [ ] **Step 2: Add fields to `KiroProvider`**

In `pub struct KiroProvider`, after `default_endpoint: String,`, add:

```rust
    /// Preferred endpoint when route-level fallback is enabled.
    preferred_endpoint: Option<String>,
    /// Whether to try Kiro-Go-compatible fallback endpoints on the same credential.
    endpoint_fallback: bool,
```

- [ ] **Step 3: Update `with_proxy` signature and body**

Change signature from:

```rust
    pub fn with_proxy(
        token_manager: Arc<MultiTokenManager>,
        proxy: Option<ProxyConfig>,
        endpoints: HashMap<String, Arc<dyn KiroEndpoint>>,
        default_endpoint: String,
    ) -> Self {
```

to:

```rust
    pub fn with_proxy(
        token_manager: Arc<MultiTokenManager>,
        proxy: Option<ProxyConfig>,
        endpoints: HashMap<String, Arc<dyn KiroEndpoint>>,
        default_endpoint: String,
        preferred_endpoint: Option<String>,
        endpoint_fallback: bool,
    ) -> Self {
```

After the existing `assert!(endpoints.contains_key(&default_endpoint), ...)`, add:

```rust
        if let Some(preferred) = preferred_endpoint.as_deref() {
            assert!(
                endpoints.contains_key(preferred),
                "preferred endpoint {} 未在 endpoints 注册表中",
                preferred
            );
        }
```

In the returned `Self`, after `default_endpoint,`, add:

```rust
            preferred_endpoint,
            endpoint_fallback,
```

- [ ] **Step 4: Add endpoint order helper below `endpoint_for`**

Add this function after `endpoint_for()`:

```rust
    /// Build endpoint order for API calls.
    ///
    /// When endpoint fallback is disabled, this returns the current single selected
    /// endpoint. When enabled, it tries the primary endpoint first and then the
    /// remaining Kiro-Go-compatible streaming endpoints.
    fn endpoint_order_for(
        &self,
        credentials: &KiroCredentials,
    ) -> anyhow::Result<Vec<Arc<dyn KiroEndpoint>>> {
        let primary = credentials
            .endpoint
            .as_deref()
            .or(self.preferred_endpoint.as_deref())
            .unwrap_or(&self.default_endpoint);

        if !self.endpoint_fallback || primary == CLI_ENDPOINT_NAME {
            return self
                .endpoints
                .get(primary)
                .cloned()
                .map(|endpoint| vec![endpoint])
                .ok_or_else(|| anyhow::anyhow!("未知端点: {}", primary));
        }

        const FALLBACK_ORDER: [&str; 3] = [
            IDE_ENDPOINT_NAME,
            CODEWHISPERER_ENDPOINT_NAME,
            AMAZONQ_ENDPOINT_NAME,
        ];

        if !FALLBACK_ORDER.contains(&primary) {
            return self
                .endpoints
                .get(primary)
                .cloned()
                .map(|endpoint| vec![endpoint])
                .ok_or_else(|| anyhow::anyhow!("未知端点: {}", primary));
        }

        let mut names = Vec::with_capacity(3);
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
```

- [ ] **Step 5: Add fallback classification helper below `endpoint_order_for`**

```rust
    fn is_fallbackable_status(
        status: reqwest::StatusCode,
        body: &str,
        endpoint: &dyn KiroEndpoint,
    ) -> bool {
        if status.as_u16() == 524 || endpoint.is_gateway_timeout(body) {
            return false;
        }
        if endpoint.is_client_validation_error(body) {
            return false;
        }
        matches!(status.as_u16(), 408 | 429) || status.is_server_error()
    }

    fn is_fallbackable_network_error() -> bool {
        true
    }
```

- [ ] **Step 6: Add provider tests at bottom of `provider.rs`**

If `provider.rs` already has a `#[cfg(test)] mod tests`, add to it. If not, append:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fallbackable_statuses() {
        struct Dummy;
        impl KiroEndpoint for Dummy {
            fn name(&self) -> &'static str { "dummy" }
            fn api_url(&self, _ctx: &RequestContext<'_>) -> String { String::new() }
            fn mcp_url(&self, _ctx: &RequestContext<'_>) -> String { String::new() }
            fn decorate_api(&self, req: reqwest::RequestBuilder, _ctx: &RequestContext<'_>) -> reqwest::RequestBuilder { req }
            fn decorate_mcp(&self, req: reqwest::RequestBuilder, _ctx: &RequestContext<'_>) -> reqwest::RequestBuilder { req }
            fn transform_api_body(&self, body: &str, _ctx: &RequestContext<'_>) -> String { body.to_string() }
        }
        let ep = Dummy;
        assert!(KiroProvider::is_fallbackable_status(reqwest::StatusCode::TOO_MANY_REQUESTS, "{}", &ep));
        assert!(KiroProvider::is_fallbackable_status(reqwest::StatusCode::REQUEST_TIMEOUT, "{}", &ep));
        assert!(KiroProvider::is_fallbackable_status(reqwest::StatusCode::BAD_GATEWAY, "{}", &ep));
        assert!(!KiroProvider::is_fallbackable_status(reqwest::StatusCode::BAD_REQUEST, "{}", &ep));
        assert!(!KiroProvider::is_fallbackable_status(reqwest::StatusCode::UNAUTHORIZED, "{}", &ep));
        assert!(!KiroProvider::is_fallbackable_status(reqwest::StatusCode::PAYMENT_REQUIRED, "{}", &ep));
        assert!(!KiroProvider::is_fallbackable_status(reqwest::StatusCode::FORBIDDEN, "{}", &ep));
    }
}
```

- [ ] **Step 7: Update `main.rs` constructor call**

In `src/main.rs`, update `KiroProvider::with_proxy(...)` call to:

```rust
    let kiro_provider = KiroProvider::with_proxy(
        token_manager.clone(),
        proxy_config.clone(),
        endpoints,
        config.default_endpoint.clone(),
        config.preferred_endpoint.clone(),
        config.endpoint_fallback,
    );
```

- [ ] **Step 8: Run focused tests**

```bash
cd E:/kiro.rs
cargo test fallbackable --no-fail-fast
cargo build
```

Expected: tests and build pass.

- [ ] **Step 9: Commit**

```bash
cd E:/kiro.rs
git add src/kiro/provider.rs src/main.rs
git commit -m "feat: add endpoint fallback ordering"
```

---

### Task 5: Refactor One API Attempt into Helper

**Files:**
- Modify: `src/kiro/provider.rs:464-1066`
- Test: `cargo build`

**Interfaces:**
- Consumes: endpoint order helper from Task 4.
- Produces: `ApiAttempt` enum internal to `provider.rs`.
- Produces: `send_api_attempt(...)` helper that performs one upstream endpoint attempt.
- Later Task 6 uses helper in fallback loop.

- [ ] **Step 1: Add internal attempt result types above `impl KiroProvider`**

Add after `pub struct KiroCallResult`:

```rust
struct ApiAttemptFailure {
    status: Option<reqwest::StatusCode>,
    body: String,
    outcome: &'static str,
    error: anyhow::Error,
}

enum ApiAttempt {
    Success(reqwest::Response),
    Failure(ApiAttemptFailure),
}
```

- [ ] **Step 2: Add helper method before `call_api_with_retry`**

Add this method inside `impl KiroProvider`, before `call_api_with_retry`:

```rust
    #[allow(clippy::too_many_arguments)]
    async fn send_api_attempt(
        &self,
        request_body: &str,
        is_stream: bool,
        api_type: &str,
        ctx: &crate::kiro::token_manager::CallContext,
        config: &crate::model::config::Config,
        machine_id: &str,
        endpoint: Arc<dyn KiroEndpoint>,
        attempt: usize,
        max_retries: usize,
        sink: Option<&dyn TraceSink>,
        attempt_start: Instant,
    ) -> ApiAttempt {
        let endpoint_name = endpoint.name();
        let rctx = RequestContext {
            credentials: &ctx.credentials,
            token: &ctx.token,
            machine_id,
            config,
        };

        let url = endpoint.api_url(&rctx);
        let body = endpoint.transform_api_body(request_body, &rctx);

        tracing::debug!("使用端点 [{}] POST {}", endpoint_name, url);
        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!("实际发送请求体: {}", truncate_for_log(&body));
        }

        let stream_reuse = config.stream_conn_reuse_enabled;
        let http_client = match if is_stream && !stream_reuse {
            self.streaming_client_for(&ctx.credentials)
        } else {
            self.client_for(&ctx.credentials)
        } {
            Ok(client) => client,
            Err(e) => {
                let message = e.to_string();
                Self::emit_attempt(
                    sink,
                    attempt,
                    ctx.id,
                    endpoint_name,
                    None,
                    outcome::NETWORK_ERROR,
                    Some(&message),
                    attempt_start,
                );
                return ApiAttempt::Failure(ApiAttemptFailure {
                    status: None,
                    body: message.clone(),
                    outcome: outcome::NETWORK_ERROR,
                    error: anyhow::anyhow!(message),
                });
            }
        };

        let base = http_client
            .post(&url)
            .body(body)
            .header("content-type", endpoint.content_type());
        let request = endpoint.decorate_api(base, &rctx);

        let request = match request.build() {
            Ok(request) => request,
            Err(e) => {
                let message = format!("构建请求失败: {}", e);
                Self::emit_attempt(
                    sink,
                    attempt,
                    ctx.id,
                    endpoint_name,
                    None,
                    outcome::UNKNOWN,
                    Some(&message),
                    attempt_start,
                );
                return ApiAttempt::Failure(ApiAttemptFailure {
                    status: None,
                    body: message.clone(),
                    outcome: outcome::UNKNOWN,
                    error: anyhow::anyhow!(message),
                });
            }
        };

        if tracing::enabled!(tracing::Level::DEBUG) {
            for (k, v) in request.headers() {
                tracing::debug!("  header {}: {}", k, v.to_str().unwrap_or("<binary>"));
            }
        }

        let response = match http_client.execute(request).await {
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
                    attempt,
                    ctx.id,
                    endpoint_name,
                    None,
                    outcome::NETWORK_ERROR,
                    Some(&e.to_string()),
                    attempt_start,
                );
                return ApiAttempt::Failure(ApiAttemptFailure {
                    status: None,
                    body: e.to_string(),
                    outcome: outcome::NETWORK_ERROR,
                    error: e.into(),
                });
            }
        };

        let status = response.status();
        if status.is_success() {
            Self::emit_attempt(
                sink,
                attempt,
                ctx.id,
                endpoint_name,
                Some(status.as_u16()),
                outcome::SUCCESS,
                None,
                attempt_start,
            );
            return ApiAttempt::Success(response);
        }

        let body = response.text().await.unwrap_or_default();
        let failure_error = anyhow::anyhow!("{} API 请求失败: {} {}", api_type, status, body);
        ApiAttempt::Failure(ApiAttemptFailure {
            status: Some(status),
            body,
            outcome: outcome::UNKNOWN,
            error: failure_error,
        })
    }
```

- [ ] **Step 3: Build without changing call path**

```bash
cd E:/kiro.rs
cargo build
```

Expected: build succeeds. The helper may be unused; if compiler warns, ignore warning for now.

- [ ] **Step 4: Commit**

```bash
cd E:/kiro.rs
git add src/kiro/provider.rs
git commit -m "refactor: extract api endpoint attempt helper"
```

---

### Task 6: Implement Same-Account Endpoint Fallback in API Retry Loop

**Files:**
- Modify: `src/kiro/provider.rs:496-1066`
- Test: `cargo build`
- Test: `cargo test fallbackable amazonq codewhisperer`

**Interfaces:**
- Consumes: `endpoint_order_for()` from Task 4.
- Consumes: `send_api_attempt()` from Task 5.
- Produces: fallback behavior inside `call_api_with_retry`.

- [ ] **Step 1: Replace single endpoint lookup block**

In `call_api_with_retry`, replace the current `let endpoint = match self.endpoint_for...` through request-send/status read section with endpoint-order loop. Keep the existing error-handling branches after body/status, but nest them around a selected failure.

Use this structure immediately after `let machine_id = ...;`:

```rust
            let endpoints = match self.endpoint_order_for(&ctx.credentials) {
                Ok(e) => e,
                Err(e) => {
                    Self::emit_attempt(
                        sink,
                        attempt,
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

            let mut selected_failure: Option<(Arc<dyn KiroEndpoint>, ApiAttemptFailure)> = None;
            let endpoint_count = endpoints.len();

            for (endpoint_index, endpoint) in endpoints.into_iter().enumerate() {
                let endpoint_name = endpoint.name();
                let endpoint_attempt_start = if endpoint_index == 0 {
                    attempt_start
                } else {
                    Instant::now()
                };

                match self
                    .send_api_attempt(
                        request_body,
                        is_stream,
                        api_type,
                        &ctx,
                        config,
                        &machine_id,
                        endpoint.clone(),
                        attempt,
                        max_retries,
                        sink,
                        endpoint_attempt_start,
                    )
                    .await
                {
                    ApiAttempt::Success(response) => {
                        self.token_manager.report_success(ctx.id);
                        return Ok(KiroCallResult {
                            response,
                            credential_id: ctx.id,
                            account_guard: ctx,
                        });
                    }
                    ApiAttempt::Failure(failure) => {
                        let should_try_next = if let Some(status) = failure.status {
                            Self::is_fallbackable_status(status, &failure.body, endpoint.as_ref())
                        } else {
                            Self::is_fallbackable_network_error()
                        };

                        if should_try_next && endpoint_index + 1 < endpoint_count {
                            tracing::warn!(
                                "Endpoint {} failed with fallbackable error; trying next fallback endpoint on credential #{}",
                                endpoint_name,
                                ctx.id
                            );
                            selected_failure = Some((endpoint, failure));
                            continue;
                        }

                        selected_failure = Some((endpoint, failure));
                        break;
                    }
                }
            }

            let Some((endpoint, failure)) = selected_failure else {
                last_error = Some(anyhow::anyhow!("{} API 请求失败：没有可用 endpoint", api_type));
                continue;
            };
            let endpoint_name = endpoint.name();
            let Some(status) = failure.status else {
                last_error = Some(failure.error);
                if attempt + 1 < max_retries {
                    sleep(Self::retry_delay(attempt)).await;
                }
                continue;
            };
            let body = failure.body;
```

Then keep the existing status/body handling branches below this point, but remove the old duplicated request construction and response send code.

- [ ] **Step 2: Preserve existing error handling order**

Ensure the remaining branches stay in this exact order:

```text
402 quota exhausted
400 bad request
401/403 auth or account throttle
429 suspicious activity account throttle
client validation error
524 gateway timeout
429 USER_REQUEST_RATE_EXCEEDED
408/429/5xx transient
other 4xx
unknown fallback
```

Important: because fallback already happened before these branches, `429 USER_REQUEST_RATE_EXCEEDED` here means all fallback endpoints failed or fallback was disabled. Keep existing cooldown logic here.

- [ ] **Step 3: Update logging for user rate limit branch**

In the `429 USER_REQUEST_RATE_EXCEEDED` branch, change warning text from:

```rust
"API 请求失败（账号 #{} 请求速率超限，短冷却 {}s 并切换，尝试 {}/{}，剩余可用 {}）: {}"
```

to:

```rust
"API 请求失败（账号 #{} 所有可用端点均请求速率超限，短冷却 {}s 并切换，尝试 {}/{}，剩余可用 {}）: {}"
```

- [ ] **Step 4: Build and focused tests**

```bash
cd E:/kiro.rs
cargo build
cargo test fallbackable amazonq codewhisperer --no-fail-fast
```

Expected: build succeeds and focused tests pass.

- [ ] **Step 5: Commit**

```bash
cd E:/kiro.rs
git add src/kiro/provider.rs
git commit -m "feat: fallback across endpoints before account cooldown"
```

---

### Task 7: Validate Config and Document It

**Files:**
- Modify: `src/main.rs:129-148`
- Modify: `config.example.json`
- Modify: `README.md`
- Test: `cargo build`

**Interfaces:**
- Consumes: `Config.preferred_endpoint` and `Config.endpoint_fallback` from Task 1.
- Produces: startup validation catches unknown preferred endpoint.
- Produces: docs for `amazonq`, `preferredEndpoint`, `endpointFallback`.

- [ ] **Step 1: Validate preferred endpoint in `main.rs`**

After default endpoint validation, add:

```rust
    if let Some(preferred) = config.preferred_endpoint.as_deref() {
        if !endpoints.contains_key(preferred) {
            tracing::error!("preferredEndpoint \"{}\" 未注册", preferred);
            std::process::exit(1);
        }
    }
```

- [ ] **Step 2: Update `config.example.json`**

Change file to:

```json
{
  "host": "127.0.0.1",
  "port": 8990,
  "apiKey": "sk-kiro-rs-qazWSXedcRFV123456",
  "tlsBackend": "rustls",
  "region": "us-east-1",
  "adminApiKey": "sk-admin-your-secret-key",
  "defaultEndpoint": "ide",
  "preferredEndpoint": "codewhisperer",
  "endpointFallback": false,
  "updateAutoApply": false,
  "updateAutoApplyTime": "03:00",
  "traceEnabled": true,
  "traceRetentionDays": 7,
  "usageLogRetentionDays": 31
}
```

- [ ] **Step 3: Update README config field table**

In `README.md`, update endpoint rows to include:

```markdown
| `defaultEndpoint` | `ide` | 凭据未指定 endpoint 时使用的端点（`ide` / `cli` / `codewhisperer` / `amazonq`） |
| `preferredEndpoint` | 未配置 | 开启 `endpointFallback` 时优先尝试的端点；未配置时使用 `defaultEndpoint` |
| `endpointFallback` | `false` | 是否按 Kiro-Go 行为在同一账号内尝试 `ide` / `codewhisperer` / `amazonq` fallback |
```

- [ ] **Step 4: Update README credential endpoint field**

Change credential field row to:

```markdown
| `endpoint` | `ide` / `cli` / `codewhisperer` / `amazonq`，未填使用 `config.defaultEndpoint`；开启 `endpointFallback` 时作为该凭据的 primary endpoint |
```

- [ ] **Step 5: Add short usage snippet to README**

Near endpoint/config section, add:

```markdown
### Kiro-Go endpoint fallback

若要对齐 Kiro-Go 的 route-level fallback 行为，可显式开启：

```json
{
  "defaultEndpoint": "codewhisperer",
  "preferredEndpoint": "codewhisperer",
  "endpointFallback": true
}
```

开启后，API 请求会先尝试 primary endpoint，再在同一账号内按 Kiro-Go-compatible route fallback：

```text
codewhisperer -> ide -> amazonq
```

仅 `429` / `408` / `5xx` / 网络错误会触发 endpoint fallback；`400` / `401` / `402` / `403` / `524` 不会 fallback。
```

- [ ] **Step 6: Build**

```bash
cd E:/kiro.rs
cargo build
```

Expected: build succeeds.

- [ ] **Step 7: Commit**

```bash
cd E:/kiro.rs
git add src/main.rs config.example.json README.md
git commit -m "docs: document endpoint fallback config"
```

---

### Task 8: Full Verification and Manual Load Test

**Files:**
- No source changes expected.
- Runtime config: `E:/kiro.rs/config.json` local only, not committed unless explicitly requested.

**Interfaces:**
- Consumes: all implementation tasks.
- Produces: verification result and before/after comparison.

- [ ] **Step 1: Run full build and focused tests**

```bash
cd E:/kiro.rs
cargo build
cargo test amazonq --no-fail-fast
cargo test codewhisperer --no-fail-fast
cargo test fallbackable --no-fail-fast
```

Expected: all commands pass.

- [ ] **Step 2: Set local runtime config for fallback test**

Edit local `E:/kiro.rs/config.json` to include:

```json
"defaultEndpoint": "codewhisperer",
"preferredEndpoint": "codewhisperer",
"endpointFallback": true
```

Do not commit `config.json`.

- [ ] **Step 3: Restart debug server**

```bash
cd E:/kiro.rs
RUST_LOG=info ./target/debug/kiro-rs.exe
```

Expected startup logs include:

```text
已加载 30 个凭据配置
Admin API 已启用
Admin UI 已启用: /admin
启动 Anthropic API 端点: 0.0.0.0:8990
```

- [ ] **Step 4: Smoke models endpoint**

```bash
curl -s -o /tmp/models.json -w "%{http_code}\n" \
  -H "x-api-key: csk_K5rm33Jbnak7H2KimvtDdVsl3goXiVHR" \
  http://127.0.0.1:8990/v1/models
```

Expected:

```text
200
```

- [ ] **Step 5: Run 100 concurrent / 1 account test if a single-account group exists**

If Admin UI has a group containing exactly one credential, bind a client key to that group. Then run:

```bash
cd E:/kiro.rs
cat > /tmp/kiro_payload.json <<'JSON'
{"model":"claude-sonnet-4-5-20250929","max_tokens":8,"stream":false,"messages":[{"role":"user","content":"Reply with ok."}]}
JSON
rm -f /tmp/kiro_fallback_100_resp_* /tmp/kiro_fallback_100_results.txt
seq 1 100 | xargs -P100 -I{} sh -c 'curl -s -o /tmp/kiro_fallback_100_resp_{} -w "%{http_code} %{time_total}\n" -H "x-api-key: csk_K5rm33Jbnak7H2KimvtDdVsl3goXiVHR" -H "anthropic-version: 2023-06-01" -H "content-type: application/json" --data-binary @/tmp/kiro_payload.json http://127.0.0.1:8990/v1/messages' > /tmp/kiro_fallback_100_results.txt
cut -d' ' -f1 /tmp/kiro_fallback_100_results.txt | sort | uniq -c
```

Expected: status distribution printed. Compare with Kiro-Go claim rather than assuming zero 429.

- [ ] **Step 6: Run 400 concurrent / 30 accounts**

```bash
cd E:/kiro.rs
rm -f /tmp/kiro_fallback_400_resp_* /tmp/kiro_fallback_400_results.txt
seq 1 400 | xargs -P400 -I{} sh -c 'curl -s -o /tmp/kiro_fallback_400_resp_{} -w "%{http_code} %{time_total}\n" -H "x-api-key: csk_K5rm33Jbnak7H2KimvtDdVsl3goXiVHR" -H "anthropic-version: 2023-06-01" -H "content-type: application/json" --data-binary @/tmp/kiro_payload.json http://127.0.0.1:8990/v1/messages' > /tmp/kiro_fallback_400_results.txt
printf 'STATUS DISTRIBUTION\n'
cut -d' ' -f1 /tmp/kiro_fallback_400_results.txt | sort | uniq -c
printf 'RATE LIMIT BODY COUNT '
for f in /tmp/kiro_fallback_400_resp_*; do grep -q 'USER_REQUEST_RATE_EXCEEDED\|Too many requests\|429' "$f" && printf x; done | wc -c
```

Expected: collect counts. Compare to baseline `22 HTTP 200 / 378 HTTP 502` for `codewhisperer` only.

- [ ] **Step 7: Run 400 requests paced over ~60s**

```bash
cd E:/kiro.rs
rm -f /tmp/kiro_fallback_60s_status_* /tmp/kiro_fallback_60s_resp_*
request_one() {
  i="$1"
  code=$(curl -s -o "/tmp/kiro_fallback_60s_resp_${i}" -w "%{http_code} %{time_total}" \
    -H "x-api-key: csk_K5rm33Jbnak7H2KimvtDdVsl3goXiVHR" \
    -H "anthropic-version: 2023-06-01" \
    -H "content-type: application/json" \
    --data-binary @/tmp/kiro_payload.json \
    http://127.0.0.1:8990/v1/messages)
  printf '%s %s\n' "$i" "$code" > "/tmp/kiro_fallback_60s_status_${i}"
}
start=$(date +%s)
for i in $(seq 1 400); do
  request_one "$i" &
  sleep 0.15
done
wait
end=$(date +%s)
printf 'DURATION_SECONDS %s\n' "$((end-start))"
printf 'STATUS_DISTRIBUTION\n'
awk '{print $2}' /tmp/kiro_fallback_60s_status_* | sort | uniq -c
printf 'RATE_LIMIT_BODY_COUNT '
for f in /tmp/kiro_fallback_60s_resp_*; do grep -q 'USER_REQUEST_RATE_EXCEEDED\|Too many requests\|429' "$f" && printf x; done | wc -c
```

Expected: collect counts. Compare to baseline `38 HTTP 200 / 362 HTTP 502` for paced `codewhisperer` only.

- [ ] **Step 8: Inspect traces for endpoint sequence**

Open Admin UI:

```text
http://127.0.0.1:8990/admin
```

Check request traces. Expected: failed requests show multiple endpoint attempts in order, e.g.:

```text
codewhisperer -> ide -> amazonq
```

- [ ] **Step 9: Final status report**

Report:

```text
Build: PASS/FAIL
Tests: PASS/FAIL
100 concurrent / 1 account: <counts or skipped reason>
400 concurrent / 30 accounts: <counts>
400 / 60s / 30 accounts: <counts>
Trace endpoint fallback observed: yes/no
Compared to baseline: improved / unchanged / worse
Dirty files not committed: <git status --short>
```

- [ ] **Step 10: Commit verification doc only if a source/doc change was made**

If no file changed, do not commit. If README/spec/plan is updated with results:

```bash
cd E:/kiro.rs
git add <changed-docs>
git commit -m "docs: record endpoint fallback verification"
```

---

## Self-Review

Spec coverage:
- Add `amazonq` endpoint: Task 2 and Task 3.
- Add config `preferredEndpoint` / `endpointFallback`: Task 1 and Task 7.
- Preserve backward compatibility: Task 1 defaults, Task 4 order helper, Task 6 call path.
- Same-account fallback before account cooldown: Task 6.
- Fallback classification: Task 4 helper and Task 6 branch order.
- Trace/log endpoint sequence: Task 6 emits attempts per endpoint through `send_api_attempt`; Task 8 verifies in Admin traces.
- Docs/config examples: Task 7.
- Manual load tests: Task 8.

Placeholder scan:
- No TBD/TODO placeholders.
- No "similar to" references requiring hidden context.
- Each code step includes exact snippets or explicit replacement blocks.

Type consistency:
- `preferred_endpoint` and `endpoint_fallback` follow serde rename_all camelCase, producing JSON `preferredEndpoint` and `endpointFallback`.
- Endpoint struct names match existing style: `CodewhispererEndpoint`, so new type is `AmazonqEndpoint`.
- Endpoint constant names match module pattern: `AMAZONQ_ENDPOINT_NAME`.
- Provider helper signatures consistently use `Arc<dyn KiroEndpoint>`, `KiroCredentials`, `RequestContext`, and existing `TraceSink`.
