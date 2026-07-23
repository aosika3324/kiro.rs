//! i7relay webhook 接收 + status 只读端点。

use crate::admin::AdminState;
use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

/// POST /api/admin/i7relay/webhook (挂免鉴权 public 组,靠 X-Webhook-Secret 校验)。
///
/// 事件:new_keys_available → 补货;all_keys_dead → 死号对账禁用 + 补货。
/// 立即 200,重活异步 spawn(不阻塞 i7relay 回调)。
pub async fn i7relay_webhook(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let Some(rt) = super::runtime() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "i7relay 未启用").into_response();
    };
    // 共享密钥校验(常数时间比较避免时序侧信道)。
    let provided = headers
        .get("x-webhook-secret")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !secret_eq(provided, &rt.config.webhook_secret) {
        return (StatusCode::UNAUTHORIZED, "invalid webhook secret").into_response();
    }

    let event = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v.get("event").and_then(|e| e.as_str()).map(String::from))
        .unwrap_or_default();

    let service = state.service.clone();
    tokio::spawn(async move {
        let rt = super::runtime().expect("runtime present");
        let client = match super::I7relayClient::new(&rt.config, rt.tls_backend) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("i7relay webhook: 建 client 失败: {e}");
                return;
            }
        };
        match event.as_str() {
            "all_keys_dead" => {
                super::sync_dead_keys(&client, &service, &rt.audit, super::RestockTrigger::WebhookAllDead).await;
                super::restock(&client, &rt.config, &service, &rt.audit, super::RestockTrigger::WebhookAllDead).await;
            }
            "new_keys_available" => {
                super::restock(&client, &rt.config, &service, &rt.audit, super::RestockTrigger::WebhookNewKeys).await;
            }
            other => tracing::info!("i7relay webhook: 忽略未知事件 {other}"),
        }
    });

    (StatusCode::OK, "ok").into_response()
}

/// GET /api/admin/config/i7relay (鉴权)。脱敏配置。
pub async fn get_i7relay_config(State(state): State<AdminState>) -> impl IntoResponse {
    Json(state.service.get_i7relay_config())
}

/// PUT /api/admin/config/i7relay (鉴权)。保存配置(持久化+热生效)。
pub async fn set_i7relay_config(
    State(state): State<AdminState>,
    Json(payload): Json<crate::admin::types::SetI7relayConfigRequest>,
) -> impl IntoResponse {
    match state.service.set_i7relay_config(payload) {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/i7relay/restock-now (鉴权)。手动立即拉号(扣配额)。
pub async fn i7relay_restock_now(State(state): State<AdminState>) -> impl IntoResponse {
    let Some(rt) = super::runtime() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "i7relay 未就绪").into_response();
    };
    let client = match super::I7relayClient::new(&rt.config, rt.tls_backend) {
        Ok(c) => c,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("建 client 失败: {e}")).into_response(),
    };
    match super::restock(&client, &rt.config, &state.service, &rt.audit, super::RestockTrigger::Manual).await {
        Some(out) => Json(serde_json::json!({
            "imported": out.imported,
            "duplicate": out.duplicate,
            "failed": out.failed,
            "remainingQuota": out.remaining_quota,
        }))
        .into_response(),
        None => (StatusCode::TOO_MANY_REQUESTS, "已有补货进行中,请稍后再试").into_response(),
    }
}

/// GET /api/admin/i7relay/quota (鉴权)。查配额(name/used/max/remaining)。
pub async fn i7relay_quota(State(_state): State<AdminState>) -> impl IntoResponse {
    let Some(rt) = super::runtime() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "i7relay 未就绪").into_response();
    };
    let client = match super::I7relayClient::new(&rt.config, rt.tls_backend) {
        Ok(c) => c,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("建 client 失败: {e}")).into_response(),
    };
    match client.profile().await {
        Ok(p) => Json(p).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, format!("查配额失败: {e}")).into_response(),
    }
}

/// POST /api/admin/i7relay/register-webhook (鉴权)。把本服务回调地址注册到取号站。
/// body: {"webhookUrl": "https://.../api/admin/webhook/account-refill"}
pub async fn i7relay_register_webhook(
    State(_state): State<AdminState>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    let Some(url) = payload.get("webhookUrl").and_then(|v| v.as_str()) else {
        return (StatusCode::BAD_REQUEST, "缺少 webhookUrl").into_response();
    };
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return (StatusCode::BAD_REQUEST, "webhookUrl 需以 http(s):// 开头").into_response();
    }
    let Some(rt) = super::runtime() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "i7relay 未就绪").into_response();
    };
    let client = match super::I7relayClient::new(&rt.config, rt.tls_backend) {
        Ok(c) => c,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("建 client 失败: {e}")).into_response(),
    };
    match client.set_webhook(url).await {
        Ok(()) => Json(serde_json::json!({"ok": true, "webhookUrl": url})).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, format!("注册 webhook 失败: {e}")).into_response(),
    }
}

/// GET /api/admin/i7relay/status (鉴权组)。只读:配置摘要 + 池内 i7relay 凭据数 + 最近补货记录。
pub async fn i7relay_status(State(state): State<AdminState>) -> impl IntoResponse {
    let Some(rt) = super::runtime() else {
        return Json(serde_json::json!({ "enabled": false })).into_response();
    };
    let creds = state.service.get_all_credentials().credentials;
    let (mut total, mut active) = (0u32, 0u32);
    for c in &creds {
        if c.source_channel.as_deref() == Some("i7relay") {
            total += 1;
            if !c.disabled {
                active += 1;
            }
        }
    }
    let recent = rt.audit.recent(20);
    Json(serde_json::json!({
        "enabled": rt.config.enabled,
        "baseUrl": rt.config.base_url,
        "purchaseCount": rt.config.purchase_count,
        "restockThreshold": rt.config.restock_threshold,
        "pollIntervalSecs": rt.config.poll_interval_secs,
        "deadKeyAction": rt.config.dead_key_action,
        "poolI7relayTotal": total,
        "poolI7relayActive": active,
        "recentRestocks": recent,
    }))
    .into_response()
}

/// 常数时间字符串比较(空密钥视为未配置 → 拒绝)。
fn secret_eq(a: &str, b: &str) -> bool {
    if b.is_empty() || a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.bytes().zip(b.bytes()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::secret_eq;

    #[test]
    fn secret_eq_rejects_empty_config_and_mismatch() {
        assert!(!secret_eq("anything", ""), "未配置密钥应拒绝一切");
        assert!(!secret_eq("", ""), "空 vs 空也拒绝(未配置)");
        assert!(!secret_eq("abc", "abcd"), "长度不同拒绝");
        assert!(!secret_eq("abcd", "abce"), "内容不同拒绝");
        assert!(secret_eq("s3cret-token", "s3cret-token"), "完全一致通过");
    }
}
