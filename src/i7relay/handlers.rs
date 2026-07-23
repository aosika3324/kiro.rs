//! i7relay webhook 接收 + status 只读端点。

use crate::admin::AdminState;
use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use std::collections::HashMap;

/// POST /api/admin/webhook/account-refill (免鉴权 public 组)。
///
/// 供应商回调是**裸 POST 无鉴权头**(见 API 文档),故校验 URL 里的 `?token=<secret>`
/// (兼容 `X-Webhook-Secret` 头)。若未配置 webhookSecret 则放行 + WARN(仅耗自身配额)。
/// 事件:new_keys_available → 补货;all_keys_dead → 死号对账禁用 + 补货。
/// 立即 200,重活异步 spawn(不阻塞 i7relay 回调)。
pub async fn i7relay_webhook(
    State(state): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let Some(rt) = super::runtime() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "i7relay 未启用").into_response();
    };
    let secret = &rt.config.webhook_secret;
    if secret.is_empty() {
        // 未设密钥:放行但告警(低风险:仅耗自身配额 + 有冷却门)。
        tracing::warn!("i7relay webhook 未配置 secret,放行(建议设置 webhookSecret 防误触发)");
    } else {
        // token 优先取查询参数,回退 X-Webhook-Secret 头;常数时间比较。
        let provided = q
            .get("token")
            .map(|s| s.as_str())
            .or_else(|| headers.get("x-webhook-secret").and_then(|v| v.to_str().ok()))
            .unwrap_or("");
        if !secret_eq(provided, secret) {
            return (StatusCode::UNAUTHORIZED, "invalid webhook token").into_response();
        }
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
            "error": out.error,
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
    // 最近一次成功(有导入)时间 + 最近一次错误(供健康展示)。
    let last_success_at = recent
        .iter()
        .find(|r| r.imported > 0)
        .map(|r| r.at.clone());
    let last_error = recent
        .iter()
        .find(|r| r.error.is_some())
        .and_then(|r| r.error.clone());
    Json(serde_json::json!({
        "enabled": rt.config.enabled,
        "baseUrl": rt.config.base_url,
        "purchaseCount": rt.config.purchase_count,
        "restockThreshold": rt.config.restock_threshold,
        "pollIntervalSecs": rt.config.poll_interval_secs,
        "deadKeyAction": rt.config.dead_key_action,
        "poolI7relayTotal": total,
        "poolI7relayActive": active,
        "lastSuccessAt": last_success_at,
        "lastError": last_error,
        "recentRestocks": recent,
    }))
    .into_response()
}

/// GET /api/admin/i7relay/stock (鉴权组)。本轮最大可提取数量 {max}。
pub async fn i7relay_stock(State(_state): State<AdminState>) -> impl IntoResponse {
    let Some(rt) = super::runtime() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "i7relay 未就绪").into_response();
    };
    let client = match super::I7relayClient::new(&rt.config, rt.tls_backend) {
        Ok(c) => c,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("建 client 失败: {e}")).into_response(),
    };
    match client.stock_max().await {
        Ok(max) => Json(serde_json::json!({ "max": max })).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, format!("查库存失败: {e}")).into_response(),
    }
}

/// GET /api/admin/i7relay/system-status (鉴权组)。供应商系统状态(原样透传 JSON)。
pub async fn i7relay_system_status(State(_state): State<AdminState>) -> impl IntoResponse {
    let Some(rt) = super::runtime() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "i7relay 未就绪").into_response();
    };
    let client = match super::I7relayClient::new(&rt.config, rt.tls_backend) {
        Ok(c) => c,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("建 client 失败: {e}")).into_response(),
    };
    match client.system_status().await {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, format!("查系统状态失败: {e}")).into_response(),
    }
}

/// POST /api/admin/i7relay/test-webhook (鉴权组)。让供应商向我方 webhook 推测试消息。
pub async fn i7relay_test_webhook(State(_state): State<AdminState>) -> impl IntoResponse {
    let Some(rt) = super::runtime() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "i7relay 未就绪").into_response();
    };
    let client = match super::I7relayClient::new(&rt.config, rt.tls_backend) {
        Ok(c) => c,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("建 client 失败: {e}")).into_response(),
    };
    match client.test_webhook().await {
        Ok(()) => Json(serde_json::json!({ "ok": true })).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, format!("测试 webhook 失败: {e}")).into_response(),
    }
}

/// GET /api/admin/i7relay/extracts?limit= (鉴权组)。只读:最近每 key 提取记录(新→旧)。
pub async fn i7relay_extracts(
    State(_state): State<AdminState>,
    Query(q): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let Some(rt) = super::runtime() else {
        return Json(serde_json::json!({ "extracts": [] })).into_response();
    };
    let limit = q
        .get("limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 500);
    Json(serde_json::json!({ "extracts": rt.audit.recent_extracts(limit) })).into_response()
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
