//! 补货编排:purchase → 验活导入 → 审计;死号对账禁用。
//!
//! 与 [`crate::admin::service::AdminService`] 解耦——所需能力经参数传入,便于单测。

use super::audit::{I7relayAudit, RestockRecord};
use super::client::I7relayClient;
use crate::admin::service::{AdminService, ImportStatus};
use crate::admin::types::AddCredentialRequest;
use crate::model::config::I7relayConfig;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;

/// 全局补货串行 + 冷却门:同一时刻只允许一次补货,且两次间隔 >= cooldown。
/// 值为上次补货完成时刻(None = 从未)。
static RESTOCK_GATE: AsyncMutex<Option<Instant>> = AsyncMutex::const_new(None);

/// 补货触发来源(进审计的 trigger 字段)。
#[derive(Debug, Clone, Copy)]
pub enum RestockTrigger {
    WebhookNewKeys,
    WebhookAllDead,
    Poll,
    Manual,
}

impl RestockTrigger {
    fn as_str(self) -> &'static str {
        match self {
            RestockTrigger::WebhookNewKeys => "webhook:new_keys_available",
            RestockTrigger::WebhookAllDead => "webhook:all_keys_dead",
            RestockTrigger::Poll => "poll",
            RestockTrigger::Manual => "manual",
        }
    }
}

/// 一次补货的结果汇总。
#[derive(Debug, Default, Clone)]
pub struct RestockOutcome {
    pub imported: usize,
    pub duplicate: usize,
    pub failed: usize,
    pub remaining_quota: i64,
}

fn ksk_prefix(k: &str) -> String {
    k.chars().take(12).collect()
}

/// sha256(ksk_) —— 与 token_manager 的 api_key_hash 同算法,用于死号匹配。
pub fn api_key_hash(key: &str) -> String {
    let mut h = Sha256::new();
    h.update(key.as_bytes());
    format!("{:x}", h.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_key_hash_matches_sha256_hex() {
        // 与 token_manager::sha256_hex 同算法(sha256 十六进制小写)。
        let h = api_key_hash("ksk_abc123");
        assert_eq!(h.len(), 64, "sha256 hex 应 64 字符");
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
        // 稳定性:同输入同输出。
        assert_eq!(h, api_key_hash("ksk_abc123"));
        assert_ne!(h, api_key_hash("ksk_abc124"));
    }

    #[test]
    fn ksk_prefix_takes_first_12() {
        assert_eq!(ksk_prefix("ksk_AzD0xk4sMBpMWBGh"), "ksk_AzD0xk4s");
        assert_eq!(ksk_prefix("short"), "short");
    }

    #[test]
    fn build_import_req_sets_apikey_credential() {
        let req = build_import_req("ksk_xyz").expect("build ok");
        assert_eq!(req.kiro_api_key.as_deref(), Some("ksk_xyz"));
        assert_eq!(req.auth_method, "apikey");
        assert_eq!(req.source_channel.as_deref(), Some("i7relay"));
        // refresh_token 不设(API Key 凭据不需要)。
        assert!(req.refresh_token.is_none());
    }

    #[test]
    fn trigger_labels_stable() {
        assert_eq!(RestockTrigger::WebhookNewKeys.as_str(), "webhook:new_keys_available");
        assert_eq!(RestockTrigger::WebhookAllDead.as_str(), "webhook:all_keys_dead");
        assert_eq!(RestockTrigger::Poll.as_str(), "poll");
        assert_eq!(RestockTrigger::Manual.as_str(), "manual");
    }
}

/// 把一个 `ksk_` 组装成 API Key 凭据导入请求(仅设 kiroApiKey/authMethod/sourceChannel,余走默认)。
fn build_import_req(ksk: &str) -> anyhow::Result<AddCredentialRequest> {
    let v = serde_json::json!({
        "kiroApiKey": ksk,
        "authMethod": "apikey",
        "sourceChannel": "i7relay",
    });
    Ok(serde_json::from_value(v)?)
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// 自动拉取(webhook/poll)失败或空结果时的重试间隔与次数。
const RETRY_INTERVAL_SECS: u64 = 30;
const RETRY_MAX_ATTEMPTS: u32 = 3;

/// purchase 重试:`retry=true`(自动触发)时,失败**或拉到 0 个**都等 30s 再试,最多 3 次;
/// `retry=false`(手动"立即拉取")只试一次,即时返回结果。
async fn purchase_with_retry(
    client: &I7relayClient,
    count: u32,
    retry: bool,
) -> anyhow::Result<(Vec<String>, i64)> {
    let max = if retry { RETRY_MAX_ATTEMPTS } else { 1 };
    let mut last_err = None;
    for attempt in 0..max {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(RETRY_INTERVAL_SECS)).await;
            tracing::info!("i7relay purchase 重试 #{attempt}(间隔 {RETRY_INTERVAL_SECS}s)");
        }
        match client.purchase(count).await {
            // 拿到 key:成功返回。
            Ok(v) if !v.0.is_empty() => return Ok(v),
            // 拉到 0 个:自动模式视为可重试,手动模式直接返回空。
            Ok(empty) => {
                if !retry {
                    return Ok(empty);
                }
                tracing::info!("i7relay purchase 第 {} 次拉到 0 个,待重试", attempt + 1);
                last_err = Some(anyhow::anyhow!("拉取到 0 个 key"));
            }
            Err(e) => {
                tracing::warn!("i7relay purchase 第 {} 次失败: {e}", attempt + 1);
                last_err = Some(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("purchase 未知失败")))
}

/// 补货:冷却门内 purchase(count) → 逐个验活导入 → 写审计。
/// 返回 None 表示被冷却门跳过(距上次补货未到 cooldown)。
pub async fn restock(
    client: &I7relayClient,
    cfg: &I7relayConfig,
    service: &Arc<AdminService>,
    audit: &I7relayAudit,
    trigger: RestockTrigger,
) -> Option<RestockOutcome> {
    // 串行门用 try_lock:已有补货在跑(可能正处于 30s 重试等待)则**立即跳过**,
    // 绝不阻塞调用方——手动"立即拉取"因此不会被 webhook 的重试拖住。
    let is_manual = matches!(trigger, RestockTrigger::Manual);
    let mut gate = match RESTOCK_GATE.try_lock() {
        Ok(g) => g,
        Err(_) => {
            tracing::info!("i7relay 已有补货进行中,跳过(trigger={})", trigger.as_str());
            return None;
        }
    };
    // 自动触发(webhook/poll)受 30s 冷却门;手动不受限。
    if !is_manual {
        if let Some(last) = *gate {
            if last.elapsed() < Duration::from_secs(cfg.cooldown_secs) {
                tracing::info!("i7relay 补货被冷却门跳过(trigger={})", trigger.as_str());
                return None;
            }
        }
    }

    let count = cfg.purchase_count.max(1);
    let (keys, remaining) = match purchase_with_retry(client, count, !is_manual).await {
        Ok(v) => v,
        Err(e) => {
            audit.record(RestockRecord {
                at: now_rfc3339(),
                trigger: trigger.as_str().to_string(),
                requested: count,
                imported: 0,
                duplicate: 0,
                failed: 0,
                disabled: 0,
                remaining_quota: -1,
                key_prefixes: vec![],
                error: Some(format!("purchase 失败(重试耗尽): {e}")),
            });
            tracing::warn!("i7relay purchase 失败(重试耗尽): {e}");
            return Some(RestockOutcome::default());
        }
    };

    let mut out = RestockOutcome { remaining_quota: remaining, ..Default::default() };
    let mut prefixes = Vec::new();
    for ksk in &keys {
        if prefixes.len() < 8 {
            prefixes.push(ksk_prefix(ksk));
        }
        let req = match build_import_req(ksk) {
            Ok(r) => r,
            Err(_) => {
                out.failed += 1;
                continue;
            }
        };
        let res = service.import_one_credential(req, cfg.verify_on_import).await;
        match res.status {
            ImportStatus::Verified | ImportStatus::Imported => out.imported += 1,
            ImportStatus::Duplicate => out.duplicate += 1,
            ImportStatus::Failed => out.failed += 1,
        }
    }

    audit.record(RestockRecord {
        at: now_rfc3339(),
        trigger: trigger.as_str().to_string(),
        requested: count,
        imported: out.imported,
        duplicate: out.duplicate,
        failed: out.failed,
        disabled: 0,
        remaining_quota: remaining,
        key_prefixes: prefixes,
        error: None,
    });
    tracing::info!(
        "i7relay 补货完成(trigger={}): imported={} dup={} failed={} remaining_quota={}",
        trigger.as_str(), out.imported, out.duplicate, out.failed, remaining
    );

    *gate = Some(Instant::now());
    Some(out)
}

/// 死号对账:拉 history keys,找 status!=active 的 ksk_,匹配池内 api_key_hash 并禁用。
/// 返回禁用的凭据数。写一条审计。
pub async fn sync_dead_keys(
    client: &I7relayClient,
    service: &Arc<AdminService>,
    audit: &I7relayAudit,
    trigger: RestockTrigger,
) -> usize {
    let all = match client.list_keys(true).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("i7relay list_keys(history) 失败: {e}");
            return 0;
        }
    };
    let dead_hashes: std::collections::HashSet<String> = all
        .iter()
        .filter(|k| !k.status.eq_ignore_ascii_case("active"))
        .map(|k| api_key_hash(&k.key))
        .collect();
    if dead_hashes.is_empty() {
        return 0;
    }

    let mut disabled = 0usize;
    for c in service.get_all_credentials().credentials {
        if c.disabled {
            continue;
        }
        if c.source_channel.as_deref() != Some("i7relay") {
            continue;
        }
        if let Some(h) = c.api_key_hash.as_deref() {
            if dead_hashes.contains(h) && service.set_disabled(c.id, true).is_ok() {
                disabled += 1;
            }
        }
    }

    if disabled > 0 {
        audit.record(RestockRecord {
            at: now_rfc3339(),
            trigger: trigger.as_str().to_string(),
            requested: 0,
            imported: 0,
            duplicate: 0,
            failed: 0,
            disabled,
            remaining_quota: -1,
            key_prefixes: vec![],
            error: None,
        });
        tracing::info!("i7relay 死号对账:禁用 {disabled} 条失效凭据");
    }
    disabled
}
