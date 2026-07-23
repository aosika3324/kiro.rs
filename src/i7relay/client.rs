//! i7relay HTTP 客户端:封装四个接口,`X-API-Key` 鉴权。

use crate::http_client::build_client;
use crate::model::config::{I7relayConfig, TlsBackend};
use serde::Deserialize;

/// i7relay `GET /api/my/keys` 返回的单个 key 条目。
///
/// **只保留 `key`/`status`**：交付物还含 account/password/issuer_url 等敏感字段,
/// 用 `#[serde(default)]` 宽松忽略,**不入内存、不落库、不打日志**(见模块级安全说明)。
#[derive(Debug, Clone, Deserialize)]
pub struct RemoteKey {
    pub key: String,
    #[serde(default)]
    pub status: String,
}

/// 截断长响应体用于报错(避免把整段 key 打进日志)。
fn trunc(s: &str) -> String {
    s.chars().take(120).collect()
}

/// `GET /api/my/profile` 响应。实测字段是 `quota`;官方文档写 `max_quota`/`used_quota`。
/// 两套都收(兼容供应商任一实现),归一化到 Profile。`webhook_url` 用于判断是否已注册。
#[derive(Debug, Clone, Deserialize)]
struct ProfileRaw {
    #[serde(default)]
    name: String,
    /// 实测口径:总配额。
    #[serde(default)]
    quota: Option<i64>,
    /// 文档口径:总配额。
    #[serde(default)]
    max_quota: Option<i64>,
    /// 文档口径:已用。
    #[serde(default)]
    used_quota: Option<i64>,
    #[serde(default)]
    remaining: i64,
    #[serde(default)]
    webhook_url: String,
}

/// 归一化后的配额信息(供前端;`max_quota`=总配额,`used_quota`=总-剩)。
#[derive(Debug, Clone, serde::Serialize)]
pub struct Profile {
    pub name: String,
    pub remaining: i64,
    pub max_quota: i64,
    pub used_quota: i64,
    /// 供应商侧当前注册的 webhook URL(空=未注册)。
    pub webhook_url: String,
}

impl From<ProfileRaw> for Profile {
    fn from(r: ProfileRaw) -> Self {
        // 总配额:优先实测 quota,回退文档 max_quota,再回退 used+remaining。
        let max = r
            .quota
            .or(r.max_quota)
            .unwrap_or_else(|| r.used_quota.unwrap_or(0) + r.remaining);
        let used = r.used_quota.unwrap_or_else(|| (max - r.remaining).max(0));
        Profile {
            name: r.name,
            remaining: r.remaining,
            max_quota: max,
            used_quota: used,
            webhook_url: r.webhook_url,
        }
    }
}

#[derive(Debug, Deserialize)]
struct KeysResp {
    #[serde(default)]
    keys: Vec<RemoteKey>,
}

/// purchase 成功响应:文档 `{"purchased":N,"remaining":M,"keys":[{"key":"ksk_..."}]}`。
/// `keys` 兼容字符串数组或对象数组;`remaining`/`purchased` 可缺省。
#[derive(Debug, Deserialize)]
struct PurchaseResp {
    #[serde(default)]
    keys: Vec<KeyItem>,
    /// 剩余配额;缺省时为 None(不伪造 0)。
    #[serde(default)]
    remaining: Option<i64>,
    /// 本次实际发放数(文档字段;仅作日志/校验用,不强依赖)。
    #[serde(default)]
    #[allow(dead_code)]
    purchased: Option<u32>,
}

/// key 条目:兼容裸字符串 `"ksk_..."` 或对象 `{"key":"ksk_..."}`。
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum KeyItem {
    Str(String),
    Obj { key: String },
}

impl KeyItem {
    fn into_key(self) -> String {
        match self {
            KeyItem::Str(s) => s,
            KeyItem::Obj { key } => key,
        }
    }
}

/// i7relay API 客户端。持有 reqwest client + base_url + api_key(不打日志)。
pub struct I7relayClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
}

impl I7relayClient {
    /// 从配置构建。`tls_backend` 用全局配置的后端。
    pub fn new(cfg: &I7relayConfig, tls_backend: TlsBackend) -> anyhow::Result<Self> {
        let http = build_client(None, 30, tls_backend)?;
        Ok(Self {
            http,
            base_url: cfg.base_url.trim_end_matches('/').to_string(),
            api_key: cfg.api_key.clone(),
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// 完整配额信息(name/remaining/max/used)。
    pub async fn profile(&self) -> anyhow::Result<Profile> {
        let resp = self
            .http
            .get(self.url("/api/my/profile"))
            .header("X-API-Key", &self.api_key)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("i7relay profile 失败: HTTP {status}");
        }
        Ok(resp.json::<ProfileRaw>().await?.into())
    }

    /// 剩余配额(remaining)。
    pub async fn remaining_quota(&self) -> anyhow::Result<i64> {
        Ok(self.profile().await?.remaining)
    }

    /// 列出当前 key。`history=true` 含已失效的(用于对账死号)。
    pub async fn list_keys(&self, history: bool) -> anyhow::Result<Vec<RemoteKey>> {
        let mut req = self
            .http
            .get(self.url("/api/my/keys"))
            .header("X-API-Key", &self.api_key);
        if history {
            req = req.query(&[("history", "1")]);
        }
        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("i7relay list_keys 失败: HTTP {status}");
        }
        Ok(resp.json::<KeysResp>().await?.keys)
    }

    /// 购买 `count` 个 key(**扣配额**)。返回 (keys, remaining)。
    /// remaining = -1 表示响应未带该字段(未知,不伪造 0)。
    /// 非 2xx 会带上响应体里的 `error`/`message`(如"暂无可用 Key")。
    pub async fn purchase(&self, count: u32) -> anyhow::Result<(Vec<String>, i64)> {
        let resp = self
            .http
            .post(self.url("/api/my/purchase"))
            .header("X-API-Key", &self.api_key)
            .json(&serde_json::json!({ "count": count }))
            .send()
            .await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            let msg = serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| {
                    v.get("error")
                        .or_else(|| v.get("message"))
                        .and_then(|m| m.as_str())
                        .map(String::from)
                })
                .unwrap_or_else(|| format!("HTTP {status}"));
            anyhow::bail!("{msg}");
        }
        let p: PurchaseResp = serde_json::from_str(&body)
            .map_err(|e| anyhow::anyhow!("purchase 响应解析失败: {e}; body={}", trunc(&body)))?;
        let keys = p.keys.into_iter().map(KeyItem::into_key).collect();
        Ok((keys, p.remaining.unwrap_or(-1)))
    }

    /// 设置 webhook URL。
    pub async fn set_webhook(&self, webhook_url: &str) -> anyhow::Result<()> {
        let resp = self
            .http
            .put(self.url("/api/my/webhook"))
            .header("X-API-Key", &self.api_key)
            .json(&serde_json::json!({ "webhook_url": webhook_url }))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("i7relay set_webhook 失败: HTTP {status}");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_maps_real_schema() {
        // 真实响应:{"name","quota","remaining","webhook_url"}
        let raw: ProfileRaw =
            serde_json::from_str(r#"{"name":"Beatingandroid","quota":50,"remaining":30,"webhook_url":""}"#)
                .unwrap();
        let p: Profile = raw.into();
        assert_eq!(p.name, "Beatingandroid");
        assert_eq!(p.max_quota, 50);
        assert_eq!(p.remaining, 30);
        assert_eq!(p.used_quota, 20); // 50-30
    }

    #[test]
    fn profile_used_never_negative() {
        let raw: ProfileRaw = serde_json::from_str(r#"{"quota":0,"remaining":50}"#).unwrap();
        let p: Profile = raw.into();
        assert_eq!(p.used_quota, 0); // max(0)
    }

    #[test]
    fn profile_accepts_doc_schema() {
        // 官方文档口径:max_quota/used_quota
        let raw: ProfileRaw =
            serde_json::from_str(r#"{"name":"alice","max_quota":100,"used_quota":5,"remaining":95,"webhook_url":"https://x/h"}"#)
                .unwrap();
        let p: Profile = raw.into();
        assert_eq!(p.max_quota, 100);
        assert_eq!(p.used_quota, 5);
        assert_eq!(p.remaining, 95);
        assert_eq!(p.webhook_url, "https://x/h");
    }

    #[test]
    fn profile_extracts_webhook_url() {
        let raw: ProfileRaw =
            serde_json::from_str(r#"{"name":"test","quota":999999,"remaining":999999,"webhook_url":"https://example.com/hook"}"#)
                .unwrap();
        let p: Profile = raw.into();
        assert_eq!(p.webhook_url, "https://example.com/hook");
    }

    #[test]
    fn keyitem_accepts_string_and_object() {
        let a: KeyItem = serde_json::from_str(r#""ksk_abc""#).unwrap();
        let b: KeyItem = serde_json::from_str(r#"{"key":"ksk_xyz"}"#).unwrap();
        assert_eq!(a.into_key(), "ksk_abc");
        assert_eq!(b.into_key(), "ksk_xyz");
    }

    #[test]
    fn purchase_resp_remaining_optional() {
        // 缺 remaining → None(不伪造 0)
        let p: PurchaseResp = serde_json::from_str(r#"{"keys":["ksk_a","ksk_b"]}"#).unwrap();
        assert_eq!(p.remaining, None);
        assert_eq!(p.keys.len(), 2);
        // 带 remaining
        let p2: PurchaseResp = serde_json::from_str(r#"{"keys":[],"remaining":7}"#).unwrap();
        assert_eq!(p2.remaining, Some(7));
    }

    #[test]
    fn purchase_resp_doc_shape() {
        // 文档成功形态:{"purchased":5,"remaining":95,"keys":[{"key":"ksk_..."}]}
        let p: PurchaseResp =
            serde_json::from_str(r#"{"purchased":2,"remaining":95,"keys":[{"key":"ksk_a"},{"key":"ksk_b"}]}"#)
                .unwrap();
        assert_eq!(p.purchased, Some(2));
        assert_eq!(p.remaining, Some(95));
        let keys: Vec<String> = p.keys.into_iter().map(KeyItem::into_key).collect();
        assert_eq!(keys, vec!["ksk_a", "ksk_b"]);
    }

    #[test]
    fn keys_resp_ignores_sensitive_fields() {
        // /api/my/keys 条目含 account/password/issuer_url,只取 key/status。
        let r: KeysResp = serde_json::from_str(
            r#"{"count":1,"active":1,"keys":[{"key":"ksk_x","account":"u1","password":"p!","issuer_url":"https://d/","status":"active"}]}"#,
        )
        .unwrap();
        assert_eq!(r.keys.len(), 1);
        assert_eq!(r.keys[0].key, "ksk_x");
        assert_eq!(r.keys[0].status, "active");
    }

    // ===== 集成测试:打真实 test.i7relay.com。默认 #[ignore],需 env I7RELAY_TEST_KEY。 =====
    // 运行:I7RELAY_TEST_KEY=usr-... cargo test --features native-tls i7relay_live -- --ignored --nocapture
    fn live_client() -> Option<I7relayClient> {
        let key = std::env::var("I7RELAY_TEST_KEY").ok()?;
        let cfg = I7relayConfig {
            base_url: "https://test.i7relay.com".to_string(),
            api_key: key,
            ..Default::default()
        };
        I7relayClient::new(&cfg, TlsBackend::Rustls).ok()
    }

    #[tokio::test]
    #[ignore = "需 I7RELAY_TEST_KEY,打真实网络"]
    async fn i7relay_live_profile() {
        let c = live_client().expect("需设 I7RELAY_TEST_KEY");
        let p = c.profile().await.expect("profile 应成功");
        println!("profile: name={} max={} used={} remaining={} webhook={}",
            p.name, p.max_quota, p.used_quota, p.remaining, p.webhook_url);
        assert!(p.max_quota >= 0);
        assert!(p.remaining >= 0);
        // used = max - remaining 恒等(归一化保证)。
        assert_eq!(p.used_quota, (p.max_quota - p.remaining).max(0));
    }

    #[tokio::test]
    #[ignore = "需 I7RELAY_TEST_KEY,打真实网络"]
    async fn i7relay_live_list_keys() {
        let c = live_client().expect("需设 I7RELAY_TEST_KEY");
        let keys = c.list_keys(false).await.expect("list_keys 应成功");
        println!("keys: {} 个", keys.len());
        // 敏感字段应被忽略:每条只有 key/status(编译期即保证,这里验非空 key)。
        for k in keys.iter().take(3) {
            assert!(k.key.starts_with("ksk_"), "key 前缀应为 ksk_");
        }
    }

    #[tokio::test]
    #[ignore = "需 I7RELAY_TEST_KEY,会扣配额/或返回无货错误"]
    async fn i7relay_live_purchase() {
        let c = live_client().expect("需设 I7RELAY_TEST_KEY");
        match c.purchase(1).await {
            Ok((keys, remaining)) => {
                println!("purchase ok: {} keys, remaining={}", keys.len(), remaining);
                for k in &keys {
                    assert!(k.starts_with("ksk_"));
                }
            }
            // 无货是合法路径:错误消息应可读(如"暂无可用 Key")。
            Err(e) => {
                println!("purchase 无货/失败(合法): {e}");
                assert!(!e.to_string().is_empty());
            }
        }
    }

    #[tokio::test]
    #[ignore = "需 I7RELAY_TEST_KEY,会改测试账号 webhook"]
    async fn i7relay_live_set_webhook_roundtrip() {
        let c = live_client().expect("需设 I7RELAY_TEST_KEY");
        let url = "https://example.com/kiro-rs-test-hook";
        c.set_webhook(url).await.expect("set_webhook 应成功");
        let p = c.profile().await.expect("profile 应成功");
        assert_eq!(p.webhook_url, url, "profile 应回读刚设的 webhook");
    }
}
