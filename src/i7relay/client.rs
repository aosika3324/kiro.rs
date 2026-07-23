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

/// `GET /api/my/profile` 真实响应:`{"name","quota","remaining","webhook_url"}`。
#[derive(Debug, Clone, Deserialize)]
struct ProfileRaw {
    #[serde(default)]
    name: String,
    /// 总配额。
    #[serde(default)]
    quota: i64,
    #[serde(default)]
    remaining: i64,
}

/// 归一化后的配额信息(供前端;`max_quota`=总配额,`used_quota`=总-剩)。
#[derive(Debug, Clone, serde::Serialize)]
pub struct Profile {
    pub name: String,
    pub remaining: i64,
    pub max_quota: i64,
    pub used_quota: i64,
}

impl From<ProfileRaw> for Profile {
    fn from(r: ProfileRaw) -> Self {
        Profile {
            name: r.name,
            remaining: r.remaining,
            max_quota: r.quota,
            used_quota: (r.quota - r.remaining).max(0),
        }
    }
}

#[derive(Debug, Deserialize)]
struct KeysResp {
    #[serde(default)]
    keys: Vec<RemoteKey>,
}

/// purchase 成功响应:`keys` 可能是字符串数组或对象数组;`remaining` 可缺省。
#[derive(Debug, Deserialize)]
struct PurchaseResp {
    #[serde(default)]
    keys: Vec<KeyItem>,
    /// 剩余配额;缺省时为 None(不伪造 0)。
    #[serde(default)]
    remaining: Option<i64>,
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
}
