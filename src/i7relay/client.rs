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

/// `POST /api/my/purchase` 返回的单个 key(通常只含 `key`)。
#[derive(Debug, Clone, Deserialize)]
pub struct PurchasedKey {
    pub key: String,
}

/// `GET /api/my/profile` 完整配额信息。
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct Profile {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub remaining: i64,
    #[serde(default)]
    pub max_quota: i64,
    #[serde(default)]
    pub used_quota: i64,
}

#[derive(Debug, Deserialize)]
struct KeysResp {
    #[serde(default)]
    keys: Vec<RemoteKey>,
}

#[derive(Debug, Deserialize)]
struct PurchaseResp {
    #[serde(default)]
    keys: Vec<PurchasedKey>,
    #[serde(default)]
    remaining: i64,
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
        Ok(resp.json::<Profile>().await?)
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
    pub async fn purchase(&self, count: u32) -> anyhow::Result<(Vec<String>, i64)> {
        let resp = self
            .http
            .post(self.url("/api/my/purchase"))
            .header("X-API-Key", &self.api_key)
            .json(&serde_json::json!({ "count": count }))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("i7relay purchase 失败: HTTP {status}");
        }
        let p: PurchaseResp = resp.json().await?;
        let keys = p.keys.into_iter().map(|k| k.key).collect();
        Ok((keys, p.remaining))
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
