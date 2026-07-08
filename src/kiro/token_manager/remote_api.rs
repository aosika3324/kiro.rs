//! `MultiTokenManager` 的上游只读/写 REST 查询方法簇（Admin 侧 + profileArn 解析）。
//!
//! 从 `mod.rs` 的主 `impl` 中拆出（行为零变化）：这些方法共同点是「对单个凭据发起一次
//! 上游 REST 调用」——profileArn 解析、getUsageLimits、ListAvailableModels、setUserPreference，
//! 以及它们共用的 token 准备（`prepare_request_token`）与只读查询前 profileArn 兜底
//! （`resolve_profile_for_read_query`）。它们不参与并发调度核心（entries/metrics 锁的排序路径），
//! 故独立成文件便于维护。作为同一类型的第二个 `impl` 块，可直接访问父模块私有字段与方法。
//!
//! ⚠️ `resolve_profile_arn_for` 是 pub，且被 `KiroProvider::ensure_profile_arn` 调用（聊天热路径），
//! 并非纯 Admin 方法——放这里是因为它与只读查询共享 profileArn 解析逻辑。

use super::*;

impl MultiTokenManager {
    /// 解析并回填 Enterprise / IdC 账号的真实 profileArn。
    ///
    /// 流式端点（`generateAssistantResponse`）强制要求 profileArn：不带 → 400
    /// `profileArn is required`。Enterprise / IdC 账号若带 BuilderID 占位符会因
    /// token 身份不匹配触发 403，真实 profileArn 只能通过 `ListAvailableProfiles` 获取。
    ///
    /// 行为：
    /// - API Key 凭据 / 已有真实（非占位符）profileArn → 直接返回，不发起网络请求；
    /// - 否则调用上游 `ListAvailableProfiles`，命中真实 ARN 时写回凭据并持久化；
    /// - 上游无 profile（如纯 BuilderID 账号）→ 返回 `None`，由调用方回退到占位符。
    ///
    /// 返回应当用于本次请求的 profileArn（`Some` 表示真实 ARN）。
    pub async fn resolve_profile_arn_for(
        &self,
        id: u64,
        token: &str,
    ) -> anyhow::Result<Option<String>> {
        use crate::kiro::model::credentials::is_placeholder_profile_arn;

        let credentials = {
            let entries = self.entries.lock();
            entries
                .iter()
                .find(|e| e.id == id)
                .map(|e| e.credentials.clone())
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?
        };

        // API Key 凭据没有 profileArn 概念
        if credentials.is_api_key_credential() {
            return Ok(None);
        }

        // 已有真实 ARN（含 Social 共享 ARN）→ 直接用，无需查询
        if let Some(arn) = credentials.profile_arn.as_deref() {
            if !is_placeholder_profile_arn(arn) {
                return Ok(Some(arn.to_string()));
            }
        }

        let global_proxy = self.proxy.lock().clone();
        let effective_proxy = credentials.effective_proxy(global_proxy.as_ref());
        let profiles =
            list_available_profiles(&credentials, &self.config, token, effective_proxy.as_ref())
                .await?;

        let Some(arn) = profiles.first_arn().map(|s| s.to_string()) else {
            // 无 Enterprise profile（如纯 BuilderID 账号）：保持占位符回退逻辑
            return Ok(None);
        };

        // 写回真实 ARN 并持久化
        {
            let mut entries = self.entries.lock();
            if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
                entry.credentials.profile_arn = Some(arn.clone());
            }
        }
        if let Err(e) = self.persist_credentials() {
            tracing::warn!("profileArn 回填后持久化失败（不影响本次请求）: {}", e);
        }
        tracing::info!("凭据 #{} 已解析并回填真实 profileArn: {}", id, arn);

        Ok(Some(arn))
    }

    /// 获取指定凭据的使用额度（Admin API）
    pub async fn get_usage_limits_for(&self, id: u64) -> anyhow::Result<UsageLimitsResponse> {
        let (token, credentials) = self.prepare_request_token(id).await?;
        let credentials = self
            .resolve_profile_for_read_query(id, &token, credentials, "getUsageLimits")
            .await;

        let global_proxy = self.proxy.lock().clone();
        let effective_proxy = credentials.effective_proxy(global_proxy.as_ref());
        let usage_limits =
            get_usage_limits(&credentials, &self.config, &token, effective_proxy.as_ref()).await?;

        // 更新订阅等级到凭据（仅在发生变化时持久化）
        if let Some(subscription_title) = usage_limits.subscription_title() {
            let changed = {
                let mut entries = self.entries.lock();
                if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
                    let old_title = entry.credentials.subscription_title.clone();
                    if old_title.as_deref() != Some(subscription_title) {
                        entry.credentials.subscription_title = Some(subscription_title.to_string());
                        tracing::info!(
                            "凭据 #{} 订阅等级已更新: {:?} -> {}",
                            id,
                            old_title,
                            subscription_title
                        );
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            };

            if changed {
                if let Err(e) = self.persist_credentials() {
                    tracing::warn!("订阅等级更新后持久化失败（不影响本次请求）: {}", e);
                }
            }
        }

        // 回填邮箱：仅在凭据尚无邮箱、且上游返回了邮箱时写入
        if let Some(email) = usage_limits.email() {
            let changed = {
                let mut entries = self.entries.lock();
                if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
                    let is_empty = entry
                        .credentials
                        .email
                        .as_deref()
                        .map(|s| s.is_empty())
                        .unwrap_or(true);
                    if is_empty {
                        entry.credentials.email = Some(email.to_string());
                        tracing::info!("凭据 #{} 邮箱已回填: {}", id, email);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            };

            if changed {
                if let Err(e) = self.persist_credentials() {
                    tracing::warn!("邮箱回填后持久化失败（不影响本次请求）: {}", e);
                }
            }
        }

        Ok(usage_limits)
    }

    /// 为只读型上游查询准备有效 token 与最新凭据快照
    ///
    /// 复用 [`Self::get_usage_limits_for`] 的 token 准备流程：API Key 凭据直接用
    /// kiroApiKey；OAuth 凭据按需在 `refresh_lock` 内刷新并持久化。返回的凭据是
    /// 刷新后重新读取的最新快照，调用方据此构造请求。
    async fn prepare_request_token(&self, id: u64) -> anyhow::Result<(String, KiroCredentials)> {
        let credentials = {
            let entries = self.entries.lock();
            entries
                .iter()
                .find(|e| e.id == id)
                .map(|e| e.credentials.clone())
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?
        };

        // API Key 凭据直接使用 kiro_api_key，无需刷新
        let token = if credentials.is_api_key_credential() {
            credentials
                .kiro_api_key
                .clone()
                .ok_or_else(|| anyhow::anyhow!("API Key 凭据缺少 kiroApiKey"))?
        } else if is_token_expired(&credentials) || is_token_expiring_soon(&credentials) {
            let lock = self.refresh_lock_for(id);
            let _guard = lock.lock().await;
            let current_creds = {
                let entries = self.entries.lock();
                entries
                    .iter()
                    .find(|e| e.id == id)
                    .map(|e| e.credentials.clone())
                    .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?
            };

            if is_token_expired(&current_creds) || is_token_expiring_soon(&current_creds) {
                let global_proxy = self.proxy.lock().clone();
                let effective_proxy = current_creds.effective_proxy(global_proxy.as_ref());
                let new_creds =
                    refresh_token(&current_creds, &self.config, effective_proxy.as_ref()).await?;
                {
                    let mut entries = self.entries.lock();
                    if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
                        entry.credentials = new_creds.clone();
                    }
                }
                // 持久化失败只记录警告，不影响本次请求
                if let Err(e) = self.persist_credentials() {
                    tracing::warn!("Token 刷新后持久化失败（不影响本次请求）: {}", e);
                }
                new_creds
                    .access_token
                    .ok_or_else(|| anyhow::anyhow!("刷新后无 access_token"))?
            } else {
                current_creds
                    .access_token
                    .ok_or_else(|| anyhow::anyhow!("凭据无 access_token"))?
            }
        } else {
            credentials
                .access_token
                .clone()
                .ok_or_else(|| anyhow::anyhow!("凭据无 access_token"))?
        };

        // 重新读取最新凭据（刷新可能改写了 access_token 之外的字段）
        let credentials = {
            let entries = self.entries.lock();
            entries
                .iter()
                .find(|e| e.id == id)
                .map(|e| e.credentials.clone())
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?
        };

        Ok((token, credentials))
    }

    /// 余额 / 模型 / 用户偏好这类 REST 查询也可能需要 Enterprise 真实 profileArn。
    ///
    /// 聊天流式请求已有 `KiroProvider::ensure_profile_arn` 兜底；Admin 侧余额验活
    /// 之前没有这个步骤，external_idp / Enterprise 账号会在缺少 profileArn 时直接
    /// 用无 profile 的 bearer token 打 `getUsageLimits`，上游常以
    /// `The bearer token included in the request is invalid` 拒绝。
    async fn resolve_profile_for_read_query(
        &self,
        id: u64,
        token: &str,
        mut credentials: KiroCredentials,
        operation: &str,
    ) -> KiroCredentials {
        use crate::kiro::model::credentials::is_placeholder_profile_arn;

        if credentials.is_api_key_credential() {
            return credentials;
        }

        let needs_profile = match credentials.profile_arn.as_deref() {
            None => true,
            Some(arn) => is_placeholder_profile_arn(arn),
        };
        if !needs_profile {
            return credentials;
        }

        match self.resolve_profile_arn_for(id, token).await {
            Ok(Some(arn)) => {
                credentials.profile_arn = Some(arn);
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(
                    "凭据 #{} 在 {} 前解析真实 profileArn 失败（继续按无 profile 查询）: {}",
                    id,
                    operation,
                    e
                );
            }
        }

        credentials
    }

    /// 获取指定凭据当前可用的模型列表（Admin API）
    ///
    /// 按需实时查询上游 `ListAvailableModels`，不做缓存。
    pub async fn get_available_models_for(
        &self,
        id: u64,
    ) -> anyhow::Result<ListAvailableModelsResponse> {
        let (token, credentials) = self.prepare_request_token(id).await?;
        let credentials = self
            .resolve_profile_for_read_query(id, &token, credentials, "ListAvailableModels")
            .await;
        let global_proxy = self.proxy.lock().clone();
        let effective_proxy = credentials.effective_proxy(global_proxy.as_ref());
        get_available_models(&credentials, &self.config, &token, effective_proxy.as_ref()).await
    }

    /// 设置用户偏好（开启/关闭超额）— Admin API
    ///
    /// 与 `get_usage_limits_for` 类似的 token 准备流程，最后调用上游
    /// `setUserPreference` 接口写入新的 `overageStatus`。
    pub async fn set_user_preference_for(
        &self,
        id: u64,
        overage_status: &str,
    ) -> anyhow::Result<()> {
        // 仅接受 "ENABLED" / "DISABLED"，其它值早 fail
        if overage_status != "ENABLED" && overage_status != "DISABLED" {
            anyhow::bail!("overageStatus 必须是 ENABLED 或 DISABLED");
        }

        let (token, credentials) = self.prepare_request_token(id).await?;
        let credentials = self
            .resolve_profile_for_read_query(id, &token, credentials, "setUserPreference")
            .await;

        let global_proxy = self.proxy.lock().clone();
        let effective_proxy = credentials.effective_proxy(global_proxy.as_ref());
        set_user_preference(
            &credentials,
            &self.config,
            &token,
            effective_proxy.as_ref(),
            overage_status,
        )
        .await
    }
}
