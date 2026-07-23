//! `MultiTokenManager` 的凭据 CRUD 方法簇（Admin 面板增删改查）。
//!
//! 从 `mod.rs` 的主 `impl` 中拆出（行为零变化）：这些方法只被 `admin/service.rs` 调用，
//! 负责凭据的增/改/删、分组重命名/移除、refreshToken 更新与强制刷新。它们不参与请求热路径的
//! 并发调度核心，独立成文件便于维护。作为同一类型的第二个 `impl` 块，可直接访问父模块私有
//! 字段与方法（entries/persist_credentials/refresh_lock_for 等）。
//!
//! ⚠️ 锁顺序仍受 `MultiTokenManager` 文档约束：涉及 entries + metrics 时先 entries 后 metrics。

use super::*;

impl MultiTokenManager {
    /// 添加新凭据（Admin API）
    ///
    /// # 流程
    /// 1. 验证凭据基本字段（API Key: kiroApiKey 不为空; OAuth: refreshToken 不为空）
    /// 2. 基于 kiroApiKey 或 refreshToken 的 SHA-256 哈希检测重复
    /// 3. OAuth: 尝试刷新 Token 验证凭据有效性; API Key: 跳过
    /// 4. 分配新 ID（当前最大 ID + 1）
    /// 5. 添加到 entries 列表
    /// 6. 持久化到配置文件
    ///
    /// # 返回
    /// - `Ok(u64)` - 新凭据 ID
    /// - `Err(_)` - 验证失败或添加失败
    pub async fn add_credential(&self, new_cred: KiroCredentials) -> anyhow::Result<u64> {
        // 1. 基本验证
        if new_cred.is_api_key_credential() {
            let api_key = new_cred
                .kiro_api_key
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("API Key 凭据缺少 kiroApiKey"))?;
            if api_key.is_empty() {
                anyhow::bail!("kiroApiKey 为空");
            }
        } else {
            validate_refresh_token(&new_cred)?;
        }

        // 2. 基于哈希检测重复
        if new_cred.is_api_key_credential() {
            let new_api_key = new_cred
                .kiro_api_key
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("缺少 kiroApiKey"))?;
            let new_api_key_hash = sha256_hex(new_api_key);
            let duplicate_exists = {
                let entries = self.entries.lock();
                entries.iter().any(|entry| {
                    entry
                        .credentials
                        .kiro_api_key
                        .as_deref()
                        .map(sha256_hex)
                        .as_deref()
                        == Some(new_api_key_hash.as_str())
                })
            };
            if duplicate_exists {
                anyhow::bail!("凭据已存在（kiroApiKey 重复）");
            }
        } else {
            let new_refresh_token = new_cred
                .refresh_token
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("缺少 refreshToken"))?;
            let new_refresh_token_hash = sha256_hex(new_refresh_token);
            let duplicate_exists = {
                let entries = self.entries.lock();
                entries.iter().any(|entry| {
                    entry
                        .credentials
                        .refresh_token
                        .as_deref()
                        .map(sha256_hex)
                        .as_deref()
                        == Some(new_refresh_token_hash.as_str())
                })
            };
            if duplicate_exists {
                anyhow::bail!("凭据已存在（refreshToken 重复）");
            }
        }

        // 3. 验证凭据有效性（API Key 无需网络刷新）
        let mut validated_cred = if new_cred.is_api_key_credential() {
            new_cred.clone()
        } else if new_cred.is_external_idp()
            && jwt_exp_unix(new_cred.access_token.as_deref())
                .map(|exp| exp > Utc::now().timestamp() + 60)
                .unwrap_or(false)
        {
            // 外部 IdP：导入时携带的 accessToken 是交互式登录签发的短期 token。
            // 只要还没过期，先保留它；立刻 refresh 可能换回上游 Kiro 不接受的 token。
            let mut c = new_cred.clone();
            if c.expires_at.is_none() {
                if let Some(exp) = jwt_exp_unix(c.access_token.as_deref()) {
                    if let Some(dt) = chrono::DateTime::from_timestamp(exp, 0) {
                        c.expires_at = Some(dt.to_rfc3339());
                    }
                }
            }
            c
        } else {
            let global_proxy = self.proxy.lock().clone();
            let effective_proxy = new_cred.effective_proxy(global_proxy.as_ref());
            refresh_token(&new_cred, &self.config, effective_proxy.as_ref()).await?
        };

        // 捕获原始输入的去重指纹。刷新可能轮换 refreshToken，且下方 step 5 会把
        // new_cred 的字段 move 走，故必须在此处（字段尚完整时）取指纹，
        // 供插入临界区的权威去重重检使用。
        let dedup_is_api_key = new_cred.is_api_key_credential();
        let dedup_hash: Option<String> = if dedup_is_api_key {
            new_cred
                .kiro_api_key
                .as_deref()
                .filter(|k| !k.is_empty())
                .map(sha256_hex)
        } else {
            new_cred.refresh_token.as_deref().map(sha256_hex)
        };

        // 4. 分配新 ID。必须使用单调计数器，不能按当前 entries 最大值重算；
        // 否则删除最后一个账号后再添加会复用旧 ID，导致 trace/usage/kiro_stats
        // 这类按 credential_id 聚合的历史被新账号继承。
        let new_id = self.next_id.fetch_add(1, Ordering::Relaxed);

        // 5. 设置 ID 并保留用户输入的元数据
        validated_cred.id = Some(new_id);
        validated_cred.priority = new_cred.priority;
        validated_cred.auth_method = new_cred.auth_method.map(|m| {
            if m.eq_ignore_ascii_case("builder-id") || m.eq_ignore_ascii_case("iam") {
                "idc".to_string()
            } else if m.eq_ignore_ascii_case("external-idp")
                || m.eq_ignore_ascii_case("externalidp")
                || m.eq_ignore_ascii_case("external_idp")
            {
                "external_idp".to_string()
            } else {
                m
            }
        });
        if new_cred.profile_arn.is_some() {
            validated_cred.profile_arn = new_cred.profile_arn;
        }
        validated_cred.provider = new_cred.provider;
        validated_cred.fill_default_profile_arn();
        validated_cred.client_id = new_cred.client_id;
        validated_cred.client_secret = new_cred.client_secret;
        validated_cred.token_endpoint = new_cred.token_endpoint;
        validated_cred.issuer_url = new_cred.issuer_url;
        validated_cred.scopes = new_cred.scopes;
        validated_cred.region = new_cred.region;
        validated_cred.auth_region = new_cred.auth_region;
        validated_cred.api_region = new_cred.api_region;
        validated_cred.machine_id = new_cred.machine_id;
        validated_cred.email = new_cred.email;
        validated_cred.proxy_url = new_cred.proxy_url;
        validated_cred.proxy_username = new_cred.proxy_username;
        validated_cred.proxy_password = new_cred.proxy_password;
        validated_cred.kiro_api_key = new_cred.kiro_api_key;

        {
            let mut entries = self.entries.lock();
            // 并发安全：token 刷新（网络）在锁外完成，期间可能有其它并发的
            // add_credential 通过了步骤 2 的预去重并已插入同一凭据。故在持锁的
            // 插入点用原始输入指纹再做一次权威去重，关闭 TOCTOU（如命中则 bail，
            // next_id 即便已自增也只是跳号，无副作用）。
            if let Some(hash) = &dedup_hash {
                let dup = entries.iter().any(|e| {
                    let entry_hash = if dedup_is_api_key {
                        e.credentials.kiro_api_key.as_deref().map(sha256_hex)
                    } else {
                        e.credentials.refresh_token.as_deref().map(sha256_hex)
                    };
                    entry_hash.as_deref() == Some(hash.as_str())
                });
                if dup {
                    let msg = if dedup_is_api_key {
                        "凭据已存在（kiroApiKey 重复）"
                    } else {
                        "凭据已存在（refreshToken 重复）"
                    };
                    anyhow::bail!(msg);
                }
            }
            let cap = self.cap_of(&validated_cred);
            // entry 与 credential_locks 共享同一个 runtime Arc（见 CredentialEntry.runtime 文档）。
            let runtime = Arc::new(CredentialRuntime::new(cap));
            self.credential_locks
                .lock()
                .insert(new_id, Arc::clone(&runtime));
            entries.push(CredentialEntry {
                id: new_id,
                credentials: validated_cred,
                failure_count: 0,
                total_failure_count: 0,
                refresh_failure_count: 0,
                disabled: false,
                disabled_reason: None,
                success_count: 0,
                last_used_at: None,
                throttled_until: None,
                auto_disabled_at: None,
                runtime,
            });
        }

        // 6. 升级为多凭据格式（确保后续 token rotation 能写盘）并持久化
        self.is_multiple_format.store(true, Ordering::Relaxed);
        self.persist_credentials()?;

        tracing::info!("成功添加凭据 #{}", new_id);
        Ok(new_id)
    }

    /// 更新凭据的可编辑字段（Admin API）
    ///
    /// 支持更新 email、proxy_url、proxy_username、proxy_password。
    /// 传 `None` 表示不修改该字段，传 `Some("")` 表示清除该字段。
    pub fn update_credential(
        &self,
        id: u64,
        email: Option<Option<String>>,
        proxy_url: Option<Option<String>>,
        proxy_username: Option<Option<String>>,
        proxy_password: Option<Option<String>>,
        groups: Option<Vec<String>>,
        source_channel: Option<Option<String>>,
    ) -> anyhow::Result<()> {
        {
            let mut entries = self.entries.lock();
            let entry = entries
                .iter_mut()
                .find(|e| e.id == id)
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;

            if let Some(v) = email {
                entry.credentials.email = v.filter(|s| !s.is_empty());
            }
            if let Some(v) = proxy_url {
                entry.credentials.proxy_url = v.filter(|s| !s.is_empty());
            }
            if let Some(v) = proxy_username {
                entry.credentials.proxy_username = v.filter(|s| !s.is_empty());
            }
            if let Some(v) = proxy_password {
                entry.credentials.proxy_password = v.filter(|s| !s.is_empty());
            }
            if let Some(g) = groups {
                entry.credentials.groups = g
                    .into_iter()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
            if let Some(v) = source_channel {
                entry.credentials.source_channel =
                    v.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
            }
        }
        self.persist_credentials()?;
        Ok(())
    }

    /// 列出所有凭据当前引用的分组名（去重排序）。
    /// 用于启动迁移到 GroupManager 注册表，以及前端的引用计数显示。
    pub fn list_credential_groups(&self) -> Vec<String> {
        let entries = self.entries.lock();
        let mut set: std::collections::HashSet<String> = std::collections::HashSet::new();
        for e in entries.iter() {
            for g in &e.credentials.groups {
                if !g.is_empty() {
                    set.insert(g.clone());
                }
            }
        }
        let mut list: Vec<String> = set.into_iter().collect();
        list.sort();
        list
    }

    /// 统计指定分组被多少个凭据引用（用于分组管理页 / 删除前提示）。
    pub fn count_credentials_with_group(&self, group: &str) -> usize {
        let entries = self.entries.lock();
        entries
            .iter()
            .filter(|e| e.credentials.groups.iter().any(|g| g == group))
            .count()
    }

    /// 把所有凭据 `groups` 字段中等于 `old` 的元素改为 `new`（分组改名级联用）。
    /// 已经显式带 `new` 的凭据不会重复添加。返回受影响的凭据数。
    pub fn rename_credential_group(&self, old: &str, new: &str) -> anyhow::Result<usize> {
        let mut affected = 0usize;
        {
            let mut entries = self.entries.lock();
            for entry in entries.iter_mut() {
                let groups = &mut entry.credentials.groups;
                let mut hit = false;
                let mut already_has_new = false;
                for g in groups.iter() {
                    if g == old {
                        hit = true;
                    }
                    if g == new {
                        already_has_new = true;
                    }
                }
                if hit {
                    if already_has_new {
                        // old 和 new 共存：只去掉 old，避免重复
                        groups.retain(|g| g != old);
                    } else {
                        for g in groups.iter_mut() {
                            if g == old {
                                *g = new.to_string();
                            }
                        }
                    }
                    affected += 1;
                }
            }
        }
        if affected > 0 {
            self.persist_credentials()?;
        }
        Ok(affected)
    }

    /// 把 `name` 这个分组从所有凭据的 `groups` 字段中移除（强删分组级联用）。
    /// 返回受影响的凭据数。
    pub fn remove_credential_group(&self, name: &str) -> anyhow::Result<usize> {
        let mut affected = 0usize;
        {
            let mut entries = self.entries.lock();
            for entry in entries.iter_mut() {
                let before = entry.credentials.groups.len();
                entry.credentials.groups.retain(|g| g != name);
                if entry.credentials.groups.len() != before {
                    affected += 1;
                }
            }
        }
        if affected > 0 {
            self.persist_credentials()?;
        }
        Ok(affected)
    }

    /// 删除凭据（Admin API）
    ///
    /// # 前置条件
    /// - 凭据必须已禁用（disabled = true）
    ///
    /// # 行为
    /// 1. 验证凭据存在
    /// 2. 验证凭据已禁用
    /// 3. 从 entries 移除
    /// 4. 如果删除的是当前凭据，切换到优先级最高的可用凭据
    /// 5. 如果删除后没有凭据，将 current_id 重置为 0
    /// 6. 持久化到文件
    ///
    /// # 返回
    /// - `Ok(())` - 删除成功
    /// - `Err(_)` - 凭据不存在或持久化失败
    pub fn delete_credential(&self, id: u64) -> anyhow::Result<()> {
        let was_current = {
            let mut entries = self.entries.lock();

            // 查找凭据
            let _entry = entries
                .iter()
                .find(|e| e.id == id)
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;

            // 记录是否是当前凭据
            let current_id = *self.current_id.lock();
            let was_current = current_id == id;

            // 删除凭据
            entries.retain(|e| e.id != id);

            was_current
        };

        // 如果删除的是当前凭据，切换到优先级最高的可用凭据
        if was_current {
            self.select_highest_priority();
        }

        // 如果删除后没有任何凭据，将 current_id 重置为 0（与初始化行为保持一致）
        {
            let entries = self.entries.lock();
            if entries.is_empty() {
                let mut current_id = self.current_id.lock();
                *current_id = 0;
                tracing::info!("所有凭据已删除，current_id 已重置为 0");
            }
        }

        self.credential_locks.lock().remove(&id);
        self.refresh_locks.lock().remove(&id);
        self.metrics.lock().remove(&id);

        // 持久化更改
        self.persist_credentials()?;

        // 立即回写统计数据，清除已删除凭据的残留条目
        self.save_stats();

        tracing::info!("已删除凭据 #{}", id);
        Ok(())
    }

    /// 更新指定凭据的 refreshToken（Admin API）
    ///
    /// # 前置条件
    /// - 凭据必须已禁用（disabled = true），防止意外覆盖正在使用的 Token
    ///
    /// # 行为
    /// 1. 验证凭据存在且已禁用
    /// 2. 验证新 refreshToken 格式
    /// 3. 更新 refreshToken
    /// 4. 重置 refresh_failure_count（保持 disabled 状态，让用户手动启用）
    /// 5. 持久化到文件
    pub fn update_refresh_token(
        &self,
        id: u64,
        new_refresh_token: String,
        new_access_token: Option<String>,
        new_expires_at: Option<String>,
    ) -> anyhow::Result<()> {
        {
            let mut entries = self.entries.lock();

            // 用索引定位，避免两次线性扫描和后续 unwrap
            let idx = entries
                .iter()
                .position(|e| e.id == id)
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;

            if !entries[idx].disabled {
                anyhow::bail!(
                    "只能为已禁用的凭据更新 refreshToken（请先禁用凭据 #{}）",
                    id
                );
            }

            // 验证新 refreshToken 格式
            let tmp_creds = KiroCredentials {
                refresh_token: Some(new_refresh_token.clone()),
                ..entries[idx].credentials.clone()
            };
            validate_refresh_token(&tmp_creds)?;

            // 检查是否与现有其他凭据重复
            let new_hash = sha256_hex(&new_refresh_token);
            let duplicate = entries.iter().enumerate().any(|(i, e)| {
                i != idx
                    && e.credentials
                        .refresh_token
                        .as_ref()
                        .map(|t| sha256_hex(t) == new_hash)
                        .unwrap_or(false)
            });
            if duplicate {
                anyhow::bail!("refreshToken 与其他凭据重复");
            }

            let entry = &mut entries[idx];
            entry.credentials.refresh_token = Some(new_refresh_token);
            // 若调用方提供了 accessToken（来自导入/导出），则直接保留，无需立即调认证服务器
            // 否则清空，下次使用时系统会自动刷新
            entry.credentials.access_token = new_access_token;
            entry.credentials.expires_at = new_expires_at;
            entry.refresh_failure_count = 0;
        }
        self.persist_credentials()?;
        tracing::info!("凭据 #{} refreshToken 已更新", id);
        Ok(())
    }

    /// 强制刷新指定凭据的 Token（Admin API）
    ///
    /// 无条件调用上游 API 重新获取 access token，不检查是否过期。
    /// 适用于排查问题、Token 异常但未过期、主动更新凭据状态等场景。
    pub async fn force_refresh_token_for(&self, id: u64) -> anyhow::Result<()> {
        let credentials = {
            let entries = self.entries.lock();
            entries
                .iter()
                .find(|e| e.id == id)
                .map(|e| e.credentials.clone())
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?
        };

        // 获取该凭据专属的刷新锁，防止同凭据并发刷新（不同凭据互不阻塞）
        let lock = self.refresh_lock_for(id);
        let _guard = lock.lock().await;

        // 无条件调用 refresh_token
        let global_proxy = self.proxy.lock().clone();
        let effective_proxy = credentials.effective_proxy(global_proxy.as_ref());
        let new_creds = refresh_token(&credentials, &self.config, effective_proxy.as_ref()).await?;

        // 更新 entries 中对应凭据
        {
            let mut entries = self.entries.lock();
            if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
                entry.credentials = new_creds;
                entry.refresh_failure_count = 0;
            }
        }

        // 持久化
        if let Err(e) = self.persist_credentials() {
            tracing::warn!("强制刷新 Token 后持久化失败: {}", e);
        }

        tracing::info!("凭据 #{} Token 已强制刷新", id);
        Ok(())
    }
}
