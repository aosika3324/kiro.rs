//! `AdminService` 的 SSO/OAuth 登录状态机（Social Portal PKCE + IdC Device Flow + 重登录）。
//!
//! 从 `mod.rs` 的主 `impl` 中拆出（行为零变化）：这是一个自成一体的登录会话状态机——
//! start/poll/complete Social 登录、远程回调投递、IdC 设备流登录、以及 Social/IdC 重登录，
//! 与 AdminService 其余方法零耦合（唯一产出是 `token_manager.add_credential(...)`）。
//! 独立成文件便于单独维护/测试。作为同一类型的第二个 `impl` 块，可访问父模块私有字段与助手。

use super::*;

impl AdminService {
    // ── Social 登录（Portal PKCE OAuth）────────────────────────────────────────

    /// 发起 Social 登录，返回 portal URL 供用户在浏览器打开
    ///
    /// 回调模式由 `config.callbackBaseUrl` 决定：
    /// - 已配置 → 远程模式：redirect_uri 使用公网地址，由本服务的 `/auth/callback` GET 路由接收回调
    /// - 未配置 → 本地模式：启动临时 TCP 回调服务器（浏览器与服务端须同机）
    pub async fn start_social_login(
        &self,
        req: StartSocialLoginRequest,
    ) -> Result<StartSocialLoginResponse, AdminServiceError> {
        let global_proxy = self.token_manager.proxy();
        let proxy = req
            .proxy_url
            .as_deref()
            .map(ProxyConfig::new)
            .or(global_proxy);

        let auth_endpoint = req
            .auth_endpoint
            .unwrap_or_else(|| social::KIRO_AUTH_ENDPOINT.to_string());

        let (code_verifier, code_challenge) = social::generate_pkce();
        let state = uuid::Uuid::new_v4().to_string();

        // 回调模式：配置了 callbackBaseUrl → 远程模式（公网回调路由自动接收）；
        // 否则本地模式（启动临时 TCP 端口，仅本机浏览器可达）。
        let remote_base = self.resolve_callback_base(req.callback_base_url.as_deref());
        let (redirect_uri, server_handle, remote_callback_tx, rx) = match remote_base.clone() {
            Some(base) => {
                let (tx, rx) = tokio::sync::oneshot::channel::<social::OAuthCallbackData>();
                // 远程模式：暂存 Sender，由公网 GET 回调路由投递回调数据
                (base, None, Some(Mutex::new(Some(tx))), rx)
            }
            None => {
                let (tx, rx) = tokio::sync::oneshot::channel::<social::OAuthCallbackData>();
                let (port, server_handle) = social::start_callback_server(tx)
                    .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;
                (
                    format!("http://127.0.0.1:{}", port),
                    Some(server_handle),
                    None,
                    rx,
                )
            }
        };
        let portal_url = social::build_portal_url(&state, &code_challenge, &redirect_uri);

        let expires_at = Utc::now() + Duration::minutes(10);
        let session_id = uuid::Uuid::new_v4().to_string();

        let cred_template = KiroCredentials {
            auth_method: Some("social".to_string()),
            priority: req.priority,
            email: req.email,
            proxy_url: req.proxy_url,
            ..Default::default()
        };

        let session = SocialAuthSession {
            auth_endpoint,
            state,
            code_verifier,
            redirect_uri,
            expires_at,
            callback_rx: tokio::sync::Mutex::new(rx),
            cred_template,
            proxy,
            _server_handle: server_handle,
            remote_callback_tx,
            relogin_target_id: None,
        };

        self.social_sessions
            .lock()
            .insert(session_id.clone(), session);

        Ok(StartSocialLoginResponse {
            session_id,
            portal_url,
            expires_at: expires_at.to_rfc3339(),
            remote: remote_base.is_some(),
        })
    }

    /// 轮询一次 Social 登录状态
    pub async fn poll_social_login(
        &self,
        session_id: &str,
    ) -> Result<PollIdcLoginResponse, AdminServiceError> {
        use tokio::sync::oneshot::error::TryRecvError;

        // 一次加锁同时完成：过期检查 + 非阻塞回调接收，消除 TOCTOU
        enum PollOutcome {
            Expired,
            Closed,
            Pending,
            Received(social::OAuthCallbackData),
        }

        let outcome = {
            let sessions = self.social_sessions.lock();
            let Some(session) = sessions.get(session_id) else {
                return Err(AdminServiceError::NotFound { id: 0 });
            };

            if Utc::now() >= session.expires_at {
                PollOutcome::Expired
            } else {
                match session.callback_rx.try_lock() {
                    Ok(mut rx) => match rx.try_recv() {
                        Ok(data) => PollOutcome::Received(data),
                        Err(TryRecvError::Empty) => PollOutcome::Pending,
                        Err(TryRecvError::Closed) => PollOutcome::Closed,
                    },
                    Err(_) => PollOutcome::Pending,
                }
            }
        };

        match outcome {
            PollOutcome::Pending => return Ok(PollIdcLoginResponse::Pending),
            PollOutcome::Expired => {
                self.social_sessions.lock().remove(session_id);
                return Ok(PollIdcLoginResponse::Expired);
            }
            PollOutcome::Closed => {
                self.social_sessions.lock().remove(session_id);
                return Err(AdminServiceError::InternalError(
                    "Social 登录回调服务器已关闭，请重新发起登录".to_string(),
                ));
            }
            PollOutcome::Received(callback) => {
                self.do_complete_social_login(session_id, callback).await
            }
        }
    }

    /// 内部：完成 Social 登录的 token 兑换和凭据创建（供轮询回调和手动完成共用）
    ///
    /// 调用前须确认 session 存在且未过期。会在内部做 state CSRF 校验。
    async fn do_complete_social_login(
        &self,
        session_id: &str,
        callback: social::OAuthCallbackData,
    ) -> Result<PollIdcLoginResponse, AdminServiceError> {
        // 先做 CSRF 校验（不移除 session，校验失败时保持 session 可继续轮询）
        {
            let sessions = self.social_sessions.lock();
            let s = sessions
                .get(session_id)
                .ok_or(AdminServiceError::NotFound { id: 0 })?;
            if callback.state != s.state {
                tracing::warn!(
                    "Social 登录 state 不匹配（期望 {}, 收到 {}），已拒绝",
                    s.state,
                    callback.state
                );
                return Err(AdminServiceError::InternalError(
                    "OAuth state 不匹配，请重新发起登录".to_string(),
                ));
            }
        }

        // 移除 session（含 code_verifier 等敏感数据）
        let session = self
            .social_sessions
            .lock()
            .remove(session_id)
            .ok_or(AdminServiceError::NotFound { id: 0 })?;

        let config = self.token_manager.config();

        // 构建完整的 redirect_uri（与 IDE 行为一致）
        let full_redirect_uri = if callback.login_option.is_empty() {
            format!("{}{}", session.redirect_uri, callback.path)
        } else {
            format!(
                "{}{}?login_option={}",
                session.redirect_uri,
                callback.path,
                urlencoding::encode(&callback.login_option),
            )
        };

        let token = social::exchange_code_for_token(
            &session.auth_endpoint,
            &callback.code,
            &session.code_verifier,
            &full_redirect_uri,
            config,
            session.proxy.as_ref(),
        )
        .await
        .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

        // 重新登录模式：更新已有凭据而非创建新凭据
        if let Some(target_id) = session.relogin_target_id {
            let refresh_token = token.refresh_token.ok_or_else(|| {
                AdminServiceError::InternalError(
                    "Social 登录未返回 refreshToken，无法更新凭据".to_string(),
                )
            })?;
            self.do_relogin_update(target_id, refresh_token)
                .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;
            tracing::info!("Social 重新登录成功，凭据 #{} Token 已更新", target_id);
            return Ok(PollIdcLoginResponse::Success {
                credential_id: target_id,
            });
        }

        let mut new_cred = session.cred_template;
        new_cred.access_token = Some(token.access_token);
        new_cred.refresh_token = token.refresh_token;
        new_cred.expires_at = token.expires_at.or_else(|| {
            token
                .expires_in
                .map(|secs| (Utc::now() + Duration::seconds(secs)).to_rfc3339())
        });
        if let Some(arn) = token.profile_arn {
            new_cred.profile_arn = Some(arn);
        }

        let credential_id = self
            .token_manager
            .add_credential(new_cred)
            .await
            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

        // 主动刷新余额（含订阅等级 / 邮箱）并写入缓存，登录后立即可见
        if let Err(e) = self.get_balance(credential_id).await {
            tracing::warn!("Social 登录后刷新余额失败（不影响登录）: {}", e);
        }

        tracing::info!("Social 登录成功，已添加凭据 #{}", credential_id);
        Ok(PollIdcLoginResponse::Success { credential_id })
    }

    /// 手动完成 Social 登录：远程访问时从浏览器地址栏粘贴的回调 URL 中提取参数，直接完成 token 兑换
    pub async fn complete_social_login(
        &self,
        session_id: &str,
        code: String,
        state: String,
        login_option: String,
        path: String,
    ) -> Result<PollIdcLoginResponse, AdminServiceError> {
        // 过期检查
        {
            let sessions = self.social_sessions.lock();
            let s = sessions
                .get(session_id)
                .ok_or(AdminServiceError::NotFound { id: 0 })?;
            if Utc::now() >= s.expires_at {
                return Ok(PollIdcLoginResponse::Expired);
            }
        }

        let callback = social::OAuthCallbackData {
            code,
            login_option,
            path,
            state,
        };
        self.do_complete_social_login(session_id, callback).await
    }

    /// 解析远程回调 base，优先级：`config.callbackBaseUrl`（显式覆盖 / 逃生口）> 请求自带 base > None（本地模式）。
    ///
    /// 返回 None 表示回落本地模式（都未提供 / 提供的值非法时记 warn）。
    fn resolve_callback_base(&self, req_base: Option<&str>) -> Option<String> {
        // 优先用 config 显式配置；否则用前端按当前访问地址派生的请求值
        let raw = self
            .token_manager
            .config()
            .callback_base_url
            .as_deref()
            .map(str::to_string)
            .or_else(|| req_base.map(str::to_string))?;
        let trimmed = raw.trim().trim_end_matches('/');
        if trimmed.is_empty() {
            return None;
        }
        if !(trimmed.starts_with("http://") || trimmed.starts_with("https://")) {
            tracing::warn!(
                "callbackBaseUrl 非法（须以 http:// 或 https:// 开头），回落本地回调模式: {}",
                raw
            );
            return None;
        }
        Some(trimmed.to_string())
    }

    /// 公网 GET 回调路由调用：按 OAuth state 定位会话并投递回调数据。
    ///
    /// 命中且未过期 → 投递进会话 oneshot channel（由 poll_social_login 统一完成 token 兑换）；
    /// 不存在 / 已过期 / 非远程会话 → 返回相应结果，由调用方渲染提示页。
    pub fn deliver_remote_social_callback(
        &self,
        state: &str,
        data: social::OAuthCallbackData,
    ) -> RemoteCallbackOutcome {
        let sessions = self.social_sessions.lock();
        // 找到 state 匹配的会话（state 每会话随机，提供 CSRF 保护）
        let session_id = sessions
            .iter()
            .find_map(|(id, s)| (s.state == state).then_some(id.clone()));

        let Some(session_id) = session_id else {
            return RemoteCallbackOutcome::NotFound;
        };
        let session = sessions.get(&session_id).expect("刚查到的会话必然存在");
        if Utc::now() >= session.expires_at {
            return RemoteCallbackOutcome::Expired;
        }
        let tx_slot = match session.remote_callback_tx.as_ref() {
            Some(slot) => slot,
            None => return RemoteCallbackOutcome::NotFound, // 本地模式会话：不应由公网路由投递
        };
        // 释放外层锁后再投递（send 不阻塞，但避免持锁发送）
        let tx = tx_slot.lock().take();
        drop(sessions);
        match tx {
            Some(tx) => {
                if tx.send(data).is_ok() {
                    RemoteCallbackOutcome::Delivered
                } else {
                    // 接收端已消失（会话被并发完成/移除）→ 视为已处理
                    RemoteCallbackOutcome::AlreadyCompleted
                }
            }
            None => RemoteCallbackOutcome::AlreadyCompleted,
        }
    }

    // ── IdC 设备授权登录 ──────────────────────────────────────────────────────

    /// 发起 IdC 设备授权，返回验证码和 URL
    pub async fn start_idc_login(
        &self,
        req: StartIdcLoginRequest,
    ) -> Result<StartIdcLoginResponse, AdminServiceError> {
        let config = self.token_manager.config();
        let global_proxy = self.token_manager.proxy();

        // 代理：优先用请求级，否则回退全局
        let proxy = req
            .proxy_url
            .as_deref()
            .map(ProxyConfig::new)
            .or(global_proxy);

        let start_url = req.start_url.as_deref().unwrap_or(BUILDER_ID_START_URL);

        // 1. 注册 OIDC 客户端
        let reg = idc::register_client(&req.region, start_url, config, proxy.as_ref())
            .await
            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

        // 2. 发起设备授权
        let device = idc::start_device_authorization(
            &req.region,
            start_url,
            &reg.client_id,
            &reg.client_secret,
            config,
            proxy.as_ref(),
        )
        .await
        .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

        let expires_at = Utc::now() + Duration::seconds(device.expires_in);
        let session_id = Uuid::new_v4().to_string();

        // 身份提供商：默认 Start URL 为 AWS Builder ID，自定义 Start URL 为企业 IAM Identity Center
        let provider = if start_url == BUILDER_ID_START_URL {
            "BuilderId"
        } else {
            "Enterprise"
        };

        // 构建登录成功后写入的凭据模板
        let cred_template = KiroCredentials {
            auth_method: Some("idc".to_string()),
            provider: Some(provider.to_string()),
            client_id: Some(reg.client_id.clone()),
            client_secret: Some(reg.client_secret.clone()),
            start_url: Some(start_url.to_string()),
            region: Some(req.region.clone()),
            priority: req.priority,
            email: req.email,
            proxy_url: req.proxy_url,
            ..Default::default()
        };

        let session = IdcAuthSession {
            region: req.region,
            client_id: reg.client_id,
            client_secret: reg.client_secret,
            device_code: device.device_code,
            expires_at,
            poll_interval: device.interval.max(5),
            cred_template,
            proxy,
            relogin_target_id: None,
        };

        let poll_interval = session.poll_interval;
        self.idc_sessions.lock().insert(session_id.clone(), session);

        Ok(StartIdcLoginResponse {
            session_id,
            user_code: device.user_code,
            verification_uri: device.verification_uri,
            verification_uri_complete: device.verification_uri_complete,
            expires_at: expires_at.to_rfc3339(),
            poll_interval,
        })
    }

    /// 轮询一次 IdC 登录状态
    pub async fn poll_idc_login(
        &self,
        session_id: &str,
    ) -> Result<PollIdcLoginResponse, AdminServiceError> {
        let (
            region,
            client_id,
            client_secret,
            device_code,
            _expires_at,
            proxy,
            cred_template,
            relogin_target_id,
        ) = {
            let sessions = self.idc_sessions.lock();
            let s = sessions
                .get(session_id)
                .ok_or_else(|| AdminServiceError::NotFound { id: 0 })?;

            if Utc::now() >= s.expires_at {
                return Ok(PollIdcLoginResponse::Expired);
            }

            (
                s.region.clone(),
                s.client_id.clone(),
                s.client_secret.clone(),
                s.device_code.clone(),
                s.expires_at,
                s.proxy.clone(),
                s.cred_template.clone(),
                s.relogin_target_id,
            )
        };

        let config = self.token_manager.config();

        match idc::poll_token(
            &region,
            &client_id,
            &client_secret,
            &device_code,
            config,
            proxy.as_ref(),
        )
        .await
        {
            idc::PollResult::Pending => Ok(PollIdcLoginResponse::Pending),
            idc::PollResult::Expired => {
                self.idc_sessions.lock().remove(session_id);
                Ok(PollIdcLoginResponse::Expired)
            }
            idc::PollResult::Error(e) => Err(AdminServiceError::InternalError(e.to_string())),
            idc::PollResult::Success(token) => {
                self.idc_sessions.lock().remove(session_id);

                // 重新登录模式：更新已有凭据而非创建新凭据
                if let Some(target_id) = relogin_target_id {
                    if let Some(refresh_token) = token.refresh_token {
                        self.do_relogin_update(target_id, refresh_token)
                            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;
                    }
                    tracing::info!("IdC 重新登录成功，凭据 #{} Token 已更新", target_id);
                    return Ok(PollIdcLoginResponse::Success {
                        credential_id: target_id,
                    });
                }

                // 写入凭据
                let mut new_cred = cred_template;
                new_cred.access_token = Some(token.access_token);
                new_cred.refresh_token = token.refresh_token;
                if let Some(secs) = token.expires_in {
                    new_cred.expires_at = Some((Utc::now() + Duration::seconds(secs)).to_rfc3339());
                }

                let credential_id = self
                    .token_manager
                    .add_credential(new_cred)
                    .await
                    .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

                // 主动刷新余额（含订阅等级 / 邮箱）并写入缓存，登录后立即可见
                if let Err(e) = self.get_balance(credential_id).await {
                    tracing::warn!("IdC 登录后刷新余额失败（不影响登录）: {}", e);
                }

                tracing::info!("IdC 设备授权登录成功，已添加凭据 #{}", credential_id);
                Ok(PollIdcLoginResponse::Success { credential_id })
            }
        }
    }

    /// 内部：重新登录完成后更新已有凭据的 Token（禁用→更新→重置→启用）
    fn do_relogin_update(&self, target_id: u64, refresh_token: String) -> anyhow::Result<()> {
        // 先禁用（update_refresh_token 要求凭据处于禁用状态）
        self.token_manager.set_disabled(target_id, true)?;
        // 更新 refreshToken（同时清空 accessToken 和 expiresAt，系统会在下次使用时自动刷新）
        self.token_manager
            .update_refresh_token(target_id, refresh_token, None, None)?;
        // 重置失败计数并重新启用
        self.token_manager.reset_and_enable(target_id)?;
        Ok(())
    }

    /// 发起 Social 重新登录（更新已有凭据的 Token 而非创建新凭据）
    pub async fn start_social_relogin(
        &self,
        target_id: u64,
        req: StartSocialLoginRequest,
    ) -> Result<StartSocialLoginResponse, AdminServiceError> {
        // 验证目标凭据存在
        {
            let snapshot = self.token_manager.snapshot();
            if !snapshot.entries.iter().any(|e| e.id == target_id) {
                return Err(AdminServiceError::NotFound { id: target_id });
            }
        }

        let global_proxy = self.token_manager.proxy();
        let proxy = req
            .proxy_url
            .as_deref()
            .map(ProxyConfig::new)
            .or(global_proxy);

        let auth_endpoint = req
            .auth_endpoint
            .unwrap_or_else(|| social::KIRO_AUTH_ENDPOINT.to_string());

        let (code_verifier, code_challenge) = social::generate_pkce();
        let state = uuid::Uuid::new_v4().to_string();

        // 回调模式同 start_social_login：远程模式走公网回调路由，本地模式走临时端口
        let remote_base = self.resolve_callback_base(req.callback_base_url.as_deref());
        let (redirect_uri, server_handle, remote_callback_tx, rx) = match remote_base.clone() {
            Some(base) => {
                let (tx, rx) = tokio::sync::oneshot::channel::<social::OAuthCallbackData>();
                (base, None, Some(Mutex::new(Some(tx))), rx)
            }
            None => {
                let (tx, rx) = tokio::sync::oneshot::channel::<social::OAuthCallbackData>();
                let (port, server_handle) = social::start_callback_server(tx)
                    .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;
                (
                    format!("http://127.0.0.1:{}", port),
                    Some(server_handle),
                    None,
                    rx,
                )
            }
        };
        let portal_url = social::build_portal_url(&state, &code_challenge, &redirect_uri);

        let expires_at = Utc::now() + Duration::minutes(10);
        let session_id = uuid::Uuid::new_v4().to_string();

        let session = SocialAuthSession {
            auth_endpoint,
            state,
            code_verifier,
            redirect_uri,
            expires_at,
            callback_rx: tokio::sync::Mutex::new(rx),
            cred_template: KiroCredentials::default(),
            proxy,
            _server_handle: server_handle,
            remote_callback_tx,
            relogin_target_id: Some(target_id),
        };

        self.social_sessions
            .lock()
            .insert(session_id.clone(), session);

        Ok(StartSocialLoginResponse {
            session_id,
            portal_url,
            expires_at: expires_at.to_rfc3339(),
            remote: remote_base.is_some(),
        })
    }

    /// 发起 IdC 重新登录（更新已有凭据的 Token 而非创建新凭据）
    pub async fn start_idc_relogin(
        &self,
        target_id: u64,
        req: StartIdcLoginRequest,
    ) -> Result<StartIdcLoginResponse, AdminServiceError> {
        // 验证目标凭据存在
        {
            let snapshot = self.token_manager.snapshot();
            if !snapshot.entries.iter().any(|e| e.id == target_id) {
                return Err(AdminServiceError::NotFound { id: target_id });
            }
        }

        let config = self.token_manager.config();
        let global_proxy = self.token_manager.proxy();

        let proxy = req
            .proxy_url
            .as_deref()
            .map(ProxyConfig::new)
            .or(global_proxy);

        let start_url = req.start_url.as_deref().unwrap_or(BUILDER_ID_START_URL);

        let reg = idc::register_client(&req.region, start_url, config, proxy.as_ref())
            .await
            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

        let device = idc::start_device_authorization(
            &req.region,
            start_url,
            &reg.client_id,
            &reg.client_secret,
            config,
            proxy.as_ref(),
        )
        .await
        .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

        let expires_at = Utc::now() + Duration::seconds(device.expires_in);
        let session_id = Uuid::new_v4().to_string();

        let session = IdcAuthSession {
            region: req.region,
            client_id: reg.client_id,
            client_secret: reg.client_secret,
            device_code: device.device_code,
            expires_at,
            poll_interval: device.interval.max(5),
            cred_template: KiroCredentials::default(),
            proxy,
            relogin_target_id: Some(target_id),
        };

        let poll_interval = session.poll_interval;
        self.idc_sessions.lock().insert(session_id.clone(), session);

        Ok(StartIdcLoginResponse {
            session_id,
            user_code: device.user_code,
            verification_uri: device.verification_uri,
            verification_uri_complete: device.verification_uri_complete,
            expires_at: expires_at.to_rfc3339(),
            poll_interval,
        })
    }
}
