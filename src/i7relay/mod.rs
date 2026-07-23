//! i7relay 自动补号对接(消费端)。
//!
//! 从上游账号供货商 i7relay 拉取 Kiro API Key(`ksk_`)凭据补进本地池:
//!   - [`I7relayClient`]：封装 profile/list_keys/purchase/set_webhook 四个接口。
//!   - [`restock`]：purchase → 组 `AddCredentialRequest` → 验活导入 → 审计。
//!   - webhook 端点 + 定时轮询兜底触发补货（见 admin handlers / main.rs）。
//!
//! 交付物 `ksk_` 是 kiro.rs 原生 API Key 凭据(`kiro_api_key`,当 Bearer Token 直接用)。
//! 安全:apiKey/webhookSecret 不打日志；交付物的 account/password 字段不落库不记录。

mod audit;
mod client;
mod restock;

pub mod handlers;
mod poll;

pub use audit::{I7relayAudit, KeyExtractRecord};
pub use client::I7relayClient;
pub use poll::spawn_poll_loop;
pub use restock::{restock, sync_dead_keys, RestockTrigger};

use crate::model::config::{I7relayConfig, TlsBackend};
use parking_lot::RwLock;
use std::sync::{Arc, OnceLock};

/// 运行时依赖:配置 + 审计器 + TLS 后端。**运行时可变**(前端保存即热生效)。
#[derive(Clone)]
pub struct Runtime {
    pub config: I7relayConfig,
    pub audit: Arc<I7relayAudit>,
    pub tls_backend: TlsBackend,
}

/// 全局运行时。始终存在(审计器/后端固定),`config` 可经 [`update_config`] 热替换。
/// None = 从未 init(进程未接通 i7relay)。
static RUNTIME: OnceLock<RwLock<Runtime>> = OnceLock::new();

/// 启动时初始化一次(建审计器 + 固定 TLS 后端)。重复调用只更新 config。
pub fn init_runtime(config: I7relayConfig, audit: Arc<I7relayAudit>, tls_backend: TlsBackend) {
    if let Some(rt) = RUNTIME.get() {
        rt.write().config = config;
    } else {
        let _ = RUNTIME.set(RwLock::new(Runtime { config, audit, tls_backend }));
    }
}

/// 热替换配置(前端保存配置时调用)。
pub fn update_config(config: I7relayConfig) {
    if let Some(rt) = RUNTIME.get() {
        rt.write().config = config;
    }
}

/// 取运行时快照(clone;None = 未 init)。
pub fn runtime() -> Option<Runtime> {
    RUNTIME.get().map(|rt| rt.read().clone())
}

/// 是否已 init(用于 main 判断是否需要 init)。
pub fn is_initialized() -> bool {
    RUNTIME.get().is_some()
}
