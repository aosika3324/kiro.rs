//! 定时轮询兜底:周期检查配额告警 + 池内可用 i7relay 凭据数,不足则补货;并对账死号。

use crate::admin::service::AdminService;
use std::sync::Arc;
use std::time::Duration;

/// 启动轮询任务(runtime init 后调用一次)。每 tick 重读运行时配置:
/// disabled 则空转,间隔按当前配置动态取。进程生命周期常驻(只起一次)。
pub fn spawn_poll_loop(service: Arc<AdminService>) {
    tokio::spawn(async move {
        // 启动后稍等,让凭据池先加载。
        tokio::time::sleep(Duration::from_secs(20)).await;
        loop {
            let rt = super::runtime();
            let interval = match &rt {
                Some(r) if r.config.enabled => {
                    if let Err(e) = tick(&service, r).await {
                        tracing::warn!("i7relay 轮询 tick 出错: {e}");
                    }
                    Duration::from_secs(r.config.poll_interval_secs.max(30))
                }
                // 未 init 或已禁用:低频空转,等待被启用。
                _ => Duration::from_secs(60),
            };
            tokio::time::sleep(interval).await;
        }
    });
}

async fn tick(service: &Arc<AdminService>, rt: &super::Runtime) -> anyhow::Result<()> {
    let client = super::I7relayClient::new(&rt.config, rt.tls_backend)?;

    // 配额告警。
    match client.remaining_quota().await {
        Ok(rem) if rem <= 0 => tracing::warn!("i7relay 配额已耗尽(remaining={rem})"),
        Ok(_) => {}
        Err(e) => tracing::warn!("i7relay profile 查询失败: {e}"),
    }

    // 死号对账。
    super::sync_dead_keys(&client, service, &rt.audit, super::RestockTrigger::Poll).await;

    // 池内可用 i7relay 凭据数。
    let active = service
        .get_all_credentials()
        .credentials
        .iter()
        .filter(|c| c.source_channel.as_deref() == Some("i7relay") && !c.disabled)
        .count() as u32;

    if active < rt.config.restock_threshold {
        tracing::info!(
            "i7relay 池内可用 {active} < 阈值 {} → 触发补货",
            rt.config.restock_threshold
        );
        super::restock(&client, &rt.config, service, &rt.audit, super::RestockTrigger::Poll).await;
    }
    Ok(())
}
