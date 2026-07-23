//! i7relay 补货审计:每次补货/清理落一条 JSONL + 内存近况环(供 status 端点只读)。
//! **脱敏**:只记 ksk_ 前缀(前 12 字符),绝不记全 key / account / password。

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::Write;
use std::path::PathBuf;

/// 单条补货记录(下发到 status 端点 + 落盘)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestockRecord {
    /// RFC3339 时间。
    pub at: String,
    /// 触发来源:"webhook:new_keys_available" / "webhook:all_keys_dead" / "poll" / "manual"。
    pub trigger: String,
    /// 本次 purchase 请求数量。
    pub requested: u32,
    /// 成功导入数。
    pub imported: usize,
    /// 重复(已在池)数。
    pub duplicate: usize,
    /// 失败数。
    pub failed: usize,
    /// 因 all_keys_dead 禁用的凭据数(非补货时为 0)。
    pub disabled: usize,
    /// i7relay 剩余配额(purchase 后;未知为 -1)。
    pub remaining_quota: i64,
    /// ksk_ 前缀样本(脱敏,最多前几条)。
    pub key_prefixes: Vec<String>,
    /// 错误信息(如有)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// 单个 key 的提取记录(比 RestockRecord 更细:一 key 一条)。**脱敏**:仅 ksk_ 前缀。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyExtractRecord {
    /// RFC3339 提取时间。
    pub at: String,
    /// ksk_ 前缀(前 12 字符,脱敏)。
    pub key_prefix: String,
    /// 触发来源(同 RestockRecord.trigger)。
    pub trigger: String,
    /// 导入结果:"imported" | "duplicate" | "failed"。
    pub import_status: String,
    /// 是否验活有效(None=未验活/未知)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valid: Option<bool>,
    /// 导入后的凭据 id(如有)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<i64>,
}

const RECENT_CAP: usize = 200;
const EXTRACT_CAP: usize = 500;

/// 审计器:线程安全,持有落盘路径 + 近况环(批级 + key 级两个)。
pub struct I7relayAudit {
    path: PathBuf,
    extract_path: PathBuf,
    recent: Mutex<VecDeque<RestockRecord>>,
    extracts: Mutex<VecDeque<KeyExtractRecord>>,
}

impl I7relayAudit {
    /// `data_dir` 为凭据/配置所在目录(容器内 /app/config)。
    pub fn new(data_dir: &std::path::Path) -> Self {
        Self {
            path: data_dir.join("i7relay_audit.jsonl"),
            extract_path: data_dir.join("i7relay_extracts.jsonl"),
            recent: Mutex::new(VecDeque::with_capacity(RECENT_CAP)),
            extracts: Mutex::new(VecDeque::with_capacity(EXTRACT_CAP)),
        }
    }

    /// 记录一条:追加落盘(失败仅告警,不阻断) + 入内存环。
    pub fn record(&self, rec: RestockRecord) {
        if let Ok(line) = serde_json::to_string(&rec) {
            match std::fs::OpenOptions::new().create(true).append(true).open(&self.path) {
                Ok(mut f) => {
                    let _ = writeln!(f, "{line}");
                }
                Err(e) => tracing::warn!("i7relay 审计落盘失败: {e}"),
            }
        }
        let mut q = self.recent.lock();
        if q.len() >= RECENT_CAP {
            q.pop_front();
        }
        q.push_back(rec);
    }

    /// 最近 `n` 条(新→旧)。
    pub fn recent(&self, n: usize) -> Vec<RestockRecord> {
        let q = self.recent.lock();
        q.iter().rev().take(n).cloned().collect()
    }

    /// 记录一条 key 提取:追加落盘 + 入内存环。
    pub fn record_extract(&self, rec: KeyExtractRecord) {
        if let Ok(line) = serde_json::to_string(&rec) {
            match std::fs::OpenOptions::new().create(true).append(true).open(&self.extract_path) {
                Ok(mut f) => {
                    let _ = writeln!(f, "{line}");
                }
                Err(e) => tracing::warn!("i7relay 提取记录落盘失败: {e}"),
            }
        }
        let mut q = self.extracts.lock();
        if q.len() >= EXTRACT_CAP {
            q.pop_front();
        }
        q.push_back(rec);
    }

    /// 最近 `n` 条提取记录(新→旧)。
    pub fn recent_extracts(&self, n: usize) -> Vec<KeyExtractRecord> {
        let q = self.extracts.lock();
        q.iter().rev().take(n).cloned().collect()
    }
}
