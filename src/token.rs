//! Token 计算模块
//!
//! 提供文本 token 数量计算功能。
//!
//! # 计算规则
//! - 非西文字符：每个计 4.5 个字符单位
//! - 西文字符：每个计 1 个字符单位
//! - 4 个字符单位 = 1 token（四舍五入）

use crate::anthropic::types::{
    CountTokensRequest, CountTokensResponse, Message, SystemMessage, Tool,
};
use crate::http_client::{ProxyConfig, build_client};
use crate::model::config::TlsBackend;
use std::sync::OnceLock;

/// Count Tokens API 配置
#[derive(Clone, Default)]
pub struct CountTokensConfig {
    /// 外部 count_tokens API 地址
    pub api_url: Option<String>,
    /// count_tokens API 密钥
    pub api_key: Option<String>,
    /// count_tokens API 认证类型（"x-api-key" 或 "bearer"）
    pub auth_type: String,
    /// 代理配置
    pub proxy: Option<ProxyConfig>,

    pub tls_backend: TlsBackend,
}

/// 全局配置存储
static COUNT_TOKENS_CONFIG: OnceLock<CountTokensConfig> = OnceLock::new();

/// 初始化 count_tokens 配置
///
/// 应在应用启动时调用一次
pub fn init_config(config: CountTokensConfig) {
    let _ = COUNT_TOKENS_CONFIG.set(config);
}

/// 获取配置
fn get_config() -> Option<&'static CountTokensConfig> {
    COUNT_TOKENS_CONFIG.get()
}

/// 判断字符是否为非西文字符
///
/// 西文字符包括：
/// - ASCII 字符 (U+0000..U+007F)
/// - 拉丁字母扩展 (U+0080..U+024F)
/// - 拉丁字母扩展附加 (U+1E00..U+1EFF)
///
/// 返回 true 表示该字符是非西文字符（如中文、日文、韩文、阿拉伯文等）
fn is_non_western_char(c: char) -> bool {
    !matches!(c,
        // 基本 ASCII
        '\u{0000}'..='\u{007F}' |
        // 拉丁字母扩展-A (Latin Extended-A)
        '\u{0080}'..='\u{00FF}' |
        // 拉丁字母扩展-B (Latin Extended-B)
        '\u{0100}'..='\u{024F}' |
        // 拉丁字母扩展附加 (Latin Extended Additional)
        '\u{1E00}'..='\u{1EFF}' |
        // 拉丁字母扩展-C/D/E
        '\u{2C60}'..='\u{2C7F}' |
        '\u{A720}'..='\u{A7FF}' |
        '\u{AB30}'..='\u{AB6F}'
    )
}

/// 计算文本的 token 数量
///
/// # 计算规则
/// - 非西文字符：每个计 4.5 个字符单位
/// - 西文字符：每个计 1 个字符单位
/// - 4 个字符单位 = 1 token（四舍五入）
/// ```
pub fn count_tokens(text: &str) -> u64 {
    // println!("text: {}", text);

    let char_units: f64 = text
        .chars()
        .map(|c| if is_non_western_char(c) { 4.0 } else { 1.0 })
        .sum();

    let tokens = char_units / 4.0;

    let acc_token = if tokens < 100.0 {
        tokens * 1.5
    } else if tokens < 200.0 {
        tokens * 1.3
    } else if tokens < 300.0 {
        tokens * 1.25
    } else if tokens < 800.0 {
        tokens * 1.2
    } else {
        tokens * 1.0
    } as u64;

    // println!("tokens: {}, acc_tokens: {}", tokens, acc_token);
    acc_token
}

/// 估算请求的输入 tokens
///
/// 优先调用远程 API，失败时回退到本地计算
pub(crate) fn count_all_tokens(
    model: &str,
    system: &Option<Vec<SystemMessage>>,
    messages: &[Message],
    tools: &Option<Vec<Tool>>,
) -> u64 {
    // 检查是否配置了远程 API
    if let Some(config) = get_config() {
        if let Some(api_url) = &config.api_url {
            // 尝试调用远程 API
            let result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(call_remote_count_tokens(
                    api_url, config, model, system, messages, tools,
                ))
            });

            match result {
                Ok(tokens) => {
                    tracing::debug!("远程 count_tokens API 返回: {}", tokens);
                    return tokens;
                }
                Err(e) => {
                    tracing::warn!("远程 count_tokens API 调用失败，回退到本地计算: {}", e);
                }
            }
        }
    }

    // 本地计算
    count_all_tokens_local(system, messages, tools)
}

/// 调用远程 count_tokens API
async fn call_remote_count_tokens(
    api_url: &str,
    config: &CountTokensConfig,
    model: &str,
    system: &Option<Vec<SystemMessage>>,
    messages: &[Message],
    tools: &Option<Vec<Tool>>,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let client = build_client(config.proxy.as_ref(), 300, config.tls_backend)?;

    // 构建请求体
    let request = CountTokensRequest {
        model: model.to_string(), // 模型名称用于 token 计算
        messages: messages.to_vec(),
        system: system.clone(),
        tools: tools.clone(),
    };

    // 构建请求
    let mut req_builder = client.post(api_url);

    // 设置认证头
    if let Some(api_key) = &config.api_key {
        if config.auth_type == "bearer" {
            req_builder = req_builder.header("Authorization", format!("Bearer {}", api_key));
        } else {
            req_builder = req_builder.header("x-api-key", api_key);
        }
    }

    // 发送请求
    let response = req_builder
        .header("Content-Type", "application/json")
        .json(&request)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(format!("API 返回错误状态: {}", response.status()).into());
    }

    let result: CountTokensResponse = response.json().await?;
    Ok(result.input_tokens as u64)
}

/// 单张内联 base64 图片的保底 token 数。
///
/// 不在此处解码图片取真实尺寸：`count_all_tokens` 处于请求热路径、且在转换器
/// 之前运行，为"上报下限"而解码数 MB base64 不划算。用 Anthropic 单图上限附近
/// 的固定值，确保图片始终贡献非零 token，把本地估算抬到接近真实即可。
const IMAGE_TOKEN_ESTIMATE: u64 = 1_600;

/// 统计单个 ContentBlock(裸 JSON)的 token。
///
/// 按块 `type` 完整分派，覆盖 agent 负载里的全部块类型：
/// - `text` / `thinking`：直接计文本
/// - `tool_use`：name + 序列化后的 input(工具调用参数)
/// - `tool_result`：递归统计 content(string 或 [{text}|{image}] 数组)
/// - `image`：固定保底值
///
/// 用 `.get()` 宽松取值而非严格反序列化：单个字段缺失/异形时只少计该块，
/// 不会整块丢弃，保证下限估算的鲁棒性。
fn count_block_tokens(item: &serde_json::Value) -> u64 {
    let mut sum = 0u64;

    if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
        sum += count_tokens(text);
    }
    if let Some(thinking) = item.get("thinking").and_then(|v| v.as_str()) {
        sum += count_tokens(thinking);
    }

    match item.get("type").and_then(|v| v.as_str()) {
        Some("tool_use") => {
            if let Some(name) = item.get("name").and_then(|v| v.as_str()) {
                sum += count_tokens(name);
            }
            if let Some(input) = item.get("input") {
                let s = serde_json::to_string(input).unwrap_or_default();
                sum += count_tokens(&s);
            }
        }
        Some("tool_result") => {
            sum += count_tool_result_content_tokens(item.get("content"));
        }
        Some("image") => {
            // 仅内联 base64 计入；url 模式图片不直接进消息体，跳过。
            if item
                .get("source")
                .and_then(|s| s.get("type"))
                .and_then(|v| v.as_str())
                == Some("base64")
            {
                sum += IMAGE_TOKEN_ESTIMATE;
            }
        }
        _ => {}
    }

    sum
}

/// 统计 tool_result.content 的 token。content 可能是 string，或
/// `[{type:text,text}|{type:image,source}]` 数组——与转换器
/// `extract_tool_result_content` 的解析形态一致。
fn count_tool_result_content_tokens(content: Option<&serde_json::Value>) -> u64 {
    match content {
        Some(serde_json::Value::String(s)) => count_tokens(s),
        Some(serde_json::Value::Array(arr)) => {
            let mut sum = 0u64;
            for item in arr {
                if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
                    sum += count_tokens(text);
                } else if item.get("type").and_then(|v| v.as_str()) == Some("image") {
                    sum += IMAGE_TOKEN_ESTIMATE;
                }
            }
            sum
        }
        // 其它异形(如对象):序列化兜底，宁可略多计也不漏。
        Some(v) => count_tokens(&v.to_string()),
        None => 0,
    }
}

/// 本地计算请求的输入 tokens
fn count_all_tokens_local(
    system: &Option<Vec<SystemMessage>>,
    messages: &[Message],
    tools: &Option<Vec<Tool>>,
) -> u64 {
    let mut total = 0;

    // 系统消息
    if let Some(system) = system {
        for msg in system {
            total += count_tokens(&msg.text);
        }
    }

    // 用户/助手消息
    //
    // content 可能是裸 string，或 ContentBlock 数组。数组里除 `text` 块外，
    // 还有 `tool_result`(text 嵌在 content[] 里，顶层无 text)、`tool_use`
    // (参数在 input)、`image`、`thinking`。对 agent 负载，tool_result 往往是
    // 历史里体量最大的部分——这些块都必须计入，否则本地估算会系统性低估，
    // 导致客户端永不触发 auto-compact 而撞上游上下文上限。
    for msg in messages {
        match &msg.content {
            serde_json::Value::String(s) => total += count_tokens(s),
            serde_json::Value::Array(arr) => {
                for item in arr {
                    total += count_block_tokens(item);
                }
            }
            _ => {}
        }
    }

    // 工具定义
    if let Some(tools) = tools {
        for tool in tools {
            total += count_tokens(&tool.name);
            total += count_tokens(&tool.description);
            let input_schema_json = serde_json::to_string(&tool.input_schema).unwrap_or_default();
            total += count_tokens(&input_schema_json);
        }
    }

    total.max(1)
}

/// 估算输出 tokens
pub(crate) fn estimate_output_tokens(content: &[serde_json::Value]) -> i32 {
    let mut total = 0;

    for block in content {
        if let Some(text) = block.get("text").and_then(|v| v.as_str()) {
            total += count_tokens(text) as i32;
        }
        if let Some(thinking) = block.get("thinking").and_then(|v| v.as_str()) {
            total += count_tokens(thinking) as i32;
        }
        if block.get("type").and_then(|v| v.as_str()) == Some("redacted_thinking") {
            total += 8;
        }
        if block.get("type").and_then(|v| v.as_str()) == Some("tool_use") {
            // 工具调用开销
            if let Some(input) = block.get("input") {
                let input_str = serde_json::to_string(input).unwrap_or_default();
                total += count_tokens(&input_str) as i32;
            }
        }
    }

    total.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn estimate_output_tokens_counts_thinking_blocks() {
        let with_thinking = estimate_output_tokens(&[json!({
            "type": "thinking",
            "thinking": "需要计入输出 token"
        })]);
        let text_only = estimate_output_tokens(&[json!({
            "type": "text",
            "text": ""
        })]);

        assert!(with_thinking > text_only);
    }

    #[test]
    fn estimate_output_tokens_counts_redacted_thinking() {
        let tokens = estimate_output_tokens(&[json!({
            "type": "redacted_thinking",
            "data": "encrypted"
        })]);

        assert!(tokens >= 8);
    }

    fn msg(content: serde_json::Value) -> Message {
        Message {
            role: "user".to_string(),
            content,
        }
    }

    // 回归核心：历史里最大头的 tool_result 文本必须被计入。
    // 修复前只数顶层 `text` 块，tool_result 的 text 嵌在 content[] 里，整段被漏掉。
    #[test]
    fn tool_result_string_content_is_counted() {
        let big = "x".repeat(4000); // ~1000+ tokens
        let messages = vec![msg(json!([{
            "type": "tool_result",
            "tool_use_id": "toolu_1",
            "content": big,
        }]))];
        let total = count_all_tokens_local(&None, &messages, &None);
        assert!(
            total > 500,
            "tool_result string content 必须计入，实得 {total}"
        );
    }

    #[test]
    fn tool_result_array_text_blocks_are_counted() {
        let big = "y".repeat(4000);
        let messages = vec![msg(json!([{
            "type": "tool_result",
            "tool_use_id": "toolu_1",
            "content": [{"type": "text", "text": big}],
        }]))];
        let total = count_all_tokens_local(&None, &messages, &None);
        assert!(
            total > 500,
            "tool_result 数组内的 text 块必须计入，实得 {total}"
        );
    }

    #[test]
    fn tool_use_input_is_counted() {
        let big = "z".repeat(4000);
        let messages = vec![msg(json!([{
            "type": "tool_use",
            "id": "toolu_1",
            "name": "write_file",
            "input": {"path": "a.rs", "content": big},
        }]))];
        let total = count_all_tokens_local(&None, &messages, &None);
        assert!(total > 500, "tool_use.input 必须计入，实得 {total}");
    }

    #[test]
    fn inline_base64_image_contributes_nonzero() {
        let messages = vec![msg(json!([{
            "type": "image",
            "source": {"type": "base64", "media_type": "image/png", "data": "iVBORw0KGgo="},
        }]))];
        let total = count_all_tokens_local(&None, &messages, &None);
        assert!(
            total >= IMAGE_TOKEN_ESTIMATE,
            "内联 base64 图片必须贡献非零 token，实得 {total}"
        );
    }

    #[test]
    fn url_image_is_skipped() {
        // url 模式图片不直接进消息体，不应贡献图片 token。
        let messages = vec![msg(json!([{
            "type": "image",
            "source": {"type": "url", "url": "https://example.com/a.png"},
        }]))];
        let total = count_all_tokens_local(&None, &messages, &None);
        assert!(total < IMAGE_TOKEN_ESTIMATE, "url 图片应跳过，实得 {total}");
    }

    #[test]
    fn plain_text_block_and_string_still_work() {
        // 回归：原有的顶层 text 块与裸 string 仍正常计数。
        let from_block = count_all_tokens_local(
            &None,
            &[msg(json!([{"type": "text", "text": "hello world"}]))],
            &None,
        );
        let from_string =
            count_all_tokens_local(&None, &[msg(json!("hello world"))], &None);
        assert_eq!(from_block, from_string);
        assert!(from_block > 1);
    }

    #[test]
    fn mixed_blocks_sum_all_contributions() {
        let messages = vec![msg(json!([
            {"type": "text", "text": "前导说明"},
            {"type": "tool_use", "id": "t1", "name": "read", "input": {"p": "f.rs"}},
            {"type": "tool_result", "tool_use_id": "t1", "content": "file body here"},
        ]))];
        let total = count_all_tokens_local(&None, &messages, &None);
        // 应同时包含三块；明显大于任一单块。
        let only_text =
            count_all_tokens_local(&None, &[msg(json!([{"type": "text", "text": "前导说明"}]))], &None);
        assert!(total > only_text, "混合块应累加全部贡献");
    }
}
