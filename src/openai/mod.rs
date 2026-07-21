//! Fork-only OpenAI 辅助模块。
//!
//! 自研的 OpenAI 端点(chat/completions handler、convert、types、response)在合并
//! 上游 v0.7.1 时已删除,改用上游的 `crate::anthropic::openai` / `crate::anthropic::responses`。
//! 此处仅保留 fork 独有、上游未提供、且被 config/admin/middleware 依赖的
//! **模型名重映射**功能。
pub mod model_mapping;
