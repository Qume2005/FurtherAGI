//! # LLM 大模型调用服务
//!
//! 提供统一的 LLM 调用接口和 OpenAI、Anthropic 的具体实现。
//!
//! ## 可用服务
//!
//! | 服务 | 说明 |
//! |------|------|
//! | [`LlmService`] | 统一 trait，定义 `complete` 和 `complete_with_tools` |
//! | [`OpenAiService`] | OpenAI API 实现 |
//! | [`AnthropicService`] | Anthropic API 实现 |
//!
//! ## 有状态服务模式
//!
//! LLM 服务持有 API key、默认模型和 HTTP client，是有状态服务。
//! 使用时通过 `Arc<dyn LlmService>` 共享给 builtin workflow：
//!
//! ```rust,ignore
//! let service = Arc::new(OpenAiService::new("sk-...", "gpt-4o"));
//! registry.register("llm_chat", move || {
//!     into_erased(LlmComplete::new(service.clone(), None))
//! });
//! ```

mod anthropic;
mod openai;

pub use anthropic::AnthropicService;
pub use openai::OpenAiService;

use std::fmt;

use async_trait::async_trait;

/// LLM 错误类型。
#[derive(Debug)]
pub enum LlmError {
    /// HTTP 请求失败。
    Http(String),
    /// API 返回错误响应。
    Api { status: u16, message: String },
    /// 响应解析失败。
    Parse(String),
}

impl fmt::Display for LlmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LlmError::Http(msg) => write!(f, "LLM HTTP error: {msg}"),
            LlmError::Api { status, message } => write!(f, "LLM API error ({status}): {message}"),
            LlmError::Parse(msg) => write!(f, "LLM parse error: {msg}"),
        }
    }
}

impl std::error::Error for LlmError {}

/// 聊天消息角色。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChatRole {
    System,
    User,
    Assistant,
    Tool,
}

/// 聊天消息。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChatMessage {
    pub role: ChatRole,
    pub content: String,
}

/// LLM 请求。
#[derive(Debug, Clone)]
pub struct LlmRequest {
    pub messages: Vec<ChatMessage>,
    pub model: Option<String>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
}

/// 工具（函数）定义。
#[derive(Debug, Clone, serde::Serialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

/// 工具调用结果。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

/// Token 使用量。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Usage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

/// LLM 响应。
#[derive(Debug, Clone)]
pub struct LlmResponse {
    pub content: String,
    pub tool_calls: Option<Vec<ToolCall>>,
    pub usage: Option<Usage>,
}

/// LLM 服务统一 trait。
///
/// 有状态服务：实现持有 API key、默认模型和 HTTP client。
#[async_trait]
pub trait LlmService: Send + Sync {
    /// 发送聊天请求。
    async fn complete(&self, request: LlmRequest) -> Result<LlmResponse, LlmError>;

    /// 发送带工具定义的聊天请求。
    async fn complete_with_tools(
        &self,
        request: LlmRequest,
        tools: Vec<ToolDefinition>,
    ) -> Result<LlmResponse, LlmError>;
}
