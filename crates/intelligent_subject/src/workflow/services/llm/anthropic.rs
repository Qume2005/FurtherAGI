//! # Anthropic LLM 服务
//!
//! 通过 Anthropic Messages API 调用大模型。

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::{ChatMessage, LlmError, LlmRequest, LlmResponse, LlmService, ToolDefinition, Usage};

/// Anthropic API 服务。
pub struct AnthropicService {
    api_key: String,
    base_url: String,
    default_model: String,
    client: Client,
}

impl AnthropicService {
    pub fn new(api_key: impl Into<String>, default_model: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: "https://api.anthropic.com/v1".to_string(),
            default_model: default_model.into(),
            client: Client::new(),
        }
    }

    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }
}

// -- Anthropic request/response types --

#[derive(Serialize)]
struct AnthropicRequest {
    model: String,
    messages: Vec<AnthropicMessage>,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<AnthropicTool>>,
}

#[derive(Serialize, Deserialize)]
struct AnthropicMessage {
    role: String,
    content: String,
}

#[derive(Serialize)]
struct AnthropicTool {
    name: String,
    description: String,
    input_schema: serde_json::Value,
}

#[derive(Deserialize)]
struct AnthropicResponse {
    content: Vec<AnthropicContentBlock>,
    usage: AnthropicUsage,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum AnthropicContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
}

#[derive(Deserialize)]
struct AnthropicUsage {
    input_tokens: u32,
    output_tokens: u32,
}

#[derive(Deserialize)]
struct AnthropicError {
    error: AnthropicErrorDetail,
}

#[derive(Deserialize)]
struct AnthropicErrorDetail {
    message: String,
}

fn split_system_message(messages: &[ChatMessage]) -> (Option<String>, Vec<AnthropicMessage>) {
    let mut system = None;
    let anthropic_msgs: Vec<AnthropicMessage> = messages
        .iter()
        .filter_map(|m| {
            if matches!(m.role, super::ChatRole::System) {
                system = Some(m.content.clone());
                None
            } else {
                Some(AnthropicMessage {
                    role: match m.role {
                        super::ChatRole::User => "user".to_string(),
                        super::ChatRole::Assistant => "assistant".to_string(),
                        super::ChatRole::Tool => "user".to_string(),
                        super::ChatRole::System => unreachable!(),
                    },
                    content: m.content.clone(),
                })
            }
        })
        .collect();
    (system, anthropic_msgs)
}

#[async_trait]
impl LlmService for AnthropicService {
    async fn complete(&self, request: LlmRequest) -> Result<LlmResponse, LlmError> {
        self.complete_with_tools(request, vec![]).await
    }

    async fn complete_with_tools(
        &self,
        request: LlmRequest,
        tools: Vec<ToolDefinition>,
    ) -> Result<LlmResponse, LlmError> {
        let (system, messages) = split_system_message(&request.messages);

        let anthropic_tools = if tools.is_empty() {
            None
        } else {
            Some(
                tools
                    .into_iter()
                    .map(|t| AnthropicTool {
                        name: t.name,
                        description: t.description,
                        input_schema: t.parameters,
                    })
                    .collect(),
            )
        };

        let body = AnthropicRequest {
            model: request.model.unwrap_or_else(|| self.default_model.clone()),
            messages,
            max_tokens: request.max_tokens.unwrap_or(4096),
            system,
            temperature: request.temperature,
            tools: anthropic_tools,
        };

        let resp = self
            .client
            .post(format!("{}/messages", self.base_url))
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| LlmError::Http(e.to_string()))?;

        let status = resp.status().as_u16();
        let text = resp
            .text()
            .await
            .map_err(|e| LlmError::Http(e.to_string()))?;

        if status != 200 {
            let msg = serde_json::from_str::<AnthropicError>(&text)
                .map(|e| e.error.message)
                .unwrap_or_else(|_| text.clone());
            return Err(LlmError::Api { status, message: msg });
        }

        let parsed: AnthropicResponse = serde_json::from_str(&text)
            .map_err(|e| LlmError::Parse(format!("{e}: {}", &text[..text.len().min(200)])))?;

        let mut content = String::new();
        let mut tool_calls = Vec::new();

        for block in parsed.content {
            match block {
                AnthropicContentBlock::Text { text } => {
                    if !content.is_empty() {
                        content.push('\n');
                    }
                    content.push_str(&text);
                }
                AnthropicContentBlock::ToolUse { id, name, input } => {
                    tool_calls.push(super::ToolCall {
                        id,
                        name,
                        arguments: input.to_string(),
                    });
                }
            }
        }

        Ok(LlmResponse {
            content,
            tool_calls: if tool_calls.is_empty() {
                None
            } else {
                Some(tool_calls)
            },
            usage: Some(Usage {
                prompt_tokens: parsed.usage.input_tokens,
                completion_tokens: parsed.usage.output_tokens,
                total_tokens: parsed.usage.input_tokens + parsed.usage.output_tokens,
            }),
        })
    }
}
